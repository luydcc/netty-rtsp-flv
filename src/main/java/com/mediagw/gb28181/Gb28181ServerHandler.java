package com.mediagw.gb28181;

import com.mediagw.config.GatewayConfig;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * GB28181 信令处理（UDP/TCP 共用）：
 * <ul>
 *   <li>REGISTER —— 标准 RFC 3261 Digest 挑战流程（401 -> 携带 Authorization 重注册 -> 200），
 *       注册 / 注销信息打印到控制台；</li>
 *   <li>MESSAGE —— 解析 MANSCDP XML，Keepalive 打印心跳，Catalog 响应/通知解析通道列表，
 *       其余命令类型也打印提示。</li>
 * </ul>
 * 配置了 {@link Gb28181Service} 时，注册/心跳/目录同步更新内存通道存储，
 * SIP 响应与设备 BYE 路由给点播信令层；否则仅打印日志。
 */
@ChannelHandler.Sharable
public final class Gb28181ServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(Gb28181ServerHandler.class);

    /** nonce 有效期（毫秒），超期作废。 */
    private static final long NONCE_TTL_MS = 5 * 60_000L;
    /** 匹配目录响应中的单个通道条目（Item）。 */
    private static final Pattern ITEM_PATTERN = Pattern.compile(
            "<(?:[\\w.-]+:)?Item>(.*?)</(?:[\\w.-]+:)?Item>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final String serverId;
    private final String realm;
    private final String password;
    /** 国标业务门面，可为 null（纯日志模式，测试用）。 */
    private final Gb28181Service service;
    /** 待校验的一次性 nonce -> 签发时间戳（毫秒）。 */
    private final ConcurrentHashMap<String, Long> nonces = new ConcurrentHashMap<>();
    /** 目录查询的 SN / CSeq 递增计数。 */
    private final AtomicInteger seqGen = new AtomicInteger();

    public Gb28181ServerHandler(GatewayConfig config) {
        this(config, null);
    }

    public Gb28181ServerHandler(GatewayConfig config, Gb28181Service service) {
        this.serverId = config.gb28181ServerId();
        String configured = config.gb28181Realm();
        // 鉴权域缺省取平台编码前 10 位（GB28181 惯例）
        this.realm = !configured.isEmpty() ? configured
                : serverId.length() >= 10 ? serverId.substring(0, 10) : serverId;
        this.password = config.gb28181Password();
        this.service = service;
    }

    /** 实际生效的鉴权域（供服务端启动日志展示）。 */
    public String realm() { return realm; }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof DatagramPacket packet) {
            SipMessage sip = SipMessage.parse(packet.content().toString(StandardCharsets.UTF_8));
            handleRequest(ctx, sip, packet.sender(), true);
        } else if (msg instanceof SipMessage sip) {
            handleRequest(ctx, sip, (InetSocketAddress) ctx.channel().remoteAddress(), false);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[GB28181] 信道异常: {}", cause.toString());
        // UDP 无连接不关闭；TCP 出现异常后断开，由设备重连
        if (!(ctx.channel() instanceof DatagramChannel)) {
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // TCP 长连接断开：对应设备离线，进行中的点播随之终止
        if (service != null && !(ctx.channel() instanceof DatagramChannel)) {
            service.store().offlineByChannel(ctx.channel());
            service.signaling().dropDialogsOn(ctx.channel());
        }
        super.channelInactive(ctx);
    }

    private void handleRequest(ChannelHandlerContext ctx, SipMessage sip, InetSocketAddress from, boolean udp) {
        if (!sip.isRequest()) {
            // 设备上行的 INVITE 响应（200/失败），路由给点播信令层
            if (service != null) {
                service.signaling().handleResponse(sip);
            }
            return;
        }
        switch (sip.method()) {
            case "REGISTER" -> handleRegister(ctx, sip, from, udp);
            case "MESSAGE" -> handleMessage(ctx, sip, from, udp);
            case "BYE" -> {
                if (service != null) {
                    service.signaling().handleBye(sip, ctx.channel(), from, udp);
                } else {
                    respond(ctx, sip, from, udp, 200, "OK", null);
                }
            }
            // 其余方法（INFO 等）简单确认，避免设备反复重发
            default -> respond(ctx, sip, from, udp, 200, "OK", null);
        }
    }

    private void handleRegister(ChannelHandlerContext ctx, SipMessage sip, InetSocketAddress from, boolean udp) {
        String device = sip.fromUser();
        int expires = sip.expires();
        String action = expires > 0 ? "注册" : "注销";
        String proto = udp ? "UDP" : "TCP";
        if (password.isEmpty()) {
            log.info("[GB28181] 设备{}: id={}, addr={}, expires={}s, transport={}",
                    action, device, from, expires, proto);
            registerOk(ctx, sip, device, expires, from, udp);
            return;
        }
        String authorization = sip.header("Authorization");
        if (authorization == null || authorization.isEmpty()) {
            // 首次注册：下发 401 挑战
            String nonce = randomToken();
            nonces.put(nonce, System.currentTimeMillis());
            String www = "WWW-Authenticate: Digest realm=\"" + realm
                    + "\", nonce=\"" + nonce + "\"\r\n";
            respond(ctx, sip, from, udp, 401, "Unauthorized", www);
            return;
        }
        if (verifyDigest(authorization, sip.method(), sip.uri(), device)) {
            log.info("[GB28181] 设备{}(鉴权通过): id={}, addr={}, expires={}s, transport={}",
                    action, device, from, expires, proto);
            registerOk(ctx, sip, device, expires, from, udp);
        } else {
            log.warn("[GB28181] 设备注册鉴权失败: id={}, addr={}, transport={}", device, from, proto);
            respond(ctx, sip, from, udp, 403, "Forbidden", null);
        }
    }

    private void handleMessage(ChannelHandlerContext ctx, SipMessage sip, InetSocketAddress from, boolean udp) {
        String body = sip.body();
        String cmd = xmlTag(body, "CmdType");
        String device = xmlTag(body, "DeviceID");
        if (device.isEmpty()) {
            device = sip.fromUser();
        }
        if ("Keepalive".equalsIgnoreCase(cmd)) {
            String status = xmlTag(body, "Status");
            String sn = xmlTag(body, "SN");
            log.info("[GB28181] 设备心跳: id={}, status={}, sn={}, addr={}, transport={}",
                    device, status.isEmpty() ? "-" : status, sn.isEmpty() ? "-" : sn, from,
                    udp ? "UDP" : "TCP");
            if (service != null) {
                service.store().touch(device, from, udp);
            }
        } else if ("Catalog".equalsIgnoreCase(cmd)) {
            // 目录查询响应/通道变更通知：逐条解析通道，打印并更新内存存储
            handleCatalog(device, body, from);
        } else {
            log.info("[GB28181] 收到设备消息: id={}, cmdType={}, addr={}, transport={}",
                    device, cmd.isEmpty() ? "-" : cmd, from, udp ? "UDP" : "TCP");
        }
        respond(ctx, sip, from, udp, 200, "OK", null);
    }

    /** 注册成功收尾：应答 200、更新内存存储，注册（非注销）成功后延迟下发目录（Catalog）查询。 */
    private void registerOk(ChannelHandlerContext ctx, SipMessage sip, String device, int expires,
                            InetSocketAddress from, boolean udp) {
        respond(ctx, sip, from, udp, 200, "OK", contactAndExpires(sip, expires));
        if (service != null) {
            if (expires > 0) {
                service.store().onRegister(device, from, udp, expires, ctx.channel());
            } else {
                service.store().onUnregister(device);
            }
        }
        if (expires > 0) {
            // 延迟 1s 发送目录查询，给设备留出注册后的就绪时间
            ctx.executor().schedule(() -> sendCatalogQuery(ctx, device, from, udp), 1, TimeUnit.SECONDS);
        }
    }

    /** 向设备发送目录（Catalog）查询请求（MANSCDP XML over MESSAGE）。 */
    private void sendCatalogQuery(ChannelHandlerContext ctx, String device, InetSocketAddress to, boolean udp) {
        if (!ctx.channel().isActive()) {
            return; // 通道已断开（TCP 断连或服务停止）
        }
        int seq = seqGen.incrementAndGet();
        String body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
                + "<Query>\r\n<CmdType>Catalog</CmdType>\r\n<SN>" + seq + "</SN>\r\n"
                + "<DeviceID>" + device + "</DeviceID>\r\n</Query>\r\n";
        StringBuilder sb = new StringBuilder(384);
        sb.append("MESSAGE sip:").append(device).append('@').append(realm).append(" SIP/2.0\r\n");
        sb.append("Via: ").append(viaHeader(ctx, udp)).append("\r\n");
        sb.append("From: <sip:").append(serverId).append('@').append(realm)
                .append(">;tag=").append(randomToken()).append("\r\n");
        sb.append("To: <sip:").append(device).append('@').append(realm).append(">\r\n");
        sb.append("Call-ID: catalog-").append(seq).append('-').append(randomToken()).append("\r\n");
        sb.append("CSeq: ").append(seq).append(" MESSAGE\r\n");
        sb.append("Content-Type: Application/MANSCDP+xml\r\n");
        sb.append("Max-Forwards: 70\r\n");
        sb.append("Content-Length: ").append(body.getBytes(StandardCharsets.UTF_8).length)
                .append("\r\n\r\n");
        sb.append(body);
        ByteBuf out = Unpooled.wrappedBuffer(sb.toString().getBytes(StandardCharsets.UTF_8));
        if (udp) {
            ctx.writeAndFlush(new DatagramPacket(out, to));
        } else {
            ctx.writeAndFlush(out);
        }
        log.info("[GB28181] 已发送目录查询: id={}, sn={}, addr={}", device, seq, to);
    }

    /** 解析并打印目录（Catalog）响应/通知中的通道列表，同步更新内存存储。 */
    private void handleCatalog(String device, String body, InetSocketAddress from) {
        String sn = xmlTag(body, "SN");
        String sum = xmlTag(body, "SumNum");
        List<GbChannelStore.ChannelInfo> channels = new ArrayList<>();
        Matcher items = ITEM_PATTERN.matcher(body);
        while (items.find()) {
            String item = items.group(1);
            channels.add(new GbChannelStore.ChannelInfo(
                    xmlTag(item, "DeviceID"), xmlTag(item, "Name"), xmlTag(item, "Manufacturer"),
                    xmlTag(item, "Model"), xmlTag(item, "Status"), xmlTag(item, "ParentID")));
        }
        int idx = 0;
        for (GbChannelStore.ChannelInfo c : channels) {
            log.info("[GB28181] 目录通道 #{}: id={}, name={}, manufacturer={}, model={}, status={}, parent={}, addr={}",
                    ++idx, c.channelId(), c.name(), c.manufacturer(), c.model(), c.status(), c.parentId(), from);
        }
        if (channels.isEmpty()) {
            log.info("[GB28181] 目录查询响应(无通道): id={}, sn={}, sum={}, addr={}", device, sn, sum, from);
            return;
        }
        log.info("[GB28181] 目录查询响应: id={}, sn={}, sum={}, channels={}", device, sn, sum, channels.size());
        if (service != null) {
            if (body.contains("<Notify") || body.contains("<notify")) {
                // 设备主动推送的通道变更：增量覆盖，不清空存量
                for (GbChannelStore.ChannelInfo c : channels) {
                    service.store().upsertChannel(device, c);
                }
            } else {
                service.store().replaceChannels(device, channels);
            }
        }
    }

    /** 平台侧 Via 头：取信令通道本地地址，branch 使用随机值。 */
    private static String viaHeader(ChannelHandlerContext ctx, boolean udp) {
        InetSocketAddress local = (InetSocketAddress) ctx.channel().localAddress();
        String host = local.getAddress() != null ? local.getAddress().getHostAddress() : local.getHostString();
        return "SIP/2.0/" + (udp ? "UDP" : "TCP") + " " + host + ":" + local.getPort()
                + ";rport;branch=z9hG4bK" + randomToken();
    }

    /** 应答请求：UDP 回源地址，TCP 直接写回连接。 */
    private void respond(ChannelHandlerContext ctx, SipMessage req, InetSocketAddress to, boolean udp,
                         int code, String reason, String extraHeader) {
        String text = req.toResponse(code, reason, extraHeader,
                to.getAddress().getHostAddress(), to.getPort());
        ByteBuf out = Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.UTF_8));
        if (udp) {
            ctx.writeAndFlush(new DatagramPacket(out, to));
        } else {
            ctx.writeAndFlush(out);
        }
    }

    /** REGISTER 200 OK 应答中的 Contact 与 Expires 头。 */
    private static String contactAndExpires(SipMessage sip, int expires) {
        String contact = sip.header("Contact");
        StringBuilder sb = new StringBuilder(96);
        if (contact != null && !contact.isEmpty()) {
            sb.append("Contact: ").append(contact).append("\r\n");
        }
        sb.append("Expires: ").append(Math.max(0, expires)).append("\r\n");
        return sb.toString();
    }

    /** 校验设备上行的 Digest 响应（MD5，兼容 qop=auth 与无 qop 两种形式）。 */
    private boolean verifyDigest(String authorization, String method, String uri, String device) {
        String nonce = authParam(authorization, "nonce");
        String response = authParam(authorization, "response");
        if (nonce == null || response == null) {
            return false;
        }
        Long issued = nonces.remove(nonce); // nonce 一次性使用
        if (issued == null || System.currentTimeMillis() - issued > NONCE_TTL_MS) {
            return false;
        }
        String username = authParam(authorization, "username");
        if (username == null || username.isEmpty()) {
            username = device;
        }
        String authRealm = authParam(authorization, "realm");
        String authUri = authParam(authorization, "uri");
        String qop = authParam(authorization, "qop");
        String ha1 = md5Hex(username + ":" + (authRealm != null ? authRealm : realm) + ":" + password);
        String ha2 = md5Hex(method + ":" + (authUri != null ? authUri : uri));
        String expected = qop != null && !qop.isEmpty()
                ? md5Hex(ha1 + ":" + nonce + ":" + defaultStr(authParam(authorization, "nc"), "00000001")
                        + ":" + defaultStr(authParam(authorization, "cnonce"), "") + ":" + qop + ":" + ha2)
                : md5Hex(ha1 + ":" + nonce + ":" + ha2);
        return expected.equalsIgnoreCase(response);
    }

    /** 从 Digest 头提取参数值，支持带引号与不带引号形式，并避免 cnonce 误匹配 nonce。 */
    static String authParam(String header, String key) {
        String lower = header.toLowerCase(Locale.ROOT);
        String token = key.toLowerCase(Locale.ROOT) + "=";
        int idx = -1;
        while ((idx = lower.indexOf(token, idx + 1)) >= 0) {
            char prev = idx == 0 ? ' ' : lower.charAt(idx - 1);
            if (prev == ',' || prev == ';' || Character.isWhitespace(prev)) {
                break;
            }
        }
        if (idx < 0) {
            return null;
        }
        int valStart = idx + token.length();
        if (valStart >= header.length()) {
            return null;
        }
        if (header.charAt(valStart) == '"') {
            int end = header.indexOf('"', valStart + 1);
            return end < 0 ? null : header.substring(valStart + 1, end);
        }
        int end = header.indexOf(',', valStart);
        if (end < 0) {
            end = header.length();
        }
        return header.substring(valStart, end).trim();
    }

    /** 提取 MANSCDP XML 中单个标签的文本值，兼容带命名空间前缀的标签。 */
    static String xmlTag(String xml, String tag) {
        if (xml == null || xml.isEmpty()) {
            return "";
        }
        String regex = "<(?:[\\w.-]+:)?" + tag + ">\\s*(.*?)\\s*</(?:[\\w.-]+:)?" + tag + ">";
        Matcher m = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL).matcher(xml);
        return m.find() ? m.group(1).trim() : "";
    }

    private static String md5Hex(String s) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 unavailable", e);
        }
    }

    private static String defaultStr(String v, String def) {
        return v == null || v.isEmpty() ? def : v;
    }

    private static String randomToken() {
        return String.format("%08x", ThreadLocalRandom.current().nextInt());
    }
}
