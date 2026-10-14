package com.mediagw.gb28181;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * GB28181 点播信令客户端（平台侧 UA）：向设备发送 INVITE / ACK / BYE，
 * 并把设备上行的 SIP 响应与 BYE 请求路由回对应的会话（dialog）。
 *
 * <p>发送通道复用注册时的链路：UDP 走 SIP 服务端共享的 DatagramChannel，
 * TCP 走设备注册时保持的长连接，保证设备按 Via/rport 能收到回包。
 * dialog 以 Call-ID 索引，200 OK 到达即回 ACK，收到设备 BYE 视为流终止。
 */
final class GbSignaling {

    private static final Logger log = LoggerFactory.getLogger(GbSignaling.class);

    /** 一次点播的 SIP 会话上下文。 */
    static final class Dialog {
        final String channelId;
        final String deviceId;
        final String callId;
        final String localTag;
        final InetSocketAddress target;  // 设备信令地址
        final boolean udp;
        final Channel tcpChannel;
        volatile String remoteTag = "";
        volatile int cseq = 1;
        /** 点播结果与终止回调（由 {@link GbPlayManager} 注入）。 */
        volatile Runnable onOk;
        volatile java.util.function.Consumer<String> onFail;  // 参数为失败原因
        volatile Runnable onBye;

        Dialog(String channelId, String deviceId, String callId, String localTag,
               InetSocketAddress target, boolean udp, Channel tcpChannel) {
            this.channelId = channelId;
            this.deviceId = deviceId;
            this.callId = callId;
            this.localTag = localTag;
            this.target = target;
            this.udp = udp;
            this.tcpChannel = tcpChannel;
        }
    }

    private final String serverId;
    private final String realm;
    private final Map<String, Dialog> dialogs = new ConcurrentHashMap<>();
    /** SIP 服务端 UDP 通道（启动后 attach），UDP 设备的外发均走它。 */
    private volatile Channel sipUdpChannel;

    GbSignaling(String serverId, String realm) {
        this.serverId = serverId;
        this.realm = realm;
    }

    void attachSipUdp(Channel udpChannel) {
        this.sipUdpChannel = udpChannel;
    }

    boolean hasDialog(String channelId) {
        return dialogs.values().stream().anyMatch(d -> d.channelId.equals(channelId));
    }

    Dialog dialogOf(String channelId) {
        for (Dialog d : dialogs.values()) {
            if (d.channelId.equals(channelId)) {
                return d;
            }
        }
        return null;
    }

    // ===== 请求发送 =====

    /** 发送 INVITE 点播；结果经 dialog 回调异步返回，不阻塞调用线程。 */
    void invite(GbChannelStore.Device device, String channelId, String sdp, Dialog template,
                Runnable onOk, java.util.function.Consumer<String> onFail, Runnable onBye) {
        Dialog d = template;
        d.onOk = onOk;
        d.onFail = onFail;
        d.onBye = onBye;
        dialogs.put(d.callId, d);
        String text = "INVITE sip:" + channelId + "@" + realm + " SIP/2.0\r\n"
                + viaHeader(d) + "\r\n"
                + "From: <sip:" + serverId + "@" + realm + ">;tag=" + d.localTag + "\r\n"
                + "To: <sip:" + channelId + "@" + realm + ">\r\n"
                + "Call-ID: " + d.callId + "\r\n"
                + "CSeq: " + d.cseq + " INVITE\r\n"
                + "Contact: <sip:" + serverId + "@" + contactHost(d) + ">\r\n"
                + "Subject: " + channelId + ":0000000001," + serverId + ":0\r\n"
                + "Expires: 3600\r\n"
                + "Content-Type: application/sdp\r\n"
                + "Max-Forwards: 70\r\n"
                + "Content-Length: " + sdp.getBytes(StandardCharsets.UTF_8).length + "\r\n\r\n"
                + sdp;
        send(d, text);
        log.info("[GB28181] 已发送INVITE: channel={}, device={}, callId={}",
                channelId, device.deviceId, d.callId);
    }

    /** 创建 dialog（callId/tag 生成），供 invite 与测试使用。 */
    Dialog newDialog(GbChannelStore.Device device, String channelId) {
        String callId = "play-" + Integer.toHexString(ThreadLocalRandom.current().nextInt())
                + "-" + System.currentTimeMillis();
        return new Dialog(channelId, device.deviceId, callId, randomToken(),
                device.address, device.udp, device.tcpChannel);
    }

    /** 主动结束点播：发 BYE 并移除 dialog。 */
    void bye(Dialog d) {
        if (d == null || dialogs.remove(d.callId) == null) {
            return;
        }
        d.cseq++;
        String text = "BYE sip:" + d.channelId + "@" + realm + " SIP/2.0\r\n"
                + viaHeader(d) + "\r\n"
                + "From: <sip:" + serverId + "@" + realm + ">;tag=" + d.localTag + "\r\n"
                + "To: <sip:" + d.channelId + "@" + realm
                + (d.remoteTag.isEmpty() ? "" : ";tag=" + d.remoteTag) + ">\r\n"
                + "Call-ID: " + d.callId + "\r\n"
                + "CSeq: " + d.cseq + " BYE\r\n"
                + "Max-Forwards: 70\r\n"
                + "Content-Length: 0\r\n\r\n";
        send(d, text);
        log.info("[GB28181] 已发送BYE: channel={}, callId={}", d.channelId, d.callId);
    }

    // ===== 上行路由（信令 Handler 转交） =====

    /** 设备上行的 INVITE 响应（200/4xx/5xx）。 */
    void handleResponse(SipMessage resp) {
        String callId = resp.header("Call-ID");
        if (callId == null) {
            return;
        }
        Dialog d = dialogs.get(callId);
        if (d == null) {
            return; // 非本平台发起的会话，忽略
        }
        if (!"INVITE".equals(resp.cseqMethod())) {
            return; // ACK/BYE 的响应无需处理
        }
        int code = resp.statusCode();
        if (code >= 200 && code < 300) {
            d.remoteTag = tagOf(resp.header("To"));
            sendAck(d);
            Runnable ok = d.onOk;
            if (ok != null) {
                ok.run();
            }
        } else if (code >= 400) {
            dialogs.remove(callId);
            var fail = d.onFail;
            if (fail != null) {
                fail.accept(code + " " + resp.header("CSeq"));
            }
        }
        // 1xx（ Trying/Ringing ）忽略，等待最终响应
    }

    /** 设备主动 BYE：确认并通知会话终止。 */
    void handleBye(SipMessage req, Channel inboundChannel, InetSocketAddress from, boolean udp) {
        String callId = req.header("Call-ID");
        if (callId == null) {
            return;
        }
        Dialog d = dialogs.remove(callId);
        // 应答 200（设备可能发到其登记的 Contact 地址，直接沿上行通道回）
        String text = req.toResponse(200, "OK", null,
                from.getAddress().getHostAddress(), from.getPort());
        ByteBuf out = Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.UTF_8));
        if (udp) {
            inboundChannel.writeAndFlush(new DatagramPacket(out, from));
        } else {
            inboundChannel.writeAndFlush(out);
        }
        if (d != null) {
            log.info("[GB28181] 设备挂断(BYE): channel={}, callId={}", d.channelId, callId);
            Runnable bye = d.onBye;
            if (bye != null) {
                bye.run();
            }
        }
    }

    /** 连接断开时清理该链路上的 dialog（设备掉线，点播随之终止）。 */
    void dropDialogsOn(Channel channel) {
        for (Dialog d : dialogs.values()) {
            if (d.tcpChannel == channel) {
                dialogs.remove(d.callId);
                var fail = d.onFail;
                if (fail != null) {
                    fail.accept("signaling connection lost");
                }
            }
        }
    }

    private void sendAck(Dialog d) {
        String text = "ACK sip:" + d.channelId + "@" + realm + " SIP/2.0\r\n"
                + viaHeader(d) + "\r\n"
                + "From: <sip:" + serverId + "@" + realm + ">;tag=" + d.localTag + "\r\n"
                + "To: <sip:" + d.channelId + "@" + realm
                + (d.remoteTag == null || d.remoteTag.isEmpty() ? "" : ";tag=" + d.remoteTag) + ">\r\n"
                + "Call-ID: " + d.callId + "\r\n"
                + "CSeq: " + d.cseq + " ACK\r\n"
                + "Max-Forwards: 70\r\n"
                + "Content-Length: 0\r\n\r\n";
        send(d, text);
    }

    // ===== 传输与工具 =====

    private void send(Dialog d, String text) {
        ByteBuf buf = Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.UTF_8));
        if (d.udp) {
            Channel ch = sipUdpChannel;
            if (ch == null || !ch.isActive()) {
                release(d, buf);
                return;
            }
            ch.writeAndFlush(new DatagramPacket(buf, d.target));
        } else {
            Channel ch = d.tcpChannel;
            if (ch == null || !ch.isActive()) {
                release(d, buf);
                return;
            }
            ch.writeAndFlush(buf);
        }
    }

    private void release(Dialog d, ByteBuf buf) {
        buf.release();
        var fail = d.onFail;
        if (fail != null) {
            fail.accept("signaling channel unavailable");
        }
    }

    private String viaHeader(Dialog d) {
        return "Via: SIP/2.0/" + (d.udp ? "UDP" : "TCP") + " " + contactHost(d)
                + ";rport;branch=z9hG4bK" + randomToken();
    }

    /** 平台侧信令地址：取实际发送通道的本地地址。 */
    private String contactHost(Dialog d) {
        Channel ch = d.udp ? sipUdpChannel : d.tcpChannel;
        InetSocketAddress local = ch == null ? null : (InetSocketAddress) ch.localAddress();
        if (local == null) {
            return "127.0.0.1:0";
        }
        String host = local.getAddress() != null ? local.getAddress().getHostAddress() : local.getHostString();
        return host + ":" + local.getPort();
    }

    /** 从 To/From 头提取 tag 参数值。 */
    static String tagOf(String header) {
        if (header == null) {
            return "";
        }
        int i = header.indexOf("tag=");
        if (i < 0) {
            return "";
        }
        int end = i + 4;
        while (end < header.length() && (Character.isLetterOrDigit(header.charAt(end))
                || header.charAt(end) == '-' || header.charAt(end) == '_')) {
            end++;
        }
        return header.substring(i + 4, end);
    }

    private static String randomToken() {
        return String.format("%08x", ThreadLocalRandom.current().nextInt());
    }
}
