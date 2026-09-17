package com.mediagw.rtsp;

import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.config.GatewayConfig;
import com.mediagw.session.StreamSessionManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * RTSP 转发服务端 Handler：网关作为 RTSP 服务端，把已拉取的流重新打包成 RTP 发给播放客户端。
 *
 * <p>请求状态机：OPTIONS -> DESCRIBE（订阅会话，等参数集就绪后回 SDP）-> SETUP（协商
 * TCP interleaved 或 UDP）-> PLAY（下发 GOP 快照后转实时）-> PAUSE / TEARDOWN。
 * GET_PARAMETER 作为保活应答。播放地址形如 {@code rtsp://host:8554/live/{streamId}}，
 * 也接受省略 {@code /live} 前缀的 {@code rtsp://host:8554/{streamId}}。
 *
 * <p>与 FLV 输出共享同一路 RTSP 拉流：首个订阅者触发拉流，最后一个离开后延迟关闭。
 */
public final class RtspForwardHandler extends ChannelInboundHandlerAdapter implements RtspSender.Listener {

    private static final Logger log = LoggerFactory.getLogger(RtspForwardHandler.class);
    private static final String PUBLIC_METHODS =
            "OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN, GET_PARAMETER";
    private static final String AUTH_REALM = "netty-rtsp-flv";
    /** UDP 端口对绑定失败时的重试次数与端口范围（RTP 取偶数端口，RTCP 取其后一个奇数端口）。 */
    private static final int MAX_UDP_BIND_ATTEMPTS = 8;
    private static final int UDP_PORT_MIN = 20000;
    private static final int UDP_PORT_MAX = 60000;

    private final StreamSessionManager manager;
    private final GatewayConfig config;
    private final EventLoopGroup udpGroup;

    private ChannelHandlerContext ctx;
    private RtspSender sender;
    private String streamId;
    private String sessionId;
    /** DESCRIBE 的 URI（去查询参数），作为应答的 Content-Base 供客户端解析相对 control。 */
    private String baseUri;
    /** 等待参数集就绪的 DESCRIBE 请求序号，-1 表示没有待应答的 DESCRIBE。 */
    private int pendingDescribeCSeq = -1;
    private ScheduledFuture<?> describeTimeout;
    private boolean released;

    public RtspForwardHandler(StreamSessionManager manager, GatewayConfig config, EventLoopGroup udpGroup) {
        this.manager = manager;
        this.config = config;
        this.udpGroup = udpGroup;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof InterleavedFrame frame) {
            frame.release(); // 客户端上行的 RTCP，转发侧不处理
            return;
        }
        if (!(msg instanceof RtspServerRequest req)) {
            return;
        }
        dispatch(ctx, req);
    }

    private void dispatch(ChannelHandlerContext ctx, RtspServerRequest req) {
        int cseq = cseq(req);
        log.debug("RTSP forward request {} from {}", req, ctx.channel().remoteAddress());
        if (!"OPTIONS".equals(req.method()) && !authorized(req)) {
            log.warn("unauthorized RTSP forward {} from {}", req.method(), ctx.channel().remoteAddress());
            respond(ctx, error(cseq, 401).header("WWW-Authenticate", "Basic realm=\"" + AUTH_REALM + "\""));
            return;
        }
        switch (req.method()) {
            case "OPTIONS" -> respond(ctx, RtspServerResponse.ok(cseq).header("Public", PUBLIC_METHODS));
            case "DESCRIBE" -> describe(ctx, req, cseq);
            case "SETUP" -> setup(ctx, req, cseq);
            case "PLAY" -> play(ctx, req, cseq);
            case "PAUSE" -> pause(ctx, req, cseq);
            case "TEARDOWN" -> teardown(ctx, req, cseq);
            case "GET_PARAMETER", "SET_PARAMETER" ->
                    respond(ctx, withSession(RtspServerResponse.ok(cseq)));
            default -> respond(ctx, error(cseq, 501).header("Allow", PUBLIC_METHODS));
        }
    }

    // ===== DESCRIBE =====

    private void describe(ChannelHandlerContext ctx, RtspServerRequest req, int cseq) {
        if (sender != null) { // 重复 DESCRIBE：参数集已知则立即重新应答
            ParamSets ps = sender.params();
            CodecType codec = sender.codec();
            if (ps != null && codec != null) {
                sendSdp(ctx, cseq, codec, ps);
            } else {
                pendingDescribeCSeq = cseq;
            }
            return;
        }
        String id = streamId(req.path());
        if (id == null || manager.streamUrl(id) == null) {
            log.warn("unknown RTSP forward stream '{}' requested by {}", req.path(), ctx.channel().remoteAddress());
            respond(ctx, error(cseq, 404));
            return;
        }
        if (manager.pullLimitReached(id)) {
            log.warn("pull limit {} reached, reject RTSP forward '{}' from {}",
                    config.maxPullStreams(), id, ctx.channel().remoteAddress());
            respond(ctx, error(cseq, 503));
            return;
        }
        streamId = id;
        baseUri = stripQuery(req.uri());
        RtspSender s = new RtspSender(ctx.channel(), id, config.rtspForwardMaxPayload(), this);
        StreamSessionManager.RawSubscribeResult result = manager.subscribeRaw(id, s);
        if (result != StreamSessionManager.RawSubscribeResult.OK) {
            int status = switch (result) {
                case UNKNOWN_STREAM -> 404;
                case TOO_MANY_STREAMS -> 503;
                default -> 500;
            };
            respond(ctx, error(cseq, status));
            return;
        }
        sender = s;
        // 首个订阅者会触发 RTSP 拉流，参数集就绪前无法生成 SDP，故挂起本次 DESCRIBE
        pendingDescribeCSeq = cseq;
        describeTimeout = ctx.executor().schedule(() -> onDescribeTimeout(ctx),
                config.rtspResponseTimeoutMs(), TimeUnit.MILLISECONDS);
        log.info("[{}] RTSP forward DESCRIBE from {}, waiting for stream info", id, ctx.channel().remoteAddress());
    }

    private void onDescribeTimeout(ChannelHandlerContext ctx) {
        describeTimeout = null;
        int cseq = pendingDescribeCSeq;
        if (cseq < 0) {
            return;
        }
        pendingDescribeCSeq = -1;
        log.warn("[{}] RTSP forward DESCRIBE timed out after {}ms, no stream info from upstream",
                streamId, config.rtspResponseTimeoutMs());
        respond(ctx, error(cseq, 503));
        cleanup();
        ctx.close();
    }

    /** RtspSender.Listener：参数集就绪（在 RTSP 控制连接的 EventLoop 线程回调）。 */
    @Override
    public void onStreamReady(CodecType codec, ParamSets params) {
        int cseq = pendingDescribeCSeq;
        if (cseq < 0) {
            return; // SDP 已发出，参数集变化由 sender 带内更新
        }
        pendingDescribeCSeq = -1;
        cancelDescribeTimeout();
        sendSdp(ctx, cseq, codec, params);
    }

    private void sendSdp(ChannelHandlerContext ctx, int cseq, CodecType codec, ParamSets params) {
        String sdp = RtspSdpBuilder.build(streamId, codec, params);
        respond(ctx, RtspServerResponse.ok(cseq)
                .header("Content-Base", baseUri.endsWith("/") ? baseUri : baseUri + "/")
                .body("application/sdp", sdp));
        log.info("[{}] RTSP forward DESCRIBE answered: {} sdp={}B for {}",
                streamId, codec, sdp.length(), ctx.channel().remoteAddress());
    }

    /** RtspSender.Listener：上游流终止。 */
    @Override
    public void onStreamClosed() {
        int cseq = pendingDescribeCSeq;
        pendingDescribeCSeq = -1;
        cancelDescribeTimeout();
        log.warn("[{}] upstream stream closed, terminating RTSP forward {}", streamId, describeSink());
        if (cseq >= 0) {
            respond(ctx, error(cseq, 503));
        }
        cleanup();
        ctx.close();
    }

    // ===== SETUP =====

    private void setup(ChannelHandlerContext ctx, RtspServerRequest req, int cseq) {
        if (sender == null || streamId == null) {
            respond(ctx, error(cseq, 454));
            return;
        }
        String transport = req.header("Transport");
        if (transport == null || transport.isEmpty()) {
            respond(ctx, error(cseq, 461));
            return;
        }
        if (isTcp(transport)) {
            int[] interleaved = portPair(transport, "interleaved", 0, 1);
            sender.attachSink(new RtspSender.TcpSink(ctx.channel(), interleaved[0]));
            sessionId = newSessionId();
            respond(ctx, withSession(RtspServerResponse.ok(cseq).header("Transport",
                    "RTP/AVP/TCP;unicast;interleaved=" + interleaved[0] + "-" + interleaved[1]
                            + ";ssrc=" + ssrcHex())));
            log.info("[{}] RTSP forward SETUP tcp interleaved={} for {}",
                    streamId, interleaved[0] + "-" + interleaved[1], ctx.channel().remoteAddress());
            return;
        }
        if (!config.rtspForwardUdp()) {
            log.info("[{}] UDP transport disabled, reject SETUP from {}", streamId, ctx.channel().remoteAddress());
            respond(ctx, error(cseq, 461));
            return;
        }
        int[] clientPort = portPair(transport, "client_port", -1, -1);
        if (clientPort[0] <= 0) {
            respond(ctx, error(cseq, 461));
            return;
        }
        bindUdpPair(ctx, cseq, clientPort, 0);
    }

    /** 绑定一对本地 UDP 端口（偶=RTP、奇=RTCP），端口被占用时换一对重试。 */
    private void bindUdpPair(ChannelHandlerContext ctx, int cseq, int[] clientPort, int attempt) {
        if (attempt >= MAX_UDP_BIND_ATTEMPTS) {
            log.warn("[{}] no free UDP port pair after {} attempts", streamId, MAX_UDP_BIND_ATTEMPTS);
            respond(ctx, error(cseq, 461));
            return;
        }
        int base = randomEvenPort();
        udpBootstrap(true).bind(base).addListener((ChannelFutureListener) f1 -> {
            if (!f1.isSuccess()) {
                bindUdpPair(ctx, cseq, clientPort, attempt + 1);
                return;
            }
            Channel rtpChannel = f1.channel();
            udpBootstrap(false).bind(base + 1).addListener((ChannelFutureListener) f2 -> {
                if (!f2.isSuccess()) {
                    rtpChannel.close();
                    bindUdpPair(ctx, cseq, clientPort, attempt + 1);
                    return;
                }
                udpSetupReady(ctx, cseq, clientPort, rtpChannel, f2.channel());
            });
        });
    }

    private void udpSetupReady(ChannelHandlerContext ctx, int cseq, int[] clientPort,
                               Channel rtpChannel, Channel rtcpChannel) {
        InetSocketAddress remote = new InetSocketAddress(clientAddress(ctx), clientPort[0]);
        RtspSender.UdpSink sink = new RtspSender.UdpSink(rtpChannel, rtcpChannel, remote);
        sender.attachSink(sink);
        sessionId = newSessionId();
        respond(ctx, withSession(RtspServerResponse.ok(cseq).header("Transport",
                "RTP/AVP;unicast;client_port=" + clientPort[0] + "-" + clientPort[1]
                        + ";server_port=" + sink.rtpPort() + "-" + sink.rtcpPort()
                        + ";ssrc=" + ssrcHex())));
        log.info("[{}] RTSP forward SETUP udp server_port={}-{} -> {} for {}",
                streamId, sink.rtpPort(), sink.rtcpPort(), remote, ctx.channel().remoteAddress());
    }

    /**
     * UDP 通道管线：RTP 通道收到客户端上行报文时锁存其真实地址（NAT 兼容），
     * RTCP 通道只收不发；两者均自动释放报文。
     */
    private Bootstrap udpBootstrap(boolean latchSourceAddress) {
        return new Bootstrap()
                .group(udpGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_BROADCAST, false)
                .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext c, DatagramPacket pkt) {
                        if (latchSourceAddress) {
                            latchRemote(pkt.sender());
                        }
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext c, Throwable cause) {
                        log.debug("RTSP forward udp channel error: {}", cause.toString());
                    }
                });
    }

    private void latchRemote(InetSocketAddress from) {
        RtspSender s = sender;
        if (s != null && s.sink() instanceof RtspSender.UdpSink udp) {
            udp.latch(from);
        }
    }

    // ===== PLAY / PAUSE / TEARDOWN =====

    private void play(ChannelHandlerContext ctx, RtspServerRequest req, int cseq) {
        if (sender == null || sessionId == null || !sessionMatches(req)) {
            respond(ctx, error(cseq, 454));
            return;
        }
        // TCP interleaved 下响应与 RTP 帧共用一条连接，必须先写响应再开始分发
        ctx.writeAndFlush(withSession(RtspServerResponse.ok(cseq).header("Range", "npt=now-"))
                        .encode(ctx.alloc()))
                .addListener((ChannelFutureListener) f -> {
                    manager.startRawPlayback(streamId, sender);
                    log.info("[{}] RTSP forward PLAY via {}", streamId, describeSink());
                });
    }

    private void pause(ChannelHandlerContext ctx, RtspServerRequest req, int cseq) {
        if (sender == null || sessionId == null || !sessionMatches(req)) {
            respond(ctx, error(cseq, 454));
            return;
        }
        manager.stopRawPlayback(streamId, sender);
        respond(ctx, withSession(RtspServerResponse.ok(cseq)));
        log.info("[{}] RTSP forward PAUSE {}", streamId, describeSink());
    }

    private void teardown(ChannelHandlerContext ctx, RtspServerRequest req, int cseq) {
        respond(ctx, withSession(RtspServerResponse.ok(cseq)));
        log.info("[{}] RTSP forward TEARDOWN {}", streamId, describeSink());
        cleanup();
        ctx.channel().close();
    }

    // ===== 连接生命周期 =====

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            log.info("[{}] RTSP forward idle {}s without keepalive, closing {}",
                    streamId, config.rtspForwardSessionTimeoutSec(), describeSink());
            cleanup();
            ctx.close();
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("RTSP forward error on {}: {}", ctx.channel().remoteAddress(), cause.toString());
        cleanup();
        ctx.close();
    }

    /** 幂等退订：解除会话订阅并关闭 UDP 端口对（RTSP 控制连接由调用方关闭）。 */
    private void cleanup() {
        if (released) {
            return;
        }
        released = true;
        cancelDescribeTimeout();
        RtspSender s = sender;
        if (s != null && streamId != null) {
            manager.unsubscribeRaw(streamId, s);
            s.closeSink();
            log.info("[{}] RTSP forward subscriber {} released (packets={} bytes={} dropped={})",
                    streamId, s.id(), s.packets(), s.bytes(), s.dropped());
        }
    }

    private void cancelDescribeTimeout() {
        ScheduledFuture<?> f = describeTimeout;
        if (f != null) {
            f.cancel(false);
            describeTimeout = null;
        }
    }

    // ===== 辅助 =====

    private void respond(ChannelHandlerContext ctx, RtspServerResponse resp) {
        ctx.writeAndFlush(resp.encode(ctx.alloc()));
    }

    private RtspServerResponse withSession(RtspServerResponse resp) {
        return sessionId == null ? resp : resp.header("Session", sessionId);
    }

    private static RtspServerResponse error(int cseq, int status) {
        return RtspServerResponse.of(status).header("CSeq", String.valueOf(cseq));
    }

    private boolean sessionMatches(RtspServerRequest req) {
        String value = req.header("Session");
        if (value == null || value.isEmpty()) {
            return true; // 客户端未带 Session 头时不做强制校验
        }
        int semi = value.indexOf(';');
        return sessionId.equals((semi < 0 ? value : value.substring(0, semi)).trim());
    }

    private String ssrcHex() {
        return String.format(Locale.ROOT, "%08X", sender.ssrc());
    }

    private String describeSink() {
        RtspSender s = sender;
        if (s == null) {
            return String.valueOf(ctx.channel().remoteAddress());
        }
        RtspSender.Sink sink = s.sink();
        return s.id() + " (" + (sink == null ? "no transport" : sink.describe()) + ")";
    }

    /** 配置了 auth.token 时校验播放口令：URL 查询参数 token 或 Basic 认证（密码为 token）。 */
    private boolean authorized(RtspServerRequest req) {
        String expected = config.authToken();
        if (expected == null || expected.isEmpty()) {
            return true;
        }
        if (expected.equals(req.query("token"))) {
            return true;
        }
        String auth = req.header("Authorization");
        if (auth == null || !auth.regionMatches(true, 0, "Basic ", 0, "Basic ".length())) {
            return false;
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(auth.substring("Basic ".length()).trim()),
                    StandardCharsets.UTF_8);
            int colon = decoded.indexOf(':');
            return colon >= 0 && expected.equals(decoded.substring(colon + 1));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * 从 URI 路径解析 streamId：支持 {@code /live/{id}} 与 {@code /{id}}，
     * 并忽略 {@code /trackID=0} 之类的控制后缀。
     */
    static String streamId(String path) {
        if (path == null) {
            return null;
        }
        String p = path.startsWith("/") ? path.substring(1) : path;
        if (p.startsWith("live/")) {
            p = p.substring("live/".length());
        }
        int slash = p.indexOf('/');
        if (slash >= 0) {
            p = p.substring(0, slash);
        }
        return p.isEmpty() ? null : p;
    }

    private static String stripQuery(String uri) {
        int q = uri.indexOf('?');
        return q < 0 ? uri : uri.substring(0, q);
    }

    private static int cseq(RtspServerRequest req) {
        String v = req.header("CSeq");
        if (v == null) {
            return 0;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Transport 首选项是否为 TCP。 */
    private static boolean isTcp(String transport) {
        String first = transport.split(",")[0];
        int semi = first.indexOf(';');
        String proto = (semi < 0 ? first : first.substring(0, semi)).trim().toUpperCase(Locale.ROOT);
        return proto.endsWith("/TCP");
    }

    /** 解析 Transport 中的 {@code key=a-b}；缺失或非法时返回默认值。 */
    static int[] portPair(String transport, String key, int defA, int defB) {
        String lower = transport.toLowerCase(Locale.ROOT);
        int idx = lower.indexOf(key + "=");
        if (idx < 0) {
            return new int[]{defA, defB};
        }
        int start = idx + key.length() + 1;
        int end = transport.length();
        for (int i = start; i < end; i++) {
            char c = transport.charAt(i);
            if (c == ';' || c == ',') {
                end = i;
                break;
            }
        }
        String value = transport.substring(start, end).trim();
        int dash = value.indexOf('-');
        try {
            int a = Integer.parseInt((dash < 0 ? value : value.substring(0, dash)).trim());
            int b = dash < 0 ? a + 1 : Integer.parseInt(value.substring(dash + 1).trim());
            return new int[]{a, b};
        } catch (NumberFormatException e) {
            return new int[]{defA, defB};
        }
    }

    private static String newSessionId() {
        return String.format(Locale.ROOT, "%08X", ThreadLocalRandom.current().nextInt());
    }

    private static int randomEvenPort() {
        return UDP_PORT_MIN + 2 * ThreadLocalRandom.current().nextInt((UDP_PORT_MAX - UDP_PORT_MIN) / 2);
    }

    /** 客户端 IP：UDP 目的地与控制连接同源（端口取自 SETUP 的 client_port）。 */
    private static InetAddress clientAddress(ChannelHandlerContext ctx) {
        return ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();
    }
}
