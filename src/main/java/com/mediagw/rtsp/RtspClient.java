package com.mediagw.rtsp;

import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.config.GatewayConfig;
import com.mediagw.config.GlobalSocks5ProxyConfig;
import com.mediagw.rtp.RtpPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RTSP 拉流客户端：OPTIONS -> DESCRIBE -> SETUP(TCP interleaved) -> PLAY，
 * 播放后周期保活（GET_PARAMETER，失败降级 OPTIONS）。
 * 一个实例对应一次 TCP 连接生命周期；断线重连由上层（StreamSession）负责重建实例。
 * 回调均在 RTSP EventLoop 线程触发。
 */
public class RtspClient {

    private static final Logger log = LoggerFactory.getLogger(RtspClient.class);
    private static final String USER_AGENT = "MediaGateway/1.0";
    private static final Pattern INTERLEAVED_PATTERN = Pattern.compile("interleaved=(\\d+)-(\\d+)");

    public interface Listener {
        /**
         * DESCRIBE 成功、SDP 解析完成。返回用于重组 RTP 视频载荷的 depacketizer；
         * 返回 null 或抛异常视为启动失败（如无受支持的视频轨）。
         */
        VideoDepacketizer onSdpParsed(SdpInfo sdp);

        /** PLAY 成功，RTP 数据开始到达。 */
        void onPlaying();

        /** 启动阶段失败（连接/信令/SDP/超时），不会自动重连。 */
        void onStartupFailure(Throwable cause);

        /** 已进入 PLAYING 后连接断开或致命错误，由上层决定重连。 */
        void onDisconnected(Throwable cause);
    }

    private enum Phase { CONNECTING, OPTIONS, DESCRIBE, SETUP, PLAY, PLAYING, CLOSED }

    private final String requestUrl;   // 不含 userinfo 的完整 URL，用作请求 URI
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final GatewayConfig config;
    private final EventLoopGroup group;
    private final Listener listener;

    private final AtomicInteger cseq = new AtomicInteger();
    private final AtomicLong rtpPackets = new AtomicLong();
    private final AtomicLong rtpBytes = new AtomicLong();
    private final AtomicLong rtcpPackets = new AtomicLong();

    private volatile Channel channel;
    private volatile Phase phase = Phase.CONNECTING;
    private volatile String sessionId;
    private volatile VideoDepacketizer depacketizer;
    private volatile boolean stopped;

    private RtspAuth auth = RtspAuth.none();
    private int authAttempts;
    private int rtpChannelId = 0;
    private int rtcpChannelId = 1;
    private String keepaliveMethod = "GET_PARAMETER";
    private ScheduledFuture<?> responseTimeoutFuture;
    private ScheduledFuture<?> keepaliveFuture;

    // 401 重试所需的最近一次请求信息
    private String lastMethod;
    private String lastUri;
    private Map<String, String> lastExtraHeaders;
    private boolean lastWithSession;

    public RtspClient(String url, GatewayConfig config, EventLoopGroup group, Listener listener) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new RtspException("invalid RTSP URL: " + GatewayConfig.sanitizeUrl(url), e);
        }
        if (!"rtsp".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null) {
            throw new RtspException("invalid RTSP URL: " + GatewayConfig.sanitizeUrl(url));
        }
        this.host = uri.getHost();
        this.port = uri.getPort() > 0 ? uri.getPort() : 554;
        String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        if (uri.getRawQuery() != null) {
            path = path + "?" + uri.getRawQuery();
        }
        this.requestUrl = "rtsp://" + host + (uri.getPort() > 0 ? ":" + port : "") + path;
        String u = null;
        String p = null;
        if (uri.getUserInfo() != null && !uri.getUserInfo().isEmpty()) {
            int colon = uri.getUserInfo().indexOf(':');
            if (colon >= 0) {
                u = uri.getUserInfo().substring(0, colon);
                p = uri.getUserInfo().substring(colon + 1);
            } else {
                u = uri.getUserInfo();
            }
        }
        this.user = u;
        this.password = p;
        this.config = config;
        this.group = group;
        this.listener = listener;
    }

    public void start() {
        Bootstrap b = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.rtspResponseTimeoutMs())
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new RtspClientInitializer(this));
        b.connect(host, port).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                fail(new RtspException("RTSP connect to " + host + ":" + port + " failed", f.cause()));
            }
        });
    }

    /** 主动停止（TEARDOWN + 关闭），不触发任何回调。 */
    public void stop() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            phase = Phase.CLOSED;
        }
        cancelTimers();
        Channel ch = channel;
        if (ch != null && ch.isActive()) {
            if (sessionId != null) {
                RtspRequest req = newRequest("TEARDOWN", requestUrl, null, true);
                ch.writeAndFlush(req.encode(ch.alloc())).addListener(f -> ch.close());
            } else {
                ch.close();
            }
        } else if (ch != null) {
            ch.close();
        }
    }

    public long rtpPackets() {
        return rtpPackets.get();
    }

    public long rtpBytes() {
        return rtpBytes.get();
    }

    public long rtcpPackets() {
        return rtcpPackets.get();
    }

    public boolean isPlaying() {
        return phase == Phase.PLAYING;
    }

    // ===== 由 RtspClientHandler 在 EventLoop 线程回调 =====

    void onActive(Channel ch) {
        this.channel = ch;
        log.debug("[{}] connected, sending OPTIONS", requestUrl);
        synchronized (this) {
            phase = Phase.OPTIONS;
        }
        sendRequest("OPTIONS", requestUrl, null, false);
    }

    void handleResponse(RtspResponse resp) {
        synchronized (this) {
            if (phase == Phase.CLOSED) {
                return;
            }
        }
        cancelResponseTimeout();
        log.debug("[{}] << {}", requestUrl, resp);

        if (resp.status() == 401) {
            handleUnauthorized(resp);
            return;
        }
        if (resp.status() >= 400) {
            if (isPlayingPhase()) {
                // 保活请求失败：405/501 时降级为 OPTIONS，其余仅告警，不断流
                if (resp.status() == 405 || resp.status() == 501) {
                    log.info("[{}] keepalive {} rejected ({}), falling back to OPTIONS",
                            requestUrl, keepaliveMethod, resp.status());
                    keepaliveMethod = "OPTIONS";
                } else {
                    log.warn("[{}] keepalive {} failed: {}", requestUrl, keepaliveMethod, resp.status());
                }
                return;
            }
            fail(new RtspException("RTSP " + lastMethod + " failed: " + resp.status() + " " + resp.reason()));
            return;
        }
        authAttempts = 0;

        switch (phase) {
            case OPTIONS -> {
                phase = Phase.DESCRIBE;
                sendRequest("DESCRIBE", requestUrl, Map.of("Accept", "application/sdp"), false);
            }
            case DESCRIBE -> handleDescribe(resp);
            case SETUP -> handleSetup(resp);
            case PLAY -> {
                phase = Phase.PLAYING;
                startKeepalive();
                log.info("[{}] PLAY ok, session={}", requestUrl, sessionId);
                listener.onPlaying();
            }
            case PLAYING -> log.trace("[{}] keepalive response: {}", requestUrl, resp.status());
            default -> log.debug("[{}] unexpected response in phase {}: {}", requestUrl, phase, resp.status());
        }
    }

    private void handleDescribe(RtspResponse resp) {
        SdpInfo sdp;
        try {
            sdp = SdpParser.parse(resp.body(), resp.header("Content-Base"));
        } catch (RuntimeException e) {
            fail(new RtspException("SDP parse error", e));
            return;
        }
        SdpInfo.Track track = sdp.firstVideo();
        if (track == null) {
            fail(new RtspException("no supported video track (H264/H265) in SDP"));
            return;
        }
        VideoDepacketizer dep;
        try {
            dep = listener.onSdpParsed(sdp);
        } catch (RuntimeException e) {
            fail(new RtspException("codec init failed", e));
            return;
        }
        if (dep == null) {
            fail(new RtspException("listener rejected SDP"));
            return;
        }
        depacketizer = dep;
        String trackUrl = resolveTrackUrl(sdp, track);
        log.info("[{}] video track: {} pt={} control={}", requestUrl, track.encoding(), track.payloadType(), trackUrl);
        phase = Phase.SETUP;
        sendRequest("SETUP", trackUrl,
                Map.of("Transport", "RTP/AVP/TCP;unicast;interleaved=0-1"), false);
    }

    private void handleSetup(RtspResponse resp) {
        String session = resp.header("Session");
        if (session == null || session.isBlank()) {
            fail(new RtspException("SETUP response missing Session header"));
            return;
        }
        sessionId = session.split(";")[0].trim();
        String transport = resp.header("Transport");
        if (transport != null) {
            Matcher m = INTERLEAVED_PATTERN.matcher(transport);
            if (m.find()) {
                rtpChannelId = Integer.parseInt(m.group(1));
                rtcpChannelId = Integer.parseInt(m.group(2));
            }
        }
        phase = Phase.PLAY;
        sendRequest("PLAY", requestUrl, Map.of("Range", "npt=0.000-"), true);
    }

    private void handleUnauthorized(RtspResponse resp) {
        if (user == null || authAttempts++ >= 2) {
            fail(new RtspException("RTSP unauthorized (401), attempts=" + authAttempts
                    + ", credentials=" + (user != null ? "present" : "missing")));
            return;
        }
        RtspAuth next = RtspAuth.fromChallenge(resp.header("WWW-Authenticate"), user, password);
        if (!next.hasCredentials() || next.scheme() == RtspAuth.Scheme.NONE) {
            fail(new RtspException("unsupported WWW-Authenticate challenge"));
            return;
        }
        auth = next;
        log.info("[{}] auth challenge accepted, retrying {} with {}", requestUrl, lastMethod, auth.scheme());
        sendRequest(lastMethod, lastUri, lastExtraHeaders, lastWithSession);
    }

    void handleInterleaved(InterleavedFrame frame) {
        try {
            if (frame.channel() == rtpChannelId) {
                rtpPackets.incrementAndGet();
                rtpBytes.addAndGet(frame.payload().readableBytes());
                RtpPacket pkt = RtpPacket.parse(frame.payload());
                VideoDepacketizer dep = depacketizer;
                if (pkt != null && dep != null) {
                    dep.onRtpPacket(pkt);
                }
            } else if (frame.channel() == rtcpChannelId) {
                rtcpPackets.incrementAndGet(); // RTCP 暂不处理
            } else {
                log.debug("[{}] unknown interleaved channel {}", requestUrl, frame.channel());
            }
        } finally {
            frame.release();
        }
    }

    void handleInactive() {
        Phase p;
        synchronized (this) {
            if (phase == Phase.CLOSED) {
                return;
            }
            p = phase;
            phase = Phase.CLOSED;
        }
        cancelTimers();
        if (p == Phase.PLAYING) {
            listener.onDisconnected(new RtspException("RTSP connection closed while playing"));
        } else {
            listener.onStartupFailure(new RtspException("RTSP connection closed during " + p));
        }
    }

    void handleException(Throwable cause) {
        fail(cause);
    }

    void handleReaderIdle() {
        if (isPlayingPhase()) {
            fail(new RtspException("no RTSP/RTP data timeout"));
        }
    }

    private void fail(Throwable cause) {
        Phase p;
        synchronized (this) {
            if (phase == Phase.CLOSED) {
                return;
            }
            p = phase;
            phase = Phase.CLOSED;
        }
        cancelTimers();
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        if (p == Phase.PLAYING) {
            listener.onDisconnected(cause);
        } else {
            listener.onStartupFailure(cause);
        }
    }

    private boolean isPlayingPhase() {
        return phase == Phase.PLAYING;
    }

    private void sendRequest(String method, String uri, Map<String, String> extra, boolean withSession) {
        Channel ch = channel;
        if (ch == null || !ch.isActive() || stopped) {
            return;
        }
        lastMethod = method;
        lastUri = uri;
        lastExtraHeaders = extra;
        lastWithSession = withSession;
        RtspRequest req = newRequest(method, uri, extra, withSession);
        log.debug("[{}] >> {} {}", requestUrl, method, uri);
        ch.writeAndFlush(req.encode(ch.alloc()));
        if (!isPlayingPhase()) {
            scheduleResponseTimeout(method);
        }
    }

    private RtspRequest newRequest(String method, String uri, Map<String, String> extra, boolean withSession) {
        RtspRequest req = new RtspRequest(method, uri);
        req.header("CSeq", String.valueOf(cseq.incrementAndGet()));
        req.header("User-Agent", USER_AGENT);
        if (withSession && sessionId != null) {
            req.header("Session", sessionId);
        }
        String authHeader = auth.authorizationHeader(method, uri);
        if (authHeader != null) {
            req.header("Authorization", authHeader);
        }
        if (extra != null) {
            extra.forEach(req::header);
        }
        return req;
    }

    private void scheduleResponseTimeout(String method) {
        cancelResponseTimeout();
        Channel ch = channel;
        if (ch == null) {
            return;
        }
        responseTimeoutFuture = ch.eventLoop().schedule(
                () -> fail(new RtspException("RTSP " + method + " response timeout ("
                        + config.rtspResponseTimeoutMs() + "ms)")),
                config.rtspResponseTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private void cancelResponseTimeout() {
        ScheduledFuture<?> f = responseTimeoutFuture;
        if (f != null) {
            f.cancel(false);
            responseTimeoutFuture = null;
        }
    }

    private void startKeepalive() {
        Channel ch = channel;
        if (ch == null) {
            return;
        }
        int sec = Math.max(5, config.rtspKeepaliveSec());
        keepaliveFuture = ch.eventLoop().scheduleWithFixedDelay(() -> {
            if (isPlayingPhase() && !stopped) {
                sendRequest(keepaliveMethod, requestUrl, null, true);
            }
        }, sec, sec, TimeUnit.SECONDS);
    }

    private void cancelTimers() {
        cancelResponseTimeout();
        ScheduledFuture<?> k = keepaliveFuture;
        if (k != null) {
            k.cancel(false);
            keepaliveFuture = null;
        }
    }

    private String resolveTrackUrl(SdpInfo sdp, SdpInfo.Track track) {
        String control = track.control();
        if (control == null || control.isBlank() || "*".equals(control)) {
            return requestUrl;
        }
        if (control.startsWith("rtsp://") || control.startsWith("rtspu://")) {
            return control;
        }
        String base = sdp.contentBase() != null && !sdp.contentBase().isBlank()
                ? sdp.contentBase() : requestUrl;
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        String c = control.startsWith("./") ? control.substring(2) : control;
        return base + "/" + c;
    }

    int readerIdleSeconds() {
        return Math.max(15, config.rtspKeepaliveSec() + 10);
    }

    /** Pipeline：TCP 解码器 -> 空闲检测 -> 客户端 Handler。 */
    private static final class RtspClientInitializer extends io.netty.channel.ChannelInitializer<io.netty.channel.socket.SocketChannel> {
        private final RtspClient client;

        RtspClientInitializer(RtspClient client) {
            this.client = client;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            Socks5ProxyHandler proxyHandler = GlobalSocks5ProxyConfig.getSocks5ProxyHandler(client.host);
            if(proxyHandler != null){
                ch.pipeline().addFirst(proxyHandler);
            }
            ch.pipeline()
                    .addLast(new RtspTcpDecoder())
                    .addLast(new IdleStateHandler(client.readerIdleSeconds(), 0, 0))
                    .addLast(new RtspClientHandler(client));
        }
    }
}
