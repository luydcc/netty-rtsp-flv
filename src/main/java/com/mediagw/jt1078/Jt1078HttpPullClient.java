package com.mediagw.jt1078;

import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.config.GatewayConfig;
import com.mediagw.config.GlobalSocks5ProxyConfig;
import com.mediagw.flv.FlvPassthroughParser;
import com.mediagw.flv.StreamBodyParser;
import com.mediagw.util.StreamUrl;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * HTTP 拉流客户端：GET {@code http(s)://host[:port]/path}，收到 200 后先攒住响应体前缀，按前缀分流：
 * <ul>
 *   <li>首字符 {@code '{'} —— 地址是取流接口而非视频流（如 {@code {"url":"rtsp://..."}}），
 *       提取真实流地址后经 {@link Listener#onStreamUrlResolved} 交回上层改用该地址拉流；</li>
 *   <li>FLV Header(<code>46 4C 56</code>) 开头 —— 不带 1078 标志，就是普通 FLV 视频，
 *       经 {@link Listener#onFlvStreamStarted} 向会话要 {@link FlvPassthroughParser}，
 *       不转码、原样把 Tag 透传给订阅者；</li>
 *   <li>其余 —— 按 JT/T 1078 帧流处理，逐帧喂给 {@link Jt1078FrameParser} 重组 H.264
 *       （其魔数 {@code 30 31 63 64} 与 FLV 签名互斥，不会误判）。</li>
 * </ul>
 * 一个实例对应一次 TCP 连接生命周期；断线重连与取流地址重试由上层（StreamSession）负责重建实例。
 * 回调均在连接 EventLoop 线程触发，且一律在客户端锁外执行。
 */
public class Jt1078HttpPullClient {

    private static final Logger log = LoggerFactory.getLogger(Jt1078HttpPullClient.class);
    private static final String USER_AGENT = "MediaGateway/1.0";
    /** 收流读空闲：正常视频流每秒都有数据，15s 无数据视为中断。 */
    private static final int READER_IDLE_SEC = 15;
    /** 判定上游格式所需的前缀字节数（FLV Header 定长部分）。 */
    private static final int SNIFF_BYTES = 9;
    /** 嗅探阶段的首包缓存上限。 */
    private static final int SNIFF_BUFFER_BYTES = 64 * 1024;
    /** 取流接口响应体上限：超过则视为异常响应。 */
    private static final int MAX_JSON_BODY_BYTES = 64 * 1024;

    public interface Listener {
        /**
         * HTTP 200 就绪且响应体前缀未带 FLV 标志，按 JT/T 1078 帧流处理。
         * 返回用于重组视频帧的 depacketizer；
         * 返回 null 或抛异常视为启动失败。
         */
        VideoDepacketizer onStreamStarted();

        /**
         * HTTP 200 就绪且响应体以 FLV Header 开头：不带 1078 标志，按普通视频处理。
         * 返回 FLV 裸流透传解析器；返回 null 或抛异常视为启动失败。
         */
        StreamBodyParser onFlvStreamStarted();

        /**
         * 响应体首字符为 '{'：该地址是取流接口而非视频流，需改用接口给出的地址拉流。
         * 本实例到此结束，不会再触发其它回调。
         *
         * @param url 接口中解析出的流地址；未解析到为 null，由上层延迟重试
         */
        void onStreamUrlResolved(String url);

        /** 响应头接收完成，流数据开始到达。 */
        void onPlaying();

        /** 启动阶段失败（连接/响应/超时），不会自动重连。 */
        void onStartupFailure(Throwable cause);

        /** 已进入收流后连接断开或致命错误，由上层决定重连。 */
        void onDisconnected(Throwable cause);
    }

    private enum Phase { CONNECTING, AWAITING_RESPONSE, SNIFFING, STREAMING, CLOSED }

    private final String requestUri;   // 含 path 与 query 的请求 URI
    private final String host;
    private final int port;
    private final boolean tls;
    private final GatewayConfig config;
    private final EventLoopGroup group;
    private final Listener listener;

    private volatile Channel channel;
    private volatile Phase phase = Phase.CONNECTING;
    private volatile boolean stopped;
    /** 响应体解析器：1078 帧流或 FLV 透传，由前缀判定结果决定。 */
    private volatile StreamBodyParser parser;
    /** 前缀嗅探缓冲（guarded by this）。 */
    private ByteBuf sniffBuf;

    private ScheduledFuture<?> responseTimeoutFuture;

    public Jt1078HttpPullClient(String url, GatewayConfig config, EventLoopGroup group, Listener listener) {
        // 不用 URI.create：token 与密码里的 # ? 会被当作 fragment/query 截断，改用宽松解析
        StreamUrl uri = StreamUrl.parse(url);
        if (uri == null || !uri.http()) {
            throw new IllegalArgumentException("invalid HTTP URL: " + GatewayConfig.sanitizeUrl(url));
        }
        this.tls = uri.https();
        this.host = uri.host();
        this.port = uri.portExplicit() ? uri.port() : (this.tls ? 443 : 80);
        this.requestUri = uri.pathAndQuery().isEmpty() ? "/" : uri.pathAndQuery();
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
                .handler(new PullClientInitializer(this));
        b.connect(host, port).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                fail(new RuntimeException("HTTP connect to " + host + ":" + port + " failed", f.cause()));
            }
        });
    }

    /** 主动停止（关闭连接），不触发任何回调。 */
    public void stop() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            phase = Phase.CLOSED;
            releaseSniffLocked();
        }
        cancelResponseTimeout();
        StreamBodyParser p = parser;
        if (p != null) {
            p.close();
            parser = null;
        }
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
    }

    public long frameCount() {
        StreamBodyParser p = parser;
        return p == null ? 0 : p.frameCount();
    }

    public long byteCount() {
        StreamBodyParser p = parser;
        return p == null ? 0 : p.byteCount();
    }

    int readerIdleSeconds() {
        return READER_IDLE_SEC;
    }

    /** 调用者持有锁：释放嗅探缓冲。 */
    private void releaseSniffLocked() {
        if (sniffBuf != null) {
            sniffBuf.release();
            sniffBuf = null;
        }
    }

    // ===== 由 PullClientHandler 在 EventLoop 线程回调 =====

    void onActive(Channel ch) {
        this.channel = ch;
        synchronized (this) {
            if (phase == Phase.CLOSED) {
                ch.close();
                return;
            }
            phase = Phase.AWAITING_RESPONSE;
        }
        DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestUri);
        req.headers().set(HttpHeaderNames.HOST, host + (uriPortExplicit() ? ":" + port : ""));
        req.headers().set(HttpHeaderNames.USER_AGENT, USER_AGENT);
        req.headers().set(HttpHeaderNames.ACCEPT, "*/*");
        req.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        log.debug("[{}] >> GET {}", host, requestUri);
        ch.writeAndFlush(req);
        scheduleResponseTimeout();
    }

    void handleResponse(HttpResponse resp) {
        synchronized (this) {
            if (phase != Phase.AWAITING_RESPONSE) {
                return;
            }
        }
        cancelResponseTimeout();
        if (resp.status().code() != HttpResponseStatus.OK.code()) {
            fail(new RuntimeException("HTTP GET failed: " + resp.status()));
            return;
        }
        // 先攒响应体前缀，再决定按 1078 帧流还是普通 FLV 裸流解析
        synchronized (this) {
            if (phase == Phase.AWAITING_RESPONSE) {
                phase = Phase.SNIFFING;
                sniffBuf = Unpooled.buffer(SNIFF_BUFFER_BYTES);
            }
        }
    }

    void handleContent(HttpContent content) {
        boolean last = content instanceof LastHttpContent;
        ByteBuf prefix = null;
        boolean flv = false;
        boolean json = false;
        boolean overflow = false;
        boolean empty = false;
        Phase current;
        synchronized (this) {
            if (phase == Phase.SNIFFING && sniffBuf != null) {
                sniffBuf.writeBytes(content.content());
                if (sniffBuf.readableBytes() > 0 && sniffBuf.getByte(sniffBuf.readerIndex()) == '{') {
                    // 取流接口：响应体是 JSON，等收完再整体提取流地址
                    if (sniffBuf.readableBytes() > MAX_JSON_BODY_BYTES) {
                        overflow = true;
                    } else if (last) {
                        prefix = sniffBuf;
                        sniffBuf = null;
                        json = true;
                    }
                } else if (sniffBuf.readableBytes() >= SNIFF_BYTES || last) {
                    if (last && sniffBuf.readableBytes() == 0) {
                        // 空响应体：既不是流也不是接口，不必等读空闲超时，直接快速失败
                        empty = true;
                    } else {
                        prefix = sniffBuf;
                        sniffBuf = null;
                        flv = FlvPassthroughParser.isFlvStream(prefix);
                    }
                }
            }
            current = phase;
        }
        if (overflow) {
            fail(new RuntimeException("json api response exceeds " + MAX_JSON_BODY_BYTES + " bytes"));
            return;
        }
        if (empty) {
            fail(new RuntimeException("empty response body"));
            return;
        }
        if (json) {
            try {
                resolveStreamUrl(prefix);
            } finally {
                prefix.release();
            }
        } else if (prefix != null) {
            try {
                startParsing(prefix, flv);
            } finally {
                prefix.release();
            }
        } else if (current == Phase.STREAMING) {
            StreamBodyParser p = parser;
            if (p != null) {
                p.feed(content.content());
                if (p.isBroken()) {
                    fail(new RuntimeException("stream body out of frame boundary"));
                }
            }
        }
        if (last && !json) {
            // 响应体提前结束：按断开处理，由上层决定重连
            Channel ch = channel;
            if (ch != null) {
                ch.close();
            }
        }
    }

    /**
     * 取流接口响应收完：按双引号对提取第一个有效流地址，交给上层改用该地址拉流。
     * 无论成否本实例均告结束（置 CLOSED，不再上报断链），回调一律在客户端锁外执行。
     */
    private void resolveStreamUrl(ByteBuf body) {
        String url = StreamUrl.extractStreamUrl(body.toString(StandardCharsets.UTF_8));
        synchronized (this) {
            if (phase == Phase.CLOSED) { // 解析期间已被停止
                return;
            }
            phase = Phase.CLOSED;
            releaseSniffLocked();
        }
        cancelResponseTimeout();
        log.info("[{}] stream url from api: {}", host,
                url == null ? "not found" : GatewayConfig.sanitizeUrl(url));
        listener.onStreamUrlResolved(url);
    }

    /**
     * 前缀判定完成：建立对应解析器、回放嗅探到的前缀字节并通知上层开始收流。
     * 上层回调一律在客户端锁外执行，避免与会话锁形成反向等待。
     */
    private void startParsing(ByteBuf prefix, boolean flv) {
        StreamBodyParser created;
        if (flv) {
            StreamBodyParser p;
            try {
                p = listener.onFlvStreamStarted();
            } catch (RuntimeException e) {
                fail(e);
                return;
            }
            if (p == null) {
                fail(new RuntimeException("flv passthrough parser unavailable"));
                return;
            }
            created = p;
        } else {
            VideoDepacketizer dep;
            try {
                dep = listener.onStreamStarted();
            } catch (RuntimeException e) {
                fail(e);
                return;
            }
            if (dep == null) {
                fail(new RuntimeException("video depacketizer unavailable"));
                return;
            }
            created = new Jt1078FrameParser(dep);
        }
        synchronized (this) {
            if (phase != Phase.SNIFFING) { // 判定期间已被停止
                created.close();
                return;
            }
            parser = created;
            phase = Phase.STREAMING;
        }
        created.feed(prefix);
        listener.onPlaying();
    }

    void handleInactive() {
        Phase p;
        synchronized (this) {
            if (phase == Phase.CLOSED) {
                return;
            }
            p = phase;
            phase = Phase.CLOSED;
            releaseSniffLocked();
        }
        cancelResponseTimeout();
        StreamBodyParser parser0 = parser;
        parser = null;
        if (parser0 != null) {
            parser0.close();
        }
        if (p == Phase.STREAMING) {
            listener.onDisconnected(new RuntimeException("HTTP stream closed by peer"));
        } else {
            listener.onStartupFailure(new RuntimeException("HTTP connection closed during " + p));
        }
    }

    void handleException(Throwable cause) {
        fail(cause);
    }

    void handleReaderIdle() {
        if (phase == Phase.STREAMING) {
            fail(new RuntimeException("no stream data for " + READER_IDLE_SEC + "s"));
        } else if (phase == Phase.SNIFFING) {
            fail(new RuntimeException("no response body for " + READER_IDLE_SEC + "s"));
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
            releaseSniffLocked();
        }
        cancelResponseTimeout();
        StreamBodyParser parser0 = parser;
        parser = null;
        if (parser0 != null) {
            parser0.close();
        }
        Channel ch = channel;
        if (ch != null) {
            ch.close();
        }
        if (p == Phase.STREAMING) {
            listener.onDisconnected(cause);
        } else {
            listener.onStartupFailure(cause);
        }
    }

    private boolean uriPortExplicit() {
        // 默认端口在 Host 头中可省略；此处不做严格区分，非 80/443 才显式携带
        return port != 80 && port != 443;
    }

    private void scheduleResponseTimeout() {
        cancelResponseTimeout();
        Channel ch = channel;
        if (ch == null) {
            return;
        }
        responseTimeoutFuture = ch.eventLoop().schedule(
                () -> fail(new RuntimeException("HTTP response timeout ("
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

    /** Pipeline：SOCKS5(可选) -> TLS(https) -> HTTP 编解码 -> 空闲检测 -> 收流 Handler。 */
    private static final class PullClientInitializer extends ChannelInitializer<SocketChannel> {
        private final Jt1078HttpPullClient client;

        PullClientInitializer(Jt1078HttpPullClient client) {
            this.client = client;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws SSLException {
            Socks5ProxyHandler proxyHandler = GlobalSocks5ProxyConfig.getSocks5ProxyHandler(client.host);
            if (proxyHandler != null) {
                ch.pipeline().addFirst(proxyHandler);
            }
            if (client.tls) {
                // 监控平台多用自签名证书，忽略证书校验
                SslContext ssl = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build();
                ch.pipeline().addLast(ssl.newHandler(ch.alloc(), client.host, client.port));
            }
            ch.pipeline()
                    .addLast(new HttpClientCodec())
                    .addLast(new IdleStateHandler(client.readerIdleSeconds(), 0, 0))
                    .addLast(new PullClientHandler(client));
        }
    }

    private static final class PullClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        private final Jt1078HttpPullClient client;

        PullClientHandler(Jt1078HttpPullClient client) {
            this.client = client;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            client.onActive(ctx.channel());
            ctx.fireChannelActive();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof HttpResponse resp) {
                client.handleResponse(resp);
            } else if (msg instanceof HttpContent content) {
                client.handleContent(content);
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent event && event.state() == IdleState.READER_IDLE) {
                client.handleReaderIdle();
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            client.handleInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            client.handleException(cause);
        }
    }
}
