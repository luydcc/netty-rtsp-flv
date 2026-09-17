package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.StreamSessionManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FLV over WebSocket / HTTP-FLV 服务端（两者共用同一端口与路径）。
 * Pipeline：HttpServerCodec -> CorsHandler(跨域预检与响应头) -> HttpObjectAggregator
 * -> HttpControlHandler(/stats、/api/streams、页面)
 * -> HttpFlvHandler(/live 普通 GET，chunked 下发 FLV)
 * -> WebSocketServerProtocolHandler(/live 前缀匹配) -> FlvWsHandler(订阅分发)。
 */
public final class FlvWsServer {

    private static final Logger log = LoggerFactory.getLogger(FlvWsServer.class);

    private final GatewayConfig config;
    private final StreamSessionManager manager;
    /** 跨域配置不可变，各连接共用；CorsHandler 有状态，需每连接新建。 */
    private final CorsConfig corsConfig;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public FlvWsServer(GatewayConfig config, StreamSessionManager manager) {
        this.config = config;
        this.manager = manager;
        this.corsConfig = buildCorsConfig(config);
    }

    /** 来源白名单取自 server.corsOrigins（"*" 为任意来源），预检结果缓存 corsMaxAgeSec 秒。 */
    private static CorsConfig buildCorsConfig(GatewayConfig config) {
        // 任意来源时额外放行 Origin: null（本地 file:// 页面）；白名单模式下只认列出的来源
        CorsConfigBuilder builder = config.corsAnyOrigin()
                ? CorsConfigBuilder.forAnyOrigin().allowNullOrigin()
                : CorsConfigBuilder.forOrigins(config.corsOrigins().toArray(new String[0]));
        builder.maxAge(config.corsMaxAgeSec())
                .allowedRequestMethods(HttpMethod.GET, HttpMethod.POST, HttpMethod.OPTIONS)
                .allowedRequestHeaders("Content-Type", "Authorization", "X-Requested-With");
        if (config.corsAllowCredentials()) {
            builder.allowCredentials();
        }
        return builder.build();
    }

    public void start() throws InterruptedException {
        bossGroup = new MultiThreadIoEventLoopGroup(1, new DefaultThreadFactory("ws-boss", true), NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(0, new DefaultThreadFactory("ws-worker", true), NioIoHandler.newFactory());
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(
                config.writeBufferLowKb() * 1024, config.writeBufferHighKb() * 1024);
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 256)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, waterMark)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                // 跨域：需在聚合前处理，预检 OPTIONS 由此直接应答；WebSocket 握手响应也会带上跨域头
                                .addLast(new CorsHandler(corsConfig))
                                .addLast(new HttpObjectAggregator(64 * 1024))
                                .addLast(new HttpControlHandler(manager, config))
                                // HTTP-FLV：/live 的普通 GET（无 Upgrade 头）在此接管，升级请求继续下传
                                .addLast(new HttpFlvHandler(manager, config))
                                // allowExtensions=false：视频流已压缩，禁用 permessage-deflate 省 CPU
                                .addLast(new WebSocketServerProtocolHandler("/live", null, false, 65536, false, true))
                                .addLast(new FlvWsHandler(manager, config));
                    }
                });
        if(config.ip().isEmpty()) {
            serverChannel = b.bind(config.port()).sync().channel();
        }else {
            serverChannel = b.bind(config.ip(), config.port()).sync().channel();
        }
        log.info("FLV WebSocket / HTTP-FLV gateway listening on port {}", config.port());
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}
