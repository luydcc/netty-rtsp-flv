package com.mediagw.rtsp;

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
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * RTSP 转发服务端：把网关已拉取的流以 RTSP/RTP 形式对外提供，
 * 播放地址形如 {@code rtsp://host:{@literal <rtsp.forwardPort>}/live/{streamId}}。
 *
 * <p>Pipeline：IdleStateHandler(保活超时) -> RtspServerDecoder -> RtspForwardHandler。
 * UDP 传输所需的 DatagramChannel 复用 workerGroup。
 * 端口被占用等绑定失败只记日志，不影响 WebSocket / HTTP-FLV 服务。
 */
public final class RtspForwardServer {

    private static final Logger log = LoggerFactory.getLogger(RtspForwardServer.class);

    private final GatewayConfig config;
    private final StreamSessionManager manager;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public RtspForwardServer(GatewayConfig config, StreamSessionManager manager) {
        this.config = config;
        this.manager = manager;
    }

    /** {@code rtsp.forwardPort <= 0} 时不启动（关闭 RTSP 转发能力）。 */
    public void start() {
        int port = config.rtspForwardPort();
        if (port <= 0) {
            log.info("RTSP forward disabled (rtsp.forwardPort={})", port);
            return;
        }
        bossGroup = new MultiThreadIoEventLoopGroup(1,
                new DefaultThreadFactory("rtsp-fwd-boss", true), NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(0,
                new DefaultThreadFactory("rtsp-fwd-worker", true), NioIoHandler.newFactory());
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(
                config.writeBufferLowKb() * 1024, config.writeBufferHighKb() * 1024);
        int idleSec = Math.max(0, config.rtspForwardSessionTimeoutSec());
        ServerBootstrap b = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, waterMark)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new IdleStateHandler(idleSec, 0, 0, TimeUnit.SECONDS))
                                .addLast(new RtspServerDecoder())
                                .addLast(new RtspForwardHandler(manager, config, workerGroup));
                    }
                });
        try {
            serverChannel = (config.ip().isEmpty() ? b.bind(port) : b.bind(config.ip(), port)).sync().channel();
            log.info("RTSP forward server listening on port {} (udp={}, maxPayload={})",
                    port, config.rtspForwardUdp(), config.rtspForwardMaxPayload());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("RTSP forward server interrupted while binding port {}", port);
        } catch (Exception e) {
            // 端口冲突等不影响 FLV 输出，记录后关闭本服务端的线程资源
            log.error("RTSP forward server failed to bind port {}: {}", port, e.toString());
            stop();
        }
    }

    public boolean isRunning() {
        return serverChannel != null;
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
    }
}
