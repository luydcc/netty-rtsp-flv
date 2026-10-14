package com.mediagw.gb28181;

import com.mediagw.config.GatewayConfig;
import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GB28181 信令服务端：在 {@code gb28181.port} 上同时监听 SIP over UDP 与 TCP，
 * 接收设备注册（REGISTER）与心跳（MESSAGE/Keepalive）并打印到控制台，不做设备存储。
 *
 * <p>Pipeline（TCP）：SipTcpFramer -> Gb28181ServerHandler；UDP 直接进入 Handler。
 * 端口被占用等绑定失败只记日志，不影响 WebSocket / HTTP-FLV 服务。
 * {@code gb28181.port <= 0} 时不启动（关闭 GB28181 信令能力）。
 */
public final class Gb28181Server {

    private static final Logger log = LoggerFactory.getLogger(Gb28181Server.class);

    private final GatewayConfig config;
    private final Gb28181ServerHandler handler;
    private final Gb28181Service service;
    private EventLoopGroup group;
    private Channel udpChannel;
    private Channel tcpChannel;

    public Gb28181Server(GatewayConfig config) {
        this(config, null);
    }

    public Gb28181Server(GatewayConfig config, Gb28181Service service) {
        this.config = config;
        this.service = service;
        this.handler = new Gb28181ServerHandler(config, service);
    }

    /** SIP over UDP 监听通道（启动后供信令层复用作外发源）。 */
    public Channel udpChannel() {
        return udpChannel;
    }

    public void start() {
        int port = config.gb28181Port();
        if (port <= 0) {
            log.info("GB28181 disabled (gb28181.port={})", port);
            return;
        }
        // 信令量小，UDP/TCP 共用一个事件循环组；守护线程随 JVM 退出
        group = new MultiThreadIoEventLoopGroup(0,
                new DefaultThreadFactory("gb28181-sip", true), NioIoHandler.newFactory());
        try {
            Bootstrap udpBoot = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                    .handler(new ChannelInitializer<DatagramChannel>() {
                        @Override
                        protected void initChannel(DatagramChannel ch) {
                            ch.pipeline().addLast(handler);
                        }
                    });
            udpChannel = bind(udpBoot, port);

            ServerBootstrap tcpBoot = new ServerBootstrap()
                    .group(group, group)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new SipTcpFramer())
                                    .addLast(handler);
                        }
                    });
            tcpChannel = bind(tcpBoot, port);
            if (service != null) {
                service.attachSipUdp(udpChannel);
            }
            log.info("GB28181 SIP server listening on port {} udp+tcp (serverId={}, realm={}, auth={})",
                    port, config.gb28181ServerId(), handler.realm(),
                    config.gb28181Password().isEmpty() ? "off" : "digest");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("GB28181 server interrupted while binding port {}", port);
            stop();
        } catch (Exception e) {
            // 端口冲突等只影响 GB28181 信令，不影响 FLV 输出
            log.error("GB28181 server failed to bind port {}: {}", port, e.toString());
            stop();
        }
    }

    private <B extends AbstractBootstrap<B, ?>> Channel bind(B boot, int port) throws InterruptedException {
        return config.ip().isEmpty()
                ? boot.bind(port).sync().channel()
                : boot.bind(config.ip(), port).sync().channel();
    }

    public boolean isRunning() {
        return udpChannel != null || tcpChannel != null;
    }

    public void stop() {
        if (udpChannel != null) {
            udpChannel.close().awaitUninterruptibly();
            udpChannel = null;
        }
        if (tcpChannel != null) {
            tcpChannel.close().awaitUninterruptibly();
            tcpChannel = null;
        }
        if (group != null) {
            group.shutdownGracefully();
            group = null;
        }
    }
}
