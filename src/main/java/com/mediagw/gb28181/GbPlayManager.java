package com.mediagw.gb28181;

import com.mediagw.config.GatewayConfig;
import com.mediagw.rtp.RtpPacket;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * GB28181 点播编排：为通道分配 Netty UDP 收流端口、拼装收流 SDP、经
 * {@link GbSignaling} 发送 INVITE；设备推流到达后由 {@link PsDepacketizer}
 * 解复用、{@link GbStreamSession} 以 FLV 广播给播放连接。
 *
 * <p>生命周期：play → INVITE（等待 streamWaitSec 内首个 RTP 包）→ 持续分发
 * → 主动 stop / 设备 BYE / INVITE 失败 / 收流超时 → 释放端口并回收会话。
 * 最后一个观看者离开 closeDelaySec 秒后自动停流。
 */
final class GbPlayManager {

    private static final Logger log = LoggerFactory.getLogger(GbPlayManager.class);

    /** 一路播放：FLV 会话 + 收流 UDP 通道 + 各类定时器。 */
    private static final class Playback {
        final GbStreamSession session;
        final PsDepacketizer depacketizer;
        volatile Channel mediaChannel;                 // Netty 收流端口
        volatile GbSignaling.Dialog dialog;
        volatile ScheduledFuture<?> waitTimeout;       // 等流超时
        volatile ScheduledFuture<?> idleTimer;         // 无人观看停流
        volatile ScheduledFuture<?> diag;              // 周期诊断日志
        volatile long lastDiagAus;
        volatile boolean streamArrived;
        volatile int mediaPort;

        Playback(GbStreamSession session, PsDepacketizer depacketizer) {
            this.session = session;
            this.depacketizer = depacketizer;
        }
    }

    private final GatewayConfig config;
    private final GbChannelStore store;
    private final GbSignaling signaling;
    private final EventLoopGroup mediaGroup;
    private final Map<String, Playback> playbacks = new HashMap<>();
    private final Set<Integer> usedPorts = new HashSet<>();
    /** 播放状态变化通知（页面 SSE 刷新）。 */
    private volatile Runnable onPlayStateChanged;

    GbPlayManager(GatewayConfig config, GbChannelStore store, GbSignaling signaling) {
        this.config = config;
        this.store = store;
        this.signaling = signaling;
        this.mediaGroup = new MultiThreadIoEventLoopGroup(0,
                new DefaultThreadFactory("gb28181-media", true), NioIoHandler.newFactory());
    }

    void setOnPlayStateChanged(Runnable r) {
        this.onPlayStateChanged = r;
    }

    // ===== 点播 / 停流 =====

    /**
     * 发起点播。返回 null 表示 INVITE 已发出（流数据异步到达）；非空为失败原因。
     * 同一通道重复点播直接复用现有会话。
     */
    String play(String deviceId, String channelId) {
        GbChannelStore.Device device;
        synchronized (this) {
            Playback exist = playbacks.get(channelId);
            if (exist != null) {
                fireStateChanged();
                return null; // 已在点播中，复用
            }
            device = store.device(deviceId);
            if (device == null) {
                device = store.findByChannel(channelId);
            }
            if (device == null) {
                return "设备未注册: " + deviceId;
            }
            if (!device.online || !GbChannelStore.signalingAvailable(device)) {
                return "设备离线或信令连接不可用";
            }
        }
        int port = allocatePort();
        if (port < 0) {
            return "收流端口池已耗尽(" + config.gb28181MediaPortStart()
                    + "-" + config.gb28181MediaPortEnd() + ")";
        }
        GbStreamSession session = new GbStreamSession(channelId, device.deviceId, config, ByteBufAllocator.DEFAULT);
        PsDepacketizer psd = new PsDepacketizer(session, ByteBufAllocator.DEFAULT);
        Playback pb = new Playback(session, psd);

        String mediaIp = resolveMediaIp(device.address);
        String sdp = buildSdp(channelId, mediaIp, port, makeSsrc());
        try {
            pb.mediaChannel = bindMediaPort(port, pb, channelId);
        } catch (Exception e) {
            releasePort(port);
            session.close();
            return "收流端口绑定失败: " + e;
        }
        pb.mediaPort = port;
        log.info("[GB28181] 点播下发: channel={}, 收流端口={}:{}, 等待设备推流(设备需将 PS/RTP 发往此地址)",
                channelId, mediaIp, port);

        GbSignaling.Dialog dialog = signaling.newDialog(device, channelId);
        pb.dialog = dialog;
        synchronized (this) {
            playbacks.put(channelId, pb);
        }
        // INVITE 失败 / 设备 BYE：回收资源
        signaling.invite(device, channelId, sdp, dialog,
                () -> log.info("[GB28181] 点播成功(200+ACK): channel={}", channelId),
                reason -> {
                    log.warn("[GB28181] 点播失败: channel={}, reason={}", channelId, reason);
                    cleanup(channelId);
                },
                () -> {
                    log.info("[GB28181] 设备挂断停流: channel={}", channelId);
                    cleanup(channelId);
                });
        // 等待首个 RTP 包超时
        pb.waitTimeout = mediaGroup.next().schedule(() -> {
            if (!pb.streamArrived) {
                log.warn("[GB28181] 等待收流超时({}s): channel={}", config.gb28181StreamWaitSec(), channelId);
                signaling.bye(dialog);
                cleanup(channelId);
            }
        }, config.gb28181StreamWaitSec(), TimeUnit.SECONDS);
        // 周期诊断：出帧正常则静默；收流到达但 5s 无新帧（或根本没收到流）则 WARN 带各环节计数
        pb.diag = mediaGroup.next().scheduleAtFixedRate(() -> {
            long aus = psd.accessUnits();
            if (session.hasConfig() && aus != pb.lastDiagAus) {
                pb.lastDiagAus = aus;
                return;
            }
            log.warn("[GB28181] 点播诊断: channel={}, RTP已到达={}, rtp包数={}, 载荷={}B, "
                            + "PS滞留={}B, AU产出={}, codec={}, 观众={}",
                    channelId, pb.streamArrived, psd.rtpPacketCount(), psd.rtpByteCount(),
                    psd.bufferedBytes(), aus, psd.codec(), session.viewerCount());
        }, 5, 5, TimeUnit.SECONDS);
        fireStateChanged();
        return null;
    }

    /** 主动停流（页面停止按钮）。 */
    void stop(String channelId) {
        Playback pb;
        synchronized (this) {
            pb = playbacks.get(channelId);
        }
        if (pb != null) {
            signaling.bye(pb.dialog);
        }
        cleanup(channelId);
    }

    // ===== 订阅（供 GbFlvHandler） =====

    GbStreamSession sessionOf(String channelId) {
        synchronized (this) {
            Playback pb = playbacks.get(channelId);
            return pb == null ? null : pb.session;
        }
    }

    void subscribe(String channelId, Channel ch) {
        Playback pb;
        synchronized (this) {
            pb = playbacks.get(channelId);
        }
        if (pb == null) {
            ch.close();
            return;
        }
        synchronized (pb) {
            if (pb.idleTimer != null) {
                pb.idleTimer.cancel(false);
                pb.idleTimer = null;
            }
        }
        pb.session.subscribe(ch);
    }

    /** 播放连接断开：无人观看则延迟停流（BYE + 释放端口）。 */
    void unsubscribe(String channelId, Channel ch) {
        Playback pb;
        synchronized (this) {
            pb = playbacks.get(channelId);
        }
        if (pb == null) {
            return;
        }
        pb.session.unsubscribe(ch);
        if (pb.session.viewerCount() == 0) {
            synchronized (pb) {
                if (pb.idleTimer == null) {
                    pb.idleTimer = mediaGroup.next().schedule(() -> {
                        if (pb.session.viewerCount() == 0) {
                            log.info("[GB28181] 无人观看{}s，自动停流: channel={}",
                                    config.closeDelaySec(), channelId);
                            stop(channelId);
                        }
                    }, config.closeDelaySec(), TimeUnit.SECONDS);
                }
            }
        }
        fireStateChanged();
    }

    // ===== 内部 =====

    /** 释放会话：关收流端口、断播放连接、回 BYE（若仍在）。 */
    private void cleanup(String channelId) {
        Playback pb;
        synchronized (this) {
            pb = playbacks.remove(channelId);
        }
        if (pb == null) {
            return;
        }
        synchronized (pb) {
            if (pb.waitTimeout != null) {
                pb.waitTimeout.cancel(false);
            }
            if (pb.idleTimer != null) {
                pb.idleTimer.cancel(false);
            }
            if (pb.diag != null) {
                pb.diag.cancel(false);
            }
        }
        if (pb.mediaChannel != null) {
            pb.mediaChannel.close();
            releasePort(pb.mediaPort);
        }
        pb.depacketizer.close();
        pb.session.close();
        fireStateChanged();
    }

    /** 在端口池范围内抢一个空闲收流端口。 */
    private int allocatePort() {
        synchronized (usedPorts) {
            for (int p = config.gb28181MediaPortStart(); p <= config.gb28181MediaPortEnd(); p++) {
                if (!usedPorts.contains(p)) {
                    usedPorts.add(p);
                    return p;
                }
            }
            return -1;
        }
    }

    private void releasePort(int port) {
        if (port > 0) {
            synchronized (usedPorts) {
                usedPorts.remove(port);
            }
        }
    }

    /** Netty UDP 收流服务：绑定端口，RTP → PS 解复用 → FLV。 */
    private Channel bindMediaPort(int port, Playback pb, String channelId) throws InterruptedException {
        PsDepacketizer psd = pb.depacketizer;
        Bootstrap b = new Bootstrap()
                .group(mediaGroup)
                .channel(NioDatagramChannel.class)
                .option(ChannelOption.SO_RCVBUF, 4 * 1024 * 1024)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<DatagramPacket>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                                RtpPacket rtp = RtpPacket.parse(packet.content());
                                if (rtp == null) {
                                    return;
                                }
                                if (!pb.streamArrived) {
                                    pb.streamArrived = true;
                                    log.info("[GB28181] 收流到达: channel={}, from={}, port={}",
                                            channelId, packet.sender(), port);
                                }
                                if (rtp.payload().readableBytes() < 12) {
                                    return; // 载荷过短，不可能是有效 PS 分片
                                }
                                psd.onRtpPacket(rtp);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                log.warn("[GB28181] 收流异常: channel={}, {}", channelId, cause.toString());
                            }
                        });
                    }
                });
        // 通配绑定：设备按 SDP c= 行地址推流（可能是回环/任意网卡），Wildcard 确保都能收到
        return b.bind(port).sync().channel();
    }

    /** SDP（GB28181 实时点播）：recvonly + PS/90000 + y= 行 SSRC，首位 0 表示实时流。 */
    private static String buildSdp(String channelId, String mediaIp, int port, String ssrc) {
        return "v=0\r\n"
                + "o=" + channelId + " 0 0 IN IP4 " + mediaIp + "\r\n"
                + "s=Play\r\n"
                + "c=IN IP4 " + mediaIp + "\r\n"
                + "t=0 0\r\n"
                + "m=video " + port + " RTP/AVP 96\r\n"
                + "a=recvonly\r\n"
                + "a=rtpmap:96 PS/90000\r\n"
                + "y=" + ssrc + "\r\n";
    }

    private static String makeSsrc() {
        return "0" + String.format("%09d", ThreadLocalRandom.current().nextInt(1_000_000_000));
    }

    /** 收流对外 IP：配置为空时按设备地址推断本机出口 IP（connect 不发包）。 */
    private String resolveMediaIp(InetSocketAddress deviceAddr) {
        String cfg = config.gb28181MediaIp();
        if (!cfg.isEmpty()) {
            return cfg;
        }
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(deviceAddr);
            return socket.getLocalAddress().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }

    private void fireStateChanged() {
        Runnable r = onPlayStateChanged;
        if (r != null) {
            r.run();
        }
    }

    /** 服务停止：全部停流并关闭收流事件循环。 */
    void shutdown() {
        Set<String> ids;
        synchronized (this) {
            ids = new HashSet<>(playbacks.keySet());
        }
        for (String id : ids) {
            stop(id);
        }
        mediaGroup.shutdownGracefully();
    }
}
