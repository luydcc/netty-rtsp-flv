package com.mediagw.gb28181;

import com.mediagw.config.GatewayConfig;
import com.mediagw.util.JsonBuilder;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * GB28181 对外门面：聚合内存通道存储、点播信令与收流管理，向 HTTP 层提供
 * 设备/通道 JSON 查询、点播/停流入口与 SSE 变更推送。
 *
 * <p>离线巡检：周期检查设备心跳与注册有效期，超时设备标记离线；
 * 通道播放的 FLV 地址为 {@code /gb/live/<channelId>.flv}。
 */
public final class Gb28181Service {

    private static final Logger log = LoggerFactory.getLogger(Gb28181Service.class);
    /** 离线巡检周期（秒）。 */
    private static final int SWEEP_INTERVAL_SEC = 30;

    private final GatewayConfig config;
    private final GbChannelStore store = new GbChannelStore();
    private final GbSignaling signaling;
    private final GbPlayManager playManager;
    /** SSE 长连接（text/event-stream），存储/播放状态变化时推送刷新事件。 */
    private final List<Channel> sseClients = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService sweeper =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "gb28181-sweeper");
                t.setDaemon(true);
                return t;
            });

    public Gb28181Service(GatewayConfig config) {
        this.config = config;
        String realm = config.gb28181Realm();
        String serverId = config.gb28181ServerId();
        if (realm.isEmpty()) {
            realm = serverId.length() >= 10 ? serverId.substring(0, 10) : serverId;
        }
        this.signaling = new GbSignaling(serverId, realm);
        this.playManager = new GbPlayManager(config, store, signaling);
        store.addChangeListener((event, deviceId) -> pushEvent(event, deviceId));
        playManager.setOnPlayStateChanged(() -> pushEvent("play", ""));
        sweeper.scheduleWithFixedDelay(this::sweep, SWEEP_INTERVAL_SEC, SWEEP_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    public GbChannelStore store() {
        return store;
    }

    public GbSignaling signaling() {
        return signaling;
    }

    /** SIP 服务端 UDP 通道（信令外发复用）。 */
    public void attachSipUdp(Channel udpChannel) {
        signaling.attachSipUdp(udpChannel);
    }

    // ===== 点播 API =====

    /** 发起点播，返回 null 成功（流异步到达）/ 非空为失败原因。 */
    public String play(String deviceId, String channelId) {
        return playManager.play(deviceId, channelId);
    }

    public void stop(String channelId) {
        playManager.stop(channelId);
    }

    public GbStreamSession findSession(String channelId) {
        return playManager.sessionOf(channelId);
    }

    public void subscribe(String channelId, Channel ch) {
        playManager.subscribe(channelId, ch);
    }

    public void unsubscribe(String channelId, Channel ch) {
        playManager.unsubscribe(channelId, ch);
    }

    // ===== JSON 输出 =====

    /** 设备 + 通道 + 播放状态全量 JSON（页面数据源）。 */
    public String devicesJson() {
        StringBuilder sb = new StringBuilder(512);
        long now = System.currentTimeMillis();
        sb.append('[');
        for (GbChannelStore.Device d : store.snapshot()) {
            sb.append("{\"deviceId\":").append(JsonBuilder.str(d.deviceId))
                    .append(",\"online\":").append(d.online)
                    .append(",\"transport\":").append(JsonBuilder.str(d.udp ? "UDP" : "TCP"))
                    .append(",\"address\":").append(JsonBuilder.str(d.address == null ? "" : d.address.toString()))
                    .append(",\"expiresSec\":").append(d.expiresSec)
                    .append(",\"lastSeenAgoSec\":").append(d.lastSeen == 0 ? -1 : (now - d.lastSeen) / 1000)
                    .append(",\"channels\":[");
            boolean first = true;
            for (GbChannelStore.ChannelInfo c : List.copyOf(d.channels.values())) {
                GbStreamSession session = findSession(c.channelId());
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append("{\"channelId\":").append(JsonBuilder.str(c.channelId()))
                        .append(",\"name\":").append(JsonBuilder.str(c.name()))
                        .append(",\"manufacturer\":").append(JsonBuilder.str(c.manufacturer()))
                        .append(",\"model\":").append(JsonBuilder.str(c.model()))
                        .append(",\"status\":").append(JsonBuilder.str(c.status()))
                        .append(",\"parentId\":").append(JsonBuilder.str(c.parentId()))
                        .append(",\"deviceOnline\":").append(d.online)
                        .append(",\"playing\":").append(session != null)
                        .append(",\"viewers\":").append(session == null ? 0 : session.viewerCount())
                        .append(",\"flvUrl\":").append(JsonBuilder.str("/gb/live/" + c.channelId() + ".flv"))
                        .append('}');
            }
            sb.append("]}");
        }
        return sb.append(']').toString();
    }

    // ===== SSE 变更推送 =====

    public void addSseClient(Channel ch) {
        sseClients.add(ch);
        push("connected", "");
    }

    public void removeSseClient(Channel ch) {
        sseClients.remove(ch);
    }

    private void pushEvent(String event, String deviceId) {
        // 收流/信令线程回调，量大时仅靠 SSE 客户端数量限制即可，无需额外节流
        sweeper.execute(() -> push(event, deviceId));
    }

    private void push(String event, String deviceId) {
        if (sseClients.isEmpty()) {
            return;
        }
        String data = "data: {\"event\":" + JsonBuilder.str(event)
                + ",\"deviceId\":" + JsonBuilder.str(deviceId) + "}\n\n";
        for (Channel ch : sseClients) {
            if (!ch.isActive()) {
                sseClients.remove(ch);
                continue;
            }
            ch.writeAndFlush(new DefaultHttpContent(
                    Unpooled.copiedBuffer(data, StandardCharsets.UTF_8)));
        }
    }

    // ===== 离线巡检 =====

    private void sweep() {
        try {
            long now = System.currentTimeMillis();
            long timeoutMs = config.gb28181KeepaliveTimeoutSec() * 1000L;
            for (GbChannelStore.Device d : store.snapshot()) {
                if (d.online && d.lastSeen > 0 && now - d.lastSeen > timeoutMs) {
                    log.info("[GB28181] 设备{}s无心跳，标记离线: id={}",
                            config.gb28181KeepaliveTimeoutSec(), d.deviceId);
                    store.markOffline(d.deviceId);
                }
            }
        } catch (RuntimeException e) {
            log.warn("[GB28181] 离线巡检异常", e);
        }
    }

    public void shutdown() {
        playManager.shutdown();
        sweeper.shutdownNow();
        for (Channel ch : sseClients) {
            ch.close();
        }
        sseClients.clear();
    }
}
