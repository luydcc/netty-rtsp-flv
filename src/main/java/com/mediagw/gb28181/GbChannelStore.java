package com.mediagw.gb28181;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * GB28181 设备/通道内存注册表：由信令层（REGISTER / Keepalive / Catalog / Notify）驱动更新，
 * 供通道页面查询。进程重启即清空，不做持久化。
 *
 * <p>线程模型：信令 EventLoop 线程写入，HTTP 线程读取，方法以 store 实例锁互斥；
 * 变更后回调 {@link ChangeListener}（页面 SSE 推送依赖该通知）。
 */
public final class GbChannelStore {

    /** 通道信息（目录 Item 的最小字段集）。 */
    public record ChannelInfo(String channelId, String name, String manufacturer,
                              String model, String status, String parentId) {
    }

    /** 内存中的设备：注册状态 + 所属通道。字段仅在本类锁内变更，快照读取返回拷贝。 */
    public static final class Device {
        public final String deviceId;
        public volatile InetSocketAddress address;   // 最近一次信令来源地址（含 NAT 映射后的出口）
        public volatile boolean udp = true;          // 最近一次注册使用的传输
        public volatile Channel tcpChannel;          // TCP 注册时的长连接（UDP 为 null）
        public volatile boolean online;
        public volatile long registeredAt;
        public volatile long lastSeen;               // 最近注册刷新/心跳时间（毫秒）
        public volatile int expiresSec;
        public final Map<String, ChannelInfo> channels = new LinkedHashMap<>();

        Device(String deviceId) {
            this.deviceId = deviceId;
        }
    }

    /** 存储内容变更事件（type 供前端区分刷新粒度）。 */
    public static final String EV_REGISTER = "register";
    public static final String EV_UNREGISTER = "unregister";
    public static final String EV_CHANNELS = "channels";
    public static final String EV_ONLINE = "online";
    public static final String EV_OFFLINE = "offline";

    private final Map<String, Device> devices = new LinkedHashMap<>();
    private final List<ChangeListener> listeners = new CopyOnWriteArrayList<>();

    public interface ChangeListener {
        /** 存储发生变化，event 为 EV_* 常量，回调线程不限，实现需自行保证线程安全。 */
        void onStoreChange(String event, String deviceId);
    }

    public void addChangeListener(ChangeListener l) {
        listeners.add(l);
    }

    // ===== 信令层写入 =====

    /** REGISTER 200 后登记/刷新设备。 */
    public synchronized void onRegister(String deviceId, InetSocketAddress from, boolean udp,
                                        int expires, Channel signalingChannel) {
        Device d = devices.computeIfAbsent(deviceId, Device::new);
        d.address = from;
        d.udp = udp;
        d.tcpChannel = udp ? null : signalingChannel;
        d.online = true;
        long now = System.currentTimeMillis();
        if (d.registeredAt == 0) {
            d.registeredAt = now;
        }
        d.lastSeen = now;
        d.expiresSec = expires;
        fire(expires > 0 ? EV_REGISTER : EV_UNREGISTER, deviceId);
    }

    /** 心跳/信令触达：刷新活跃时间，静默恢复在线（不逐个心跳发事件，避免刷屏 SSE）。 */
    public synchronized void touch(String deviceId, InetSocketAddress from, boolean udp) {
        Device d = devices.get(deviceId);
        if (d == null) {
            return;
        }
        boolean wasOffline = !d.online;
        d.lastSeen = System.currentTimeMillis();
        if (from != null) {
            d.address = from;
            d.udp = udp;
        }
        d.online = true;
        if (wasOffline) {
            fire(EV_ONLINE, deviceId);
        }
    }

    /** Expires=0 注销或平台主动剔除。 */
    public synchronized void onUnregister(String deviceId) {
        Device d = devices.remove(deviceId);
        if (d != null) {
            fire(EV_UNREGISTER, deviceId);
        }
    }

    /** 目录查询响应整体替换通道列表（GB28181 目录以最后一次查询为准）。 */
    public synchronized void replaceChannels(String deviceId, List<ChannelInfo> channels) {
        Device d = devices.computeIfAbsent(deviceId, Device::new);
        d.channels.clear();
        for (ChannelInfo c : channels) {
            d.channels.put(c.channelId(), c);
        }
        fire(EV_CHANNELS, deviceId);
    }

    /** 通道变更通知（Notify/Alert 单条更新），新增或覆盖。 */
    public synchronized void upsertChannel(String deviceId, ChannelInfo channel) {
        Device d = devices.computeIfAbsent(deviceId, Device::new);
        d.channels.put(channel.channelId(), channel);
        fire(EV_CHANNELS, deviceId);
    }

    /** 心跳超时 / TCP 连接断开：标记离线（保留通道数据便于页面查看）。 */
    public synchronized void markOffline(String deviceId) {
        Device d = devices.get(deviceId);
        if (d == null || !d.online) {
            return;
        }
        d.online = false;
        d.tcpChannel = null;
        fire(EV_OFFLINE, deviceId);
    }

    /** TCP 信令连接断开时按连接反查设备置离线。 */
    public synchronized void offlineByChannel(Channel channel) {
        for (Device d : devices.values()) {
            if (d.tcpChannel == channel) {
                markOffline(d.deviceId);
                return;
            }
        }
    }

    // ===== 读取 =====

    public synchronized Device device(String deviceId) {
        return devices.get(deviceId);
    }

    /** 设备快照拷贝（浅拷贝仅取标量字段，通道另取）。 */
    public synchronized List<Device> snapshot() {
        return new ArrayList<>(devices.values());
    }

    /** 按通道编码全局查找所属设备（页面播放请求只带 channelId 时使用）。 */
    public synchronized Device findByChannel(String channelId) {
        for (Device d : devices.values()) {
            if (d.channels.containsKey(channelId)) {
                return d;
            }
        }
        return null;
    }

    /** 信令发送通道是否仍然可用（TCP 长连接断开后为 false）。 */
    public static boolean signalingAvailable(Device d) {
        if (d.tcpChannel != null && !d.tcpChannel.isActive()) {
            return false;
        }
        return d.udp || d.tcpChannel != null;
    }

    private void fire(String event, String deviceId) {
        for (ChangeListener l : listeners) {
            try {
                l.onStoreChange(event, deviceId);
            } catch (RuntimeException ignored) {
                // 监听器异常不影响存储主流程
            }
        }
    }
}
