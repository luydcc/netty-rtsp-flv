package com.mediagw.gb28181;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.ParamSets;
import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.config.GatewayConfig;
import com.mediagw.flv.FlvMuxer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * GB28181 单路点播的 FLV 分发会话：接收 {@link PsDepacketizer} 输出的访问单元，
 * 经 {@link FlvMuxer} 封装为 FLV Tag 后广播给订阅的播放连接（HTTP-FLV）。
 *
 * <p>与 RTSP 侧 StreamSession 的分工一致但实现独立（国标链路不依赖 RTSP 拉流管线）：
 * 维护当前 GOP 供新订阅者首屏加速；连续多帧仍不可写的慢消费者直接断开。
 *
 * <p>线程模型：媒体收流线程与播放连接线程均通过本实例锁互斥；
 * 对订阅 Channel 只用 writeAndFlush（线程安全），写序由各 Channel 的 EventLoop 保证。
 */
final class GbStreamSession implements VideoDepacketizer.Listener {

    private static final Logger log = LoggerFactory.getLogger(GbStreamSession.class);

    /** 慢消费者容忍的连续不可写帧数，达到即断开该播放连接。 */
    private static final int NOT_WRITABLE_STREAK = 3;

    private final String channelId;
    private final String deviceId;
    private final ByteBufAllocator alloc;
    private final long gopMaxBytes;

    private FlvMuxer muxer;                       // 首个完整参数集到达后创建
    private final List<Channel> actives = new ArrayList<>();
    private final Map<Channel, Integer> slowStreak = new HashMap<>();
    private final List<ByteBuf> gop = new ArrayList<>();
    private long gopBytes;
    private volatile boolean closed;
    private boolean firstAuLogged;
    private long errCount;
    private long lastErrLogAt;

    GbStreamSession(String channelId, String deviceId, GatewayConfig config, ByteBufAllocator alloc) {
        this.channelId = channelId;
        this.deviceId = deviceId;
        this.alloc = alloc;
        this.gopMaxBytes = config.gopCacheMaxBytes();
    }

    public String channelId() {
        return channelId;
    }

    public String deviceId() {
        return deviceId;
    }

    public boolean hasConfig() {
        return muxer != null && muxer.hasConfig();
    }

    // ===== 订阅管理 =====

    /** 新订阅者：先补发 FLV Header + 解码配置 + GOP 快照，再进入实时广播集合。 */
    synchronized void subscribe(Channel ch) {
        if (closed || actives.contains(ch)) {
            return;
        }
        ch.writeAndFlush(Unpooled.wrappedBuffer(FlvMuxer.FLV_HEADER));
        if (muxer != null && muxer.hasConfig()) {
            ByteBuf cfg = muxer.buildConfigTag();
            if (cfg != null) {
                ch.writeAndFlush(cfg);
            }
            for (ByteBuf tag : gop) {
                ch.writeAndFlush(tag.retainedDuplicate());
            }
        }
        actives.add(ch);
        log.info("[GB28181] 播放订阅: channel={}, client={}, viewers={}",
                channelId, ch.remoteAddress(), actives.size());
    }

    synchronized void unsubscribe(Channel ch) {
        if (actives.remove(ch)) {
            slowStreak.remove(ch);
            log.info("[GB28181] 播放断开: channel={}, client={}, viewers={}",
                    channelId, ch.remoteAddress(), actives.size());
        }
    }

    synchronized int viewerCount() {
        return actives.size();
    }

    // ===== PsDepacketizer 回调（收流 EventLoop 线程） =====

    @Override
    public synchronized void onParameterSets(ParamSets ps) {
        if (closed) {
            return;
        }
        if (muxer == null) {
            muxer = new FlvMuxer(ps.codec(), alloc);
        }
        if (muxer.updateConfig(ps)) {
            log.info("[GB28181] 解码配置就绪: channel={}, codec={}", channelId, ps.codec());
            // 参数集中途变化：向已连接播放者重发 sequence header
            broadcastConfig();
        }
    }

    @Override
    public synchronized void onAccessUnit(AccessUnit au) {
        if (closed || muxer == null) {
            return;
        }
        ByteBuf tag = muxer.mux(au);
        if (tag == null) {
            return;
        }
        if (!firstAuLogged) {
            firstAuLogged = true;
            log.info("[GB28181] 首帧产出: channel={}, key={}, codec={}", channelId, au.keyFrame(), muxer.codec());
        }
        if (au.keyFrame()) {
            clearGop();
        }
        if (gopBytes < gopMaxBytes) {
            gop.add(tag.retain());
            gopBytes += tag.readableBytes();
        }
        broadcast(tag);
        tag.release();
    }

    @Override
    public void onError(String message) {
        // 限频 warn：垃圾流持续报错时每 5s 最多一条，带累计计数便于定位收流解复用问题
        errCount++;
        long now = System.currentTimeMillis();
        if (now - lastErrLogAt >= 5000) {
            lastErrLogAt = now;
            log.warn("[GB28181] 收流解复用异常(累计{}次): channel={}, {}", errCount, channelId, message);
        }
    }

    // ===== 内部 =====

    private void broadcastConfig() {
        ByteBuf cfg = muxer.buildConfigTag();
        if (cfg == null) {
            return;
        }
        broadcast(cfg);
        cfg.release();
    }

    /** 广播一个 FLV Tag；剔除已断开连接，断开持续不可写的慢消费者。 */
    private void broadcast(ByteBuf tag) {
        for (Iterator<Channel> it = actives.iterator(); it.hasNext(); ) {
            Channel ch = it.next();
            if (!ch.isActive()) {
                it.remove();
                slowStreak.remove(ch);
                continue;
            }
            if (!ch.isWritable()) {
                int streak = slowStreak.merge(ch, 1, Integer::sum);
                if (streak >= NOT_WRITABLE_STREAK) {
                    log.warn("[GB28181] 播放端消费过慢，断开: channel={}, client={}", channelId, ch.remoteAddress());
                    it.remove();
                    slowStreak.remove(ch);
                    ch.close();
                }
                continue; // 不可写期间跳帧，恢复后从关键帧重新补
            }
            slowStreak.remove(ch);
            ch.writeAndFlush(tag.retainedDuplicate());
        }
    }

    private void clearGop() {
        for (ByteBuf b : gop) {
            b.release();
        }
        gop.clear();
        gopBytes = 0;
    }

    /** 会话终止：释放 GOP 缓存并断开所有播放连接。 */
    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        clearGop();
        for (Channel ch : actives) {
            ch.close();
        }
        actives.clear();
        slowStreak.clear();
    }
}
