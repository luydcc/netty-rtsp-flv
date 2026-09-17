package com.mediagw.session;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.H264Depacketizer;
import com.mediagw.codec.H265Depacketizer;
import com.mediagw.codec.ParamSets;
import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.config.GatewayConfig;
import com.mediagw.flv.FlvMuxer;
import com.mediagw.rtsp.RtspClient;
import com.mediagw.rtsp.SdpInfo;
import com.mediagw.util.JsonBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单条 RTSP 流会话：RTSP 拉流生命周期、FLV 封装、GOP 缓存与多订阅者广播。
 *
 * 状态机：IDLE -> STARTING -> PLAYING -> STOPPING -> CLOSED（可由 STOPPING 回到 PLAYING）。
 *
 * 锁约定：状态、订阅者列表、引用计数、GOP 缓存、延迟关闭任务的变更全部在
 * synchronized(this) 临界区内完成；广播临界区内只做非阻塞的 write 入队。
 * 新订阅者首屏顺序：BOOTSTRAPPING 期间实时帧进入其 pending 队列，快照发送完毕后
 * 在同一临界区内 drain 并切换到 ACTIVE，保证与实时广播不乱序。
 *
 * 输出分两类，共享同一路拉流与会话生命周期：
 * FLV 订阅者（WebSocket / HTTP-FLV）收 FLV Tag；原始帧订阅者（RTSP 转发）收访问单元。
 */
public class StreamSession implements VideoDepacketizer.Listener {

    private static final Logger log = LoggerFactory.getLogger(StreamSession.class);

    static final WebSocketCloseStatus CLOSE_STREAM_ERROR = new WebSocketCloseStatus(4500, "stream unavailable");
    private static final WebSocketCloseStatus CLOSE_SHUTDOWN = new WebSocketCloseStatus(1001, "server shutting down");
    /** 关键帧连续 N 次遇到 Channel 不可写则判定为慢消费者并断开。 */
    private static final int MAX_KEYFRAME_NOT_WRITABLE_STREAK = 3;

    public enum State { IDLE, STARTING, PLAYING, STOPPING, CLOSED }

    private final String streamId;
    private final String rtspUrl;
    private final GatewayConfig config;
    private final StreamSessionManager manager;
    private final GopCache gopCache;
    /** 原始帧（访问单元）GOP 缓存，供 RTSP 转发订阅者首屏加速。 */
    private final AuGopCache auGopCache;
    /** 未开启 RTSP 转发时无需缓存访问单元，避免白占内存。 */
    private final boolean keepAuGopCache;

    private volatile State state = State.IDLE;
    private CompletableFuture<Void> startFuture;      // guarded by this
    private ScheduledFuture<?> pendingCloseFuture;    // guarded by this
    private RtspClient rtspClient;                    // guarded by this
    private volatile FlvMuxer muxer;
    private volatile VideoDepacketizer depacketizer;
    /** 最新参数集，用于 RTSP 转发的 SDP 生成与带内参数集插入。 */
    private volatile ParamSets currentParams;         // guarded by this

    private final List<Subscriber> subscribers = new ArrayList<>(); // guarded by this
    private final List<RawSubscription> rawSubscribers = new ArrayList<>(); // guarded by this
    private int subscriberCount;                                    // guarded by this

    private int reconnectAttempts;      // guarded by this
    private boolean reconnectScheduled; // guarded by this

    // ===== 监控指标 =====
    private final AtomicLong framesIn = new AtomicLong();
    private final AtomicLong bytesOut = new AtomicLong();
    private final AtomicLong droppedFrames = new AtomicLong();
    private final AtomicLong kickedSlow = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private final AtomicLong paramUpdates = new AtomicLong();
    private final AtomicLong depacketizeErrors = new AtomicLong();
    private final AtomicLong bootstraps = new AtomicLong();
    private final AtomicLong bootstrapTotalMs = new AtomicLong();
    private final long createdAtMs = System.currentTimeMillis();
    private volatile long playingSinceMs = -1;
    private volatile long lastFrameAtMs = -1;

    StreamSession(String streamId, String rtspUrl, GatewayConfig config, StreamSessionManager manager) {
        this.streamId = streamId;
        this.rtspUrl = rtspUrl;
        this.config = config;
        this.manager = manager;
        this.gopCache = new GopCache(config.gopCacheMaxBytes());
        this.auGopCache = new AuGopCache(config.gopCacheMaxBytes());
        this.keepAuGopCache = config.rtspForwardPort() > 0;
    }

    public String streamId() {
        return streamId;
    }

    public State state() {
        return state;
    }

    public boolean isClosed() {
        return state == State.CLOSED;
    }

    // ===== 订阅 / 退订 =====

    /**
     * 订阅本会话（FLV 输出）。首个订阅者触发唯一一次 RTSP 启动（startFuture 共享），
     * 后续订阅者等待同一 future 或直接首屏加速。
     *
     * @param transport 下行封装方式：WebSocket 二进制帧或 HTTP-FLV 分块
     * @return false 表示会话已关闭，调用方应以新会话重试
     */
    boolean subscribe(Channel ws, FlvTransport transport) {
        CompletableFuture<Void> startF;
        boolean starter;
        int total;
        Subscriber sub = new Subscriber(ws, config.subscriberMaxPendingBytes(), transport);
        synchronized (this) {
            if (state == State.CLOSED) {
                return false;
            }
            starter = joinLocked();
            subscribers.add(sub);
            subscriberCount++;
            total = totalSubscribersLocked();
            startF = startFutureLocked();
        }
        log.info("[{}] {} subscriber +1 ({}) total={} state={}", streamId, transport, sub.id(), total, state);

        if (starter) {
            startRtsp();
        }
        final CompletableFuture<Void> f = startF;
        f.whenComplete((v, e) -> ws.eventLoop().execute(() -> {
            if (e != null) {
                removeSubscriber(sub);
                closeStream(ws, transport, CLOSE_STREAM_ERROR);
            } else if (ws.isActive()) {
                bootstrap(sub);
            } else {
                removeSubscriber(sub);
            }
        }));
        return true;
    }

    /**
     * 调用者持有锁：取消延迟关闭、STOPPING 复用回 PLAYING，首个订阅者时切到 STARTING。
     *
     * @return true 表示本次调用方负责启动 RTSP
     */
    private boolean joinLocked() {
        if (pendingCloseFuture != null) {
            pendingCloseFuture.cancel(false);
            pendingCloseFuture = null;
        }
        if (state == State.STOPPING) {
            state = State.PLAYING;
            log.info("[{}] pending close cancelled, reusing RTSP stream", streamId);
        }
        if (state == State.IDLE) {
            state = State.STARTING;
            startFuture = new CompletableFuture<>();
            return true;
        }
        return false;
    }

    /** 调用者持有锁：本次订阅需等待的启动 future（已在拉流时为已完成 future）。 */
    private CompletableFuture<Void> startFutureLocked() {
        return state == State.STARTING ? startFuture : CompletableFuture.completedFuture(null);
    }

    /** 调用者持有锁：FLV 与原始帧订阅者总数。 */
    private int totalSubscribersLocked() {
        return subscriberCount + rawSubscribers.size();
    }

    void unsubscribe(Channel ws) {
        Subscriber found = null;
        synchronized (this) {
            for (Subscriber s : subscribers) {
                if (s.channel() == ws) {
                    found = s;
                    break;
                }
            }
        }
        if (found != null) {
            removeSubscriber(found);
        }
    }

    private void removeSubscriber(Subscriber sub) {
        boolean last = false;
        boolean closeScheduled = false;
        synchronized (this) {
            if (!sub.markRemoved()) {
                return;
            }
            subscribers.remove(sub);
            subscriberCount--;
            last = totalSubscribersLocked() == 0;
            if (last) {
                closeScheduled = onLastSubscriberLeftLocked();
            }
        }
        if (last) {
            logLastSubscriberLeft(closeScheduled);
        }
    }

    private void logLastSubscriberLeft(boolean closeScheduled) {
        if (closeScheduled) {
            log.info("[{}] last subscriber left, closing RTSP in {}s", streamId, config.closeDelaySec());
        } else {
            log.info("[{}] last subscriber left while {}, deferring idle close", streamId, state);
        }
    }

    // ===== 原始帧订阅（RTSP 转发等非 FLV 输出） =====

    /**
     * 订阅原始访问单元。与 FLV 订阅者共享同一路拉流：首个订阅者触发 RTSP 启动；
     * 参数集就绪后回调 {@link RawSubscriber#onStreamInfo}，启动失败回调
     * {@link RawSubscriber#onStreamClosed}。
     *
     * @return false 表示会话已关闭，调用方应以新会话重试
     */
    boolean subscribeRaw(RawSubscriber sub) {
        CompletableFuture<Void> startF;
        boolean starter;
        int total;
        RawSubscription rs = new RawSubscription(sub);
        synchronized (this) {
            if (state == State.CLOSED) {
                return false;
            }
            starter = joinLocked();
            rawSubscribers.add(rs);
            total = totalSubscribersLocked();
            startF = startFutureLocked();
        }
        log.info("[{}] raw subscriber +1 ({}) total={} state={}", streamId, sub.id(), total, state);

        if (starter) {
            startRtsp();
        }
        final CompletableFuture<Void> f = startF;
        f.whenComplete((v, e) -> {
            if (e != null) {
                removeRaw(rs);
                notifyStreamClosed(sub);
            } else {
                notifyStreamInfo(rs);
            }
        });
        return true;
    }

    /** 开始播放：会话锁内回调 GOP 快照并切到实时分发，保证与后续实时帧不乱序。 */
    void startRawPlayback(RawSubscriber sub) {
        synchronized (this) {
            RawSubscription rs = findRawLocked(sub);
            if (rs == null || rs.removed || state == State.CLOSED) {
                return;
            }
            rs.playing = true;
            List<AccessUnit> snapshot = new ArrayList<>(auGopCache.size());
            auGopCache.snapshotTo(snapshot);
            try {
                sub.onGopSnapshot(snapshot);
            } catch (RuntimeException e) {
                log.warn("[{}] raw subscriber {} gop snapshot failed: {}", streamId, sub.id(), e.toString());
            }
        }
    }

    /** 暂停播放：停止实时分发但保留订阅（再次 PLAY 时重新下发 GOP 快照）。 */
    void stopRawPlayback(RawSubscriber sub) {
        synchronized (this) {
            RawSubscription rs = findRawLocked(sub);
            if (rs != null) {
                rs.playing = false;
            }
        }
    }

    void unsubscribeRaw(RawSubscriber sub) {
        RawSubscription found;
        synchronized (this) {
            found = findRawLocked(sub);
        }
        if (found != null) {
            removeRaw(found);
        }
    }

    private void removeRaw(RawSubscription rs) {
        boolean last = false;
        boolean closeScheduled = false;
        synchronized (this) {
            if (rs.removed) {
                return;
            }
            rs.removed = true;
            rs.playing = false;
            rawSubscribers.remove(rs);
            last = totalSubscribersLocked() == 0;
            if (last) {
                closeScheduled = onLastSubscriberLeftLocked();
            }
        }
        if (last) {
            logLastSubscriberLeft(closeScheduled);
        }
    }

    /** 参数集就绪后通知原始帧订阅者；尚未就绪时留待 onParameterSets 触发。 */
    private void notifyStreamInfo(RawSubscription rs) {
        CodecType codec;
        ParamSets ps;
        synchronized (this) {
            if (rs.removed || state == State.CLOSED) {
                return;
            }
            FlvMuxer m = muxer;
            ps = currentParams;
            if (m == null || ps == null || !ps.isComplete()) {
                return;
            }
            rs.readyNotified = true;
            codec = m.codec();
        }
        deliverStreamInfo(rs, codec, ps);
    }

    private void deliverStreamInfo(RawSubscription rs, CodecType codec, ParamSets ps) {
        try {
            rs.sub.onStreamInfo(codec, ps);
        } catch (RuntimeException e) {
            log.warn("[{}] raw subscriber {} onStreamInfo failed: {}", streamId, rs.sub.id(), e.toString());
        }
    }

    private void notifyStreamClosed(RawSubscriber sub) {
        try {
            sub.onStreamClosed();
        } catch (RuntimeException e) {
            log.warn("[{}] raw subscriber {} onStreamClosed failed: {}", streamId, sub.id(), e.toString());
        }
    }

    /** 调用者持有锁。 */
    private RawSubscription findRawLocked(RawSubscriber sub) {
        for (RawSubscription rs : rawSubscribers) {
            if (rs.sub == sub) {
                return rs;
            }
        }
        return null;
    }

    /** 原始帧订阅的会话内状态。 */
    private static final class RawSubscription {
        final RawSubscriber sub;
        boolean readyNotified;  // 已回调过 onStreamInfo
        boolean playing;        // 实时分发中
        boolean removed;

        RawSubscription(RawSubscriber sub) {
            this.sub = sub;
        }
    }

    /**
     * 调用者持有锁。
     *
     * @return true 表示已启动延迟关闭定时器
     */
    private boolean onLastSubscriberLeftLocked() {
        if (state == State.PLAYING) {
            state = State.STOPPING;
            pendingCloseFuture = manager.scheduler().schedule(
                    this::delayedClose, config.closeDelaySec(), TimeUnit.SECONDS);
            return true;
        }
        // STARTING：由 onPlaying/onStartupFailure 处理；STOPPING：定时器已在运行
        return false;
    }

    private void delayedClose() {
        synchronized (this) {
            pendingCloseFuture = null;
            if (totalSubscribersLocked() == 0 && state == State.STOPPING) {
                state = State.CLOSED;
                closeRtspLocked();
                cleanupMediaLocked();
                manager.removeSession(streamId, this);
                log.info("[{}] session closed after {}s without subscribers", streamId, config.closeDelaySec());
            }
        }
    }

    /** 新订阅者首屏：FLV Header + 最新配置帧 + 最近 GOP，然后 drain pending 转 ACTIVE。 */
    private void bootstrap(Subscriber sub) {
        List<ByteBuf> snapshot = new ArrayList<>();
        boolean ok;
        synchronized (this) {
            ok = state != State.CLOSED;
            if (ok) {
                snapshot.add(Unpooled.wrappedBuffer(FlvMuxer.FLV_HEADER));
                FlvMuxer m = muxer;
                if (m != null) {
                    ByteBuf cfg = m.buildConfigTag();
                    if (cfg != null) {
                        snapshot.add(cfg);
                    }
                    gopCache.snapshotTo(snapshot);
                }
            }
        }
        Channel ch = sub.channel();
        if (!ok) {
            for (ByteBuf b : snapshot) {
                b.release();
            }
            removeSubscriber(sub);
            closeStream(ch, sub.transport(), CLOSE_STREAM_ERROR);
            return;
        }
        for (ByteBuf b : snapshot) {
            sub.write(b); // 所有权移交 pipeline，写失败由 Netty 释放
        }
        List<ByteBuf> pending;
        synchronized (this) {
            pending = sub.activateAndDrain();
        }
        for (ByteBuf b : pending) {
            sub.write(b);
        }
        ch.flush();
        sub.markBootstrapped();
        bootstraps.incrementAndGet();
        bootstrapTotalMs.addAndGet(sub.bootstrappedAtMs() - sub.subscribedAtMs());
    }

    // ===== RTSP 生命周期 =====

    private void startRtsp() {
        RtspListener listener = new RtspListener();
        RtspClient client;
        try {
            client = new RtspClient(rtspUrl, config, manager.rtspGroup(), listener);
        } catch (RuntimeException e) {
            listener.onStartupFailure(e);
            return;
        }
        synchronized (this) {
            if (state == State.CLOSED) {
                client.stop();
                return;
            }
            rtspClient = client;
        }
        log.info("[{}] starting RTSP: {}", streamId, GatewayConfig.sanitizeUrl(rtspUrl));
        client.start();
    }

    private void scheduleReconnect() {
        long delayMs;
        int attempt;
        synchronized (this) {
            reconnectAttempts++;
            attempt = reconnectAttempts;
            delayMs = Math.min(1000L << Math.min(attempt - 1, 5), 30_000L);
            reconnects.incrementAndGet();
        }
        log.info("[{}] reconnect attempt #{} in {}ms", streamId, attempt, delayMs);
        manager.scheduler().schedule(() -> {
            boolean go;
            synchronized (this) {
                reconnectScheduled = false;
                go = state == State.PLAYING && totalSubscribersLocked() > 0 && rtspClient == null;
                if (go) {
                    FlvMuxer m = muxer;
                    if (m != null) {
                        m.onStreamReset(); // 时间轴延续，不回跳
                    }
                    gopCache.clear();
                    auGopCache.clear(); // 上游时间戳跳变，旧帧不再适用于转发
                }
            }
            if (go) {
                startRtsp();
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void closeRtspLocked() {
        RtspClient c = rtspClient;
        rtspClient = null;
        if (c != null) {
            c.stop();
        }
    }

    private void cleanupMediaLocked() {
        gopCache.clear();
        auGopCache.clear();
        currentParams = null;
        muxer = null;
        depacketizer = null;
    }

    /** 网关停机：关闭所有订阅者与 RTSP。 */
    void shutdown() {
        List<Subscriber> toClose = new ArrayList<>();
        List<RawSubscriber> toNotify = new ArrayList<>();
        CompletableFuture<Void> f;
        synchronized (this) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            if (pendingCloseFuture != null) {
                pendingCloseFuture.cancel(false);
                pendingCloseFuture = null;
            }
            closeRtspLocked();
            cleanupMediaLocked();
            for (Subscriber s : subscribers) {
                s.markRemoved();
                toClose.add(s);
            }
            subscribers.clear();
            subscriberCount = 0;
            collectAndClearRawLocked(toNotify);
            f = startFuture;
            startFuture = null;
        }
        if (f != null) {
            f.completeExceptionally(new IllegalStateException("gateway shutting down"));
        }
        for (Subscriber s : toClose) {
            closeStream(s.channel(), s.transport(), CLOSE_SHUTDOWN);
        }
        for (RawSubscriber s : toNotify) {
            notifyStreamClosed(s);
        }
    }

    /** 调用者持有锁：取出全部原始帧订阅者并清空列表。 */
    private void collectAndClearRawLocked(List<RawSubscriber> out) {
        for (RawSubscription rs : rawSubscribers) {
            rs.removed = true;
            rs.playing = false;
            out.add(rs.sub);
        }
        rawSubscribers.clear();
    }

    // ===== RtspClient.Listener =====

    private final class RtspListener implements RtspClient.Listener {

        @Override
        public VideoDepacketizer onSdpParsed(SdpInfo sdp) {
            SdpInfo.Track track = sdp.firstVideo();
            if (track == null) {
                return null;
            }
            CodecType codec = track.codec();
            FlvMuxer m;
            synchronized (StreamSession.this) {
                if (state == State.CLOSED) {
                    return null;
                }
                m = muxer;
                if (m == null || m.codec() != codec) {
                    m = new FlvMuxer(codec, PooledByteBufAllocator.DEFAULT);
                    muxer = m;
                } else {
                    m.onStreamReset(); // 重连：输出时间轴延续递增
                }
                gopCache.clear();
                auGopCache.clear();
            }
            ParamSets seed = track.toParamSets();
            if (seed.isComplete()) {
                synchronized (StreamSession.this) {
                    currentParams = seed; // RTSP 转发可立即拿到 SDP sprop 参数集
                }
                if (m.updateConfig(seed)) {
                    log.info("[{}] initial decoder config from SDP sprop ({})", streamId, codec);
                }
            }
            VideoDepacketizer dep;
            if (codec == CodecType.H264) {
                H264Depacketizer h = new H264Depacketizer(StreamSession.this);
                if (seed.isComplete()) {
                    h.seedParamSets(seed.sps(), seed.pps());
                }
                dep = h;
            } else {
                H265Depacketizer h = new H265Depacketizer(StreamSession.this);
                if (seed.isComplete()) {
                    h.seedParamSets(seed.vps(), seed.sps(), seed.pps());
                }
                dep = h;
            }
            depacketizer = dep;
            return dep;
        }

        @Override
        public void onPlaying() {
            CompletableFuture<Void> f;
            int total;
            synchronized (StreamSession.this) {
                if (state == State.CLOSED) {
                    return;
                }
                state = State.PLAYING;
                playingSinceMs = System.currentTimeMillis();
                reconnectAttempts = 0;
                reconnectScheduled = false;
                f = startFuture;
                total = totalSubscribersLocked();
                if (total == 0) {
                    onLastSubscriberLeftLocked(); // 启动期间订阅者全部离开
                }
            }
            log.info("[{}] RTSP PLAYING, subscribers={}", streamId, total);
            if (f != null) {
                f.complete(null);
            }
        }

        @Override
        public void onStartupFailure(Throwable cause) {
            CompletableFuture<Void> f;
            List<Subscriber> toClose = new ArrayList<>();
            List<RawSubscriber> toNotify = new ArrayList<>();
            synchronized (StreamSession.this) {
                if (state == State.CLOSED) {
                    return;
                }
                state = State.CLOSED;
                f = startFuture;
                startFuture = null;
                if (pendingCloseFuture != null) {
                    pendingCloseFuture.cancel(false);
                    pendingCloseFuture = null;
                }
                closeRtspLocked();
                cleanupMediaLocked();
                for (Subscriber s : subscribers) {
                    s.markRemoved();
                    toClose.add(s);
                }
                subscribers.clear();
                subscriberCount = 0;
                collectAndClearRawLocked(toNotify);
                manager.removeSession(streamId, StreamSession.this);
            }
            log.error("[{}] RTSP startup failed: {}", streamId, String.valueOf(cause.getMessage()));
            if (f != null) {
                f.completeExceptionally(cause);
            }
            for (Subscriber s : toClose) {
                closeStream(s.channel(), s.transport(), CLOSE_STREAM_ERROR);
            }
            for (RawSubscriber s : toNotify) {
                notifyStreamClosed(s);
            }
        }

        @Override
        public void onDisconnected(Throwable cause) {
            boolean reconnect = false;
            boolean closedNow = false;
            synchronized (StreamSession.this) {
                if (state == State.CLOSED) {
                    return;
                }
                rtspClient = null;
                if (totalSubscribersLocked() > 0 && state == State.PLAYING) {
                    reconnect = !reconnectScheduled;
                    reconnectScheduled = true;
                } else {
                    // 无订阅者（延迟关闭中）且上游断开：直接终止会话
                    state = State.CLOSED;
                    closedNow = true;
                    if (pendingCloseFuture != null) {
                        pendingCloseFuture.cancel(false);
                        pendingCloseFuture = null;
                    }
                    closeRtspLocked();
                    cleanupMediaLocked();
                    manager.removeSession(streamId, StreamSession.this);
                }
            }
            if (closedNow) {
                log.info("[{}] upstream disconnected with no subscribers, session closed: {}",streamId, cause.getMessage());
            } else {
                log.warn("[{}] RTSP disconnected: {}", streamId, cause.getMessage());
                if (reconnect) {
                    scheduleReconnect();
                }
            }
        }
    }

    // ===== VideoDepacketizer.Listener（RTSP EventLoop 线程回调） =====

    @Override
    public void onParameterSets(ParamSets ps) {
        List<RawSubscription> notify;
        synchronized (this) {
            currentParams = ps;
            notify = new ArrayList<>(rawSubscribers);
            for (RawSubscription rs : notify) {
                rs.readyNotified = true;
            }
        }
        FlvMuxer m = muxer;
        if (m != null && m.updateConfig(ps)) {
            paramUpdates.incrementAndGet();
            log.info("[{}] parameter sets updated, config version {}", streamId, m.configVersion());
            ByteBuf tag = m.buildConfigTag();
            if (tag != null) {
                broadcast(tag, true);
            }
        }
        // RTSP 转发订阅者据此更新 SDP 与带内参数集
        for (RawSubscription rs : notify) {
            if (!rs.removed) {
                deliverStreamInfo(rs, ps.codec(), ps);
            }
        }
    }

    @Override
    public void onAccessUnit(AccessUnit au) {
        FlvMuxer m = muxer;
        ByteBuf flv = m == null ? null : m.mux(au);
        if (flv != null) {
            framesIn.incrementAndGet();
            lastFrameAtMs = System.currentTimeMillis();
        }
        try {
            synchronized (this) {
                if (state == State.CLOSED) {
                    return;
                }
                if (flv != null) {
                    gopCache.add(flv, au.keyFrame());
                    broadcastLocked(flv, au.keyFrame());
                }
                if (keepAuGopCache) {
                    auGopCache.add(au); // 无转发订阅者也缓存，保证新客户端秒开
                }
                if (!rawSubscribers.isEmpty()) {
                    broadcastRawLocked(au);
                }
            }
        } finally {
            if (flv != null) {
                flv.release();
            }
        }
    }

    @Override
    public void onError(String message) {
        depacketizeErrors.incrementAndGet();
        log.debug("[{}] depacketizer: {}", streamId, message);
    }

    // ===== 广播 =====

    private void broadcast(ByteBuf flv, boolean keyFrame) {
        try {
            synchronized (this) {
                if (state == State.CLOSED) {
                    return;
                }
                broadcastLocked(flv, keyFrame);
            }
        } finally {
            flv.release();
        }
    }

    /**
     * 调用者持有锁。为每个订阅者 retainedDuplicate，原始 buffer 由调用方 release。
     * 背压策略：不可写时丢非关键帧；关键帧连续多次不可写则断开慢消费者。
     */
    private void broadcastLocked(ByteBuf flv, boolean keyFrame) {
        if (subscribers.isEmpty()) {
            return;
        }
        List<Subscriber> removals = null;
        for (Subscriber s : subscribers) {
            Channel ch = s.channel();
            if (!ch.isActive()) {
                if (removals == null) {
                    removals = new ArrayList<>(2);
                }
                removals.add(s);
                continue;
            }
            if (!s.isActivePhase()) {
                // BOOTSTRAPPING：进入 pending 队列，保证与首屏快照不乱序
                if (!s.queue(flv.retainedDuplicate())) {
                    kickedSlow.incrementAndGet();
                    log.warn("[{}] subscriber {} bootstrap queue overflow, closing", streamId, s.id());
                }
                continue;
            }
            if (!ch.isWritable()) {
                if (!keyFrame) {
                    s.droppedFrames++;
                    droppedFrames.incrementAndGet();
                    continue;
                }
                if (++s.notWritableKeyStreak >= MAX_KEYFRAME_NOT_WRITABLE_STREAK) {
                    kickedSlow.incrementAndGet();
                    log.warn("[{}] subscriber {} too slow ({} consecutive unwritable keyframes), closing",
                            streamId, s.id(), s.notWritableKeyStreak);
                    ch.close();
                    continue;
                }
            } else {
                s.notWritableKeyStreak = 0;
            }
            bytesOut.addAndGet(flv.readableBytes());
            s.writeAndFlush(flv.retainedDuplicate());
        }
        if (removals != null) {
            for (Subscriber s : removals) {
                removeSubscriber(s);
            }
        }
    }

    /**
     * 调用者持有锁。向原始帧订阅者（RTSP 转发）分发访问单元，仅传递引用不复制。
     * 背压由订阅者自行处理；isActive() 为 false 或回调异常时在此移除。
     */
    private void broadcastRawLocked(AccessUnit au) {
        List<RawSubscription> removals = null;
        for (RawSubscription rs : rawSubscribers) {
            if (!rs.playing) {
                continue;
            }
            boolean dead = !rs.sub.isActive();
            if (!dead) {
                try {
                    rs.sub.onAccessUnit(au);
                } catch (RuntimeException e) {
                    log.warn("[{}] raw subscriber {} dispatch failed: {}", streamId, rs.sub.id(), e.toString());
                    dead = true;
                }
            }
            if (dead) {
                if (removals == null) {
                    removals = new ArrayList<>(2);
                }
                removals.add(rs);
            }
        }
        if (removals != null) {
            for (RawSubscription rs : removals) {
                removeRaw(rs);
            }
        }
    }

    /** 结束下行并关闭连接：WebSocket 发关闭帧，HTTP-FLV 发最后一个空块。 */
    static void closeStream(Channel ch, FlvTransport transport, WebSocketCloseStatus status) {
        if (ch.isActive()) {
            ch.writeAndFlush(transport.endMessage(status)).addListener(ChannelFutureListener.CLOSE);
        } else {
            ch.close();
        }
    }

    // ===== 监控 =====

    public int subscriberCount() {
        synchronized (this) {
            return subscriberCount;
        }
    }

    /** 原始帧（RTSP 转发）订阅者数量。 */
    public int rawSubscriberCount() {
        synchronized (this) {
            return rawSubscribers.size();
        }
    }

    /** 全部订阅者数量（FLV + 原始帧）。 */
    public int totalSubscriberCount() {
        synchronized (this) {
            return totalSubscribersLocked();
        }
    }

    /** 输出本会话的 JSON 统计片段。 */
    JsonBuilder statsJson() {
        FlvMuxer m = muxer;
        long rtpPkts = 0;
        long bytesIn = 0;
        long lostPkts = 0;
        RtspClient c;
        VideoDepacketizer dep = depacketizer;
        synchronized (this) {
            c = rtspClient;
        }
        if (c != null) {
            rtpPkts = c.rtpPackets();
            bytesIn = c.rtpBytes();
        }
        if (dep != null) {
            lostPkts = dep.lostPackets();
        }
        long gopBytes;
        int gopTags;
        long auGopBytes;
        int auGopFrames;
        synchronized (this) {
            gopBytes = gopCache.bytes();
            gopTags = gopCache.tagCount();
            auGopBytes = auGopCache.bytes();
            auGopFrames = auGopCache.size();
        }
        long now = System.currentTimeMillis();
        long bootAvg = bootstraps.get() > 0 ? bootstrapTotalMs.get() / bootstraps.get() : 0;
        return new JsonBuilder(384)
                .append("streamId", streamId)
                .append("rtspUrl", GatewayConfig.sanitizeUrl(rtspUrl))
                .append("state", state.name())
                .append("codec", m == null ? "null" : m.codec().name())
                .append("subscribers", subscriberCount())
                .append("rtspSubscribers", rawSubscriberCount())
                .append("rtpPackets", rtpPkts)
                .append("rtpLost", lostPkts)
                .append("bytesIn", bytesIn)
                .append("framesIn", framesIn.get())
                .append("bytesOut", bytesOut.get())
                .append("droppedFrames", droppedFrames.get())
                .append("kickedSlow", kickedSlow.get())
                .append("reconnects", reconnects.get())
                .append("paramUpdates", paramUpdates.get())
                .append("depacketizeErrors", depacketizeErrors.get())
                .append("gopCacheBytes", gopBytes)
                .append("gopCacheTags", gopTags)
                .append("auGopCacheBytes", auGopBytes)
                .append("auGopCacheFrames", auGopFrames)
                .append("bootstraps", bootstraps.get())
                .append("bootstrapAvgMs", bootAvg)
                .append("createdAt", createdAtMs)
                .append("uptimeSec", (now - createdAtMs) / 1000)
                .append("playingSec", playingSinceMs < 0 ? 0 : (now - playingSinceMs) / 1000)
                .append("lastFrameAgeMs", lastFrameAtMs < 0 ? -1 : now - lastFrameAtMs)
                .end();
    }
}
