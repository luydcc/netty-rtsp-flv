package com.mediagw.session;

import com.mediagw.config.GatewayConfig;
import com.mediagw.util.JsonBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * 会话管理器：维护 streamId -> StreamSession 映射。
 * 同一 streamId 复用同一会话（同一条 RTSP）；已关闭的会话在下次订阅时原子替换。
 * 两类订阅入口：FLV 输出（WebSocket / HTTP-FLV）与原始帧输出（RTSP 转发）。
 */
public class StreamSessionManager {

    private static final Logger log = LoggerFactory.getLogger(StreamSessionManager.class);
    private static final WebSocketCloseStatus CLOSE_UNKNOWN_STREAM = new WebSocketCloseStatus(4404, "unknown stream");
    private static final WebSocketCloseStatus CLOSE_TOO_MANY_STREAMS = new WebSocketCloseStatus(4503, "too many streams");

    /** 原始帧（RTSP 转发）订阅结果，供上层映射为 RTSP 响应状态码。 */
    public enum RawSubscribeResult { OK, UNKNOWN_STREAM, TOO_MANY_STREAMS, STREAM_ERROR }

    private final ConcurrentHashMap<String, StreamSession> sessions = new ConcurrentHashMap<>();
    /** 运行时临时添加的流（streamId -> 地址与最近使用时间），仅存内存，重启即失效。 */
    private final ConcurrentHashMap<String, DynamicStream> dynamicStreams = new ConcurrentHashMap<>();
    /** 拉流名额校验与会话创建的互斥锁，保证并发订阅不会突破 session.maxPullStreams。 */
    private final Object pullLock = new Object();
    private final GatewayConfig config;
    private final EventLoopGroup rtspGroup;
    private final ScheduledExecutorService scheduler;
    private final long startedAtMs = System.currentTimeMillis();

    /** 临时流添加结果。 */
    public record AddResult(boolean ok,String streamId, String message) {
    }

    /** 临时流条目：RTSP 地址 + 最近一次使用时间（数量超限时按此淘汰最久未使用的流）。 */
    private static final class DynamicStream {
        final String url;
        volatile long lastUsedAtMs;

        DynamicStream(String url, long lastUsedAtMs) {
            this.url = url;
            this.lastUsedAtMs = lastUsedAtMs;
        }
    }

    public StreamSessionManager(GatewayConfig config) {
        this.config = config;
        this.rtspGroup = new MultiThreadIoEventLoopGroup(0, new DefaultThreadFactory("rtsp-client", true), NioIoHandler.newFactory());
        this.scheduler = Executors.newScheduledThreadPool(2, new DefaultThreadFactory("session-sched", true));
    }

    /** FLV 订阅入口（WebSocket / HTTP-FLV）：解析/创建会话并订阅；失败时按传输方式关闭连接。 */
    public void subscribe(String streamId, Channel ch, FlvTransport transport) {
        String url = streamUrl(streamId);
        if (url == null) {
            log.warn("unknown streamId '{}' requested by {}", streamId, ch.remoteAddress());
            StreamSession.closeStream(ch, transport, CLOSE_UNKNOWN_STREAM);
            return;
        }
        // 极少数情况下会话恰好在订阅瞬间关闭，重试获取新会话
        for (int attempt = 0; attempt < 3; attempt++) {
            StreamSession session = acquireSession(streamId, url);
            if (session == null) {
                log.warn("pull limit {} reached, reject '{}' requested by {}",
                        config.maxPullStreams(), streamId, ch.remoteAddress());
                StreamSession.closeStream(ch, transport, CLOSE_TOO_MANY_STREAMS);
                return;
            }
            if (session.subscribe(ch, transport)) {
                return;
            }
        }
        StreamSession.closeStream(ch, transport, StreamSession.CLOSE_STREAM_ERROR);
    }

    /**
     * 原始帧订阅入口（RTSP 转发）：与 FLV 订阅共享同一路拉流。
     * 参数集就绪后回调 {@link RawSubscriber#onStreamInfo}。
     */
    public RawSubscribeResult subscribeRaw(String streamId, RawSubscriber sub) {
        String url = streamUrl(streamId);
        if (url == null) {
            log.warn("unknown streamId '{}' requested by raw subscriber {}", streamId, sub.id());
            return RawSubscribeResult.UNKNOWN_STREAM;
        }
        for (int attempt = 0; attempt < 3; attempt++) {
            StreamSession session = acquireSession(streamId, url);
            if (session == null) {
                log.warn("pull limit {} reached, reject raw subscriber {} on '{}'",
                        config.maxPullStreams(), sub.id(), streamId);
                return RawSubscribeResult.TOO_MANY_STREAMS;
            }
            if (session.subscribeRaw(sub)) {
                return RawSubscribeResult.OK;
            }
        }
        return RawSubscribeResult.STREAM_ERROR;
    }

    /** 开始向原始帧订阅者分发：下发 GOP 快照后转实时帧。 */
    public void startRawPlayback(String streamId, RawSubscriber sub) {
        StreamSession session = sessions.get(streamId);
        if (session != null) {
            session.startRawPlayback(sub);
        }
    }

    /** 暂停向原始帧订阅者分发（保留订阅，再次 PLAY 重新下发快照）。 */
    public void stopRawPlayback(String streamId, RawSubscriber sub) {
        StreamSession session = sessions.get(streamId);
        if (session != null) {
            session.stopRawPlayback(sub);
        }
    }

    public void unsubscribeRaw(String streamId, RawSubscriber sub) {
        StreamSession session = sessions.get(streamId);
        if (session != null) {
            session.unsubscribeRaw(sub);
        }
    }

    /**
     * 播放前名额预检：该流尚无活动会话且拉流路数已达上限时返回 true。
     * HTTP-FLV 据此先回 503，避免先写 200 响应头再断流。
     */
    public boolean pullLimitReached(String streamId) {
        int max = config.maxPullStreams();
        if (max <= 0) {
            return false;
        }
        synchronized (pullLock) {
            StreamSession existing = sessions.get(streamId);
            if (existing != null && !existing.isClosed()) {
                return false;
            }
            return activeSessions() >= max;
        }
    }

    /**
     * 取可复用会话，没有则新建；新建前校验拉流路数上限（名额校验与创建在 pullLock 内原子完成）。
     *
     * @return null 表示正在拉流的路数已达上限，拒绝本次订阅
     */
    private StreamSession acquireSession(String streamId, String url) {
        synchronized (pullLock) {
            StreamSession old = sessions.get(streamId);
            if (old != null && !old.isClosed()) {
                return old;
            }
            int max = config.maxPullStreams();
            if (max > 0 && activeSessions() >= max) {
                return null;
            }
            StreamSession session = new StreamSession(streamId, url, config, this);
            sessions.put(streamId, session);
            return session;
        }
    }

    public void unsubscribe(String streamId, Channel ws) {
        StreamSession session = sessions.get(streamId);
        if (session != null) {
            session.unsubscribe(ws);
        }
    }

    /** 流地址查找：配置文件优先，其次运行时临时添加的流；命中临时流时刷新其使用时间。 */
    public String streamUrl(String streamId) {
        String url = config.streamUrl(streamId);
        if (url != null) {
            return url;
        }
        DynamicStream dynamic = dynamicStreams.get(streamId);
        if (dynamic == null) {
            return null;
        }
        dynamic.lastUsedAtMs = System.currentTimeMillis();
        return dynamic.url;
    }

    /**
     * 运行时临时添加一条流：仅保存在内存中，重启后消失。
     * 参数非法或 streamId 冲突时返回失败原因，不抛异常。
     */
    public AddResult addDynamicStream(String rawId, String rawUrl) {
        String id = rawId == null ? "" : rawId.trim();
        String url = rawUrl == null ? "" : rawUrl.trim();
        if (id.isEmpty()) {
            return new AddResult(false,"", "streamId 不能为空");
        }
        if (id.length() > 64) {
            return new AddResult(false,"","streamId 长度不能超过 64");
        }
        for (int i = 0; i < id.length(); i++) {
            char c = id.charAt(i);
            boolean legal = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '_' || c == '-' || c == '.';
            if (!legal) {
                return new AddResult(false, "", "streamId 仅允许字母、数字、下划线、短横线和点");
            }
        }
        if (config.streamUrl(id) != null) {
            return new AddResult(false, "", "streamId '" + id + "' 与配置文件中的流冲突");
        }
        if (!isRtspUrl(url)) {
            return new AddResult(false,"", "非法 RTSP 地址，应形如 rtsp://[user:pass@]host[:port]/path");
        }
        int index = url.indexOf('?');
        if (index > 0) {
            String urlKey = url.substring(0, index);
            for (Map.Entry<String, DynamicStream> e : dynamicStreams.entrySet()) {
                if (e.getValue().url.startsWith(urlKey)) {
                    return new AddResult(true,e.getKey(), "流已存在, id='" + e.getKey() + "'");
                }
            }
        }
        // 已存在的临时流直接返回，避免为重复添加而白白淘汰其他流
        if (dynamicStreams.containsKey(id)) {
            return new AddResult(false,"", "临时流 '" + id + "' 已存在");
        }
        // 添加前先判断拉流名额：上限已满时直接失败，不去淘汰已有临时流
        int maxPull = config.maxPullStreams();
        if (maxPull > 0 && activeSessions() >= maxPull) {
            return new AddResult(false,"", "正在拉流的路数已达上限 " + maxPull + "，停止部分流后再添加");
        }
        int maxDynamic = config.maxDynamicStreams();
        String evicted = null;
        while (maxDynamic > 0 && dynamicStreams.size() >= maxDynamic) {
            evicted = evictLeastRecentlyUsed();
            if (evicted == null) {
                return new AddResult(false,"", "临时流数量已达上限 " + maxDynamic + "，且没有可淘汰的流");
            }
        }
        DynamicStream existing = dynamicStreams.putIfAbsent(id, new DynamicStream(url, System.currentTimeMillis()));
        if (existing != null) {
            return new AddResult(false,"", "临时流 '" + id + "' 已存在");
        }
        log.info("dynamic stream added: '{}' -> {} (total {} in memory{})",
                id, GatewayConfig.sanitizeUrl(url), dynamicStreams.size(),
                evicted == null ? "" : ", evicted '" + evicted + "'");
        return new AddResult(true, id, evicted == null ? "已添加临时流 '" + id + "'"
                : "已添加临时流 '" + id + "'，已淘汰最久未使用的 '" + evicted + "'");
    }

    /**
     * 淘汰最久未使用的临时流：优先淘汰当前未在拉流的流，全部在拉流时才淘汰最久未使用的那条。
     * 正在拉流的会话持有自己的地址，仅移除注册表条目不会中断正在观看的客户端。
     *
     * @return 被淘汰的 streamId，无可淘汰项返回 null
     */
    private String evictLeastRecentlyUsed() {
        String victim = leastRecentlyUsed(false);
        if (victim == null) {
            victim = leastRecentlyUsed(true);
        }
        if (victim == null) {
            return null;
        }
        dynamicStreams.remove(victim);
        log.warn("dynamic stream limit {} reached, evicted least-recently-used '{}'",
                config.maxDynamicStreams(), victim);
        return victim;
    }

    /** 取最久未使用的临时流 id；includePulling=false 时跳过正在拉流的流。 */
    private String leastRecentlyUsed(boolean includePulling) {
        String victim = null;
        long oldest = Long.MAX_VALUE;
        for (Map.Entry<String, DynamicStream> e : dynamicStreams.entrySet()) {
            if (!includePulling && isPulling(e.getKey())) {
                continue;
            }
            if (e.getValue().lastUsedAtMs < oldest) {
                oldest = e.getValue().lastUsedAtMs;
                victim = e.getKey();
            }
        }
        return victim;
    }

    /** 该流当前是否占用一路拉流（会话未关闭即占用，含待关闭状态）。 */
    private boolean isPulling(String streamId) {
        StreamSession session = sessions.get(streamId);
        return session != null && !session.isClosed();
    }

    private static boolean isRtspUrl(String url) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return "rtsp".equalsIgnoreCase(uri.getScheme()) && uri.getHost() != null;
    }

    /** 看板 JSON：全部已知流（配置 + 临时）及各自会话指标；无会话的流 session 为 null。 */
    public String streamsJson() {
        Map<String, String> configStreams = config.streams();
        Map<String, DynamicStream> dynamic = new TreeMap<>(dynamicStreams);
        List<String> parts = new ArrayList<>(configStreams.size() + dynamic.size());
        for (Map.Entry<String, String> e : configStreams.entrySet()) {
            parts.add(streamJson(e.getKey(), e.getValue(), "config"));
        }
        for (Map.Entry<String, DynamicStream> e : dynamic.entrySet()) {
            parts.add(streamJson(e.getKey(), e.getValue().url, "dynamic"));
        }

        StringBuilder sb = new StringBuilder(256 + parts.size() * 512);
        sb.append('{')
                .append("\"now\":").append(System.currentTimeMillis()).append(',')
                .append("\"startedAt\":").append(startedAtMs).append(',')
                .append("\"closeDelaySec\":").append(config.closeDelaySec()).append(',')
                .append("\"sessions\":").append(sessions.size()).append(',')
                .append("\"subscribers\":").append(totalSubscribers()).append(',')
                .append("\"rtspSubscribers\":").append(totalRawSubscribers()).append(',')
                .append("\"pulling\":").append(activeSessions()).append(',')
                .append("\"maxPullStreams\":").append(config.maxPullStreams()).append(',')
                .append("\"dynamicStreams\":").append(dynamicStreams.size()).append(',')
                .append("\"maxDynamicStreams\":").append(config.maxDynamicStreams()).append(',')
                .append("\"streams\":[");
        sb.append(String.join(",", parts));
        sb.append("]}");
        return sb.toString();
    }

    private String streamJson(String id, String url, String source) {
        StreamSession session = sessions.get(id);
        boolean active = session != null && !session.isClosed();
        return new JsonBuilder(384)
                .append("streamId",id)
                .append("rtspUrl",GatewayConfig.sanitizeUrl(url))
                .append("source",source)
                .append("active",active)
                .append("session",active ? session.statsJson() : null)
                .endJson();
    }

    void removeSession(String streamId, StreamSession session) {
        sessions.remove(streamId, session);
    }

    EventLoopGroup rtspGroup() {
        return rtspGroup;
    }

    ScheduledExecutorService scheduler() {
        return scheduler;
    }

    /** 正在拉流的路数：未关闭的会话各占一路（含连接中与待关闭）。 */
    public int activeSessions() {
        int n = 0;
        for (StreamSession s : sessions.values()) {
            if (!s.isClosed()) {
                n++;
            }
        }
        return n;
    }

    public int totalSubscribers() {
        int n = 0;
        for (StreamSession s : sessions.values()) {
            n += s.totalSubscriberCount();
        }
        return n;
    }

    /** RTSP 转发（原始帧）订阅者总数。 */
    public int totalRawSubscribers() {
        int n = 0;
        for (StreamSession s : sessions.values()) {
            n += s.rawSubscriberCount();
        }
        return n;
    }

    /** 监控 JSON：所有会话状态与指标。 */
    public String statsJson() {
        List<String> parts = new ArrayList<>();
        for (StreamSession s : sessions.values()) {
            parts.add(s.statsJson().toString());
        }
        StringBuilder sb = new StringBuilder(256 + parts.size() * 512);
        sb.append("{\"sessions\":").append(parts.size())
                .append(",\"subscribers\":").append(totalSubscribers())
                .append(",\"rtspSubscribers\":").append(totalRawSubscribers())
                .append(",\"pulling\":").append(activeSessions())
                .append(",\"maxPullStreams\":").append(config.maxPullStreams())
                .append(",\"streams\":[");
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(parts.get(i));
        }
        sb.append("]}");
        return sb.toString();
    }

    /** 优雅停机：关闭全部会话与线程资源。 */
    public void shutdown() {
        for (StreamSession s : sessions.values()) {
            try {
                s.shutdown();
            } catch (RuntimeException e) {
                log.warn("session {} shutdown error", s.streamId(), e);
            }
        }
        sessions.clear();
        scheduler.shutdownNow();
        rtspGroup.shutdownGracefully();
    }
}
