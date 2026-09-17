package com.mediagw.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** 网关全局配置，从 properties 文件加载。 */
public final class GatewayConfig {

    private static final Logger log = LoggerFactory.getLogger(GatewayConfig.class);

    private String ip = "";
    private int port = 8080;
    private Map<String, String> streams = new LinkedHashMap<>();
    private String authToken = "";
    private int closeDelaySec = 15;
    private long gopCacheMaxBytes = 2 * 1024 * 1024;
    private long subscriberMaxPendingBytes = 2 * 1024 * 1024;
    private int rtspResponseTimeoutMs = 10_000;
    private int rtspKeepaliveSec = 15; // rtsp和服务器端保持连接发送数据包的时间间隔
    private int rtspForwardPort = 8554;
    private boolean rtspForwardUdp = true;
    private int rtspForwardMaxPayload = 1400;
    private int rtspForwardSessionTimeoutSec = 60;
    private int writeBufferLowKb = 256;
    private int writeBufferHighKb = 1024;
    private int maxPullStreams = 32;
    private int maxDynamicStreams = 64;
    private List<String> corsOrigins = List.of("*");
    private boolean corsAllowCredentials = false;
    private int corsMaxAgeSec = 3600;

    public static GatewayConfig load(String path) {
        GatewayConfig cfg = new GatewayConfig();
        Properties props = new Properties();
        Path file = Path.of(path);
        if (Files.exists(file)) {
            try (InputStream in = Files.newInputStream(file)) {
                props.load(in);
            } catch (IOException e) {
                log.warn("读取配置文件失败: {}, 使用默认配置", path, e);
            }
        } else {
            log.warn("配置文件 {} 不存在，使用默认配置", path);
        }

        cfg.ip = props.getProperty("server.ip", cfg.ip).trim();
        cfg.port = intProp(props, "server.port", cfg.port);
        cfg.authToken = props.getProperty("auth.token", "").trim();
        cfg.closeDelaySec = intProp(props, "session.closeDelaySec", cfg.closeDelaySec);
        cfg.gopCacheMaxBytes = longProp(props, "gop.cacheMaxBytes", cfg.gopCacheMaxBytes);
        cfg.subscriberMaxPendingBytes = longProp(props, "subscriber.maxPendingBytes", cfg.subscriberMaxPendingBytes);
        cfg.rtspResponseTimeoutMs = intProp(props, "rtsp.responseTimeoutMs", cfg.rtspResponseTimeoutMs);
        cfg.rtspKeepaliveSec = intProp(props, "rtsp.keepaliveSec", cfg.rtspKeepaliveSec);
        cfg.rtspForwardPort = intProp(props, "rtsp.forwardPort", cfg.rtspForwardPort);
        cfg.rtspForwardUdp = boolProp(props, "rtsp.forwardUdp", cfg.rtspForwardUdp);
        cfg.rtspForwardMaxPayload = intProp(props, "rtsp.forwardMaxPayload", cfg.rtspForwardMaxPayload);
        cfg.rtspForwardSessionTimeoutSec =
                intProp(props, "rtsp.forwardSessionTimeoutSec", cfg.rtspForwardSessionTimeoutSec);
        cfg.writeBufferLowKb = intProp(props, "server.writeBufferLowKb", cfg.writeBufferLowKb);
        cfg.writeBufferHighKb = intProp(props, "server.writeBufferHighKb", cfg.writeBufferHighKb);
        cfg.maxPullStreams = intProp(props, "session.maxPullStreams", cfg.maxPullStreams);
        cfg.maxDynamicStreams = intProp(props, "dynamic.maxStreams", cfg.maxDynamicStreams);
        cfg.corsOrigins = parseOrigins(props.getProperty("server.corsOrigins"));
        cfg.corsAllowCredentials = boolProp(props, "server.corsAllowCredentials", cfg.corsAllowCredentials);
        cfg.corsMaxAgeSec = intProp(props, "server.corsMaxAgeSec", cfg.corsMaxAgeSec);

        Map<String, String> streams = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith("stream.")) {
                String id = name.substring("stream.".length()).trim();
                String url = props.getProperty(name, "").trim();
                if (!id.isEmpty() && !url.isEmpty()) {
                    streams.put(id, url);
                }
            }else if(name.startsWith("sock5.proxy.")){
                log.info("load {}: {}", name, props.getProperty(name));
                GlobalSocks5ProxyConfig.addProxy(props.getProperty(name));
            }
        }
        cfg.streams = Collections.unmodifiableMap(streams);
        return cfg;
    }

    private static int intProp(Properties p, String key, int def) {
        try {
            String v = p.getProperty(key);
            return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long longProp(Properties p, String key, long def) {
        try {
            String v = p.getProperty(key);
            return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static boolean boolProp(Properties p, String key, boolean def) {
        String v = p.getProperty(key);
        return v == null || v.isBlank() ? def : Boolean.parseBoolean(v.trim());
    }

    /** 跨域来源白名单：逗号分隔，忽略空项与重复项；未配置或配置为空时允许任意来源。 */
    private static List<String> parseOrigins(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of("*");
        }
        List<String> origins = new ArrayList<>(4);
        for (String item : raw.split(",")) {
            String origin = item.trim();
            if (!origin.isEmpty() && !origins.contains(origin)) {
                origins.add(origin);
            }
        }
        return origins.isEmpty() ? List.of("*") : Collections.unmodifiableList(origins);
    }

    public String ip() { return ip; }
    public int port() { return port; }
    public Map<String, String> streams() { return streams; }
    public String streamUrl(String streamId) { return streams.get(streamId); }
    public String authToken() { return authToken; }
    public int closeDelaySec() { return closeDelaySec; }
    public long gopCacheMaxBytes() { return gopCacheMaxBytes; }
    public long subscriberMaxPendingBytes() { return subscriberMaxPendingBytes; }
    public int rtspResponseTimeoutMs() { return rtspResponseTimeoutMs; }
    public int rtspKeepaliveSec() { return rtspKeepaliveSec; }
    /** RTSP 转发（服务端）监听端口，<=0 表示不对外转发。 */
    public int rtspForwardPort() { return rtspForwardPort; }
    /** RTSP 转发是否允许客户端选用 UDP 传输（否则仅 TCP interleaved）。 */
    public boolean rtspForwardUdp() { return rtspForwardUdp; }
    /** RTSP 转发单个 RTP 包的载荷上限（字节），超过则 FU 分片；建议不超过 MTU-40。 */
    public int rtspForwardMaxPayload() { return rtspForwardMaxPayload; }
    /** RTSP 转发会话空闲超时（秒）：客户端超过此时长无任何请求即断开。 */
    public int rtspForwardSessionTimeoutSec() { return rtspForwardSessionTimeoutSec; }
    public int writeBufferLowKb() { return writeBufferLowKb; }
    public int writeBufferHighKb() { return writeBufferHighKb; }
    /** 同时拉流的最大路数，<=0 表示不限制。 */
    public int maxPullStreams() { return maxPullStreams; }
    /** 临时流最大数量，<=0 表示不限制。 */
    public int maxDynamicStreams() { return maxDynamicStreams; }
    public List<String> corsOrigins() { return corsOrigins; }
    /** 来源白名单含 "*" 时允许任意来源跨域。 */
    public boolean corsAnyOrigin() { return corsOrigins.contains("*"); }
    public boolean corsAllowCredentials() { return corsAllowCredentials; }
    public int corsMaxAgeSec() { return corsMaxAgeSec; }

    /** 日志脱敏：隐藏 RTSP URL 中的密码。 */
    public static String sanitizeUrl(String url) {
        if (url == null) return "";
        return url.replaceAll("://([^/@:]+):([^@/]+)@", "://$1:***@");
    }
}
