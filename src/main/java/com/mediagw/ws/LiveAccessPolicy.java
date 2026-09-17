package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.List;

/**
 * 播放请求准入校验：{@code /live} 路径解析、跨域来源与 token 鉴权。
 * WebSocket（{@link FlvWsHandler}）与 HTTP-FLV（{@link HttpFlvHandler}）共用同一套规则；
 * 本类只做判定，拒绝方式（WebSocket 关闭码 / HTTP 状态码）由各 Handler 决定。
 */
final class LiveAccessPolicy {

    static final String LIVE_PREFIX = "/live/";

    private final GatewayConfig config;

    LiveAccessPolicy(GatewayConfig config) {
        this.config = config;
    }

    /**
     * 从 {@code /live/{streamId}[.flv]} 取出 streamId。
     *
     * @return 路径不合法（前缀不符、streamId 为空或含 '/'）时返回 null
     */
    static String streamId(String path) {
        if (path == null || !path.startsWith(LIVE_PREFIX)) {
            return null;
        }
        String id = path.substring(LIVE_PREFIX.length());
        if (id.endsWith(".flv")) {
            id = id.substring(0, id.length() - ".flv".length());
        }
        return id.isEmpty() || id.contains("/") ? null : id;
    }

    /** 配置了 auth.token 时校验查询参数 token；未配置时恒为 true。 */
    boolean tokenAllowed(String uri) {
        String expected = config.authToken();
        if (expected == null || expected.isEmpty()) {
            return true;
        }
        List<String> tokens = new QueryStringDecoder(uri).parameters().get("token");
        return tokens != null && expected.equals(tokens.getFirst());
    }

    /** 跨域校验，来源与 Host 取自请求头。 */
    boolean originAllowed(HttpHeaders headers) {
        return originAllowed(headers.get(HttpHeaderNames.ORIGIN), headers.get(HttpHeaderNames.HOST));
    }

    /**
     * 跨域校验：白名单为 "*" 时全部放行；不带 Origin 的非浏览器客户端放行；
     * 命中白名单或与网关同源（host 相同）时放行。
     */
    boolean originAllowed(String origin, String host) {
        if (config.corsAnyOrigin() || origin == null || origin.isEmpty()) {
            return true;
        }
        return config.corsOrigins().contains(origin) || sameHost(origin, host);
    }

    /** 同源判定只比较 host（忽略端口），避免配置白名单后网关自带页面无法播放。 */
    private static boolean sameHost(String origin, String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        int scheme = origin.indexOf("://");
        String authority = scheme < 0 ? origin : origin.substring(scheme + 3);
        int slash = authority.indexOf('/');
        if (slash >= 0) {
            authority = authority.substring(0, slash);
        }
        String originHost = hostOnly(authority);
        return !originHost.isEmpty() && originHost.equals(hostOnly(host));
    }

    /** 去掉端口取主机名；IPv6 字面量取方括号内内容。 */
    private static String hostOnly(String authority) {
        String value = authority.trim();
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            return end > 0 ? value.substring(1, end).toLowerCase() : "";
        }
        int colon = value.indexOf(':');
        return (colon < 0 ? value : value.substring(0, colon)).toLowerCase();
    }
}
