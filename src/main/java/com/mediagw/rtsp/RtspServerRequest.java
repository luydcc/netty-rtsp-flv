package com.mediagw.rtsp;

import java.util.Map;
import java.util.TreeMap;

/**
 * RTSP 文本请求（服务端解析客户端请求用）：请求行 + 头 + 可选 body。
 * 头名称大小写不敏感。与客户端侧的 {@link RtspRequest}（构建器）方向相反。
 */
public final class RtspServerRequest {

    private final String method;
    private final String uri;
    private final Map<String, String> headers;
    private final String body;

    public RtspServerRequest(String method, String uri, Map<String, String> headers, String body) {
        this.method = method;
        this.uri = uri;
        this.headers = headers;
        this.body = body;
    }

    public static RtspServerRequest parse(String headerBlock, String body) {
        String[] lines = headerBlock.split("\r\n");
        if (lines.length == 0 || lines[0].isEmpty()) {
            throw new RtspException("empty RTSP request line");
        }
        String[] parts = lines[0].split(" ");
        if (parts.length < 2 || !parts[parts.length - 1].startsWith("RTSP/")) {
            throw new RtspException("malformed RTSP request line: " + RtspException.brief(lines[0]));
        }
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 1; i < lines.length; i++) {
            int idx = lines[i].indexOf(':');
            if (idx > 0) {
                headers.put(lines[i].substring(0, idx).trim(), lines[i].substring(idx + 1).trim());
            }
        }
        return new RtspServerRequest(parts[0].toUpperCase(), parts[1], headers, body);
    }

    public String method() {
        return method;
    }

    public String uri() {
        return uri;
    }

    /** 去掉 {@code rtsp://host:port} 与查询参数后的路径；URI 不含 authority 时原样返回路径部分。 */
    public String path() {
        String p = uri;
        int scheme = p.indexOf("://");
        if (scheme >= 0) {
            int slash = p.indexOf('/', scheme + 3);
            p = slash < 0 ? "/" : p.substring(slash);
        }
        int query = p.indexOf('?');
        return query < 0 ? p : p.substring(0, query);
    }

    /** 取查询参数值；不存在返回 null。 */
    public String query(String name) {
        int query = uri.indexOf('?');
        if (query < 0) {
            return null;
        }
        for (String pair : uri.substring(query + 1).split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).equals(name)) {
                return pair.substring(eq + 1);
            }
        }
        return null;
    }

    public String header(String name) {
        return headers.get(name);
    }

    public Map<String, String> headers() {
        return headers;
    }

    public String body() {
        return body;
    }

    @Override
    public String toString() {
        return method + " " + uri;
    }
}
