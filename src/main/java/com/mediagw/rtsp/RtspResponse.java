package com.mediagw.rtsp;

import java.util.Map;
import java.util.TreeMap;

/** RTSP 文本响应（状态行 + 头 + 可选 body）。头名称大小写不敏感。 */
public final class RtspResponse {

    private final int status;
    private final String reason;
    private final Map<String, String> headers;
    private final String body;

    public RtspResponse(int status, String reason, Map<String, String> headers, String body) {
        this.status = status;
        this.reason = reason;
        this.headers = headers;
        this.body = body;
    }

    public static RtspResponse parse(String headerBlock, String body) {
        String[] lines = headerBlock.split("\r\n");
        if (lines.length == 0 || !lines[0].startsWith("RTSP/1.0")) {
            throw new RtspException("malformed RTSP status line: "
                    + RtspException.brief(lines.length > 0 ? lines[0] : null));
        }
        String[] parts = lines[0].split(" ", 3);
        if (parts.length < 2) {
            throw new RtspException("malformed RTSP status line: " + RtspException.brief(lines[0]));
        }
        int status;
        try {
            status = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new RtspException("malformed RTSP status code: " + RtspException.brief(parts[1]));
        }
        String reason = parts.length > 2 ? parts[2] : "";
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 1; i < lines.length; i++) {
            int idx = lines[i].indexOf(':');
            if (idx > 0) {
                headers.put(lines[i].substring(0, idx).trim(), lines[i].substring(idx + 1).trim());
            }
        }
        return new RtspResponse(status, reason, headers, body);
    }

    public int status() {
        return status;
    }

    public String reason() {
        return reason;
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
        return "RTSP/1.0 " + status + " " + reason;
    }
}
