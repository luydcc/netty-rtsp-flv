package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * RTSP 文本响应构建器（网关作为 RTSP 服务端时使用）：状态行 + 头 + 可选 body。
 * 头按插入顺序输出，CSeq 由调用方回填客户端请求中的值。
 */
public final class RtspServerResponse {

    private final int status;
    private final String reason;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private String contentType;
    private String body = "";

    public RtspServerResponse(int status, String reason) {
        this.status = status;
        this.reason = reason;
    }

    /** 按状态码构建响应，reason 取 RTSP 常用短语。 */
    public static RtspServerResponse of(int status) {
        return new RtspServerResponse(status, reasonOf(status));
    }

    /** 200 OK 并回填 CSeq（绝大多数成功响应都要带头）。 */
    public static RtspServerResponse ok(int cseq) {
        return of(200).header("CSeq", String.valueOf(cseq));
    }

    public static String reasonOf(int status) {
        return switch (status) {
            case 200 -> "OK";
            case 400 -> "Bad Request";
            case 401 -> "Unauthorized";
            case 403 -> "Forbidden";
            case 404 -> "Not Found";
            case 405 -> "Method Not Allowed";
            case 451 -> "Parameter Not Understood";
            case 453 -> "Not Enough Bandwidth";
            case 454 -> "Session Not Found";
            case 455 -> "Method Not Valid in This State";
            case 461 -> "Unsupported Transport";
            case 500 -> "Internal Server Error";
            case 501 -> "Not Implemented";
            case 503 -> "Service Unavailable";
            case 505 -> "RTSP Version Not Supported";
            default -> "Unknown";
        };
    }

    public RtspServerResponse header(String name, String value) {
        headers.put(name, value);
        return this;
    }

    /** 设置 body 与 Content-Type，并自动写入 Content-Length（编码时）。 */
    public RtspServerResponse body(String contentType, String body) {
        this.contentType = contentType;
        this.body = body == null ? "" : body;
        return this;
    }

    public int status() {
        return status;
    }

    public String body() {
        return body;
    }

    /** 编码为完整响应文本（含 Content-Length 与结尾空行）。 */
    public String encodeText() {
        StringBuilder sb = new StringBuilder(128 + body.length());
        sb.append("RTSP/1.0 ").append(status).append(' ').append(reason).append("\r\n");
        for (Map.Entry<String, String> e : headers.entrySet()) {
            sb.append(e.getKey()).append(": ").append(e.getValue()).append("\r\n");
        }
        if (contentType != null) {
            sb.append("Content-Type: ").append(contentType).append("\r\n");
        }
        if (!body.isEmpty()) {
            sb.append("Content-Length: ").append(body.getBytes(StandardCharsets.UTF_8).length).append("\r\n");
        }
        sb.append("\r\n").append(body);
        return sb.toString();
    }

    public ByteBuf encode(ByteBufAllocator alloc) {
        byte[] bytes = encodeText().getBytes(StandardCharsets.UTF_8);
        return alloc.buffer(bytes.length).writeBytes(bytes);
    }

    @Override
    public String toString() {
        return "RTSP/1.0 " + status + " " + reason;
    }
}
