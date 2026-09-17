package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** RTSP 文本请求构建器。 */
public final class RtspRequest {

    private final String method;
    private final String uri;
    private final Map<String, String> headers = new LinkedHashMap<>();

    public RtspRequest(String method, String uri) {
        this.method = method;
        this.uri = uri;
    }

    public RtspRequest header(String name, String value) {
        headers.put(name, value);
        return this;
    }

    public String method() {
        return method;
    }

    public String uri() {
        return uri;
    }

    public ByteBuf encode(ByteBufAllocator alloc) {
        StringBuilder sb = new StringBuilder(256);
        sb.append(method).append(' ').append(uri).append(" RTSP/1.0\r\n");
        for (Map.Entry<String, String> e : headers.entrySet()) {
            sb.append(e.getKey()).append(": ").append(e.getValue()).append("\r\n");
        }
        sb.append("\r\n");
        byte[] bytes = sb.toString().getBytes(StandardCharsets.US_ASCII);
        return alloc.buffer(bytes.length).writeBytes(bytes);
    }

    @Override
    public String toString() {
        return method + " " + uri;
    }
}
