package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.StreamSessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HTTP-FLV 分发 Handler 测试：只覆盖不触发拉流的准入分支，
 * 200 长连接下发由 e2e 校验器 {@code HttpFlvVerifier} 覆盖。
 */
class HttpFlvHandlerTest {

    private StreamSessionManager manager;
    private EmbeddedChannel ch;

    @AfterEach
    void tearDown() {
        if (ch != null) {
            ch.finishAndReleaseAll();
            ch = null;
        }
        if (manager != null) {
            manager.shutdown();
            manager = null;
        }
    }

    private void newChannel(Path dir, String... lines) throws IOException {
        Path file = dir.resolve("test.properties");
        Files.writeString(file, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        GatewayConfig config = GatewayConfig.load(file.toString());
        manager = new StreamSessionManager(config);
        ch = new EmbeddedChannel(new HttpFlvHandler(manager, config));
    }

    private static FullHttpRequest get(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    }

    private FullHttpResponse expectResponse() {
        FullHttpResponse resp = assertInstanceOf(FullHttpResponse.class, ch.readOutbound());
        return resp;
    }

    private static String body(FullHttpResponse resp) {
        ByteBuf content = resp.content();
        byte[] bytes = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Test
    void rejectsNonGetWith405(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        ch.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/live/cam1.flv"));

        FullHttpResponse resp = expectResponse();
        assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED, resp.status());
        assertTrue(body(resp).startsWith("405"), body(resp));
        resp.release();
    }

    @Test
    void rejectsIllegalPathWith400(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        ch.writeInbound(get("/live/"));

        FullHttpResponse resp = expectResponse();
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
        resp.release();
    }

    @Test
    void rejectsForeignOriginWith403(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live", "server.corsOrigins=http://a.example");
        FullHttpRequest req = get("/live/cam1.flv");
        req.headers().set(HttpHeaderNames.ORIGIN, "http://evil.example");
        req.headers().set(HttpHeaderNames.HOST, "gw.local:8080");
        ch.writeInbound(req);

        FullHttpResponse resp = expectResponse();
        assertEquals(HttpResponseStatus.FORBIDDEN, resp.status());
        resp.release();
    }

    @Test
    void rejectsBadTokenWith401(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live", "auth.token=sekrit");
        ch.writeInbound(get("/live/cam1.flv?token=nope"));

        FullHttpResponse resp = expectResponse();
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        resp.release();
    }

    @Test
    void rejectsUnknownStreamWith404(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        ch.writeInbound(get("/live/nope.flv"));

        FullHttpResponse resp = expectResponse();
        assertEquals(HttpResponseStatus.NOT_FOUND, resp.status());
        assertEquals("404 unknown stream", body(resp));
        // 错误响应带 Content-Length 且默认 keep-alive，不关闭连接
        assertEquals("18", resp.headers().get(HttpHeaderNames.CONTENT_LENGTH));
        assertTrue(resp.headers().containsValue(HttpHeaderNames.CONNECTION, "keep-alive", true));
        assertTrue(ch.isActive());
        resp.release();
    }

    @Test
    void passesWebSocketUpgradeThrough(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        FullHttpRequest req = get("/live/cam1.flv");
        req.headers().set(HttpHeaderNames.UPGRADE, "websocket");
        ch.writeInbound(req);

        // 未被接管：原对象继续下传给 WebSocketServerProtocolHandler，且无响应写出
        assertSame(req, ch.readInbound());
        assertNull(ch.readOutbound());
        req.release();
    }

    @Test
    void passesNonLivePathThrough(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        FullHttpRequest req = get("/stats");
        ch.writeInbound(req);

        assertSame(req, ch.readInbound());
        req.release();
    }

    @Test
    void passesNonHttpRequestThrough(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        ByteBuf other = Unpooled.wrappedBuffer("ping".getBytes(StandardCharsets.US_ASCII));
        ch.writeInbound(other);

        assertSame(other, ch.readInbound());
        other.release();
    }
}
