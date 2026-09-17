package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 播放准入校验测试：WS-FLV 与 HTTP-FLV 共用同一套规则。 */
class LiveAccessPolicyTest {

    private static LiveAccessPolicy policy(Path dir, String... lines) throws IOException {
        Path file = dir.resolve("test.properties");
        Files.writeString(file, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return new LiveAccessPolicy(GatewayConfig.load(file.toString()));
    }

    @Test
    void extractsStreamIdFromLivePath() {
        assertEquals("cam1", LiveAccessPolicy.streamId("/live/cam1.flv"));
        assertEquals("cam1", LiveAccessPolicy.streamId("/live/cam1"));
        assertEquals("cam_1-x.2", LiveAccessPolicy.streamId("/live/cam_1-x.2.flv"));
    }

    @Test
    void rejectsIllegalLivePath() {
        assertNull(LiveAccessPolicy.streamId(null));
        assertNull(LiveAccessPolicy.streamId(""));
        assertNull(LiveAccessPolicy.streamId("/"));
        assertNull(LiveAccessPolicy.streamId("/stats"));
        assertNull(LiveAccessPolicy.streamId("/live/"));
        assertNull(LiveAccessPolicy.streamId("/live/.flv"));
        assertNull(LiveAccessPolicy.streamId("/live/a/b.flv"));
        assertNull(LiveAccessPolicy.streamId("live/cam1.flv"));
    }

    @Test
    void tokenOptionalWhenNotConfigured(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "server.port=9100");
        assertTrue(p.tokenAllowed("/live/cam1.flv"));
        assertTrue(p.tokenAllowed("/live/cam1.flv?token=whatever"));
    }

    @Test
    void tokenMustMatchWhenConfigured(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "auth.token=sekrit");
        assertTrue(p.tokenAllowed("/live/cam1.flv?token=sekrit"));
        assertTrue(p.tokenAllowed("/live/cam1.flv?a=1&token=sekrit"));
        assertFalse(p.tokenAllowed("/live/cam1.flv"));
        assertFalse(p.tokenAllowed("/live/cam1.flv?token=other"));
        assertFalse(p.tokenAllowed("/live/cam1.flv?token="));
    }

    @Test
    void anyOriginAllowsEverything(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "server.corsOrigins=*");
        assertTrue(p.originAllowed("http://evil.example", "gw.local:8080"));
        assertTrue(p.originAllowed(null, null));
    }

    @Test
    void missingOriginAlwaysAllowed(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "server.corsOrigins=http://a.example");
        assertTrue(p.originAllowed(null, "gw.local:8080"));  // ffmpeg / curl / 后端服务
        assertTrue(p.originAllowed("", "gw.local:8080"));
    }

    @Test
    void whitelistAndSameHostAllowed(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "server.corsOrigins=http://a.example,https://b.example:9000");
        assertTrue(p.originAllowed("http://a.example", "gw.local:8080"));
        assertTrue(p.originAllowed("https://b.example:9000", "gw.local:8080"));
        assertFalse(p.originAllowed("http://c.example", "gw.local:8080"));
        // 网关自带页面按同源放行（只比较 host，忽略端口与协议）
        assertTrue(p.originAllowed("http://gw.local:5555", "gw.local:8080"));
        assertTrue(p.originAllowed("http://GW.LOCAL", "gw.local:8080"));
        assertFalse(p.originAllowed("http://other.local", "gw.local:8080"));
        assertFalse(p.originAllowed("http://gw.local.evil.example", "gw.local:8080"));
    }

    @Test
    void readsOriginAndHostFromHeaders(@TempDir Path dir) throws IOException {
        LiveAccessPolicy p = policy(dir, "server.corsOrigins=http://a.example");
        HttpHeaders ok = new DefaultHttpHeaders()
                .set(HttpHeaderNames.ORIGIN, "http://a.example")
                .set(HttpHeaderNames.HOST, "gw.local:8080");
        HttpHeaders bad = new DefaultHttpHeaders()
                .set(HttpHeaderNames.ORIGIN, "http://c.example")
                .set(HttpHeaderNames.HOST, "gw.local:8080");
        assertTrue(p.originAllowed(ok));
        assertFalse(p.originAllowed(bad));
        assertTrue(p.originAllowed(new DefaultHttpHeaders())); // 无 Origin 头
    }
}
