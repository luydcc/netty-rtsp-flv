package com.mediagw.config;

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

class GatewayConfigTest {

    @Test
    void loadsStreamsAndSettings(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("test.properties");
        Files.writeString(file, String.join("\n",
                "server.port=9000",
                "auth.token=sekrit",
                "stream.cam1=rtsp://admin:pw@192.168.1.10:554/stream1",
                "stream.cam2 = rtsp://192.168.1.11:554/s2 ",
                "stream.=rtsp://ignored",
                "session.closeDelaySec=10",
                "gop.cacheMaxBytes=1024",
                "server.writeBufferLowKb=notanumber",
                ""), StandardCharsets.UTF_8);

        GatewayConfig cfg = GatewayConfig.load(file.toString());
        assertEquals(9000, cfg.port());
        assertEquals("sekrit", cfg.authToken());
        assertEquals(2, cfg.streams().size());
        assertEquals("rtsp://admin:pw@192.168.1.10:554/stream1", cfg.streamUrl("cam1"));
        assertEquals("rtsp://192.168.1.11:554/s2", cfg.streamUrl("cam2")); // trim
        assertEquals(10, cfg.closeDelaySec());
        assertEquals(1024, cfg.gopCacheMaxBytes());
        assertEquals(256, cfg.writeBufferLowKb()); // 非法数字回落默认值
        assertNull(cfg.streamUrl("nope"));
    }

    @Test
    void loadsRtspForwardSettings(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("test.properties");
        Files.writeString(file, String.join("\n",
                "rtsp.forwardPort=8555",
                "rtsp.forwardUdp=false",
                "rtsp.forwardMaxPayload=1200",
                "rtsp.forwardSessionTimeoutSec=notanumber",
                ""), StandardCharsets.UTF_8);

        GatewayConfig cfg = GatewayConfig.load(file.toString());
        assertEquals(8555, cfg.rtspForwardPort());
        assertFalse(cfg.rtspForwardUdp());
        assertEquals(1200, cfg.rtspForwardMaxPayload());
        assertEquals(60, cfg.rtspForwardSessionTimeoutSec()); // 非法数字回落默认值
    }

    @Test
    void missingFileUsesDefaults() {
        GatewayConfig cfg = GatewayConfig.load("/nonexistent/path/x.properties");
        assertEquals(8080, cfg.port());
        assertTrue(cfg.streams().isEmpty());
        assertEquals("", cfg.authToken());
        assertEquals(30, cfg.closeDelaySec());
        assertEquals(8554, cfg.rtspForwardPort());
        assertTrue(cfg.rtspForwardUdp());
        assertEquals(1400, cfg.rtspForwardMaxPayload());
        assertEquals(60, cfg.rtspForwardSessionTimeoutSec());
    }

    @Test
    void sanitizeUrlHidesPassword() {
        assertEquals("rtsp://admin:***@192.168.1.10:554/stream1",
                GatewayConfig.sanitizeUrl("rtsp://admin:pw@192.168.1.10:554/stream1"));
        assertEquals("rtsp://user:***@host/p?a=b",
                GatewayConfig.sanitizeUrl("rtsp://user:p%40ss@host/p?a=b"));
        // 无凭据不变
        assertEquals("rtsp://192.168.1.10:554/s", GatewayConfig.sanitizeUrl("rtsp://192.168.1.10:554/s"));
        assertEquals("", GatewayConfig.sanitizeUrl(null));
    }
}
