package com.mediagw.session;

import com.mediagw.config.GatewayConfig;
import org.junit.jupiter.api.AfterEach;
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

class StreamSessionManagerTest {

    private StreamSessionManager manager;

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
            manager = null;
        }
    }

    private StreamSessionManager newManager(Path dir) throws IOException {
        Path file = dir.resolve("test.properties");
        Files.writeString(file, String.join("\n",
                "server.port=9100",
                "stream.cam1=rtsp://admin:pw@192.168.1.10:554/stream1",
                "session.closeDelaySec=7",
                ""), StandardCharsets.UTF_8);
        manager = new StreamSessionManager(GatewayConfig.load(file.toString()));
        return manager;
    }

    @Test
    void addsTemporaryStreamResolvableByUrlLookup(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        StreamSessionManager.AddResult r = m.addDynamicStream("cam9", "rtsp://10.0.0.5:554/live");
        assertTrue(r.ok(), r.message());
        assertEquals("rtsp://10.0.0.5:554/live", m.streamUrl("cam9"));
        // 配置文件中的流仍优先可查
        assertEquals("rtsp://admin:pw@192.168.1.10:554/stream1", m.streamUrl("cam1"));
        assertNull(m.streamUrl("nope"));
    }

    @Test
    void trimsSurroundingWhitespace(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        assertTrue(m.addDynamicStream("  cam9 ", "  rtsp://10.0.0.5:554/live  ").ok());
        assertEquals("rtsp://10.0.0.5:554/live", m.streamUrl("cam9"));
    }

    @Test
    void rejectsIllegalStreamIds(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        assertFalse(m.addDynamicStream("", "rtsp://h/p").ok());
        assertFalse(m.addDynamicStream(null, "rtsp://h/p").ok());
        assertFalse(m.addDynamicStream("a/b", "rtsp://h/p").ok());
        assertFalse(m.addDynamicStream("a b", "rtsp://h/p").ok());
        assertFalse(m.addDynamicStream("a?b", "rtsp://h/p").ok());
        assertFalse(m.addDynamicStream("x".repeat(65), "rtsp://h/p").ok());
        assertTrue(m.addDynamicStream("Cam_9-x.1", "rtsp://h/p").ok());
        assertNull(m.streamUrl("a/b"));
    }

    @Test
    void rejectsNonRtspUrls(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        assertFalse(m.addDynamicStream("a", null).ok());
        assertFalse(m.addDynamicStream("b", "").ok());
        assertFalse(m.addDynamicStream("c", "http://10.0.0.5/live").ok());
        assertFalse(m.addDynamicStream("d", "rtsp://").ok());           // 无 host
        assertFalse(m.addDynamicStream("e", "10.0.0.5:554/live").ok()); // 无 scheme
        assertFalse(m.addDynamicStream("f", "rtsp://ho st/p").ok());    // URI 非法
        assertNull(m.streamUrl("a"));
    }

    @Test
    void rejectsConflictWithConfiguredStream(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        StreamSessionManager.AddResult r = m.addDynamicStream("cam1", "rtsp://10.0.0.9:554/x");
        assertFalse(r.ok());
        // 配置文件中的地址未被覆盖
        assertEquals("rtsp://admin:pw@192.168.1.10:554/stream1", m.streamUrl("cam1"));
    }

    @Test
    void rejectsDuplicateTemporaryStream(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        assertTrue(m.addDynamicStream("cam9", "rtsp://10.0.0.5:554/live").ok());
        assertFalse(m.addDynamicStream("cam9", "rtsp://10.0.0.5:554/live").ok());
        assertFalse(m.addDynamicStream("cam9", "rtsp://10.0.0.6:554/other").ok());
        assertEquals("rtsp://10.0.0.5:554/live", m.streamUrl("cam9"));
    }

    @Test
    void streamsJsonCoversConfigAndTemporaryStreams(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        m.addDynamicStream("cam9", "rtsp://user:secret@10.0.0.5:554/live");
        String json = m.streamsJson();
        assertTrue(json.contains("\"closeDelaySec\":7"), json);
        assertTrue(json.contains("\"sessions\":0"), json);
        assertTrue(json.contains("\"streamId\":\"cam1\""), json);
        assertTrue(json.contains("\"source\":\"config\""), json);
        assertTrue(json.contains("\"streamId\":\"cam9\""), json);
        assertTrue(json.contains("\"source\":\"dynamic\""), json);
        // 未拉流的流没有会话
        assertTrue(json.contains("\"active\":false"), json);
        assertTrue(json.contains("\"session\":null"), json);
        // 密码脱敏
        assertTrue(json.contains("rtsp://user:***@10.0.0.5:554/live"), json);
        assertFalse(json.contains("secret"), json);
    }

    @Test
    void temporaryStreamsAreNotPersisted(@TempDir Path dir) throws IOException {
        StreamSessionManager m = newManager(dir);
        m.addDynamicStream("cam9", "rtsp://10.0.0.5:554/live");
        m.shutdown();
        // 重新加载同一配置：临时流消失（仅存内存）
        manager = new StreamSessionManager(GatewayConfig.load(dir.resolve("test.properties").toString()));
        assertNull(manager.streamUrl("cam9"));
        assertFalse(manager.streamsJson().contains("cam9"));
    }
}
