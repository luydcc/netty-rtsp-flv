package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 服务端侧 RTSP 响应编码测试。 */
class RtspServerResponseTest {

    @Test
    void reasonPhraseCoversUsedStatusCodes() {
        assertEquals("OK", RtspServerResponse.reasonOf(200));
        assertEquals("Bad Request", RtspServerResponse.reasonOf(400));
        assertEquals("Unauthorized", RtspServerResponse.reasonOf(401));
        assertEquals("Not Found", RtspServerResponse.reasonOf(404));
        assertEquals("Method Not Allowed", RtspServerResponse.reasonOf(405));
        assertEquals("Session Not Found", RtspServerResponse.reasonOf(454));
        assertEquals("Method Not Valid in This State", RtspServerResponse.reasonOf(455));
        assertEquals("Unsupported Transport", RtspServerResponse.reasonOf(461));
        assertEquals("Not Implemented", RtspServerResponse.reasonOf(501));
        assertEquals("Service Unavailable", RtspServerResponse.reasonOf(503));
        assertEquals("Unknown", RtspServerResponse.reasonOf(599));
    }

    @Test
    void encodesStatusLineAndHeadersInOrder() {
        String text = RtspServerResponse.ok(7)
                .header("Public", "OPTIONS, DESCRIBE")
                .header("Session", "1A2B3C4D")
                .encodeText();
        assertEquals("RTSP/1.0 200 OK\r\n"
                + "CSeq: 7\r\n"
                + "Public: OPTIONS, DESCRIBE\r\n"
                + "Session: 1A2B3C4D\r\n"
                + "\r\n", text);
    }

    @Test
    void encodesErrorWithoutBody() {
        String text = RtspServerResponse.of(404).header("CSeq", "3").encodeText();
        assertEquals("RTSP/1.0 404 Not Found\r\nCSeq: 3\r\n\r\n", text);
        assertTrue(text.endsWith("\r\n\r\n"));
    }

    @Test
    void bodyAddsContentTypeAndLength() {
        RtspServerResponse resp = RtspServerResponse.ok(2)
                .header("Content-Base", "rtsp://h/live/cam1/")
                .body("application/sdp", "v=0\r\n");
        String text = resp.encodeText();

        assertEquals("v=0\r\n", resp.body());
        assertEquals(200, resp.status());
        assertTrue(text.contains("Content-Type: application/sdp\r\n"), text);
        assertTrue(text.contains("Content-Length: 5\r\n"), text);
        assertTrue(text.endsWith("\r\nv=0\r\n"), text);
    }

    @Test
    void nullBodyBecomesEmpty() {
        RtspServerResponse resp = RtspServerResponse.ok(1).body("application/sdp", null);
        assertEquals("", resp.body());
        assertFalse(resp.encodeText().contains("Content-Length"), resp.encodeText());
    }

    @Test
    void contentLengthCountsUtf8Bytes() {
        String text = RtspServerResponse.ok(1).body("text/plain", "中文").encodeText();
        assertTrue(text.contains("Content-Length: 6\r\n"), text); // 2 个汉字各 3 字节
    }

    @Test
    void encodeProducesSameBytesAsText() {
        RtspServerResponse resp = RtspServerResponse.of(461).header("CSeq", "9");
        ByteBuf buf = resp.encode(UnpooledByteBufAllocator.DEFAULT);
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            assertEquals(resp.encodeText(), new String(bytes, StandardCharsets.UTF_8));
            assertEquals("RTSP/1.0 461 Unsupported Transport", resp.toString());
        } finally {
            buf.release();
        }
    }
}
