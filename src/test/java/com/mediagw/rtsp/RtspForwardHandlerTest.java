package com.mediagw.rtsp;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.StreamSessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RTSP 转发 Handler 测试：路径/Transport 解析辅助方法，以及不触发拉流的信令分支
 * （成功链路 DESCRIBE→SETUP→PLAY 由 e2e 校验器 {@code RtspForwardVerifier} 覆盖）。
 */
class RtspForwardHandlerTest {

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
        ch = new EmbeddedChannel(new RtspServerDecoder(), new RtspForwardHandler(manager, config, null));
    }

    private void request(String text) {
        ch.writeInbound(Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.US_ASCII)));
    }

    /** 读出服务端写回的响应文本。 */
    private String response() {
        ByteBuf buf = ch.readOutbound();
        if (buf == null) {
            return null;
        }
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        } finally {
            buf.release();
        }
    }

    // ===== 路径与 Transport 解析 =====

    @Test
    void streamIdAcceptsLiveAndBarePrefix() {
        assertEquals("cam1", RtspForwardHandler.streamId("/live/cam1"));
        assertEquals("cam1", RtspForwardHandler.streamId("/cam1"));
        assertEquals("cam1", RtspForwardHandler.streamId("live/cam1"));
        // SETUP 常带控制后缀
        assertEquals("cam1", RtspForwardHandler.streamId("/live/cam1/trackID=0"));
        assertEquals("cam1", RtspForwardHandler.streamId("/live/cam1/"));
    }

    @Test
    void streamIdRejectsEmptyPath() {
        assertNull(RtspForwardHandler.streamId(null));
        assertNull(RtspForwardHandler.streamId(""));
        assertNull(RtspForwardHandler.streamId("/"));
        assertNull(RtspForwardHandler.streamId("/live/"));
    }

    @Test
    void portPairParsesTransportParameters() {
        assertArrayEquals(new int[]{0, 1},
                RtspForwardHandler.portPair("RTP/AVP/TCP;unicast;interleaved=0-1", "interleaved", 0, 1));
        assertArrayEquals(new int[]{2, 3},
                RtspForwardHandler.portPair("RTP/AVP/TCP;interleaved=2-3;ssrc=1A2B3C4D", "interleaved", 0, 1));
        assertArrayEquals(new int[]{5000, 5001},
                RtspForwardHandler.portPair("RTP/AVP;unicast;client_port=5000-5001", "client_port", -1, -1));
        // 只给一个端口时第二个取 +1
        assertArrayEquals(new int[]{5000, 5001},
                RtspForwardHandler.portPair("RTP/AVP;client_port=5000", "client_port", -1, -1));
        // 多个 Transport 首选（逗号分隔）
        assertArrayEquals(new int[]{6000, 6001},
                RtspForwardHandler.portPair("RTP/AVP;client_port=6000-6001,RTP/AVP/TCP", "client_port", -1, -1));
    }

    @Test
    void portPairFallsBackToDefaults() {
        assertArrayEquals(new int[]{-1, -1},
                RtspForwardHandler.portPair("RTP/AVP;unicast", "client_port", -1, -1));
        assertArrayEquals(new int[]{0, 1},
                RtspForwardHandler.portPair("RTP/AVP/TCP;unicast", "interleaved", 0, 1));
        assertArrayEquals(new int[]{-1, -1},
                RtspForwardHandler.portPair("RTP/AVP;client_port=abc", "client_port", -1, -1));
    }

    // ===== 信令分支 =====

    @Test
    void optionsListsPublicMethods(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("OPTIONS rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 1\r\n\r\n");

        String resp = response();
        assertTrue(resp.startsWith("RTSP/1.0 200 OK\r\n"), resp);
        assertTrue(resp.contains("CSeq: 1\r\n"), resp);
        assertTrue(resp.contains("Public: OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN, GET_PARAMETER\r\n"), resp);
    }

    @Test
    void unknownMethodYields501(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("RECORD rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 2\r\n\r\n");

        String resp = response();
        assertTrue(resp.startsWith("RTSP/1.0 501 Not Implemented\r\n"), resp);
        assertTrue(resp.contains("CSeq: 2\r\n"), resp);
        assertTrue(resp.contains("Allow: "), resp);
    }

    @Test
    void describeUnknownStreamYields404(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("DESCRIBE rtsp://127.0.0.1:8554/live/nope RTSP/1.0\r\nCSeq: 3\r\nAccept: application/sdp\r\n\r\n");

        assertTrue(response().startsWith("RTSP/1.0 404 Not Found\r\n"));
    }

    @Test
    void setupBeforeDescribeYields454(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("SETUP rtsp://127.0.0.1:8554/live/cam1/trackID=0 RTSP/1.0\r\nCSeq: 4\r\n"
                + "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n\r\n");

        assertTrue(response().startsWith("RTSP/1.0 454 Session Not Found\r\n"));
    }

    @Test
    void playBeforeSetupYields454(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("PLAY rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 5\r\n\r\n");

        assertTrue(response().startsWith("RTSP/1.0 454 Session Not Found\r\n"));
    }

    @Test
    void teardownWithoutSessionStillAnswers200(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("TEARDOWN rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 6\r\n\r\n");

        assertTrue(response().startsWith("RTSP/1.0 200 OK\r\n"));
        assertFalse(ch.isActive(), "TEARDOWN 后连接关闭");
    }

    @Test
    void methodsOtherThanOptionsRequireToken(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live", "auth.token=sekrit");
        request("DESCRIBE rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 7\r\n\r\n");

        String resp = response();
        assertTrue(resp.startsWith("RTSP/1.0 401 Unauthorized\r\n"), resp);
        assertTrue(resp.contains("WWW-Authenticate: Basic realm=\"netty-rtsp-flv\"\r\n"), resp);

        // OPTIONS 无需鉴权
        request("OPTIONS rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 8\r\n\r\n");
        assertTrue(response().startsWith("RTSP/1.0 200 OK\r\n"));
    }

    @Test
    void tokenInQueryPassesAuthorization(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live", "auth.token=sekrit");
        request("DESCRIBE rtsp://127.0.0.1:8554/live/nope?token=sekrit RTSP/1.0\r\nCSeq: 9\r\n\r\n");

        // 已通过鉴权，因流不存在而 404（不是 401）
        assertTrue(response().startsWith("RTSP/1.0 404 Not Found\r\n"));
    }

    @Test
    void basicAuthPassesAuthorization(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live", "auth.token=sekrit");
        String basic = java.util.Base64.getEncoder()
                .encodeToString("user:sekrit".getBytes(StandardCharsets.UTF_8));
        request("DESCRIBE rtsp://127.0.0.1:8554/live/nope RTSP/1.0\r\nCSeq: 10\r\n"
                + "Authorization: Basic " + basic + "\r\n\r\n");

        assertTrue(response().startsWith("RTSP/1.0 404 Not Found\r\n"));
    }

    @Test
    void clientRtcpFramesAreDropped(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        ByteBuf in = Unpooled.buffer();
        in.writeByte('$');
        in.writeByte(1);
        in.writeShort(2);
        in.writeBytes(new byte[]{1, 2});
        ch.writeInbound(in);

        assertNull(ch.readOutbound()); // 不产生任何响应
        assertNull(ch.readInbound());
    }

    @Test
    void missingCSeqDefaultsToZero(@TempDir Path dir) throws IOException {
        newChannel(dir, "stream.cam1=rtsp://10.0.0.5:554/live");
        request("OPTIONS rtsp://127.0.0.1:8554/live/cam1 RTSP/1.0\r\n\r\n");

        assertTrue(response().contains("CSeq: 0\r\n"));
    }
}
