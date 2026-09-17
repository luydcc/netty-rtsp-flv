package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 服务端侧解码器测试：与 {@link RtspTcpDecoder} 共用切分逻辑，文本消息解析为请求。 */
class RtspServerDecoderTest {

    private EmbeddedChannel ch;

    @BeforeEach
    void setUp() {
        ch = new EmbeddedChannel(new RtspServerDecoder());
    }

    @AfterEach
    void tearDown() {
        ch.finishAndReleaseAll();
    }

    private void write(String text) {
        ch.writeInbound(Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void decodesRequest() {
        write("OPTIONS rtsp://10.0.0.1:8554/live/cam1 RTSP/1.0\r\nCSeq: 1\r\n\r\n");
        RtspServerRequest req = assertInstanceOf(RtspServerRequest.class, ch.readInbound());
        assertEquals("OPTIONS", req.method());
        assertEquals("1", req.header("CSeq"));
        assertNull(ch.readInbound());
    }

    @Test
    void decodesRequestWithBody() {
        write("SET_PARAMETER rtsp://h/live/cam1 RTSP/1.0\r\nCSeq: 4\r\nContent-Length: 6\r\n\r\nabcdef");
        RtspServerRequest req = assertInstanceOf(RtspServerRequest.class, ch.readInbound());
        assertEquals("SET_PARAMETER", req.method());
        assertEquals("abcdef", req.body());
    }

    @Test
    void waitsForPartialInput() {
        write("SETUP rtsp://h/live/cam1/trackID=0 RTSP/1.0\r\nTra");
        assertNull(ch.readInbound());
        write("nsport: RTP/AVP/TCP;unicast;interleaved=0-1\r\nCSeq: 3\r\n\r\n");
        RtspServerRequest req = assertInstanceOf(RtspServerRequest.class, ch.readInbound());
        assertEquals("SETUP", req.method());
        assertEquals("RTP/AVP/TCP;unicast;interleaved=0-1", req.header("Transport"));
    }

    @Test
    void decodesClientRtcpAsInterleavedFrame() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte('$');
        in.writeByte(1); // 客户端上行的 RTCP 通道
        in.writeShort(3);
        in.writeBytes(new byte[]{1, 2, 3});
        ch.writeInbound(in);

        InterleavedFrame f = assertInstanceOf(InterleavedFrame.class, ch.readInbound());
        assertEquals(1, f.channel());
        assertFalse(f.isRtp());
        f.release();
    }

    @Test
    void decodesTwoRequestsInOneBuffer() {
        write("PLAY rtsp://h/live/cam1 RTSP/1.0\r\nCSeq: 5\r\n\r\n"
                + "GET_PARAMETER rtsp://h/live/cam1 RTSP/1.0\r\nCSeq: 6\r\n\r\n");
        RtspServerRequest first = assertInstanceOf(RtspServerRequest.class, ch.readInbound());
        RtspServerRequest second = assertInstanceOf(RtspServerRequest.class, ch.readInbound());
        assertEquals("PLAY", first.method());
        assertEquals("GET_PARAMETER", second.method());
        assertEquals("6", second.header("CSeq"));
        assertNull(ch.readInbound());
    }

    @Test
    void rejectsResponseOnServerSide() {
        assertThrows(Exception.class, () -> {
            write("RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n");
            ch.checkException();
        });
    }
}
