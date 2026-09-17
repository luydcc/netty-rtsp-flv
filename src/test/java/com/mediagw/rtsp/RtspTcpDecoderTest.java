package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RtspTcpDecoderTest {

    private EmbeddedChannel ch;

    @BeforeEach
    void setUp() {
        ch = new EmbeddedChannel(new RtspTcpDecoder());
    }

    @AfterEach
    void tearDown() {
        ch.finishAndReleaseAll();
    }

    private void write(String text) {
        ch.writeInbound(Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.US_ASCII)));
    }

    @Test
    void decodesTextResponse() {
        write("RTSP/1.0 200 OK\r\nCSeq: 1\r\nPublic: OPTIONS, DESCRIBE\r\n\r\n");
        RtspResponse r = assertInstanceOf(RtspResponse.class, ch.readInbound());
        assertEquals(200, r.status());
        assertEquals("OK", r.reason());
        assertEquals("1", r.header("CSeq"));
        assertEquals("1", r.header("cseq")); // 大小写不敏感
        assertEquals("", r.body());
        assertNull(ch.readInbound());
    }

    @Test
    void decodesResponseWithBody() {
        write("RTSP/1.0 200 OK\r\nCSeq: 2\r\nContent-Type: application/sdp\r\nContent-Length: 5\r\n\r\nhello");
        RtspResponse r = assertInstanceOf(RtspResponse.class, ch.readInbound());
        assertEquals("hello", r.body());
        assertEquals("application/sdp", r.header("Content-Type"));
    }

    @Test
    void waitsForPartialInput() {
        write("RTSP/1.0 401 Unauthorized\r\nCSeq: 3\r\nWWW-Au");
        assertNull(ch.readInbound()); // 头未完整
        write("thenticate: Basic realm=\"cam\"\r\nContent-Length: 3\r\n\r\nabc");
        RtspResponse r = assertInstanceOf(RtspResponse.class, ch.readInbound());
        assertEquals(401, r.status());
        assertEquals("Basic realm=\"cam\"", r.header("WWW-Authenticate"));
        assertEquals("abc", r.body());
    }

    @Test
    void bodyArrivesInChunks() {
        write("RTSP/1.0 200 OK\r\nCSeq: 4\r\nContent-Length: 8\r\n\r\nABCD");
        assertNull(ch.readInbound()); // body 未到齐
        write("EFGH");
        RtspResponse r = assertInstanceOf(RtspResponse.class, ch.readInbound());
        assertEquals("ABCDEFGH", r.body());
    }

    @Test
    void decodesInterleavedFrame() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte('$');
        in.writeByte(0);   // channel 0 (RTP)
        in.writeShort(4);
        in.writeBytes(new byte[]{'a', 'b', 'c', 'd'});
        ch.writeInbound(in);

        InterleavedFrame f = assertInstanceOf(InterleavedFrame.class, ch.readInbound());
        assertEquals(0, f.channel());
        assertTrue(f.isRtp());
        byte[] payload = new byte[f.payload().readableBytes()];
        f.payload().getBytes(f.payload().readerIndex(), payload);
        assertArrayEquals(new byte[]{'a', 'b', 'c', 'd'}, payload);
        f.release();
    }

    @Test
    void rtcpChannelIsNotRtp() {
        ByteBuf in = Unpooled.buffer();
        in.writeByte('$');
        in.writeByte(1); // channel 1 (RTCP)
        in.writeShort(2);
        in.writeBytes(new byte[]{1, 2});
        ch.writeInbound(in);
        InterleavedFrame f = assertInstanceOf(InterleavedFrame.class, ch.readInbound());
        assertFalse(f.isRtp());
        f.release();
    }

    @Test
    void interleavedFrameSplitAcrossWrites() {
        ByteBuf head = Unpooled.buffer();
        head.writeByte('$');
        head.writeByte(2);
        ch.writeInbound(head);
        assertNull(ch.readInbound());

        ByteBuf rest = Unpooled.buffer();
        rest.writeShort(3);
        rest.writeBytes(new byte[]{9, 8, 7});
        ch.writeInbound(rest);
        InterleavedFrame f = assertInstanceOf(InterleavedFrame.class, ch.readInbound());
        assertEquals(2, f.channel());
        assertEquals(3, f.payload().readableBytes());
        f.release();
    }

    @Test
    void mixedTextAndBinaryInOneBuffer() {
        ByteBuf in = Unpooled.buffer();
        in.writeBytes("RTSP/1.0 200 OK\r\nCSeq: 5\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        in.writeByte('$');
        in.writeByte(0);
        in.writeShort(2);
        in.writeBytes(new byte[]{0x11, 0x22});
        ch.writeInbound(in);

        RtspResponse r = assertInstanceOf(RtspResponse.class, ch.readInbound());
        assertEquals(200, r.status());
        InterleavedFrame f = assertInstanceOf(InterleavedFrame.class, ch.readInbound());
        assertEquals(2, f.payload().readableBytes());
        f.release();
        assertNull(ch.readInbound());
    }

    @Test
    void malformedStatusLineFails() {
        assertThrows(Exception.class, () -> {
            write("HTTP/1.1 200 OK\r\n\r\n");
            ch.checkException();
        });
    }

    @Test
    void responseParseApi() {
        RtspResponse r = RtspResponse.parse("RTSP/1.0 404 Not Found\r\nCSeq: 9\r\nSession: abc;timeout=60\r\n", "");
        assertEquals(404, r.status());
        assertEquals("Not Found", r.reason());
        assertNotNull(r.headers());
        assertEquals("abc;timeout=60", r.header("session"));
    }

    @Test
    void parseRejectsGarbage() {
        assertThrows(RtspException.class, () -> RtspResponse.parse("GARBAGE\r\n\r\n", ""));
        assertThrows(RtspException.class, () -> RtspResponse.parse("RTSP/1.0 xyz\r\n\r\n", ""));
    }

    @Test
    void malformedStatusLineMessageIsBrief() {
        // 对端返回裸二进制时，异常消息不能把整段乱码带进日志
        String garbage = "BAD\u0001\u007f" + "\ufffd".repeat(200);
        RtspException e = assertThrows(RtspException.class, () -> RtspResponse.parse(garbage + "\r\n\r\n", ""));
        String msg = e.getMessage();
        assertTrue(msg.length() < 160, msg);
        assertTrue(msg.contains("\\x01"), msg);
        assertTrue(msg.contains("...(205 chars)"), msg);
        assertFalse(msg.contains("\r") || msg.contains("\n"), msg);
        // 正常的短文本原样保留，便于定位（如误连到 HTTP 端口）
        RtspException http = assertThrows(RtspException.class,
                () -> RtspResponse.parse("HTTP/1.1 200 OK\r\n\r\n", ""));
        assertTrue(http.getMessage().contains("HTTP/1.1 200 OK"), http.getMessage());
    }
}
