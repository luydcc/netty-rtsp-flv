package com.mediagw.rtp;

import com.mediagw.testutil.TestMedia;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RtpPacketTest {

    @Test
    void parsesBasicHeader() {
        byte[] payload = {1, 2, 3, 4, 5};
        ByteBuf buf = TestMedia.rtp(300, 123456, true, 96, payload);
        try {
            RtpPacket p = RtpPacket.parse(buf);
            assertNotNull(p);
            assertEquals(300, p.seq());
            assertEquals(123456, p.timestamp());
            assertEquals(TestMedia.SSRC, p.ssrc());
            assertTrue(p.marker());
            assertEquals(96, p.payloadType());
            byte[] got = new byte[p.payload().readableBytes()];
            p.payload().getBytes(p.payload().readerIndex(), got);
            assertArrayEquals(payload, got);
        } finally {
            buf.release();
        }
    }

    @Test
    void skipsCsrcList() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x82); // V=2, CC=2
        buf.writeByte(96);
        buf.writeShort(7);
        buf.writeInt(999);
        buf.writeInt(1);
        buf.writeInt(0x11111111); // CSRC 1
        buf.writeInt(0x22222222); // CSRC 2
        buf.writeBytes(new byte[]{9, 8, 7});
        RtpPacket p = RtpPacket.parse(buf);
        assertNotNull(p);
        assertEquals(3, p.payload().readableBytes());
        assertEquals(9, p.payload().getByte(p.payload().readerIndex()));
        buf.release();
    }

    @Test
    void skipsExtension() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x90); // V=2, X=1
        buf.writeByte(0x60); // PT=96, marker=0
        buf.writeShort(8);
        buf.writeInt(1000);
        buf.writeInt(2);
        buf.writeShort(0xBEDE);
        buf.writeShort(2); // 2 x 32bit words
        buf.writeBytes(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        buf.writeBytes(new byte[]{0x55, 0x66});
        RtpPacket p = RtpPacket.parse(buf);
        assertNotNull(p);
        assertFalse(p.marker());
        assertEquals(2, p.payload().readableBytes());
        assertEquals(0x55, p.payload().getByte(p.payload().readerIndex()));
        buf.release();
    }

    @Test
    void stripsPadding() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0xA0); // V=2, P=1
        buf.writeByte(0x80 | 96); // marker + PT
        buf.writeShort(9);
        buf.writeInt(2000);
        buf.writeInt(3);
        buf.writeBytes(new byte[]{1, 2, 3}); // 载荷
        buf.writeBytes(new byte[]{0, 0, 3}); // 3 字节填充，末字节=3
        RtpPacket p = RtpPacket.parse(buf);
        assertNotNull(p);
        assertTrue(p.marker());
        assertEquals(3, p.payload().readableBytes());
        buf.release();
    }

    @Test
    void rejectsPaddingClaimingWholePayload() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0xA0); // V=2, P=1
        buf.writeByte(96);
        buf.writeShort(9);
        buf.writeInt(2000);
        buf.writeInt(3);
        buf.writeBytes(new byte[]{1, 2});
        buf.writeBytes(new byte[]{0, 0, 5}); // 声称 5 字节填充 = 载荷+填充全部 → 空载荷
        assertNull(RtpPacket.parse(buf));
        buf.release();
    }

    @Test
    void rejectsInvalidPackets() {
        assertNull(RtpPacket.parse(Unpooled.wrappedBuffer(new byte[]{1, 2, 3}))); // 太短
        ByteBuf badVersion = TestMedia.rtp(1, 1, false, 96, new byte[]{1});
        badVersion.setByte(0, 0x40); // V=1
        assertNull(RtpPacket.parse(badVersion));
        badVersion.release();
    }
}
