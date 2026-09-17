package com.mediagw.rtp;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.testutil.TestMedia;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** H.264 / H.265 RTP 打包测试：单包、FU 分片、参数集带内插入、序号与时间戳。 */
class RtpPacketizerTest {

    private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
    private static final int PT = 96;
    private static final int SSRC = 0x1234ABCD;
    /** 打包载荷上限（基类最小 64），便于用小 NALU 触发分片。 */
    private static final int MAX_PAYLOAD = 64;

    private final List<ByteBuf> produced = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ByteBuf b : produced) {
            b.release();
        }
        produced.clear();
    }

    private List<ByteBuf> packetize(RtpPacketizer p, AccessUnit au) {
        p.packetize(au, produced::add);
        return new ArrayList<>(produced);
    }

    private static ParamSets h264Params() {
        return new ParamSets(CodecType.H264, List.of(),
                List.of(TestMedia.H264_SPS), List.of(TestMedia.H264_PPS));
    }

    private static ParamSets h265Params() {
        return new ParamSets(CodecType.H265, List.of(TestMedia.hevcVps()),
                List.of(TestMedia.hevcSps()), List.of(TestMedia.hevcPps()));
    }

    // ===== RTP 头字段 =====

    private static int seq(ByteBuf b) {
        return b.getUnsignedShort(2);
    }

    private static boolean marker(ByteBuf b) {
        return (b.getByte(1) & 0x80) != 0;
    }

    private static int payloadType(ByteBuf b) {
        return b.getByte(1) & 0x7F;
    }

    private static long timestamp(ByteBuf b) {
        return b.getUnsignedInt(4);
    }

    private static byte[] payload(ByteBuf b) {
        byte[] out = new byte[b.readableBytes() - 12];
        b.getBytes(12, out);
        return out;
    }

    @Test
    void writesRtpHeader() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        List<ByteBuf> out = packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_SLICE), false));

        assertEquals(1, out.size());
        ByteBuf b = out.get(0);
        assertEquals(2, (b.getByte(0) >> 6) & 3);   // V=2
        assertEquals(0, b.getByte(0) & 0x0F);        // CC=0
        assertEquals(PT, payloadType(b));
        assertEquals(SSRC, b.getInt(8));
        assertTrue(marker(b));                       // 访问单元最后一个包置 marker
        assertArrayEquals(TestMedia.H264_SLICE, payload(b));
    }

    @Test
    void sequenceNumberIncrements() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_SLICE), false));
        packetize(p, new AccessUnit(4600, List.of(TestMedia.H264_SLICE), false));

        assertEquals(2, produced.size());
        assertEquals((seq(produced.get(0)) + 1) & 0xFFFF, seq(produced.get(1)));
    }

    @Test
    void timestampFollowsUpstreamDelta() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_SLICE), false));
        packetize(p, new AccessUnit(4600, List.of(TestMedia.H264_SLICE), false));

        long first = timestamp(produced.get(0));
        assertEquals((first + 3600) & 0xFFFFFFFFL, timestamp(produced.get(1)));
    }

    @Test
    void timestampWrapsAroundUnsigned() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(0xFFFFFFF0L, List.of(TestMedia.H264_SLICE), false));
        packetize(p, new AccessUnit(0x10L, List.of(TestMedia.H264_SLICE), false));

        assertEquals((timestamp(produced.get(0)) + 0x20) & 0xFFFFFFFFL, timestamp(produced.get(1)));
    }

    @Test
    void upstreamTimestampResetDoesNotRewind() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(900000, List.of(TestMedia.H264_SLICE), false));
        packetize(p, new AccessUnit(0, List.of(TestMedia.H264_SLICE), false)); // 重连后归零

        assertEquals(timestamp(produced.get(0)), timestamp(produced.get(1)));
    }

    // ===== H.264 =====

    @Test
    void h264InsertsParamSetsBeforeKeyFrame() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        p.updateParams(h264Params());
        packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_IDR), true));

        assertEquals(3, produced.size()); // SPS + PPS + IDR
        assertArrayEquals(TestMedia.H264_SPS, payload(produced.get(0)));
        assertArrayEquals(TestMedia.H264_PPS, payload(produced.get(1)));
        assertArrayEquals(TestMedia.H264_IDR, payload(produced.get(2)));
        // marker 只在访问单元最后一个包置位
        assertFalse(marker(produced.get(0)));
        assertFalse(marker(produced.get(1)));
        assertTrue(marker(produced.get(2)));
        // 同一帧的所有包时间戳相同
        assertEquals(timestamp(produced.get(0)), timestamp(produced.get(2)));
    }

    @Test
    void h264NonKeyFrameCarriesNoParamSets() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        p.updateParams(h264Params());
        packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_SLICE), false));

        assertEquals(1, produced.size());
        assertArrayEquals(TestMedia.H264_SLICE, payload(produced.get(0)));
    }

    @Test
    void h264IgnoresForeignCodecParams() {
        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        p.updateParams(h265Params());
        p.updateParams(null);
        packetize(p, new AccessUnit(1000, List.of(TestMedia.H264_IDR), true));

        assertEquals(1, produced.size()); // 未插入任何参数集
    }

    @Test
    void h264FragmentsLargeNaluWithFuA() {
        byte[] nalu = new byte[200];
        nalu[0] = 0x65; // IDR，NRI=3
        Arrays.fill(nalu, 1, nalu.length, (byte) 0x5A);

        H264Packetizer p = new H264Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(1000, List.of(nalu), true));

        int perPacket = MAX_PAYLOAD - 2;                 // FU 指示器 + FU 头
        int expected = (nalu.length - 1 + perPacket - 1) / perPacket;
        assertEquals(expected, produced.size());

        int indicator = (0x65 & 0xE0) | 28;
        byte[] reassembled = new byte[nalu.length - 1];
        int offset = 0;
        for (int i = 0; i < produced.size(); i++) {
            ByteBuf b = produced.get(i);
            byte[] pl = payload(b);
            assertTrue(pl.length <= MAX_PAYLOAD, "fragment exceeds payload limit");
            assertEquals(indicator, pl[0] & 0xFF);
            int fuHeader = pl[1] & 0xFF;
            assertEquals(5, fuHeader & 0x1F);                       // 原 NAL 类型
            assertEquals(i == 0, (fuHeader & 0x80) != 0);           // S 位
            assertEquals(i == produced.size() - 1, (fuHeader & 0x40) != 0); // E 位
            assertEquals(i == produced.size() - 1, marker(b));
            System.arraycopy(pl, 2, reassembled, offset, pl.length - 2);
            offset += pl.length - 2;
        }
        assertArrayEquals(Arrays.copyOfRange(nalu, 1, nalu.length), reassembled);
    }

    // ===== H.265 =====

    @Test
    void h265InsertsVpsSpsPpsBeforeKeyFrame() {
        H265Packetizer p = new H265Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        p.updateParams(h265Params());
        packetize(p, new AccessUnit(1000, List.of(TestMedia.hevcIdr()), true));

        assertEquals(4, produced.size()); // VPS + SPS + PPS + IDR
        assertArrayEquals(TestMedia.hevcVps(), payload(produced.get(0)));
        assertArrayEquals(TestMedia.hevcSps(), payload(produced.get(1)));
        assertArrayEquals(TestMedia.hevcPps(), payload(produced.get(2)));
        assertArrayEquals(TestMedia.hevcIdr(), payload(produced.get(3)));
        assertTrue(marker(produced.get(3)));
        assertFalse(marker(produced.get(0)));
    }

    @Test
    void h265FragmentsLargeNaluWithFu() {
        byte[] nalu = TestMedia.hevcNalu(19, new byte[200]); // 2 字节 NAL 头 + 200 字节数据

        H265Packetizer p = new H265Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(1000, List.of(nalu), true));

        int perPacket = MAX_PAYLOAD - 3;                 // 2 字节 PayloadHdr + FU 头
        int expected = (nalu.length - 2 + perPacket - 1) / perPacket;
        assertEquals(expected, produced.size());

        int hdr0 = (nalu[0] & 0x81) | (49 << 1);
        byte[] reassembled = new byte[nalu.length - 2];
        int offset = 0;
        for (int i = 0; i < produced.size(); i++) {
            byte[] pl = payload(produced.get(i));
            assertTrue(pl.length <= MAX_PAYLOAD, "fragment exceeds payload limit");
            assertEquals(hdr0, pl[0] & 0xFF);
            assertEquals(nalu[1] & 0xFF, pl[1] & 0xFF);
            int fuHeader = pl[2] & 0xFF;
            assertEquals(19, fuHeader & 0x3F);                      // 原 NAL 类型
            assertEquals(i == 0, (fuHeader & 0x80) != 0);
            assertEquals(i == produced.size() - 1, (fuHeader & 0x40) != 0);
            assertEquals(i == produced.size() - 1, marker(produced.get(i)));
            System.arraycopy(pl, 3, reassembled, offset, pl.length - 3);
            offset += pl.length - 3;
        }
        assertArrayEquals(Arrays.copyOfRange(nalu, 2, nalu.length), reassembled);
    }

    @Test
    void h265SkipsNaluShorterThanHeader() {
        H265Packetizer p = new H265Packetizer(ALLOC, PT, SSRC, MAX_PAYLOAD);
        packetize(p, new AccessUnit(1000, List.of(TestMedia.hevcTrailR(), new byte[]{0x02}), false));

        assertEquals(1, produced.size()); // 长度 1 的 NALU 被跳过，marker 落在有效包上
        assertTrue(marker(produced.get(0)));
    }
}
