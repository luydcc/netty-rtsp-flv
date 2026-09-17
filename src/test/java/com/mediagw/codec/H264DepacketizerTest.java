package com.mediagw.codec;

import com.mediagw.rtp.RtpPacket;
import com.mediagw.testutil.TestMedia;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static com.mediagw.testutil.TestMedia.H264_IDR;
import static com.mediagw.testutil.TestMedia.H264_PPS;
import static com.mediagw.testutil.TestMedia.H264_SLICE;
import static com.mediagw.testutil.TestMedia.H264_SPS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H264DepacketizerTest {

    private RecordingListener rec;
    private H264Depacketizer dep;

    @BeforeEach
    void setUp() {
        rec = new RecordingListener();
        dep = new H264Depacketizer(rec);
    }

    private void feed(int seq, long ts, boolean marker, byte[] payload) {
        ByteBuf buf = TestMedia.rtp(seq, ts, marker, 96, payload);
        try {
            RtpPacket p = RtpPacket.parse(buf);
            dep.onRtpPacket(p);
        } finally {
            buf.release();
        }
    }

    @Test
    void singleNaluWithMarkerProducesAccessUnit() {
        feed(1, 0, true, H264_SLICE);
        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertEquals(0, au.rtpTimestamp());
        assertFalse(au.keyFrame());
        assertEquals(1, au.nalus().size());
        assertArrayEquals(H264_SLICE, au.nalus().get(0));
    }

    @Test
    void fuAReassemblesFragmentedIdr() {
        byte[] idr = new byte[301];
        idr[0] = 0x65; // NRI=3, type=5 (IDR)
        for (int i = 1; i < idr.length; i++) {
            idr[i] = (byte) i;
        }
        feed(10, 9000, false, fuA(0x80, idr, 1, 100));   // start
        feed(11, 9000, false, fuA(0x00, idr, 101, 100));  // middle
        feed(12, 9000, true, fuA(0x40, idr, 201, 100));    // end
        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertTrue(au.keyFrame());
        assertEquals(9000, au.rtpTimestamp());
        assertArrayEquals(idr, au.nalus().get(0));
    }

    /** FU-A 载荷：indicator(0x7C=NRI3+type28) + FU header(se|fuType=5) + 分片数据。 */
    private static byte[] fuA(int seBits, byte[] nalu, int offset, int len) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x7C);
        out.write(seBits | (nalu[0] & 0x1F));
        out.write(nalu, offset, len);
        return out.toByteArray();
    }

    @Test
    void stapAExtractsParamsAndIdr() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x78); // NRI=3, type=24 STAP-A
        writeAggregated(out, H264_SPS);
        writeAggregated(out, H264_PPS);
        writeAggregated(out, H264_IDR);
        feed(20, 18000, true, out.toByteArray());

        assertEquals(1, rec.paramSets.size());
        ParamSets ps = rec.paramSets.get(0);
        assertEquals(CodecType.H264, ps.codec());
        assertArrayEquals(H264_SPS, ps.sps().get(0));
        assertArrayEquals(H264_PPS, ps.pps().get(0));
        assertTrue(ps.isComplete());

        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertTrue(au.keyFrame());
        assertEquals(1, au.nalus().size()); // 参数集已剥离，仅剩 IDR
        assertArrayEquals(H264_IDR, au.nalus().get(0));
    }

    private static void writeAggregated(ByteArrayOutputStream out, byte[] nalu) {
        out.write((nalu.length >>> 8) & 0xFF);
        out.write(nalu.length & 0xFF);
        out.write(nalu, 0, nalu.length);
    }

    @Test
    void seedParamSetsEmitsOnceAndInBandDuplicatesDoNotReemit() {
        dep.seedParamSets(List.of(H264_SPS), List.of(H264_PPS));
        assertEquals(1, rec.paramSets.size());

        // 带内重复发送相同 SPS/PPS：不应重复回调
        feed(30, 27000, false, H264_SPS);
        feed(31, 27000, true, H264_PPS);
        assertEquals(1, rec.paramSets.size());
        assertTrue(rec.accessUnits.isEmpty()); // 参数集不进入帧数据
    }

    @Test
    void changedSpsReemitsParamSets() {
        dep.seedParamSets(List.of(H264_SPS), List.of(H264_PPS));
        byte[] newSps = H264_SPS.clone();
        newSps[newSps.length - 1] ^= 0x55; // 内容变化，id 不变
        feed(40, 36000, true, newSps);
        assertEquals(2, rec.paramSets.size());
        assertArrayEquals(newSps, rec.paramSets.get(1).sps().get(0));
    }

    @Test
    void seqGapDropsWholeAccessUnit() {
        feed(100, 45000, false, H264_SLICE);
        feed(102, 45000, true, H264_SLICE); // 丢了 seq=101
        assertTrue(rec.accessUnits.isEmpty());
        assertFalse(rec.errors.isEmpty());
        assertEquals(1, dep.lostPackets());
    }

    @Test
    void timestampChangeSplitsAccessUnits() {
        feed(200, 0, false, H264_SLICE);
        feed(201, 3600, false, H264_SLICE); // ts 变化 → 冲刷上一帧
        assertEquals(1, rec.accessUnits.size());
        assertEquals(0, rec.accessUnits.get(0).rtpTimestamp());
        feed(202, 3600, true, H264_SLICE);
        assertEquals(2, rec.accessUnits.size());
        assertEquals(3600, rec.accessUnits.get(1).rtpTimestamp());
    }

    @Test
    void audIsStrippedFromFrameData() {
        feed(300, 54000, false, TestMedia.H264_AUD);
        feed(301, 54000, true, H264_IDR);
        assertEquals(1, rec.accessUnits.size());
        assertEquals(1, rec.accessUnits.get(0).nalus().size());
        assertArrayEquals(H264_IDR, rec.accessUnits.get(0).nalus().get(0));
        assertTrue(rec.accessUnits.get(0).keyFrame());
    }

    @Test
    void fuStartLostDiscardsNalu() {
        // 直接发 middle/end（起始包丢失）→ NALU 丢弃且帧标记损坏
        feed(400, 63000, false, fuA(0x00, bigIdr(), 1, 50));
        feed(401, 63000, true, fuA(0x40, bigIdr(), 51, 50));
        assertTrue(rec.accessUnits.isEmpty());
    }

    private static byte[] bigIdr() {
        byte[] idr = new byte[101];
        idr[0] = 0x65;
        for (int i = 1; i < idr.length; i++) {
            idr[i] = (byte) (i * 7);
        }
        return idr;
    }
}
