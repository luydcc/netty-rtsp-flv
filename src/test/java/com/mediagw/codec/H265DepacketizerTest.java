package com.mediagw.codec;

import com.mediagw.rtp.RtpPacket;
import com.mediagw.testutil.TestMedia;
import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H265DepacketizerTest {

    private RecordingListener rec;
    private H265Depacketizer dep;

    @BeforeEach
    void setUp() {
        rec = new RecordingListener();
        dep = new H265Depacketizer(rec);
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
    void singleIdrNaluProducesKeyframeAu() {
        byte[] idr = TestMedia.hevcIdr();
        feed(1, 0, true, idr);
        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertTrue(au.keyFrame());
        assertArrayEquals(idr, au.nalus().get(0));
    }

    @Test
    void fuReassemblesWithReconstructedHeader() {
        byte[] idr = new byte[302];
        idr[0] = 0x26; // type 19 (IDR_W_RADL) << 1
        idr[1] = 0x01;
        for (int i = 2; i < idr.length; i++) {
            idr[i] = (byte) i;
        }
        // FU indicator: type=49 → byte0 = 49<<1 = 0x62；byte1 沿用原 NALU
        feed(10, 9000, false, fu(0x62, idr[1], 0x80 | 19, idr, 2, 100));  // start
        feed(11, 9000, false, fu(0x62, idr[1], 19, idr, 102, 100));        // middle
        feed(12, 9000, true, fu(0x62, idr[1], 0x40 | 19, idr, 202, 100));  // end

        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertTrue(au.keyFrame());
        assertArrayEquals(idr, au.nalus().get(0)); // header 重建为 (b0&0x81)|(19<<1)=0x26, 0x01
    }

    private static byte[] fu(int b0, int b1, int fuHeader, byte[] nalu, int offset, int len) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(b0);
        out.write(b1);
        out.write(fuHeader);
        out.write(nalu, offset, len);
        return out.toByteArray();
    }

    @Test
    void apAggregationSplitsNalus() {
        byte[] s1 = TestMedia.hevcTrailR();
        byte[] s2 = TestMedia.hevcNalu(1, new byte[]{0x77, (byte) 0x88, (byte) 0x99});
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x60); // type 48 (AP) << 1
        out.write(0x01);
        writeEntry(out, s1);
        writeEntry(out, s2);
        feed(20, 18000, true, out.toByteArray());

        assertEquals(1, rec.accessUnits.size());
        AccessUnit au = rec.accessUnits.get(0);
        assertFalse(au.keyFrame()); // TRAIL_R 非关键帧
        assertEquals(2, au.nalus().size());
        assertArrayEquals(s1, au.nalus().get(0));
        assertArrayEquals(s2, au.nalus().get(1));
    }

    private static void writeEntry(ByteArrayOutputStream out, byte[] nalu) {
        out.write((nalu.length >>> 8) & 0xFF);
        out.write(nalu.length & 0xFF);
        out.write(nalu, 0, nalu.length);
    }

    @Test
    void inBandParamSetsEmitWhenCompleteAndStayOutOfFrameData() {
        feed(30, 27000, false, TestMedia.hevcVps());
        feed(31, 27000, false, TestMedia.hevcSps());
        assertTrue(rec.paramSets.isEmpty()); // VPS/SPS 就绪但缺 PPS
        feed(32, 27000, true, TestMedia.hevcPps());

        assertEquals(1, rec.paramSets.size());
        ParamSets ps = rec.paramSets.get(0);
        assertEquals(CodecType.H265, ps.codec());
        assertEquals(1, ps.vps().size());
        assertEquals(1, ps.sps().size());
        assertEquals(1, ps.pps().size());
        assertTrue(ps.isComplete());
        assertTrue(rec.accessUnits.isEmpty()); // 参数集不进入帧数据
    }

    @Test
    void seedParamSetsEmitsOnce() {
        dep.seedParamSets(List.of(TestMedia.hevcVps()), List.of(TestMedia.hevcSps()),
                List.of(TestMedia.hevcPps()));
        assertEquals(1, rec.paramSets.size());
        assertTrue(rec.paramSets.get(0).isComplete());
        // 带内重复不重发
        feed(40, 36000, true, TestMedia.hevcSps());
        assertEquals(1, rec.paramSets.size());
    }

    @Test
    void audEosEobStripped() {
        feed(50, 45000, false, TestMedia.hevcNalu(35, new byte[]{0x58})); // AUD
        feed(51, 45000, true, TestMedia.hevcIdr());
        assertEquals(1, rec.accessUnits.size());
        assertEquals(1, rec.accessUnits.get(0).nalus().size());
        assertTrue(rec.accessUnits.get(0).keyFrame());
    }
}
