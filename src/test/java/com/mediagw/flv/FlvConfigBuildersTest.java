package com.mediagw.flv;

import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.testutil.TestMedia;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static com.mediagw.testutil.TestMedia.H264_PPS;
import static com.mediagw.testutil.TestMedia.H264_SPS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlvConfigBuildersTest {

    // ===== avcC =====

    @Test
    void avcCLayout() {
        byte[] rec = AvcConfigBuilder.build(List.of(H264_SPS), List.of(H264_PPS));
        ByteArrayOutputStream exp = new ByteArrayOutputStream();
        exp.write(1);              // configurationVersion
        exp.write(0x42);           // profile (SPS[1])
        exp.write(0xC0);           // compatibility (SPS[2])
        exp.write(0x1F);           // level (SPS[3])
        exp.write(0xFF);           // lengthSizeMinusOne=3
        exp.write(0xE1);           // numOfSPS=1
        exp.write(0);
        exp.write(H264_SPS.length);
        exp.write(H264_SPS, 0, H264_SPS.length);
        exp.write(1);              // numOfPPS
        exp.write(0);
        exp.write(H264_PPS.length);
        exp.write(H264_PPS, 0, H264_PPS.length);
        assertArrayEquals(exp.toByteArray(), rec);
    }

    @Test
    void avcCFromParamSetsMatchesListOverload() {
        ParamSets ps = new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of(H264_PPS));
        assertArrayEquals(AvcConfigBuilder.build(List.of(H264_SPS), List.of(H264_PPS)),
                AvcConfigBuilder.build(ps));
    }

    @Test
    void avcCRequiresSpsAndPps() {
        assertThrows(IllegalArgumentException.class,
                () -> AvcConfigBuilder.build(List.of(), List.of(H264_PPS)));
        assertThrows(IllegalArgumentException.class,
                () -> AvcConfigBuilder.build(List.of(H264_SPS), List.of()));
    }

    // ===== hvcC =====

    @Test
    void hvcCLayout() {
        byte[] vps = TestMedia.hevcVps();
        byte[] sps = TestMedia.hevcSps();
        byte[] pps = TestMedia.hevcPps();
        byte[] rec = HevcConfigBuilder.build(
                new ParamSets(CodecType.H265, List.of(vps), List.of(sps), List.of(pps)));

        ByteArrayOutputStream exp = new ByteArrayOutputStream();
        exp.write(1);              // configurationVersion
        exp.write(0x01);           // space=0|tier=0|profile_idc=1(Main)
        exp.write(0x60);
        exp.write(0x00);
        exp.write(0x00);
        exp.write(0x00);           // profile_compatibility_flags
        exp.write(0x90);
        for (int i = 0; i < 5; i++) {
            exp.write(0);
        }                           // constraint flags 48bit
        exp.write(120);            // level_idc
        exp.write(0xF0);
        exp.write(0x00);           // min_spatial_segmentation_idc=0
        exp.write(0xFC);           // parallelismType=0
        exp.write(0xFD);           // 0xFC | chroma=1
        exp.write(0xF8);           // bitDepthLuma=8
        exp.write(0xF8);           // bitDepthChroma=8
        exp.write(0x00);
        exp.write(0x00);           // avgFrameRate
        exp.write(0x0F);           // layers=1|nesting=1|lengthSizeMinusOne=3
        exp.write(3);              // numOfArrays
        writeArray(exp, 0xA0, vps); // VPS, array_completeness=1
        writeArray(exp, 0xA1, sps); // SPS
        writeArray(exp, 0xA2, pps); // PPS
        assertArrayEquals(exp.toByteArray(), rec);
    }

    private static void writeArray(ByteArrayOutputStream out, int header, byte[] nalu) {
        out.write(header);
        out.write(0);
        out.write(1); // numNalus=1
        out.write((nalu.length >>> 8) & 0xFF);
        out.write(nalu.length & 0xFF);
        out.write(nalu, 0, nalu.length);
    }

    @Test
    void hvcCRequiresSpsAndPps() {
        assertThrows(IllegalArgumentException.class, () -> HevcConfigBuilder.build(
                new ParamSets(CodecType.H265, List.of(TestMedia.hevcVps()), List.of(), List.of())));
    }

    @Test
    void paramSetsSignatureDetectsChange() {
        ParamSets a = new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of(H264_PPS));
        ParamSets same = new ParamSets(CodecType.H264, List.of(),
                List.of(H264_SPS.clone()), List.of(H264_PPS.clone()));
        byte[] changedPps = H264_PPS.clone();
        changedPps[3] ^= 0x11;
        ParamSets b = new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of(changedPps));
        assertEquals(a.signature(), same.signature());
        assertEquals(1, a.sps().size());
        assertNotEquals(a.signature(), b.signature());
    }
}
