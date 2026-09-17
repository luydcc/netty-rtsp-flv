package com.mediagw.flv;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.testutil.TestMedia;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlvMuxerTest {

    private static final UnpooledByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

    private static ParamSets h264Params() {
        return new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of(H264_PPS));
    }

    private static ParamSets h265Params() {
        return new ParamSets(CodecType.H265, List.of(TestMedia.hevcVps()),
                List.of(TestMedia.hevcSps()), List.of(TestMedia.hevcPps()));
    }

    private static byte[] drain(ByteBuf buf) {
        try {
            byte[] out = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), out);
            return out;
        } finally {
            buf.release();
        }
    }

    private static void medium(ByteArrayOutputStream out, int v) {
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    private static void int32(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }

    /** 从 FLV Tag 头部读出 32 位时间戳。 */
    private static long tagTs(byte[] tag) {
        return ((tag[7] & 0xFFL) << 24) | ((tag[4] & 0xFF) << 16) | ((tag[5] & 0xFF) << 8) | (tag[6] & 0xFF);
    }

    @Test
    void flvHeaderConstant() {
        assertArrayEquals(new byte[]{'F', 'L', 'V', 1, 0x01, 0, 0, 0, 9, 0, 0, 0, 0},
                FlvMuxer.FLV_HEADER);
    }

    @Test
    void configTagBytesH264() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        assertFalse(muxer.hasConfig());
        assertNull(muxer.mux(new AccessUnit(0, List.of(H264_IDR), true))); // 无配置不出帧

        assertTrue(muxer.updateConfig(h264Params()));
        assertFalse(muxer.updateConfig(h264Params())); // 相同签名去重
        assertEquals(1, muxer.configVersion());
        assertTrue(muxer.hasConfig());

        byte[] avcC = AvcConfigBuilder.build(List.of(H264_SPS), List.of(H264_PPS));
        ByteArrayOutputStream exp = new ByteArrayOutputStream();
        exp.write(9);
        medium(exp, avcC.length + 5);
        medium(exp, 0);
        exp.write(0);
        medium(exp, 0);
        exp.write(0x17); // keyframe + AVC
        exp.write(0x00); // sequence header
        medium(exp, 0);
        exp.write(avcC, 0, avcC.length);
        int32(exp, 11 + avcC.length + 5);
        assertArrayEquals(exp.toByteArray(), drain(muxer.buildConfigTag()));
    }

    @Test
    void muxKeyframeAndInterTagsH264() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        muxer.updateConfig(h264Params());

        ByteArrayOutputStream exp = new ByteArrayOutputStream();
        exp.write(9);
        medium(exp, 13);
        medium(exp, 0);
        exp.write(0);
        medium(exp, 0);
        exp.write(0x17);
        exp.write(0x01);
        medium(exp, 0);
        int32(exp, H264_IDR.length);
        exp.write(H264_IDR, 0, H264_IDR.length);
        int32(exp, 11 + 13);
        byte[] tag = drain(muxer.mux(new AccessUnit(0, List.of(H264_IDR), true)));
        assertArrayEquals(exp.toByteArray(), tag);
        assertEquals(0, tagTs(tag));

        // 90000/90kHz = 1000ms，inter 帧 byte0 = 0x27
        byte[] t2 = drain(muxer.mux(new AccessUnit(90000, List.of(H264_SLICE), false)));
        assertEquals(0x27, t2[11] & 0xFF);
        assertEquals(1000, tagTs(t2));

        // +4500 → 1050ms；相同 ts 单调递增为 1051
        assertEquals(1050, tagTs(drain(muxer.mux(new AccessUnit(94500, List.of(H264_SLICE), false)))));
        assertEquals(1051, tagTs(drain(muxer.mux(new AccessUnit(94500, List.of(H264_SLICE), false)))));
    }

    @Test
    void timestampHandles32bitWraparound() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        muxer.updateConfig(h264Params());
        assertEquals(0, tagTs(drain(muxer.mux(new AccessUnit(0xFFFFFFFAL, List.of(H264_SLICE), false)))));
        // 回绕后 +96 ticks ≈ +1ms
        assertEquals(1, tagTs(drain(muxer.mux(new AccessUnit(90, List.of(H264_SLICE), false)))));
    }

    @Test
    void streamResetContinuesTimeline() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        muxer.updateConfig(h264Params());
        assertEquals(0, tagTs(drain(muxer.mux(new AccessUnit(0, List.of(H264_SLICE), false)))));
        muxer.onStreamReset(); // 模拟 RTSP 重连，RTP ts 基准重置
        assertEquals(40, tagTs(drain(muxer.mux(new AccessUnit(500000, List.of(H264_SLICE), false)))));
    }

    @Test
    void h265EnhancedFlvTags() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H265, ALLOC);
        assertTrue(muxer.updateConfig(h265Params()));
        assertEquals(1, muxer.configVersion());

        byte[] hvcC = HevcConfigBuilder.build(h265Params());
        // sequence header: 0x90 = IsExHeader|keyframe|SequenceStart, FourCC hvc1
        byte[] tag = drain(muxer.buildConfigTag());
        assertEquals(9, tag[0]);
        assertEquals(0x90, tag[11] & 0xFF);
        assertEquals('h', tag[12]);
        assertEquals('v', tag[13]);
        assertEquals('c', tag[14]);
        assertEquals('1', tag[15]);
        assertEquals(hvcC.length + 5, ((tag[1] & 0xFF) << 16) | ((tag[2] & 0xFF) << 8) | (tag[3] & 0xFF));

        // CodedFrames：keyframe byte0 = 0x91，inter = 0xA1，FourCC 后跟 3 字节 CTS
        byte[] idr = TestMedia.hevcIdr();
        byte[] k = drain(muxer.mux(new AccessUnit(0, List.of(idr), true)));
        assertEquals(0x91, k[11] & 0xFF);
        assertEquals('h', k[12]);
        assertEquals(0, k[16]);
        assertEquals(0, k[17]);
        assertEquals(0, k[18]);
        assertEquals(idr.length, ((k[19] & 0xFF) << 24) | ((k[20] & 0xFF) << 16)
                | ((k[21] & 0xFF) << 8) | (k[22] & 0xFF));

        byte[] trail = TestMedia.hevcTrailR();
        byte[] i = drain(muxer.mux(new AccessUnit(90000, List.of(trail), false)));
        assertEquals(0xA1, i[11] & 0xFF);
        assertEquals(1000, tagTs(i));
    }

    @Test
    void incompleteParamSetsRejected() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        assertFalse(muxer.updateConfig(null));
        assertFalse(muxer.updateConfig(new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of())));
        assertFalse(muxer.hasConfig());
        assertNull(muxer.buildConfigTag());
    }

    @Test
    void changedParamSetsBumpConfigVersion() {
        FlvMuxer muxer = new FlvMuxer(CodecType.H264, ALLOC);
        muxer.updateConfig(h264Params());
        byte[] newPps = H264_PPS.clone();
        newPps[2] ^= 0x33;
        assertTrue(muxer.updateConfig(new ParamSets(CodecType.H264, List.of(),
                List.of(H264_SPS), List.of(newPps))));
        assertEquals(2, muxer.configVersion());
    }
}
