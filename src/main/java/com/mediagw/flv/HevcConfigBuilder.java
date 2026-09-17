package com.mediagw.flv;

import com.mediagw.codec.HevcSpsParser;
import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

/**
 * 构建 HEVCDecoderConfigurationRecord（ISO 14496-15，用于 H.265 Enhanced FLV sequence header）。
 * 字段值来自对首个 SPS 的解析。
 */
public final class HevcConfigBuilder {

    /** VPS=32, SPS=33, PPS=34 */
    private static final int NAL_VPS = 32;
    private static final int NAL_SPS = 33;
    private static final int NAL_PPS = 34;
    private static final int LENGTH_SIZE_MINUS_ONE = 3; // 4 字节 NALU 长度前缀

    private HevcConfigBuilder() {
    }

    public static byte[] build(ParamSets ps) {
        List<byte[]> vps = ps.vps();
        List<byte[]> sps = ps.sps();
        List<byte[]> pps = ps.pps();
        if (sps.isEmpty() || pps.isEmpty()) {
            throw new IllegalArgumentException("HEVC config requires SPS and PPS");
        }
        HevcSpsParser.Info info = HevcSpsParser.parse(sps.getFirst());

        ByteBuf buf = Unpooled.buffer();
        try {
            buf.writeByte(1); // configurationVersion

            // general_profile_space(2) | general_tier_flag(1) | general_profile_idc(5)
            int b1 = ((info.profileSpace & 0x3) << 6)
                    | ((info.tierFlag & 0x1) << 5)
                    | (info.profileIdc & 0x1F);
            buf.writeByte(b1);

            buf.writeInt((int) (info.profileCompatFlags & 0xFFFFFFFFL)); // 32 bits
            buf.writeBytes(info.constraintFlags);                        // 48 bits
            buf.writeByte(info.levelIdc & 0xFF);                         // general_level_idc

            buf.writeShort(0xF000); // 4 bits reserved(1111) + min_spatial_segmentation_idc(12)=0
            buf.writeByte(0xFC);    // 6 bits reserved(111111) + parallelismType(2)=0
            buf.writeByte(0xFC | (info.chromaFormatIdc & 0x3));      // chromaFormat
            buf.writeByte(0xF8 | (info.bitDepthLumaMinus8 & 0x7));   // bitDepthLumaMinus8
            buf.writeByte(0xF8 | (info.bitDepthChromaMinus8 & 0x7)); // bitDepthChromaMinus8

            buf.writeShort(0); // avgFrameRate

            // constantFrameRate(2)=0 | numTemporalLayers(3) | temporalIdNestingFlag(1) | lengthSizeMinusOne(2)=3
            int numTemporalLayers = info.numTemporalLayers() & 0x7;
            int nesting = info.temporalIdNesting ? 1 : 0;
            int b21 = (0 << 6) | (numTemporalLayers << 3) | (nesting << 2) | LENGTH_SIZE_MINUS_ONE;
            buf.writeByte(b21);

            // numOfArrays：VPS/SPS/PPS 三个数组（存在才计）
            int numArrays = 0;
            if (!vps.isEmpty()) numArrays++;
            if (!sps.isEmpty()) numArrays++;
            if (!pps.isEmpty()) numArrays++;
            buf.writeByte(numArrays);

            if (!vps.isEmpty()) writeArray(buf, NAL_VPS, vps);
            if (!sps.isEmpty()) writeArray(buf, NAL_SPS, sps);
            if (!pps.isEmpty()) writeArray(buf, NAL_PPS, pps);

            byte[] out = new byte[buf.readableBytes()];
            buf.readBytes(out);
            return out;
        } finally {
            buf.release();
        }
    }

    private static void writeArray(ByteBuf buf, int nalType, List<byte[]> nalus) {
        // array_completeness(1)=1 | reserved(1)=0 | NAL_unit_type(6)
        buf.writeByte(0x80 | (nalType & 0x3F));
        buf.writeShort(nalus.size()); // numNalus
        for (byte[] nalu : nalus) {
            buf.writeShort(nalu.length); // nalUnitLength
            buf.writeBytes(nalu);
        }
    }
}
