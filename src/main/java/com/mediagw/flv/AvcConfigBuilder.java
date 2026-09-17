package com.mediagw.flv;

import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;

/** 构建 AVCDecoderConfigurationRecord（ISO 14496-15，用于 H.264 FLV sequence header）。 */
public final class AvcConfigBuilder {

    private AvcConfigBuilder() {
    }

    /**
     * @param sps 至少一个 SPS（不含起始码）
     * @param pps 至少一个 PPS（不含起始码）
     * @return AVCDecoderConfigurationRecord 字节
     */
    public static byte[] build(List<byte[]> sps, List<byte[]> pps) {
        if (sps == null || sps.isEmpty() || pps == null || pps.isEmpty()) {
            throw new IllegalArgumentException("AVC config requires SPS and PPS");
        }
        byte[] firstSps = sps.getFirst();
        if (firstSps.length < 4) {
            throw new IllegalArgumentException("SPS too short");
        }
        ByteBuf buf = Unpooled.buffer();
        try {
            buf.writeByte(1);                    // configurationVersion
            buf.writeByte(firstSps[1] & 0xFF);   // AVCProfileIndication
            buf.writeByte(firstSps[2] & 0xFF);   // profile_compatibility
            buf.writeByte(firstSps[3] & 0xFF);   // AVCLevelIndication
            buf.writeByte(0xFF);                 // 6 bits reserved(111111) + lengthSizeMinusOne=3
            buf.writeByte(0xE0 | sps.size());    // 3 bits reserved(111) + numOfSPS
            for (byte[] s : sps) {
                buf.writeShort(s.length);
                buf.writeBytes(s);
            }
            buf.writeByte(pps.size());           // numOfPPS
            for (byte[] p : pps) {
                buf.writeShort(p.length);
                buf.writeBytes(p);
            }
            byte[] out = new byte[buf.readableBytes()];
            buf.readBytes(out);
            return out;
        } finally {
            buf.release();
        }
    }

    public static byte[] build(ParamSets ps) {
        return build(ps.sps(), ps.pps());
    }
}
