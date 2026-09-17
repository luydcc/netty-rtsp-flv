package com.mediagw.testutil;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;

/** 测试数据构造：RTP 包、H.264/H.265 合成 NALU、比特写入器。 */
public final class TestMedia {

    private TestMedia() {
    }

    public static final int SSRC = 0x0BADF00D;

    /** 构造一个 RTP 包（V=2，无 CSRC/扩展/填充），返回的 ByteBuf 由调用方释放。 */
    public static ByteBuf rtp(int seq, long ts, boolean marker, int pt, byte[] payload) {
        ByteBuf b = Unpooled.buffer();
        b.writeByte(0x80);
        b.writeByte((marker ? 0x80 : 0) | (pt & 0x7F));
        b.writeShort(seq);
        b.writeInt((int) ts);
        b.writeInt(SSRC);
        b.writeBytes(payload);
        return b;
    }

    // ===== H.264 =====
    public static final byte[] H264_SPS = {0x67, 0x42, (byte) 0xC0, 0x1F, (byte) 0xD9,
            0x00, (byte) 0xA0, 0x47, (byte) 0xFE, (byte) 0xC8};
    public static final byte[] H264_PPS = {0x68, (byte) 0xCE, 0x38, (byte) 0x80};
    public static final byte[] H264_IDR = {0x65, 0x11, 0x22, 0x33};
    public static final byte[] H264_SLICE = {0x41, (byte) 0x9A, 0x24, 0x0C};
    public static final byte[] H264_AUD = {0x09, (byte) 0xF0};

    // ===== H.265 =====

    /** 带 2 字节 NALU header 的合成 NALU。 */
    public static byte[] hevcNalu(int type, byte[] rbsp) {
        byte[] nalu = new byte[2 + rbsp.length];
        nalu[0] = (byte) (type << 1);
        nalu[1] = 0x01;
        System.arraycopy(rbsp, 0, nalu, 2, rbsp.length);
        return nalu;
    }

    public static byte[] hevcVps() {
        return hevcNalu(32, new Bits().u(4, 0).u(12, 0xABC).toBytes());
    }

    /**
     * 合成 SPS：Main profile(idc=1)、compat=0x60000000、constraint[0]=0x90、level=120、
     * 640x360、4:2:0、8bit、单时间层、temporal_id_nesting=1。可被 HevcSpsParser 完整解析。
     */
    public static byte[] hevcSps() {
        Bits b = new Bits()
                .u(4, 0)   // sps_video_parameter_set_id
                .u(3, 0)   // sps_max_sub_layers_minus1
                .u(1, 1)   // sps_temporal_id_nesting_flag
                .u(2, 0)   // general_profile_space
                .u(1, 0)   // general_tier_flag
                .u(5, 1);  // general_profile_idc = Main
        b.u(32, 0x60000000);
        b.u(8, 0x90);
        for (int i = 0; i < 5; i++) {
            b.u(8, 0);
        }
        b.u(8, 120)  // general_level_idc
                .ue(0)     // sps_seq_parameter_set_id
                .ue(1)     // chroma_format_idc
                .ue(640)   // pic_width_in_luma_samples
                .ue(360)   // pic_height_in_luma_samples
                .u(1, 0)   // conformance_window_flag
                .ue(0)     // bit_depth_luma_minus8
                .ue(0)     // bit_depth_chroma_minus8
                .u(8, 0x80);
        return hevcNalu(33, b.toBytes());
    }

    public static byte[] hevcPps() {
        return hevcNalu(34, new Bits().ue(0).ue(0).u(8, 0xCD).toBytes());
    }

    public static byte[] hevcIdr() {
        return hevcNalu(19, new byte[]{0x11, 0x22, 0x33, 0x44}); // IDR_W_RADL
    }

    public static byte[] hevcTrailR() {
        return hevcNalu(1, new byte[]{0x55, 0x66}); // TRAIL_R
    }

    /** 按位写入器（大端 bit 序）。 */
    public static final class Bits {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private int cur;
        private int n;

        public Bits bit(int v) {
            cur = (cur << 1) | (v & 1);
            if (++n == 8) {
                out.write(cur);
                cur = 0;
                n = 0;
            }
            return this;
        }

        public Bits u(int width, int v) {
            for (int i = width - 1; i >= 0; i--) {
                bit(v >>> i);
            }
            return this;
        }

        /** 无符号 Exp-Golomb。 */
        public Bits ue(int v) {
            int x = v + 1;
            int w = 32 - Integer.numberOfLeadingZeros(x);
            u(w - 1, 0);
            u(w, x);
            return this;
        }

        public byte[] toBytes() {
            if (n > 0) {
                out.write(cur << (8 - n));
                cur = 0;
                n = 0;
            }
            return out.toByteArray();
        }
    }
}
