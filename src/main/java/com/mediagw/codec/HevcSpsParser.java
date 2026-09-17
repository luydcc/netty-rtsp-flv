package com.mediagw.codec;

import com.mediagw.util.BitReader;

import java.util.Arrays;

/** 解析 H.265 SPS 关键字段，用于 SPS id 识别与 HEVCDecoderConfigurationRecord 构建。 */
public final class HevcSpsParser {

    private HevcSpsParser() {
    }

    public static final class Info {
        public int spsId;
        public int profileSpace;
        public int tierFlag;
        public int profileIdc;
        public long profileCompatFlags;
        public byte[] constraintFlags = new byte[6];
        public int levelIdc;
        public int chromaFormatIdc;
        public int bitDepthLumaMinus8;
        public int bitDepthChromaMinus8;
        public int maxSubLayersMinus1;
        public boolean temporalIdNesting;

        public int numTemporalLayers() {
            return maxSubLayersMinus1 + 1;
        }
    }

    public static Info parse(byte[] spsNalu) {
        if (spsNalu == null || spsNalu.length < 4) {
            throw new IllegalArgumentException("H265 SPS too short");
        }
        byte[] rbsp = BitReader.stripEmulationPrevention(Arrays.copyOfRange(spsNalu, 2, spsNalu.length));
        BitReader r = new BitReader(rbsp);
        Info info = new Info();
        r.u(4); // sps_video_parameter_set_id
        info.maxSubLayersMinus1 = r.u(3);
        info.temporalIdNesting = r.u(1) == 1;
        // profile_tier_level(1, maxSubLayersMinus1)
        info.profileSpace = r.u(2);
        info.tierFlag = r.u(1);
        info.profileIdc = r.u(5);
        info.profileCompatFlags = r.uLong(32);
        for (int i = 0; i < 6; i++) {
            info.constraintFlags[i] = (byte) r.u(8);
        }
        info.levelIdc = r.u(8);
        int n = info.maxSubLayersMinus1;
        boolean[] subProfilePresent = new boolean[n];
        boolean[] subLevelPresent = new boolean[n];
        for (int i = 0; i < n; i++) {
            subProfilePresent[i] = r.u(1) == 1;
            subLevelPresent[i] = r.u(1) == 1;
        }
        if (n > 0) {
            for (int i = n; i < 8; i++) {
                r.u(2); // reserved zero 2bits
            }
        }
        for (int i = 0; i < n; i++) {
            if (subProfilePresent[i]) {
                r.skip(88); // 子层 profile_tier_level：8 + 32 + 48 bits
            }
            if (subLevelPresent[i]) {
                r.u(8);
            }
        }
        info.spsId = r.ue();
        info.chromaFormatIdc = r.ue();
        if (info.chromaFormatIdc == 3) {
            r.u(1); // separate_colour_plane_flag
        }
        r.ue(); // pic_width_in_luma_samples
        r.ue(); // pic_height_in_luma_samples
        if (r.u(1) == 1) { // conformance_window_flag
            r.ue();
            r.ue();
            r.ue();
            r.ue();
        }
        info.bitDepthLumaMinus8 = r.ue();
        info.bitDepthChromaMinus8 = r.ue();
        return info;
    }
}
