package com.mediagw.codec;

/** 视频编码类型。 */
public enum CodecType {
    H264,
    H265;

    public static CodecType fromRtpMap(String encoding) {
        if (encoding == null) {
            return null;
        }
        String e = encoding.trim().toUpperCase();
        if (e.equals("H264") || e.equals("AVC")) {
            return H264;
        }
        if (e.equals("H265") || e.equals("HEVC")) {
            return H265;
        }
        return null;
    }
}
