package com.mediagw.codec;

import java.util.List;

/**
 * 一个完整视频访问单元（帧）。nalus 仅包含 VCL/SEI 等需要写入 FLV 帧数据的 NALU，
 * 参数集（VPS/SPS/PPS）与 AUD 已在重组阶段剥离。
 */
public final class AccessUnit {

    private final long rtpTimestamp;
    private final List<byte[]> nalus;
    private final boolean keyFrame;

    public AccessUnit(long rtpTimestamp, List<byte[]> nalus, boolean keyFrame) {
        this.rtpTimestamp = rtpTimestamp;
        this.nalus = nalus;
        this.keyFrame = keyFrame;
    }

    /** RTP 90kHz 时间戳（无符号 32 位，以 long 表示）。 */
    public long rtpTimestamp() {
        return rtpTimestamp;
    }

    public List<byte[]> nalus() {
        return nalus;
    }

    public boolean keyFrame() {
        return keyFrame;
    }
}
