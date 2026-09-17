package com.mediagw.codec;

import com.mediagw.rtp.RtpPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

/**
 * RTP 视频载荷重组基类：
 * - 按 RTP 时间戳分组 + Marker 位界定访问单元；
 * - 序列号缺口检测，缺口所在帧整体丢弃，避免花屏；
 * - FU 分片缓冲管理（起始丢失/中途丢失则丢弃整个 NALU）；
 * - 参数集与 AUD 从帧数据中剥离，参数集变化时回调通知。
 * 非线程安全，须在单一 RTSP EventLoop 线程内使用。
 */
public abstract class VideoDepacketizer {

    public interface Listener {
        /** 参数集（VPS/SPS/PPS）首次就绪或发生变化。 */
        void onParameterSets(ParamSets ps);

        /** 一个完整访问单元（帧）就绪。 */
        void onAccessUnit(AccessUnit au);

        /** 重组错误（丢包、非法载荷等），仅用于日志与计数。 */
        void onError(String message);
    }

    private static final int MAX_NALU_BYTES = 4 * 1024 * 1024;

    protected final Listener listener;
    private final CodecType codec;

    private long curTs = -1;
    private List<byte[]> curNalus = new ArrayList<>();
    private boolean curKeyFrame;
    private boolean auCorrupted;
    private int expectedSeq = -1;
    private long lostPackets;
    private ByteBuf fuBuffer;

    protected VideoDepacketizer(CodecType codec, Listener listener) {
        this.codec = codec;
        this.listener = listener;
    }

    public CodecType codec() {
        return codec;
    }

    public long lostPackets() {
        return lostPackets;
    }

    public void onRtpPacket(RtpPacket p) {
        if (expectedSeq >= 0 && p.seq() != expectedSeq) {
            int gap = (p.seq() - expectedSeq) & 0xFFFF;
            lostPackets += gap;
            auCorrupted = true;
            abortFu();
        }
        expectedSeq = (p.seq() + 1) & 0xFFFF;

        if (curTs < 0) {
            curTs = p.timestamp();
        } else if (p.timestamp() != curTs) {
            flushAccessUnit();
            curTs = p.timestamp();
        }
        try {
            depacketize(p);
        } catch (RuntimeException e) {
            auCorrupted = true;
            abortFu();
            listener.onError(codec + " depacketize error: " + e);
        }
        if (p.marker()) {
            flushAccessUnit();
        }
    }

    /** 子类实现：解析单个 RTP 载荷，通过 addNalu / FU 辅助方法输出完整 NALU。 */
    protected abstract void depacketize(RtpPacket p);

    protected abstract int naluType(byte[] nalu);

    protected abstract boolean isKeyFrameNalu(int type);

    /**
     * 子类处理非 VCL NALU（参数集缓存、AUD 丢弃等）。
     *
     * @return true 表示该 NALU 已消费，不写入帧数据
     */
    protected abstract boolean handleNonVcl(int type, byte[] nalu);

    protected void addNalu(byte[] nalu) {
        if (nalu == null || nalu.length < 2) {
            return;
        }
        int type = naluType(nalu);
        if (handleNonVcl(type, nalu)) {
            return;
        }
        if (isKeyFrameNalu(type)) {
            curKeyFrame = true;
        }
        curNalus.add(nalu);
    }

    // ===== FU 分片缓冲 =====

    protected void fuStart(byte[] header) {
        abortFu();
        fuBuffer = Unpooled.buffer(4096);
        fuBuffer.writeBytes(header);
    }

    protected void fuFragment(ByteBuf src, int index, int len) {
        if (fuBuffer == null) {
            auCorrupted = true; // FU 起始丢失
            return;
        }
        if (fuBuffer.readableBytes() + len > MAX_NALU_BYTES) {
            abortFu();
            auCorrupted = true;
            listener.onError("FU NALU exceeds " + MAX_NALU_BYTES + " bytes, aborted");
            return;
        }
        fuBuffer.writeBytes(src, index, len);
    }

    protected void fuEnd() {
        if (fuBuffer == null) {
            auCorrupted = true;
            return;
        }
        byte[] nalu = new byte[fuBuffer.readableBytes()];
        fuBuffer.readBytes(nalu);
        fuBuffer.release();
        fuBuffer = null;
        addNalu(nalu);
    }

    protected void abortFu() {
        if (fuBuffer != null) {
            fuBuffer.release();
            fuBuffer = null;
        }
    }

    protected void markCorrupted(String reason) {
        auCorrupted = true;
        listener.onError(reason);
    }

    private void flushAccessUnit() {
        if (!curNalus.isEmpty() && !auCorrupted) {
            listener.onAccessUnit(new AccessUnit(curTs, curNalus, curKeyFrame));
        } else if (!curNalus.isEmpty()) {
            listener.onError("dropped incomplete access unit (" + curNalus.size() + " NALUs)");
        }
        curNalus = new ArrayList<>();
        curKeyFrame = false;
        auCorrupted = false;
    }
}
