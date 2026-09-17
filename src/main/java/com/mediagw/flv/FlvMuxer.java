package com.mediagw.flv;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * FLV 封装器（零转码）：
 * - H.264：标准 FLV，CodecID=7，AVCPacketType 0=sequence header / 1=NALU；
 * - H.265：Enhanced FLV，IsExHeader=1，FourCC='hvc1'，PacketType 0=SequenceStart / 1=CodedFrames；
 * - 时间戳：RTP 90kHz -> 毫秒，处理 32 位回绕，重连后延续递增（不回跳）；
 * - 仅支持无 B 帧流，CompositionTime=0。
 * 时间轴状态仅由单一 RTSP EventLoop 线程访问；配置记录更新为线程安全。
 */
public final class FlvMuxer {

    /** 13 字节：FLV Header(9) + PreviousTagSize0(4)。flags=0x01：仅视频。 */
    public static final byte[] FLV_HEADER = {
            'F', 'L', 'V', 1, 0x01, 0, 0, 0, 9, 0, 0, 0, 0
    };

    public static final byte[] FOURCC_HVC1 = {'h', 'v', 'c', '1'};

    private final CodecType codec;
    private final ByteBufAllocator alloc;

    private volatile byte[] configRecord;
    private volatile String configSignature;
    private final AtomicLong configVersion = new AtomicLong();

    private boolean needBase = true;
    private long baseRtpTs;
    private long baseOutputMs;
    private long lastOutputMs = -1;

    public FlvMuxer(CodecType codec, ByteBufAllocator alloc) {
        this.codec = codec;
        this.alloc = alloc;
    }

    public CodecType codec() {
        return codec;
    }

    /**
     * 更新解码配置记录（AVCDecoderConfigurationRecord / HEVCDecoderConfigurationRecord）。
     *
     * @return true 表示参数集内容发生变化并已重建配置
     */
    public synchronized boolean updateConfig(ParamSets ps) {
        if (ps == null || !ps.isComplete()) {
            return false;
        }
        String sig = ps.signature();
        if (sig.equals(configSignature)) {
            return false;
        }
        byte[] rec = codec == CodecType.H264 ? AvcConfigBuilder.build(ps) : HevcConfigBuilder.build(ps);
        configRecord = rec;
        configSignature = sig;
        configVersion.incrementAndGet();
        return true;
    }

    public boolean hasConfig() {
        return configRecord != null;
    }

    public long configVersion() {
        return configVersion.get();
    }

    /** 构建视频 sequence header FLV Tag（含 PreviousTagSize），返回 refCnt=1 的 ByteBuf。 */
    public ByteBuf buildConfigTag() {
        byte[] rec = configRecord;
        if (rec == null) {
            return null;
        }
        ByteBuf data = alloc.buffer(8 + rec.length);
        if (codec == CodecType.H264) {
            data.writeByte(0x17); // FrameType=keyframe, CodecID=7(AVC)
            data.writeByte(0x00); // AVCPacketType=sequence header
            data.writeMedium(0);  // CompositionTime
        } else {
            data.writeByte(0x80 | (1 << 4)); // IsExHeader=1, FrameType=keyframe, PacketType=0(SequenceStart)
            data.writeBytes(FOURCC_HVC1);
        }
        data.writeBytes(rec);
        return wrapVideoTag(data, Math.max(0, lastOutputMs));
    }

    /** 将一个访问单元封装为 FLV Tag（含 PreviousTagSize），refCnt=1；配置未就绪返回 null。 */
    public ByteBuf mux(AccessUnit au) {
        byte[] rec = configRecord;
        if (rec == null) {
            return null;
        }
        long ts = computeTs(au.rtpTimestamp());
        int size = 8;
        for (byte[] n : au.nalus()) {
            size += 4 + n.length;
        }
        ByteBuf data = alloc.buffer(size);
        if (codec == CodecType.H264) {
            data.writeByte(((au.keyFrame() ? 1 : 2) << 4) | 7); // CodecID=7(AVC)
            data.writeByte(1);    // AVCPacketType=NALU
            data.writeMedium(0);  // CompositionTime=0（无 B 帧）
        } else {
            // IsExHeader=1, FrameType, PacketType=1(CodedFrames)
            data.writeByte(0x80 | ((au.keyFrame() ? 1 : 2) << 4) | 1);
            data.writeBytes(FOURCC_HVC1);
            data.writeMedium(0);  // CompositionTimeOffset=0
        }
        for (byte[] nalu : au.nalus()) {
            data.writeInt(nalu.length); // 4 字节长度前缀
            data.writeBytes(nalu);
        }
        return wrapVideoTag(data, ts);
    }

    /** RTSP 重连后调用：重置 RTP 时间戳基准，输出时间轴继续递增。 */
    public void onStreamReset() {
        needBase = true;
    }

    private long computeTs(long rtpTs) {
        if (needBase) {
            baseRtpTs = rtpTs;
            baseOutputMs = lastOutputMs < 0 ? 0 : lastOutputMs + 40;
            needBase = false;
        }
        // 无符号 32 位回绕安全：差值在 ±2^31 内
        long delta = rtpTs - baseRtpTs;
        long ms = baseOutputMs + delta / 90;
        if (ms < 0) {
            ms = 0;
        }
        if (lastOutputMs >= 0 && ms <= lastOutputMs) {
            ms = lastOutputMs + 1; // 保证单调递增
        }
        lastOutputMs = ms;
        return ms;
    }

    private ByteBuf wrapVideoTag(ByteBuf data, long ts) {
        int size = data.readableBytes();
        ByteBuf tag = alloc.buffer(11 + size + 4);
        tag.writeByte(9);                            // TagType=video
        tag.writeMedium(size);                       // DataSize
        tag.writeMedium((int) (ts & 0xFFFFFF));      // Timestamp 低 24 位
        tag.writeByte((int) ((ts >>> 24) & 0xFF));   // TimestampExtended
        tag.writeMedium(0);                          // StreamID=0
        tag.writeBytes(data);
        data.release();
        tag.writeInt(11 + size);                     // PreviousTagSize
        return tag;
    }
}
