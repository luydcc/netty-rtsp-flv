package com.mediagw.rtp;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * RTP 打包基类（RFC 3550）：把一个访问单元拆成一个或多个 RTP 包。
 * 每个转发客户端持有独立实例，各自维护 SSRC、序列号与单调递增时间戳。
 * 非线程安全，须在单一 EventLoop 线程内使用。
 */
public abstract class RtpPacketizer {

    protected static final int RTP_HEADER_SIZE = 12;

    protected final ByteBufAllocator alloc;
    protected final int payloadType;
    protected final int ssrc;
    /** 单个 RTP 包载荷上限（不含 12 字节 RTP 头），由 MTU 扣除 IP/UDP/RTP 开销得出。 */
    protected final int maxPayloadBytes;

    private int seq;
    private long lastInTs = -1;
    private long outTs;

    protected RtpPacketizer(ByteBufAllocator alloc, int payloadType, int ssrc, int maxPayloadBytes) {
        this.alloc = alloc;
        this.payloadType = payloadType;
        this.ssrc = ssrc;
        this.maxPayloadBytes = Math.max(64, maxPayloadBytes);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        this.seq = rnd.nextInt(0x10000);
        this.outTs = rnd.nextInt() & 0xFFFFFFFFL;
    }

    /** 参数集就绪或变化；子类在关键帧前带内插入，保证中途加入的客户端可解码。 */
    public abstract void updateParams(ParamSets ps);

    /** 打包一个访问单元，产出的 RTP 包经 out 交出（引用计数所有权移交调用方）。 */
    public abstract void packetize(AccessUnit au, Consumer<ByteBuf> out);

    /** 新建 RTP 包并写好 12 字节头，返回时 writerIndex 停在头之后，由子类追加载荷。 */
    protected ByteBuf newPacket(long timestamp, boolean marker) {
        ByteBuf buf = alloc.buffer(RTP_HEADER_SIZE + maxPayloadBytes);
        buf.writeByte(0x80); // V=2, P=0, X=0, CC=0
        buf.writeByte((marker ? 0x80 : 0) | (payloadType & 0x7F));
        buf.writeShort(seq);
        buf.writeInt((int) timestamp);
        buf.writeInt(ssrc);
        seq = (seq + 1) & 0xFFFF;
        return buf;
    }

    /**
     * 上游 90kHz 时间戳映射为本端时间戳：随机起点 + 上游增量，
     * 上游回跳（重连后时间戳归零）时按 0 增量处理，避免播放端时间轴倒退。
     */
    protected long mapTimestamp(long rtpTimestamp) {
        if (lastInTs < 0) {
            lastInTs = rtpTimestamp;
            return outTs;
        }
        long delta = (rtpTimestamp - lastInTs) & 0xFFFFFFFFL;
        lastInTs = rtpTimestamp;
        if (delta > 0x80000000L) {
            delta = 0;
        }
        outTs = (outTs + delta) & 0xFFFFFFFFL;
        return outTs;
    }
}
