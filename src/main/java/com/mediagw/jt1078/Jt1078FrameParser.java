package com.mediagw.jt1078;

import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.flv.StreamBodyParser;
import com.mediagw.rtp.RtpPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * JT/T 1078 视频帧解析器（TCP/HTTP 裸流）。按国标变长包头切帧：
 * <pre>
 *   0-3    帧头标识 0x30 0x31 0x63 0x64 ("01cd")
 *   4      V(2)=2 + P(1)=0 + X(1)=0 + CC(4)=1
 *   5      M(1) + PT(7)
 *   6-7    包序号(WORD)
 *   8-13   SIM 卡号 BCD[6]
 *   14     逻辑通道号
 *   15     数据类型(高4bit: 0=视频I帧 1=视频P帧 2=视频B帧 3=音频 4=透传)
 *          + 分包处理标记(低4bit: 0=原子包 1=分包第一包 2=分包最后包 3=分包中间包)
 *   16-23  时间戳 BYTE[8](单位 ms，仅诊断用；透传帧无此字段)
 *   24-25  Last I Frame Interval(单位 ms，非视频帧无此字段)
 *   26-27  Last Frame Interval(单位 ms，非视频帧无此字段，用于输出时间轴)
 *   ..-..  数据体长度(WORD，紧随其前的 2 字节)
 *   ..     数据体(H.264，标准为 Annex-B 起始码流；兼容无起始码的裸 NALU)
 * </pre>
 * 包头长度随数据类型变长：视频 30 字节、音频 26 字节、透传 18 字节。
 * 大帧（I 帧常超 950 字节）按分包处理标记跨包做字节级拼接，收齐最后包后作为完整
 * 一帧喂给 {@link VideoDepacketizer}（marker=原子/最后包），复用其起始码切分、参数集
 * 缓存与访问单元界定。输出时间轴按帧推进，间隔取帧头 Last Frame Interval(ms)，
 * 缺省 40ms(25fps@90kHz)。非线程安全，须在单一收流线程内使用。
 */
public final class Jt1078FrameParser implements StreamBodyParser {

    /** 合法帧头魔数 "01cd"。 */
    public static final byte[] MAGIC = {0x30, 0x31, 0x63, 0x64};
    /** 数据类型|分包标记字节偏移（各类型固定）。 */
    private static final int TYPE_OFFSET = 15;
    /** Last Frame Interval 字段偏移（视频帧，单位 ms）。 */
    private static final int F_INTERVAL_OFFSET = 26;
    /** 读满该字节数才能取到类型字节，进而确定变长包头总长。 */
    private static final int MIN_HEADER_LEN = TYPE_OFFSET + 1;
    /** 视频帧包头总长：含时间戳(8) + 两个间隔(2+2) + 数据体长度(2)。 */
    private static final int VIDEO_HEADER_LEN = 30;
    /** 音频帧包头总长：含时间戳(8) + 数据体长度(2)，无两个间隔字段。 */
    private static final int AUDIO_HEADER_LEN = 26;
    /** 透传帧包头总长：无时间戳、无间隔，仅数据体长度(2)。 */
    private static final int PASSTHROUGH_HEADER_LEN = 18;

    private static final int MAX_FRAME_BYTES = 2 * 1024 * 1024;
    /** 缺省帧间隔：90kHz 下 40ms(25fps)。 */
    private static final int DEFAULT_FRAME_TICKS = 3600;
    /** Last Frame Interval 单位 ms -> 90 ticks@90kHz。 */
    private static final int TICKS_PER_MS = 90;
    private static final int FAKE_SSRC = 0x4A543738; // "JT78"
    private static final int FAKE_PT = 96;

    private final VideoDepacketizer depacketizer;
    private final ByteBuf acc = Unpooled.buffer(8192);

    private int seq;                  // 伪 RTP 序号，本地连续
    private long frameTs;             // 伪 RTP 时间戳，按帧推进
    private final ByteBuf frag = Unpooled.buffer(64 * 1024); // 分包字节级拼接缓冲
    private boolean fragOpen;         // 已收分包第一包，尚未收到最后包
    private long frames;              // 已重组的视频帧数
    private long bytes;               // 累计收流字节数
    private long dropped;             // 因分包断链丢弃的帧数

    public Jt1078FrameParser(VideoDepacketizer depacketizer) {
        this.depacketizer = depacketizer;
    }

    /** 追加一段 TCP/HTTP 流数据并尽量切出完整帧。 */
    @Override
    public void feed(ByteBuf in) {
        bytes += in.readableBytes();
        acc.writeBytes(in);
        while (true) {
            int start = findMagic();
            if (start < 0 || acc.readableBytes() < MIN_HEADER_LEN) {
                break; // 帧头标识或类型字节尚未收齐
            }
            int typeByte = acc.getUnsignedByte(start + TYPE_OFFSET);
            int dataType = typeByte >>> 4;
            int packTag = typeByte & 0x0F;
            int headerLen = headerLen(dataType);
            if (headerLen < 0) {
                acc.readerIndex(start + 1); // 非法数据类型，伪帧头前跳重扫
                continue;
            }
            if (acc.readableBytes() < headerLen) {
                break; // 变长包头未收齐
            }
            int bodyLen = acc.getUnsignedShort(start + headerLen - 2);
            if (bodyLen > MAX_FRAME_BYTES) {
                acc.readerIndex(start + 1); // 长度越界，伪帧头前跳重扫
                continue;
            }
            if (acc.readableBytes() < headerLen + bodyLen) {
                break; // 数据体未收全
            }
            int lastFrameInterval = dataType <= 2 ? acc.getUnsignedShort(start + F_INTERVAL_OFFSET) : 0;
            ByteBuf body = acc.slice(start + headerLen, bodyLen);
            try {
                handleFrame(dataType, packTag, lastFrameInterval, body);
            } finally {
                acc.readerIndex(start + headerLen + bodyLen);
            }
        }
        acc.discardReadBytes();
    }

    /** 按数据类型确定国标变长包头总长；非法类型返回 -1。 */
    private static int headerLen(int dataType) {
        if (dataType <= 2) {
            return VIDEO_HEADER_LEN;        // 视频 I/P/B 帧
        }
        if (dataType == 3) {
            return AUDIO_HEADER_LEN;        // 音频
        }
        if (dataType == 4) {
            return PASSTHROUGH_HEADER_LEN;  // 透传
        }
        return -1;
    }

    /** 会话终止：释放内部缓冲。 */
    @Override
    public void close() {
        frag.release();
        acc.release();
    }

    /** 已重组输出的视频帧数。 */
    @Override
    public long frameCount() {
        return frames;
    }

    /** 累计收流字节数。 */
    @Override
    public long byteCount() {
        return bytes;
    }

    /** 因分包断链丢弃的帧数。 */
    public long droppedFrames() {
        return dropped;
    }

    // ===== 帧分发 =====

    private void handleFrame(int dataType, int packTag, int lastFrameInterval, ByteBuf body) {
        if (dataType >= 3) {
            return; // 音频/透传：本网关仅分发视频
        }
        switch (packTag) {
            case 0 -> { // 原子包：完整一帧
                if (fragOpen) {
                    discardFragment();
                }
                feedVideo(body, true);
                frames++;
                advanceTs(lastFrameInterval);
            }
            case 1 -> { // 分包第一包：开新帧缓冲，按字节拼接
                if (fragOpen) {
                    discardFragment();
                }
                frag.clear();
                frag.writeBytes(body);
                fragOpen = true;
            }
            case 2 -> { // 分包最后包：拼接完成，整帧喂入
                if (!fragOpen) {
                    dropped++; // 孤儿尾包，无重组上下文
                    return;
                }
                frag.writeBytes(body);
                feedVideo(frag, true);
                frag.clear();
                fragOpen = false;
                frames++;
                advanceTs(lastFrameInterval);
            }
            default -> { // 分包中间包
                if (!fragOpen) {
                    dropped++;
                    return;
                }
                frag.writeBytes(body);
            }
        }
    }

    /** 分包链断裂（残帧未收完就来新帧）：丢弃半截缓冲，整帧作废。 */
    private void discardFragment() {
        frag.clear();
        fragOpen = false;
        dropped++;
    }

    private void feedVideo(ByteBuf body, boolean finalPacket) {
        int n = body.readableBytes();
        if (n == 0) {
            return;
        }
        if (isAnnexB(body)) {
            feedAnnexB(body, finalPacket);
        } else {
            feedSlice(body, body.readerIndex(), n, finalPacket);
        }
    }

    /** 数据体以 Annex-B 起始码开头（非标实现）：切分 NALU 后逐个以 single-NALU 伪包喂入。 */
    private void feedAnnexB(ByteBuf body, boolean finalPacket) {
        int n = body.readableBytes();
        int naluStart = -1;
        int idx = 0;
        while (idx < n) {
            int sc = startCodeLen(body, idx);
            if (sc > 0) {
                if (naluStart >= 0 && idx > naluStart) {
                    feedSlice(body, naluStart, idx - naluStart, false);
                }
                idx += sc;
                naluStart = idx;
            } else {
                idx++;
            }
        }
        if (naluStart >= 0 && naluStart < n) {
            feedSlice(body, naluStart, n - naluStart, finalPacket);
        }
    }

    /** 把一段 H.264 数据作为伪 RTP 负载喂给 depacketizer。 */
    private void feedSlice(ByteBuf body, int index, int len, boolean marker) {
        ByteBuf buf = Unpooled.buffer(12 + len);
        writeFakeRtpHeader(buf, marker);
        buf.writeBytes(body, index, len);
        RtpPacket pkt = RtpPacket.parse(buf);
        if (pkt != null) {
            depacketizer.onRtpPacket(pkt);
        }
        buf.release();
    }

    private void writeFakeRtpHeader(ByteBuf buf, boolean marker) {
        buf.writeByte(0x80);                                       // V=2
        buf.writeByte((marker ? 0x80 : 0) | FAKE_PT);
        buf.writeShort(seq++ & 0xFFFF);
        buf.writeInt((int) frameTs);
        buf.writeInt(FAKE_SSRC);
    }

    /** 帧输出完毕后推进时间轴：Last Frame Interval 单位 ms，0 视为未提供。 */
    private void advanceTs(int lastFrameIntervalMs) {
        frameTs += lastFrameIntervalMs > 0
                ? (long) lastFrameIntervalMs * TICKS_PER_MS
                : DEFAULT_FRAME_TICKS;
    }

    // ===== 流定位辅助 =====

    /** 从当前读位置向后找帧头魔数；未找到时保留末尾最多 3 字节前缀并返回 -1。 */
    private int findMagic() {
        int end = acc.writerIndex();
        for (int i = acc.readerIndex(); i + 4 <= end; i++) {
            if (acc.getByte(i) == MAGIC[0] && acc.getByte(i + 1) == MAGIC[1]
                    && acc.getByte(i + 2) == MAGIC[2] && acc.getByte(i + 3) == MAGIC[3]) {
                acc.readerIndex(i);
                return i;
            }
        }
        acc.readerIndex(Math.max(acc.readerIndex(), end - 3));
        return -1;
    }

    private static boolean isAnnexB(ByteBuf body) {
        return startCodeLen(body, body.readerIndex()) > 0;
    }

    /** index 处 Annex-B 起始码长度（3 或 4 字节），非起始码返回 0。 */
    private static int startCodeLen(ByteBuf body, int index) {
        int n = body.readableBytes();
        if (index + 3 > n || body.getByte(index) != 0 || body.getByte(index + 1) != 0) {
            return 0;
        }
        int b2 = body.getByte(index + 2) & 0xFF;
        if (b2 == 1) {
            return 3;
        }
        if (b2 == 0 && index + 4 <= n && (body.getByte(index + 3) & 0xFF) == 1) {
            return 4;
        }
        return 0;
    }
}
