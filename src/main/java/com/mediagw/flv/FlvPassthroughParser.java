package com.mediagw.flv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FLV 裸流透传解析器：上游响应体本身是标准 FLV（HTTP-FLV）时，按 Header / Tag 切块
 * 原样下发，零转码、不改写时间戳与载荷。仅读取 Tag 头判定关键帧与解码配置，
 * 供会话做首屏缓存（FLV 透传模式没有访问单元，不参与 RTSP 转发）。
 * 非线程安全，须在单一收流线程内使用。
 */
public final class FlvPassthroughParser implements StreamBodyParser {

    private static final Logger log = LoggerFactory.getLogger(FlvPassthroughParser.class);

    /** 接收切分结果的回调，均在收流 EventLoop 线程触发。 */
    public interface Listener {
        /** FLV Header（数据偏移 + 4 字节 PreviousTagSize0），一路流一次；所有权移交回调方。 */
        void onFlvHeader(ByteBuf header);

        /** 一个完整 FLV Tag（含尾部 4 字节 PreviousTagSize）；所有权移交回调方。 */
        void onFlvTag(FlvTagKind kind, ByteBuf tag);
    }

    /** Header 定长部分：签名(3) + 版本(1) + 标志(1) + 数据偏移(4)。 */
    private static final int MIN_HEADER_LEN = 9;
    private static final int MAX_HEADER_OFFSET = 64;
    private static final int PREV_TAG_SIZE_LEN = 4;
    private static final int TAG_HEADER_LEN = 11;
    private static final int MAX_TAG_DATA_LEN = 8 * 1024 * 1024;

    private static final int TAG_AUDIO = 8;
    private static final int TAG_VIDEO = 9;
    private static final int TAG_SCRIPT_AMF0 = 17;
    private static final int TAG_SCRIPT_AMF = 18;
    /** AAC soundFormat=10，其后的 AudioPacketType=0 为 sequence header。 */
    private static final int SOUND_FORMAT_AAC = 10;
    /** 经典 FLV 视频 Tag 中带 PacketType 字段的 CodecID：7=AVC，12=H.265。 */
    private static final int CODEC_ID_AVC = 7;
    private static final int CODEC_ID_HEVC = 12;

    private final Listener listener;
    private final ByteBufAllocator alloc;
    private final ByteBuf acc = Unpooled.buffer(64 * 1024);

    private boolean headerDone;
    private boolean broken;     // Tag 边界错乱：停止解析，等待上层重连
    private long frames;
    private long bytes;

    public FlvPassthroughParser(Listener listener, ByteBufAllocator alloc) {
        this.listener = listener;
        this.alloc = alloc;
    }

    /**
     * 判断流数据开头是否为 FLV Header（即上游是普通 FLV 裸流而非 1078 帧流）。
     * 可读字节不足 {@value #MIN_HEADER_LEN} 时返回 false（无法判定）。
     */
    public static boolean isFlvStream(ByteBuf buf) {
        if (buf.readableBytes() < MIN_HEADER_LEN) {
            return false;
        }
        int i = buf.readerIndex();
        if (buf.getByte(i) != 'F' || buf.getByte(i + 1) != 'L' || buf.getByte(i + 2) != 'V') {
            return false;
        }
        int version = buf.getUnsignedByte(i + 3);
        int flags = buf.getUnsignedByte(i + 4);
        long offset = buf.getUnsignedInt(i + 5);
        return version <= 1 && (flags & 0xF8) == 0 && offset >= MIN_HEADER_LEN && offset <= MAX_HEADER_OFFSET;
    }

    @Override
    public void feed(ByteBuf in) {
        bytes += in.readableBytes();
        if (broken) {
            return;
        }
        acc.writeBytes(in);
        if (!headerDone) {
            if (!parseHeader()) {
                return; // Header 未收全或已判定为坏流
            }
            headerDone = true;
        }
        parseTags();
        acc.discardReadBytes();
    }

    @Override
    public void close() {
        acc.release();
    }

    @Override
    public long frameCount() {
        return frames;
    }

    @Override
    public long byteCount() {
        return bytes;
    }

    @Override
    public boolean isBroken() {
        return broken;
    }

    // ===== 切块 =====

    /** @return true 表示 Header 已解析并回调（数据不足时返回 false 继续等待） */
    private boolean parseHeader() {
        int n = acc.readableBytes();
        if (n < MIN_HEADER_LEN) {
            return false;
        }
        if (!isFlvStream(acc)) {
            corrupt("not an FLV stream", n);
            return false;
        }
        int r = acc.readerIndex();
        int headerLen = (int) acc.getUnsignedInt(r + 5) + PREV_TAG_SIZE_LEN;
        if (n < headerLen) {
            return false;
        }
        ByteBuf header = extract(r, headerLen);
        acc.readerIndex(r + headerLen);
        listener.onFlvHeader(header);
        return true;
    }

    private void parseTags() {
        while (!broken) {
            int r = acc.readerIndex();
            int n = acc.readableBytes();
            if (n < 1) {
                return;
            }
            // Tag 首字节即类型：边界错乱时第一时间判定，不必等整帧收全
            int tagType = acc.getUnsignedByte(r);
            if (!knownTagType(tagType)) {
                corrupt("unknown FLV tag type: " + tagType, n);
                return;
            }
            if (n < TAG_HEADER_LEN + PREV_TAG_SIZE_LEN) {
                return;
            }
            int dataLen = acc.getUnsignedMedium(r + 1);
            if (dataLen > MAX_TAG_DATA_LEN) {
                corrupt("FLV tag data out of range: dataLen=" + dataLen, n);
                return;
            }
            int tagLen = TAG_HEADER_LEN + dataLen + PREV_TAG_SIZE_LEN;
            if (n < tagLen) {
                return; // Tag 未收全，等待下一段
            }
            ByteBuf tag = extract(r, tagLen);
            acc.readerIndex(r + tagLen);
            FlvTagKind kind = kindOf(tag, tagType, dataLen);
            if (kind == FlvTagKind.VIDEO_KEYFRAME || kind == FlvTagKind.VIDEO_FRAME) {
                frames++;
            }
            listener.onFlvTag(kind, tag);
        }
    }

    /** 从累积缓冲切出一段独立副本，所有权移交回调方（回调负责 release）。 */
    private ByteBuf extract(int index, int len) {
        ByteBuf out = alloc.buffer(len);
        out.writeBytes(acc, index, len);
        return out;
    }

    /** Tag 边界不可信：丢弃缓冲并停止解析，由上层按断流重连。 */
    private void corrupt(String reason, int buffered) {
        broken = true;
        acc.clear();
        log.warn("{} after {} buffered bytes, stop parsing", reason, buffered);
    }

    private static boolean knownTagType(int tagType) {
        return tagType == TAG_AUDIO || tagType == TAG_VIDEO
                || tagType == TAG_SCRIPT_AMF0 || tagType == TAG_SCRIPT_AMF;
    }

    /** 读取 Tag 头判定类型，载荷内容不做任何改动。 */
    private static FlvTagKind kindOf(ByteBuf tag, int tagType, int dataLen) {
        int d = TAG_HEADER_LEN;
        if (tagType == TAG_VIDEO && dataLen >= 2) {
            int b0 = tag.getUnsignedByte(d);
            if ((b0 & 0x80) != 0) { // Enhanced FLV：IsExHeader=1，低 4 位为 PacketType
                int packetType = b0 & 0x0F;
                if (packetType == 0) {
                    return FlvTagKind.VIDEO_CONFIG;
                }
                return ((b0 & 0x70) >>> 4) == 1 ? FlvTagKind.VIDEO_KEYFRAME : FlvTagKind.VIDEO_FRAME;
            }
            int codecId = b0 & 0x0F;
            if ((codecId == CODEC_ID_AVC || codecId == CODEC_ID_HEVC)
                    && tag.getUnsignedByte(d + 1) == 0) { // AVCPacketType=sequence header
                return FlvTagKind.VIDEO_CONFIG;
            }
            return (b0 >>> 4) == 1 ? FlvTagKind.VIDEO_KEYFRAME : FlvTagKind.VIDEO_FRAME;
        }
        if (tagType == TAG_AUDIO) {
            if (dataLen >= 2 && (tag.getUnsignedByte(d) >>> 4) == SOUND_FORMAT_AAC
                    && tag.getUnsignedByte(d + 1) == 0) {
                return FlvTagKind.AUDIO_CONFIG;
            }
            return FlvTagKind.AUDIO_FRAME;
        }
        return FlvTagKind.OTHER;
    }
}
