package com.mediagw.rtp;

import io.netty.buffer.ByteBuf;

/**
 * RTP 包解析（RFC 3550）。payload 为输入 ByteBuf 的 slice，不持有所有权，
 * 调用方须保证在输入 buffer 生命周期内使用。
 */
public final class RtpPacket {

    private final int seq;
    private final long timestamp;
    private final int ssrc;
    private final boolean marker;
    private final int payloadType;
    private final ByteBuf payload;

    private RtpPacket(int seq, long timestamp, int ssrc, boolean marker, int payloadType, ByteBuf payload) {
        this.seq = seq;
        this.timestamp = timestamp;
        this.ssrc = ssrc;
        this.marker = marker;
        this.payloadType = payloadType;
        this.payload = payload;
    }

    /** 解析失败（非法/截断的包）返回 null。 */
    public static RtpPacket parse(ByteBuf buf) {
        int n = buf.readableBytes();
        if (n < 12) {
            return null;
        }
        int base = buf.readerIndex();
        int b0 = buf.getUnsignedByte(base);
        int version = b0 >>> 6;
        if (version != 2) {
            return null;
        }
        boolean padding = ((b0 >>> 5) & 1) == 1;
        boolean extension = ((b0 >>> 4) & 1) == 1;
        int csrcCount = b0 & 0x0F;
        int b1 = buf.getUnsignedByte(base + 1);
        boolean marker = ((b1 >>> 7) & 1) == 1;
        int payloadType = b1 & 0x7F;
        int seq = buf.getUnsignedShort(base + 2);
        long timestamp = buf.getUnsignedInt(base + 4);
        long ssrc = buf.getUnsignedInt(base + 8);

        int idx = 12 + csrcCount * 4;
        if (idx > n) {
            return null;
        }
        if (extension) {
            if (idx + 4 > n) {
                return null;
            }
            int extLen = buf.getUnsignedShort(base + idx + 2);
            idx += 4 + extLen * 4;
            if (idx > n) {
                return null;
            }
        }
        int end = n;
        if (padding) {
            int pad = buf.getUnsignedByte(base + n - 1);
            if (pad < 1 || pad > n - idx) {
                return null;
            }
            end = n - pad;
        }
        if (end <= idx) {
            return null;
        }
        return new RtpPacket(seq, timestamp, (int) ssrc, marker, payloadType, buf.slice(base + idx, end - idx));
    }

    public int seq() {
        return seq;
    }

    public long timestamp() {
        return timestamp;
    }

    public int ssrc() {
        return ssrc;
    }

    public boolean marker() {
        return marker;
    }

    public int payloadType() {
        return payloadType;
    }

    public ByteBuf payload() {
        return payload;
    }
}
