package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;

/**
 * RTSP over TCP interleaved 二进制帧：{@code $ + channel(1B) + length(2B) + payload}。
 * payload 为持有的 ByteBuf（引用计数由本对象管理），使用后须调用 {@link #release()}。
 * channel 偶数通常为 RTP，奇数为 RTCP。
 */
public final class InterleavedFrame {

    private final int channel;
    private final ByteBuf payload;

    public InterleavedFrame(int channel, ByteBuf payload) {
        this.channel = channel;
        this.payload = payload;
    }

    public int channel() {
        return channel;
    }

    public ByteBuf payload() {
        return payload;
    }

    public boolean isRtp() {
        return (channel & 1) == 0;
    }

    public void release() {
        if (payload.refCnt() > 0) {
            payload.release();
        }
    }
}
