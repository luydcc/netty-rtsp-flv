package com.mediagw.rtsp;

/**
 * RTSP 客户端流解码器：同一连接上复用文本响应与 {@code $} 开头的
 * interleaved RTP/RTCP 二进制帧。输出 {@link RtspResponse} 或 {@link InterleavedFrame}。
 */
public final class RtspTcpDecoder extends RtspTcpFramer {

    @Override
    protected Object parseText(String headerBlock, String body) {
        return RtspResponse.parse(headerBlock, body);
    }
}
