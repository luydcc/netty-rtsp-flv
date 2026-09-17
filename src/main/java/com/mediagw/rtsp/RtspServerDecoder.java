package com.mediagw.rtsp;

/**
 * RTSP 服务端流解码器：解析客户端发来的文本请求，
 * 以及 TCP interleaved 模式下客户端上行的 RTCP 报文。
 * 输出 {@link RtspServerRequest} 或 {@link InterleavedFrame}。
 */
public final class RtspServerDecoder extends RtspTcpFramer {

    @Override
    protected Object parseText(String headerBlock, String body) {
        return RtspServerRequest.parse(headerBlock, body);
    }
}
