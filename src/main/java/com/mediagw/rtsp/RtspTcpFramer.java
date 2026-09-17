package com.mediagw.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * RTSP over TCP 流切分基类：同一连接上复用文本消息与 {@code $} 开头的
 * interleaved RTP/RTCP 二进制帧。二进制帧统一输出 {@link InterleavedFrame}，
 * 文本消息交由子类解析（客户端解析 {@link RtspResponse}，服务端解析 {@link RtspServerRequest}）。
 */
public abstract class RtspTcpFramer extends ByteToMessageDecoder {

    /** 文本消息（头 + body，SDP 一般数 KB）上限。 */
    protected static final int MAX_TEXT_SIZE = 256 * 1024;
    /** 单个 interleaved 帧上限（RTP over TCP 包远小于此）。 */
    protected static final int MAX_INTERLEAVED_SIZE = 1024 * 1024;

    /** 解析一条完整的文本消息（headerBlock 含结尾 {@code \r\n\r\n}）。 */
    protected abstract Object parseText(String headerBlock, String body);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.isReadable()) {
            in.markReaderIndex();
            int first = in.getUnsignedByte(in.readerIndex());
            if (first == '$') {
                if (in.readableBytes() < 4) {
                    in.resetReaderIndex();
                    return;
                }
                int channel = in.getUnsignedByte(in.readerIndex() + 1);
                int len = in.getUnsignedShort(in.readerIndex() + 2);
                if (len > MAX_INTERLEAVED_SIZE) {
                    throw new RtspException("interleaved frame too large: " + len);
                }
                if (in.readableBytes() < 4 + len) {
                    in.resetReaderIndex();
                    return;
                }
                in.skipBytes(4);
                out.add(new InterleavedFrame(channel, in.readRetainedSlice(len)));
            } else {
                int headerLen = findHeaderEnd(in);
                if (headerLen < 0) {
                    if (in.readableBytes() > MAX_TEXT_SIZE) {
                        throw new RtspException("RTSP message headers exceed " + MAX_TEXT_SIZE + " bytes");
                    }
                    in.resetReaderIndex();
                    return;
                }
                int contentLength = peekContentLength(in, headerLen);
                if (headerLen + contentLength > MAX_TEXT_SIZE) {
                    throw new RtspException("RTSP message exceeds " + MAX_TEXT_SIZE + " bytes");
                }
                if (in.readableBytes() < headerLen + contentLength) {
                    in.resetReaderIndex();
                    return;
                }
                byte[] headerBytes = new byte[headerLen];
                in.readBytes(headerBytes);
                String body = "";
                if (contentLength > 0) {
                    byte[] bodyBytes = new byte[contentLength];
                    in.readBytes(bodyBytes);
                    body = new String(bodyBytes, StandardCharsets.UTF_8);
                }
                out.add(parseText(new String(headerBytes, StandardCharsets.US_ASCII), body));
            }
        }
    }

    /** 返回头部块长度（含结尾 \r\n\r\n），未找齐返回 -1。相对 readerIndex。 */
    private static int findHeaderEnd(ByteBuf in) {
        int start = in.readerIndex();
        int limit = in.writerIndex() - 3;
        for (int i = start; i < limit; i++) {
            if (in.getByte(i) == '\r' && in.getByte(i + 1) == '\n'
                    && in.getByte(i + 2) == '\r' && in.getByte(i + 3) == '\n') {
                return i + 4 - start;
            }
        }
        return -1;
    }

    private static int peekContentLength(ByteBuf in, int headerLen) {
        byte[] tmp = new byte[headerLen];
        in.getBytes(in.readerIndex(), tmp);
        String head = new String(tmp, StandardCharsets.US_ASCII).toLowerCase(Locale.ROOT);
        int idx = head.indexOf("content-length:");
        if (idx < 0) {
            return 0;
        }
        int eol = head.indexOf('\r', idx);
        String v = head.substring(idx + "content-length:".length(), eol < 0 ? head.length() : eol).trim();
        try {
            return Math.max(0, Integer.parseInt(v));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
