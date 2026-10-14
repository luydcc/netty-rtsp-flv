package com.mediagw.gb28181;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * SIP over TCP 流切分：按空行取出头部块，再按 Content-Length 取出消息体，
 * 每条完整报文输出为一个 {@link SipMessage}。
 */
public final class SipTcpFramer extends ByteToMessageDecoder {

    /** 单条 SIP 报文（头 + body，MANSCDP XML 一般数 KB）上限。 */
    private static final int MAX_MESSAGE_SIZE = 256 * 1024;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.isReadable()) {
            in.markReaderIndex();
            int headerLen = findHeaderEnd(in);
            if (headerLen < 0) {
                if (in.readableBytes() > MAX_MESSAGE_SIZE) {
                    throw new IllegalStateException("SIP message headers exceed " + MAX_MESSAGE_SIZE + " bytes");
                }
                in.resetReaderIndex();
                return;
            }
            int contentLength = peekContentLength(in, headerLen);
            if (headerLen + contentLength > MAX_MESSAGE_SIZE) {
                throw new IllegalStateException("SIP message exceeds " + MAX_MESSAGE_SIZE + " bytes");
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
            out.add(SipMessage.parse(new String(headerBytes, StandardCharsets.US_ASCII), body));
        }
    }

    /** 返回头部块长度（含结尾 CRLFCRLF），未找齐返回 -1。相对 readerIndex。 */
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
