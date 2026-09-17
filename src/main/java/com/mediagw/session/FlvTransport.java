package com.mediagw.session;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;

/**
 * FLV 下行传输方式：同一份 FLV Tag 字节流按不同协议封装后写出。
 * WEBSOCKET 用于 {@code ws://.../live/{id}.flv}，HTTP_FLV 用于 {@code http://.../live/{id}.flv}
 * （chunked 长连接，flv.js / VLC / ffmpeg 均可直接播放）。
 */
public enum FlvTransport {

    /** WebSocket 二进制帧。 */
    WEBSOCKET,

    /** HTTP chunked 内容块。 */
    HTTP_FLV;

    /** 封装一段 FLV 数据，数据所有权移交返回的消息对象（写出后由 pipeline 释放）。 */
    public Object frame(ByteBuf data) {
        return this == HTTP_FLV ? new DefaultHttpContent(data) : new BinaryWebSocketFrame(data);
    }

    /** 结束下行的收尾消息：WebSocket 关闭帧 / HTTP 最后一个空块。 */
    public Object endMessage(WebSocketCloseStatus status) {
        return this == HTTP_FLV ? LastHttpContent.EMPTY_LAST_CONTENT : new CloseWebSocketFrame(status);
    }
}
