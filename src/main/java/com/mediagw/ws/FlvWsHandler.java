package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.FlvTransport;
import com.mediagw.session.StreamSessionManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WebSocket FLV 分发 Handler。
 * 握手完成后解析 {@code /live/{streamId}.flv?token=xxx} 的 streamId 与 token，
 * 校验跨域来源与鉴权通过后订阅对应 StreamSession；连接关闭时退订。
 */
public final class FlvWsHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(FlvWsHandler.class);
    private static final AttributeKey<String> ATTR_STREAM_ID = AttributeKey.valueOf("flv.streamId");
    private static final WebSocketCloseStatus CLOSE_BAD_REQUEST = new WebSocketCloseStatus(4400, "bad stream path");
    private static final WebSocketCloseStatus CLOSE_UNAUTHORIZED = new WebSocketCloseStatus(4401, "unauthorized");
    private static final WebSocketCloseStatus CLOSE_ORIGIN_NOT_ALLOWED =
            new WebSocketCloseStatus(4403, "origin not allowed");

    private final StreamSessionManager manager;
    private final LiveAccessPolicy policy;

    public FlvWsHandler(StreamSessionManager manager, GatewayConfig config) {
        this.manager = manager;
        this.policy = new LiveAccessPolicy(config);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete handshake) {
            onHandshakeComplete(ctx, handshake);
            return;
        }
        super.userEventTriggered(ctx, evt);
    }

    private void onHandshakeComplete(ChannelHandlerContext ctx,
                                     WebSocketServerProtocolHandler.HandshakeComplete handshake) {
        String uri = handshake.requestUri();
        String streamId = LiveAccessPolicy.streamId(new QueryStringDecoder(uri).path());
        if (streamId == null) {
            close(ctx.channel(), CLOSE_BAD_REQUEST);
            return;
        }
        if (!policy.originAllowed(handshake.requestHeaders())) {
            log.warn("origin '{}' not allowed to access stream '{}' from {}",
                    handshake.requestHeaders().get(HttpHeaderNames.ORIGIN), streamId, ctx.channel().remoteAddress());
            close(ctx.channel(), CLOSE_ORIGIN_NOT_ALLOWED);
            return;
        }
        if (!policy.tokenAllowed(uri)) {
            log.warn("unauthorized access to stream '{}' from {}", streamId, ctx.channel().remoteAddress());
            close(ctx.channel(), CLOSE_UNAUTHORIZED);
            return;
        }
        ctx.channel().attr(ATTR_STREAM_ID).set(streamId);
        log.info("WebSocket connected: stream='{}' client={}", streamId, ctx.channel().remoteAddress());
        manager.subscribe(streamId, ctx.channel(), FlvTransport.WEBSOCKET);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
        // 视频流为服务端单向下行；客户端数据帧直接丢弃（Ping/Close 已由上游协议 Handler 处理）
        log.trace("ignoring inbound frame {} from {}", frame.getClass().getSimpleName(), ctx.channel().remoteAddress());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String streamId = ctx.channel().attr(ATTR_STREAM_ID).get();
        if (streamId != null) {
            log.info("WebSocket disconnected: stream='{}' client={}", streamId, ctx.channel().remoteAddress());
            manager.unsubscribe(streamId, ctx.channel());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("WebSocket error on {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }

    private static void close(Channel ch, WebSocketCloseStatus status) {
        if (ch.isActive()) {
            ch.writeAndFlush(new CloseWebSocketFrame(status)).addListener(ChannelFutureListener.CLOSE);
        } else {
            ch.close();
        }
    }
}
