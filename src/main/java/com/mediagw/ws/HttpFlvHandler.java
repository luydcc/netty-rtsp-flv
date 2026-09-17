package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.FlvTransport;
import com.mediagw.session.StreamSessionManager;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * HTTP-FLV 分发 Handler：{@code GET /live/{streamId}.flv} 以 chunked 长连接持续下发 FLV 字节流，
 * flv.js / VLC / ffmpeg 可直接播放（无需 WebSocket）。
 *
 * <p>位于 {@link HttpControlHandler} 与 WebSocketServerProtocolHandler 之间：
 * 只接管 {@code /live} 前缀且不带 {@code Upgrade: websocket} 的请求，其余透传给下游握手。
 * 与 WebSocket 共用同一份 FLV 输出，仅下行封装不同（见 {@link FlvTransport}）。
 */
public final class HttpFlvHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(HttpFlvHandler.class);
    private static final AttributeKey<String> ATTR_STREAM_ID = AttributeKey.valueOf("httpflv.streamId");

    private final StreamSessionManager manager;
    private final LiveAccessPolicy policy;

    public HttpFlvHandler(StreamSessionManager manager, GatewayConfig config) {
        this.manager = manager;
        this.policy = new LiveAccessPolicy(config);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ctx.fireChannelRead(msg);
            return;
        }
        String path = new QueryStringDecoder(req.uri()).path();
        if (!path.startsWith(LiveAccessPolicy.LIVE_PREFIX) || isWebSocketUpgrade(req)) {
            ctx.fireChannelRead(req); // 交给 WebSocketServerProtocolHandler，勿释放
            return;
        }
        try {
            handle(ctx, req, path);
        } finally {
            req.release();
        }
    }

    private void handle(ChannelHandlerContext ctx, FullHttpRequest req, String path) {
        if (!HttpMethod.GET.equals(req.method())) {
            sendError(ctx, req, HttpResponseStatus.METHOD_NOT_ALLOWED, "method not allowed");
            return;
        }
        String streamId = LiveAccessPolicy.streamId(path);
        if (streamId == null) {
            sendError(ctx, req, HttpResponseStatus.BAD_REQUEST, "bad stream path");
            return;
        }
        if (!policy.originAllowed(req.headers())) {
            log.warn("origin '{}' not allowed to access stream '{}' from {}",
                    req.headers().get(HttpHeaderNames.ORIGIN), streamId, ctx.channel().remoteAddress());
            sendError(ctx, req, HttpResponseStatus.FORBIDDEN, "origin not allowed");
            return;
        }
        if (!policy.tokenAllowed(req.uri())) {
            log.warn("unauthorized access to stream '{}' from {}", streamId, ctx.channel().remoteAddress());
            sendError(ctx, req, HttpResponseStatus.UNAUTHORIZED, "unauthorized");
            return;
        }
        // 响应头一旦写出就无法再改状态码，未知流与拉流名额已满必须先在此拦下
        if (manager.streamUrl(streamId) == null) {
            log.warn("unknown streamId '{}' requested by {}", streamId, ctx.channel().remoteAddress());
            sendError(ctx, req, HttpResponseStatus.NOT_FOUND, "unknown stream");
            return;
        }
        if (manager.pullLimitReached(streamId)) {
            log.warn("pull limit reached, reject HTTP-FLV '{}' from {}", streamId, ctx.channel().remoteAddress());
            sendError(ctx, req, HttpResponseStatus.SERVICE_UNAVAILABLE, "too many streams");
            return;
        }
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "video/x-flv");
        resp.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        resp.headers().set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_STORE);
        resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.channel().attr(ATTR_STREAM_ID).set(streamId);
        ctx.writeAndFlush(resp); // 立即下发响应头，避免播放器等不到首包而超时
        log.info("HTTP-FLV connected: stream='{}' client={}", streamId, ctx.channel().remoteAddress());
        manager.subscribe(streamId, ctx.channel(), FlvTransport.HTTP_FLV);
    }

    private static boolean isWebSocketUpgrade(FullHttpRequest req) {
        String upgrade = req.headers().get(HttpHeaderNames.UPGRADE);
        return upgrade != null && upgrade.toLowerCase().contains(HttpHeaderValues.WEBSOCKET.toString());
    }

    /** 错误响应带简短正文，便于 curl / 浏览器直接看到原因。 */
    private static void sendError(ChannelHandlerContext ctx, FullHttpRequest req,
                                  HttpResponseStatus status, String message) {
        byte[] body = (status.code() + " " + message).getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
        resp.headers().set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_STORE);
        resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        if (HttpUtil.isKeepAlive(req)) {
            resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(resp);
        } else {
            ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String streamId = ctx.channel().attr(ATTR_STREAM_ID).get();
        if (streamId != null) {
            log.info("HTTP-FLV disconnected: stream='{}' client={}", streamId, ctx.channel().remoteAddress());
            manager.unsubscribe(streamId, ctx.channel());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("HTTP-FLV error on {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
