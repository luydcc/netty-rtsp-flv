package com.mediagw.gb28181;

import com.mediagw.config.GatewayConfig;
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
import java.util.List;
import java.util.regex.Pattern;

/**
 * GB28181 通道的 HTTP-FLV 分发：{@code GET /gb/live/{channelId}.flv} 订阅
 * {@link GbPlayManager} 的点播会话，chunked 长连接下发 FLV。
 *
 * <p>与 RTSP 侧 HttpFlvHandler 的区别：不自动触发拉流——播放须先经页面
 * {@code POST /api/gb28181/play} 发起 INVITE，未点播的通道直接 404。
 */
public final class GbFlvHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(GbFlvHandler.class);
    private static final String PREFIX = "/gb/live/";
    private static final AttributeKey<String> ATTR_CHANNEL_ID = AttributeKey.valueOf("gbflv.channelId");
    /** channelId 白名单字符集，防路径穿越。 */
    private static final Pattern CHANNEL_ID = Pattern.compile("[\\w-]{1,64}");

    private final Gb28181Service service;
    private final GatewayConfig config;

    public GbFlvHandler(Gb28181Service service, GatewayConfig config) {
        this.service = service;
        this.config = config;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ctx.fireChannelRead(msg);
            return;
        }
        String path = new QueryStringDecoder(req.uri()).path();
        if (!path.startsWith(PREFIX) || !path.endsWith(".flv")) {
            ctx.fireChannelRead(req); // 非国标播放请求，透传给下游，勿释放
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
        String channelId = path.substring(PREFIX.length(), path.length() - ".flv".length());
        if (!CHANNEL_ID.matcher(channelId).matches()) {
            sendError(ctx, req, HttpResponseStatus.BAD_REQUEST, "bad channel path");
            return;
        }
        if (!tokenAllowed(req.uri())) {
            sendError(ctx, req, HttpResponseStatus.UNAUTHORIZED, "unauthorized");
            return;
        }
        // 未点播的通道直接 404（响应头写出后无法再改状态码）
        if (service.findSession(channelId) == null) {
            log.warn("[GB28181] 播放请求未先点播: channel={}, client={}", channelId, ctx.channel().remoteAddress());
            sendError(ctx, req, HttpResponseStatus.NOT_FOUND, "not playing, call /api/gb28181/play first");
            return;
        }
        HttpResponse resp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "video/x-flv");
        resp.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        resp.headers().set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_STORE);
        resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.channel().attr(ATTR_CHANNEL_ID).set(channelId);
        ctx.writeAndFlush(resp);
        service.subscribe(channelId, ctx.channel());
    }

    /** 与 RTSP 侧播放共用 auth.token 鉴权（查询参数 token）。 */
    private boolean tokenAllowed(String uri) {
        String expected = config.authToken();
        if (expected == null || expected.isEmpty()) {
            return true;
        }
        List<String> tokens = new QueryStringDecoder(uri).parameters().get("token");
        return tokens != null && expected.equals(tokens.getFirst());
    }

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
        String channelId = ctx.channel().attr(ATTR_CHANNEL_ID).get();
        if (channelId != null) {
            log.info("[GB28181] GB-FLV 断开: channel={}, client={}", channelId, ctx.channel().remoteAddress());
            service.unsubscribe(channelId, ctx.channel());
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("[GB28181] GB-FLV error on {}: {}", ctx.channel().remoteAddress(), cause.toString());
        ctx.close();
    }
}
