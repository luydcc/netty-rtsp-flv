package com.mediagw.rtsp;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RTSP 客户端 Pipeline 末端 Handler：分发文本响应与 interleaved 数据帧。 */
final class RtspClientHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger log = LoggerFactory.getLogger(RtspClientHandler.class);

    private final RtspClient client;

    RtspClientHandler(RtspClient client) {
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        client.onActive(ctx.channel());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof RtspResponse resp) {
            client.handleResponse(resp);
        } else if (msg instanceof InterleavedFrame frame) {
            client.handleInterleaved(frame);
        } else {
            log.debug("unexpected message type: {}", msg.getClass());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        client.handleInactive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent e && e.state() == IdleState.READER_IDLE) {
            client.handleReaderIdle();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("[{}] pipeline exception: {}", ctx.channel(), cause.toString());
        client.handleException(cause);
        ctx.close();
    }
}
