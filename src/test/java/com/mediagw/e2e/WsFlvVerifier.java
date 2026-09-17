package com.mediagw.e2e;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebSocket-FLV 端到端校验器（联调用，非单元测试）。
 * <pre>
 * 用法:
 *   flv  模式: WsFlvVerifier ws://host:port/live/id.flv [秒数，默认6]
 *   close模式: WsFlvVerifier ws://host:port/live/id.flv close &lt;期望关闭码&gt;
 * 退出码 0=PASS 1=FAIL
 * </pre>
 * flv 模式校验：FLV Header、首个 Tag 为 sequence header（H.264 avcC / H.265 hvcC 自动识别）、
 * Tag 结构（type=9、StreamID=0、PreviousTagSize）、帧类型、时间戳单调不减、关键帧/帧数下限。
 */
public final class WsFlvVerifier {

    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private final AtomicInteger messages = new AtomicInteger();
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private volatile int closeCode = -1;
    private volatile String closeReason = "";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: WsFlvVerifier <ws-url> [seconds | close <code>]");
            System.exit(2);
        }
        String url = args[0];
        boolean closeMode = args.length >= 2 && args[1].equals("close");
        int expectCode = closeMode ? Integer.parseInt(args[2]) : 0;
        int seconds = !closeMode && args.length >= 2 ? Integer.parseInt(args[1]) : 6;
        System.exit(new WsFlvVerifier().run(url, seconds, closeMode, expectCode));
    }

    private int run(String url, int seconds, boolean closeMode, int expectCode) throws Exception {
        URI uri = URI.create(url);
        int port = uri.getPort() > 0 ? uri.getPort() : 80;
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        try {
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders());
            Collector collector = new Collector();
            Bootstrap b = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new HttpClientCodec(),
                                    new HttpObjectAggregator(64 * 1024),
                                    collector,
                                    new WebSocketClientProtocolHandler(handshaker));
                        }
                    });
            Channel ch = b.connect(uri.getHost(), port).sync().channel();
            System.out.println("TCP connected, handshaking " + url);

            if (closeMode) {
                boolean closed = closeLatch.await(10, TimeUnit.SECONDS);
                System.out.println("WS CLOSE code=" + closeCode + " reason=\"" + closeReason + "\"");
                if (!closed) {
                    return fail("timeout waiting for server close");
                }
                return closeCode == expectCode
                        ? pass("close code " + expectCode)
                        : fail("expected close code " + expectCode + ", got " + closeCode);
            }

            Thread.sleep(seconds * 1000L);
            byte[] all;
            synchronized (data) {
                all = data.toByteArray();
            }
            System.out.println("messages=" + messages.get() + " bytes=" + all.length);
            if (closeCode >= 0) {
                return fail("server closed unexpectedly during pull: code=" + closeCode + " reason=" + closeReason);
            }
            ch.close().sync();
            boolean ok = verifyFlv(all);
            return ok ? pass("flv structure") : 1;
        } finally {
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS);
        }
    }

    private int pass(String what) {
        System.out.println("PASS " + what);
        return 0;
    }

    private int fail(String msg) {
        System.out.println("FAIL: " + msg);
        return 1;
    }

    private final class Collector extends ChannelDuplexHandler {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof BinaryWebSocketFrame bin) {
                ByteBuf content = bin.content();
                byte[] bytes = new byte[content.readableBytes()];
                content.readBytes(bytes);
                synchronized (data) {
                    data.write(bytes);
                }
                messages.incrementAndGet();
                bin.release();
                return;
            }
            if (msg instanceof CloseWebSocketFrame close) {
                closeCode = close.statusCode();
                closeReason = close.reasonText();
                closeLatch.countDown();
            }
            ctx.fireChannelRead(msg);
        }
    }

    // ===== FLV 结构校验 =====

    /** FLV 字节流结构校验（{@link HttpFlvVerifier} 共用：两种播放协议下发同一份字节流）。 */
    static boolean verifyFlv(byte[] all) {
        if (all.length < 13) {
            return failLog("stream shorter than FLV header");
        }
        if (all[0] != 'F' || all[1] != 'L' || all[2] != 'V' || all[3] != 1) {
            return failLog("bad FLV signature");
        }
        if ((all[4] & 1) == 0) {
            return failLog("video flag not set");
        }
        if (be(all, 5, 4) != 9) {
            return failLog("bad data offset");
        }
        if (be(all, 9, 4) != 0) {
            return failLog("bad PreviousTagSize0");
        }

        int p = 13;
        int tags = 0, configTags = 0, frames = 0, keyframes = 0, lastTs = -1;
        String codec = null;
        while (p + 11 <= all.length) {
            int tagType = all[p] & 0xFF;
            int size = be(all, p + 1, 3);
            int ts = ((all[p + 7] & 0xFF) << 24) | be(all, p + 4, 3);
            int streamId = be(all, p + 8, 3);
            if (p + 11 + size + 4 > all.length) {
                break; // 末尾不完整 Tag：实时流截断
            }
            if (tagType != 9) {
                return failLog("tag#" + tags + " type=" + tagType + " (expect 9)");
            }
            if (streamId != 0) {
                return failLog("tag#" + tags + " streamId=" + streamId);
            }
            int d = p + 11;
            int b0 = all[d] & 0xFF;

            if (codec == null) {
                if (b0 == 0x17) {
                    codec = "h264";
                } else if ((b0 & 0x80) != 0 && fourcc(all, d + 1)) {
                    codec = "h265";
                } else {
                    return failLog("cannot detect codec from first tag byte 0x"
                            + Integer.toHexString(b0));
                }
                System.out.println("codec=" + codec);
            }

            if (codec.equals("h264")) {
                boolean isConfig = b0 == 0x17 && (all[d + 1] & 0xFF) == 0x00;
                if (tags == 0 && !isConfig) {
                    return failLog("first tag not avcC sequence header");
                }
                if (isConfig) {
                    configTags++;
                    if (size - 5 < 10 || (all[d + 5] & 0xFF) != 1) {
                        return failLog("bad avcC record");
                    }
                } else {
                    int ft = b0 >> 4, codecId = b0 & 0x0F;
                    if (ft != 1 && ft != 2) {
                        return failLog("tag#" + tags + " bad frame type " + ft);
                    }
                    if (codecId != 7) {
                        return failLog("tag#" + tags + " bad codec id " + codecId);
                    }
                    if ((all[d + 1] & 0xFF) != 1) {
                        return failLog("tag#" + tags + " bad AVCPacketType");
                    }
                    if (ft == 1) {
                        keyframes++;
                    }
                    frames++;
                    if (lastTs >= 0 && ts < lastTs) {
                        return failLog("ts backwards " + lastTs + " -> " + ts);
                    }
                    lastTs = ts;
                }
            } else {
                if ((b0 & 0x80) == 0) {
                    return failLog("tag#" + tags + " missing IsExHeader");
                }
                if (!fourcc(all, d + 1)) {
                    return failLog("tag#" + tags + " bad FourCC");
                }
                int ft = (b0 >> 4) & 7, pktType = b0 & 0x0F;
                if (tags == 0 && pktType != 0) {
                    return failLog("first tag not SequenceStart");
                }
                if (pktType == 0) {
                    configTags++;
                    if (size - 5 < 10 || (all[d + 5] & 0xFF) != 1) {
                        return failLog("bad hvcC record");
                    }
                } else if (pktType == 1) {
                    if (ft != 1 && ft != 2) {
                        return failLog("tag#" + tags + " bad frame type " + ft);
                    }
                    if (ft == 1) {
                        keyframes++;
                    }
                    frames++;
                    if (lastTs >= 0 && ts < lastTs) {
                        return failLog("ts backwards " + lastTs + " -> " + ts);
                    }
                    lastTs = ts;
                } else {
                    return failLog("tag#" + tags + " unexpected packet type " + pktType);
                }
            }

            int pts = be(all, d + size, 4);
            if (pts != 11 + size) {
                return failLog("tag#" + tags + " bad PreviousTagSize " + pts + " (expect " + (11 + size) + ")");
            }
            tags++;
            p += 11 + size + 4;
        }
        System.out.println("tags=" + tags + " configTags=" + configTags + " frames=" + frames
                + " keyframes=" + keyframes + " lastTs=" + lastTs + "ms");
        if (configTags == 0) {
            return failLog("no config tag");
        }
        if (keyframes < 2) {
            return failLog("too few keyframes: " + keyframes);
        }
        if (frames < 30) {
            return failLog("too few frames: " + frames);
        }
        return true;
    }

    private static boolean failLog(String msg) {
        System.out.println("VERIFY FAIL: " + msg);
        return false;
    }

    private static int be(byte[] a, int off, int len) {
        int v = 0;
        for (int i = 0; i < len; i++) {
            v = (v << 8) | (a[off + i] & 0xFF);
        }
        return v;
    }

    private static boolean fourcc(byte[] a, int off) {
        return a[off] == 'h' && a[off + 1] == 'v' && a[off + 2] == 'c' && a[off + 3] == '1';
    }

    static {
        // 确保标准输出即时可见（重定向日志场景）
        System.setOut(new java.io.PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), true,
                StandardCharsets.UTF_8) {
        });
    }
}
