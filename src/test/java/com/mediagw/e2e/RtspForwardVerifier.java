package com.mediagw.e2e;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.H264Depacketizer;
import com.mediagw.codec.H265Depacketizer;
import com.mediagw.codec.ParamSets;
import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.config.GatewayConfig;
import com.mediagw.rtsp.RtspClient;
import com.mediagw.rtsp.SdpInfo;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RTSP 转发端到端校验器（联调用，非单元测试）：把网关当作普通 RTSP 源，
 * 用自身的 {@link RtspClient} 走 OPTIONS→DESCRIBE→SETUP(TCP interleaved)→PLAY 并重组 RTP。
 * <pre>
 * 用法: RtspForwardVerifier rtsp://host:8554/live/id [秒数，默认6]
 *       鉴权可用 rtsp://user:token@host:8554/live/id 或 ...?token=xxx
 * 退出码 0=PASS 1=FAIL 2=用法错误
 * </pre>
 * 校验：SDP 含受支持视频轨且 sprop 参数集完整、PLAY 后 RTP 持续到达、
 * 访问单元数与关键帧数达标、RTP 时间戳单调不减、无丢包与重组错误。
 */
public final class RtspForwardVerifier {

    private static final int MIN_FRAMES = 30;
    private static final int MIN_KEYFRAMES = 2;

    /** PLAY 成功或启动失败均触发，结束等待。 */
    private final CountDownLatch started = new CountDownLatch(1);

    private final AtomicInteger frames = new AtomicInteger();
    private final AtomicInteger keyframes = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger paramSetEvents = new AtomicInteger();
    private final AtomicLong lastTs = new AtomicLong(-1);

    private volatile SdpInfo.Track track;
    private volatile Throwable startupError;
    private volatile Throwable disconnected;
    private volatile boolean tsBackwards;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: RtspForwardVerifier <rtsp-url> [seconds]");
            System.exit(2);
        }
        String url = args[0];
        int seconds = args.length >= 2 ? Integer.parseInt(args[1]) : 6;
        System.exit(new RtspForwardVerifier().run(url, seconds));
    }

    private int run(String url, int seconds) throws Exception {
        GatewayConfig config = GatewayConfig.load("/nonexistent/rtsp-forward-verifier.properties");
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        Verifier verifier = new Verifier();
        RtspClient client = new RtspClient(url, config, group, verifier);
        try {
            client.start();
            if (!started.await(config.rtspResponseTimeoutMs() + 2000L, TimeUnit.MILLISECONDS)) {
                return fail("timeout waiting for PLAY");
            }
            if (startupError != null) {
                return fail("startup failed: " + startupError);
            }
            out("PLAY ok, pulling " + seconds + "s");
            Thread.sleep(seconds * 1000L);
            if (disconnected != null) {
                return fail("disconnected during pull: " + disconnected);
            }

            out("rtpPackets=" + client.rtpPackets() + " rtpBytes=" + client.rtpBytes()
                    + " frames=" + frames.get() + " keyframes=" + keyframes.get()
                    + " paramSets=" + paramSetEvents.get() + " errors=" + errors.get());
            return verify(client);
        } finally {
            client.stop();
            group.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS);
        }
    }

    private int verify(RtspClient client) {
        SdpInfo.Track t = track;
        if (t == null) {
            return fail("no video track in SDP");
        }
        out("codec=" + t.codec() + " pt=" + t.payloadType() + " control=" + t.control()
                + " vps=" + t.vps().size() + " sps=" + t.sps().size() + " pps=" + t.pps().size());
        if (!t.toParamSets().isComplete()) {
            return fail("SDP lacks sprop SPS/PPS");
        }
        if (t.codec() == CodecType.H265 && t.vps().isEmpty()) {
            return fail("H.265 SDP lacks sprop VPS");
        }
        if (client.rtpPackets() <= 0) {
            return fail("no RTP packet received");
        }
        if (paramSetEvents.get() == 0) {
            return fail("no in-band parameter set before key frame");
        }
        if (keyframes.get() < MIN_KEYFRAMES) {
            return fail("too few keyframes: " + keyframes.get());
        }
        if (frames.get() < MIN_FRAMES) {
            return fail("too few frames: " + frames.get());
        }
        if (tsBackwards) {
            return fail("RTP timestamp went backwards");
        }
        if (errors.get() > 0) {
            return fail(errors.get() + " depacketize errors");
        }
        return pass("rtsp forward stream");
    }

    /** 同时充当 RTSP 信令回调与 RTP 重组回调。 */
    private final class Verifier implements RtspClient.Listener, VideoDepacketizer.Listener {

        @Override
        public VideoDepacketizer onSdpParsed(SdpInfo sdp) {
            SdpInfo.Track t = sdp.firstVideo();
            if (t == null) {
                return null; // 交由 RtspClient 判定为启动失败
            }
            track = t;
            return t.codec() == CodecType.H265
                    ? new H265Depacketizer(this)
                    : new H264Depacketizer(this);
        }

        @Override
        public void onPlaying() {
            started.countDown();
        }

        @Override
        public void onStartupFailure(Throwable cause) {
            startupError = cause;
            started.countDown();
        }

        @Override
        public void onDisconnected(Throwable cause) {
            disconnected = cause;
            started.countDown();
        }

        @Override
        public void onParameterSets(ParamSets ps) {
            paramSetEvents.incrementAndGet();
        }

        @Override
        public void onAccessUnit(AccessUnit au) {
            frames.incrementAndGet();
            if (au.keyFrame()) {
                keyframes.incrementAndGet();
            }
            long prev = lastTs.getAndSet(au.rtpTimestamp());
            // 32 位无符号回绕：差值落在前半区视为前进
            if (prev >= 0 && ((au.rtpTimestamp() - prev) & 0xFFFFFFFFL) >= 0x80000000L) {
                tsBackwards = true;
            }
        }

        @Override
        public void onError(String message) {
            errors.incrementAndGet();
            out("depacketize error: " + message);
        }
    }

    private static void out(String msg) {
        System.out.println(msg);
        System.out.flush();
    }

    private static int pass(String what) {
        out("PASS " + what);
        return 0;
    }

    private static int fail(String msg) {
        out("FAIL: " + msg);
        return 1;
    }
}
