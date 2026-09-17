package com.mediagw.rtsp;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.rtp.H264Packetizer;
import com.mediagw.rtp.H265Packetizer;
import com.mediagw.rtp.RtpPacketizer;
import com.mediagw.session.RawSubscriber;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RTSP 转发订阅者：把会话下发的访问单元重新打包成 RTP，发给一个 RTSP 客户端。
 *
 * <p>下行通道（{@link Sink}）在 SETUP 阶段确定：TCP interleaved 复用 RTSP 连接，
 * UDP 则由网关另绑一对端口。打包器在参数集就绪（{@link #onStreamInfo}）后创建，
 * 每个客户端独立维护 SSRC、序列号与时间戳。
 *
 * <p>线程约定：{@link #onGopSnapshot} 与 {@link #onAccessUnit} 在会话锁内被调用，
 * 因此这里只做打包与非阻塞 write（Netty 保证跨线程 write 的提交顺序即执行顺序），
 * flush 由本类在一帧结束后触发；状态回调统一切回 RTSP 控制连接的 EventLoop。
 */
public final class RtspSender implements RawSubscriber {

    private static final Logger log = LoggerFactory.getLogger(RtspSender.class);
    /** 关键帧连续 N 次遇到通道不可写则判定为慢消费者并断开。 */
    private static final int MAX_KEYFRAME_NOT_WRITABLE_STREAK = 3;

    /** RTP 下行通道。write 接收 RTP 包的所有权。 */
    public interface Sink {
        void write(ByteBuf rtpPacket);

        void flush();

        boolean isWritable();

        boolean isActive();

        /** 日志用描述（传输方式与对端地址）。 */
        String describe();

        void close();
    }

    /** 转发状态回调，均在 RTSP 控制连接的 EventLoop 线程执行。 */
    public interface Listener {
        /** 参数集首次就绪（可应答 DESCRIBE）或发生变化。 */
        void onStreamReady(CodecType codec, ParamSets params);

        /** 上游流终止：拉流失败、上游不可恢复或网关停机。 */
        void onStreamClosed();
    }

    private final Channel control;
    private final String streamId;
    private final int maxPayloadBytes;
    private final Listener listener;
    private final int ssrc = ThreadLocalRandom.current().nextInt();

    private volatile Sink sink;
    private volatile RtpPacketizer packetizer;
    private volatile CodecType codec;
    private volatile ParamSets params;
    private int keyStreak;

    private final AtomicLong packets = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    public RtspSender(Channel control, String streamId, int maxPayloadBytes, Listener listener) {
        this.control = control;
        this.streamId = streamId;
        this.maxPayloadBytes = maxPayloadBytes;
        this.listener = listener;
    }

    /** SETUP 协商完成后挂上媒体通道。 */
    public void attachSink(Sink sink) {
        this.sink = sink;
    }

    /** 当前媒体通道；SETUP 前为 null。 */
    public Sink sink() {
        return sink;
    }

    /** 最新编码类型与参数集；上游未就绪时为 null（重复 DESCRIBE 据此判断能否立即应答）。 */
    public CodecType codec() {
        return codec;
    }

    public ParamSets params() {
        return params;
    }

    public int ssrc() {
        return ssrc;
    }

    public long packets() {
        return packets.get();
    }

    public long bytes() {
        return bytes.get();
    }

    public long dropped() {
        return dropped.get();
    }

    /** 断开媒体通道（UDP 端口对），RTSP 控制连接由 Handler 负责关闭。 */
    public void closeSink() {
        Sink s = sink;
        if (s != null) {
            s.close();
        }
    }

    // ===== RawSubscriber =====

    @Override
    public String id() {
        return "rtsp-" + control.id().asShortText();
    }

    @Override
    public boolean isActive() {
        Sink s = sink;
        return control.isActive() && (s == null || s.isActive());
    }

    @Override
    public void onStreamInfo(CodecType newCodec, ParamSets params) {
        control.eventLoop().execute(() -> {
            RtpPacketizer p = packetizer;
            if (p == null || newCodec != codec) {
                if (p != null) {
                    log.warn("[{}] forward {} codec changed {} -> {}, restarting packetizer",
                            streamId, id(), codec, newCodec);
                }
                p = newPacketizer(newCodec);
                packetizer = p;
                codec = newCodec;
            }
            p.updateParams(params);
            this.params = params;
            listener.onStreamReady(newCodec, params);
        });
    }

    private RtpPacketizer newPacketizer(CodecType type) {
        return type == CodecType.H265
                ? new H265Packetizer(PooledByteBufAllocator.DEFAULT, RtspSdpBuilder.PAYLOAD_TYPE, ssrc, maxPayloadBytes)
                : new H264Packetizer(PooledByteBufAllocator.DEFAULT, RtspSdpBuilder.PAYLOAD_TYPE, ssrc, maxPayloadBytes);
    }

    @Override
    public void onGopSnapshot(List<AccessUnit> gop) {
        Sink s = sink;
        RtpPacketizer p = packetizer;
        if (s == null || p == null || gop.isEmpty()) {
            return;
        }
        for (AccessUnit au : gop) {
            p.packetize(au, pkt -> send(s, pkt));
        }
        s.flush();
    }

    @Override
    public void onAccessUnit(AccessUnit au) {
        Sink s = sink;
        RtpPacketizer p = packetizer;
        if (s == null || p == null) {
            return;
        }
        if (!s.isWritable()) {
            // 通道积压：整帧丢弃（半帧会破坏解码），关键帧连续积压则判定为慢消费者
            if (!au.keyFrame()) {
                dropped.incrementAndGet();
                return;
            }
            if (++keyStreak >= MAX_KEYFRAME_NOT_WRITABLE_STREAK) {
                log.warn("[{}] forward subscriber {} too slow ({} unwritable keyframes), closing",
                        streamId, id(), keyStreak);
                s.close();
                return;
            }
        } else {
            keyStreak = 0;
        }
        p.packetize(au, pkt -> send(s, pkt));
        s.flush();
    }

    private void send(Sink s, ByteBuf pkt) {
        packets.incrementAndGet();
        bytes.addAndGet(pkt.readableBytes());
        s.write(pkt);
    }

    @Override
    public void onStreamClosed() {
        control.eventLoop().execute(listener::onStreamClosed);
    }

    // ===== Sink 实现 =====

    /** TCP interleaved：{@code $ + channel(1B) + length(2B) + RTP}，复用 RTSP 控制连接。 */
    public static final class TcpSink implements Sink {

        private final Channel channel;
        private final int rtpChannelId;

        public TcpSink(Channel channel, int rtpChannelId) {
            this.channel = channel;
            this.rtpChannelId = rtpChannelId;
        }

        @Override
        public void write(ByteBuf rtpPacket) {
            int len = rtpPacket.readableBytes();
            ByteBuf frame = channel.alloc().buffer(4 + len);
            frame.writeByte('$');
            frame.writeByte(rtpChannelId);
            frame.writeShort(len);
            frame.writeBytes(rtpPacket, rtpPacket.readerIndex(), len);
            rtpPacket.release();
            channel.write(frame);
        }

        @Override
        public void flush() {
            channel.flush();
        }

        @Override
        public boolean isWritable() {
            return channel.isWritable();
        }

        @Override
        public boolean isActive() {
            return channel.isActive();
        }

        @Override
        public String describe() {
            return "TCP interleaved/" + rtpChannelId + " " + channel.remoteAddress();
        }

        @Override
        public void close() {
            channel.close();
        }
    }

    /** UDP：RTP 与 RTCP 各占一个已绑定端口，发往客户端 SETUP 时声明的地址。 */
    public static final class UdpSink implements Sink {

        private final Channel rtpChannel;
        private final Channel rtcpChannel;
        private volatile InetSocketAddress remote;

        public UdpSink(Channel rtpChannel, Channel rtcpChannel, InetSocketAddress remote) {
            this.rtpChannel = rtpChannel;
            this.rtcpChannel = rtcpChannel;
            this.remote = remote;
        }

        public int rtpPort() {
            return ((InetSocketAddress) rtpChannel.localAddress()).getPort();
        }

        public int rtcpPort() {
            return ((InetSocketAddress) rtcpChannel.localAddress()).getPort();
        }

        /**
         * 地址锁存：收到客户端上行报文后以其真实地址为准，
         * 兼容客户端经 NAT 后源端口与 SETUP 声明不一致的情况。
         */
        public void latch(InetSocketAddress addr) {
            if (addr != null && !addr.equals(remote)) {
                remote = addr;
            }
        }

        @Override
        public void write(ByteBuf rtpPacket) {
            InetSocketAddress to = remote;
            if (to == null) {
                rtpPacket.release();
                return;
            }
            rtpChannel.write(new DatagramPacket(rtpPacket, to));
        }

        @Override
        public void flush() {
            rtpChannel.flush();
        }

        @Override
        public boolean isWritable() {
            return rtpChannel.isWritable();
        }

        @Override
        public boolean isActive() {
            return rtpChannel.isActive();
        }

        @Override
        public String describe() {
            return "UDP " + rtpPort() + "->" + remote;
        }

        @Override
        public void close() {
            rtpChannel.close();
            rtcpChannel.close();
        }
    }
}
