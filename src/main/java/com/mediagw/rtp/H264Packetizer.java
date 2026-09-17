package com.mediagw.rtp;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;
import java.util.function.Consumer;

/**
 * H.264 RTP 打包（RFC 6184）：单 NALU 包与 FU-A(28) 分片。
 * 关键帧前带内插入 SPS/PPS，使中途加入的客户端无需等待上游 SDP 即可解码。
 */
public final class H264Packetizer extends RtpPacketizer {

    /** FU-A 分片指示器的 NAL 类型。 */
    private static final int TYPE_FU_A = 28;
    /** FU-A 载荷开销：指示器 + FU 头。 */
    private static final int FU_OVERHEAD = 2;

    private List<byte[]> sps = List.of();
    private List<byte[]> pps = List.of();

    public H264Packetizer(ByteBufAllocator alloc, int payloadType, int ssrc, int maxPayloadBytes) {
        super(alloc, payloadType, ssrc, maxPayloadBytes);
    }

    @Override
    public void updateParams(ParamSets ps) {
        if (ps == null || ps.codec() != CodecType.H264) {
            return;
        }
        sps = ps.sps();
        pps = ps.pps();
    }

    @Override
    public void packetize(AccessUnit au, Consumer<ByteBuf> out) {
        long ts = mapTimestamp(au.rtpTimestamp());
        if (au.keyFrame()) {
            for (byte[] nalu : sps) {
                out.accept(singleNalu(ts, nalu, false));
            }
            for (byte[] nalu : pps) {
                out.accept(singleNalu(ts, nalu, false));
            }
        }
        List<byte[]> nalus = au.nalus();
        for (int i = 0; i < nalus.size(); i++) {
            byte[] nalu = nalus.get(i);
            boolean lastNalu = i == nalus.size() - 1;
            if (nalu.length <= maxPayloadBytes) {
                out.accept(singleNalu(ts, nalu, lastNalu));
            } else {
                fragment(ts, nalu, lastNalu, out);
            }
        }
    }

    /** 单 NALU 包：NALU 原样作为载荷。 */
    private ByteBuf singleNalu(long ts, byte[] nalu, boolean marker) {
        ByteBuf buf = newPacket(ts, marker);
        buf.writeBytes(nalu);
        return buf;
    }

    /** FU-A 分片：指示器(F|NRI|28) + FU 头(S|E|R|type) + 去掉原 NAL 头后的分片数据。 */
    private void fragment(long ts, byte[] nalu, boolean lastNalu, Consumer<ByteBuf> out) {
        int nalHeader = nalu[0] & 0xFF;
        int indicator = (nalHeader & 0xE0) | TYPE_FU_A;
        int fuType = nalHeader & 0x1F;
        int offset = 1;
        boolean first = true;
        while (offset < nalu.length) {
            int len = Math.min(maxPayloadBytes - FU_OVERHEAD, nalu.length - offset);
            boolean last = offset + len >= nalu.length;
            ByteBuf buf = newPacket(ts, last && lastNalu);
            buf.writeByte(indicator);
            buf.writeByte(fuType | (first ? 0x80 : 0) | (last ? 0x40 : 0));
            buf.writeBytes(nalu, offset, len);
            out.accept(buf);
            offset += len;
            first = false;
        }
    }
}
