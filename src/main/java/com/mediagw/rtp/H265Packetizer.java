package com.mediagw.rtp;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;
import java.util.function.Consumer;

/**
 * H.265 RTP 打包（RFC 7798）：单 NALU 包与 FU(49) 分片。
 * 关键帧前带内插入 VPS/SPS/PPS，使中途加入的客户端无需等待上游 SDP 即可解码。
 */
public final class H265Packetizer extends RtpPacketizer {

    /** FU 分片载荷头的 NAL 类型。 */
    private static final int TYPE_FU = 49;
    /** FU 载荷开销：2 字节 PayloadHdr + 1 字节 FU 头。 */
    private static final int FU_OVERHEAD = 3;
    /** H.265 NAL 头长度。 */
    private static final int NAL_HEADER_SIZE = 2;

    private List<byte[]> vps = List.of();
    private List<byte[]> sps = List.of();
    private List<byte[]> pps = List.of();

    public H265Packetizer(ByteBufAllocator alloc, int payloadType, int ssrc, int maxPayloadBytes) {
        super(alloc, payloadType, ssrc, maxPayloadBytes);
    }

    @Override
    public void updateParams(ParamSets ps) {
        if (ps == null || ps.codec() != CodecType.H265) {
            return;
        }
        vps = ps.vps();
        sps = ps.sps();
        pps = ps.pps();
    }

    @Override
    public void packetize(AccessUnit au, Consumer<ByteBuf> out) {
        long ts = mapTimestamp(au.rtpTimestamp());
        if (au.keyFrame()) {
            for (byte[] nalu : vps) {
                out.accept(singleNalu(ts, nalu, false));
            }
            for (byte[] nalu : sps) {
                out.accept(singleNalu(ts, nalu, false));
            }
            for (byte[] nalu : pps) {
                out.accept(singleNalu(ts, nalu, false));
            }
        }
        List<byte[]> nalus = au.nalus();
        int lastValid = lastValidIndex(nalus);
        for (int i = 0; i < nalus.size(); i++) {
            byte[] nalu = nalus.get(i);
            if (nalu.length < NAL_HEADER_SIZE) {
                continue;
            }
            boolean lastNalu = i == lastValid;
            if (nalu.length <= maxPayloadBytes) {
                out.accept(singleNalu(ts, nalu, lastNalu));
            } else {
                fragment(ts, nalu, lastNalu, out);
            }
        }
    }

    /** 最后一个带完整 NAL 头的 NALU 下标（marker 位落点）；全不合法时返回 -1。 */
    private static int lastValidIndex(List<byte[]> nalus) {
        for (int i = nalus.size() - 1; i >= 0; i--) {
            if (nalus.get(i).length >= NAL_HEADER_SIZE) {
                return i;
            }
        }
        return -1;
    }

    /** 单 NALU 包：NALU 原样作为载荷。 */
    private ByteBuf singleNalu(long ts, byte[] nalu, boolean marker) {
        ByteBuf buf = newPacket(ts, marker);
        buf.writeBytes(nalu);
        return buf;
    }

    /** FU 分片：PayloadHdr(F|49|layerId|TID) + FU 头(S|E|type) + 去掉原 2 字节 NAL 头后的分片数据。 */
    private void fragment(long ts, byte[] nalu, boolean lastNalu, Consumer<ByteBuf> out) {
        int b0 = nalu[0] & 0xFF;
        int hdr0 = (b0 & 0x81) | (TYPE_FU << 1); // 保留 F 位与 layerId 高位，类型改为 49
        int hdr1 = nalu[1] & 0xFF;
        int fuType = (b0 >>> 1) & 0x3F;
        int offset = NAL_HEADER_SIZE;
        boolean first = true;
        while (offset < nalu.length) {
            int len = Math.min(maxPayloadBytes - FU_OVERHEAD, nalu.length - offset);
            boolean last = offset + len >= nalu.length;
            ByteBuf buf = newPacket(ts, last && lastNalu);
            buf.writeByte(hdr0);
            buf.writeByte(hdr1);
            buf.writeByte(fuType | (first ? 0x80 : 0) | (last ? 0x40 : 0));
            buf.writeBytes(nalu, offset, len);
            out.accept(buf);
            offset += len;
            first = false;
        }
    }
}
