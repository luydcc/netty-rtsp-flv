package com.mediagw.codec;

import com.mediagw.rtp.RtpPacket;
import com.mediagw.util.BitReader;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * H.264 RTP 载荷重组（RFC 6184）：单 NALU、STAP-A(24)、FU-A(28)。
 * 缓存 SPS(id=7)/PPS(id=8)，内容变化时回调 onParameterSets。
 */
public final class H264Depacketizer extends VideoDepacketizer {

    private final Map<Integer, byte[]> spsById = new LinkedHashMap<>();
    private final Map<Integer, byte[]> ppsById = new LinkedHashMap<>();

    public H264Depacketizer(Listener listener) {
        super(CodecType.H264, listener);
    }

    /** 注入 SDP sprop-parameter-sets 解析出的初始参数集。 */
    public void seedParamSets(List<byte[]> sps, List<byte[]> pps) {
        boolean changed = false;
        if (sps != null) {
            for (byte[] s : sps) {
                changed |= storeSps(s);
            }
        }
        if (pps != null) {
            for (byte[] p : pps) {
                changed |= storePps(p);
            }
        }
        if (changed) {
            emitParamSets();
        }
    }

    @Override
    protected void depacketize(RtpPacket p) {
        ByteBuf payload = p.payload();
        int n = payload.readableBytes();
        if (n < 1) {
            return;
        }
        int base = payload.readerIndex();
        int b0 = payload.getUnsignedByte(base);
        int type = b0 & 0x1F;
        if (type >= 1 && type <= 23) {
            addNalu(copy(payload, base, n));
        } else if (type == 24) { // STAP-A
            int idx = base + 1;
            int end = base + n;
            while (idx + 2 <= end) {
                int size = payload.getUnsignedShort(idx);
                idx += 2;
                if (size < 1 || idx + size > end) {
                    markCorrupted("invalid STAP-A aggregation");
                    return;
                }
                byte[] nalu = new byte[size];
                payload.getBytes(idx, nalu);
                idx += size;
                addNalu(nalu);
            }
        } else if (type == 28) { // FU-A
            if (n < 2) {
                markCorrupted("truncated FU-A");
                return;
            }
            int fuHeader = payload.getUnsignedByte(base + 1);
            boolean start = (fuHeader & 0x80) != 0;
            boolean end = (fuHeader & 0x40) != 0;
            int fuType = fuHeader & 0x1F;
            if (start) {
                fuStart(new byte[]{(byte) ((b0 & 0xE0) | fuType)});
            }
            fuFragment(payload, base + 2, n - 2);
            if (end) {
                fuEnd();
            }
        }
        // 其它类型（如 FU-B=29）暂不支持，忽略
    }

    @Override
    protected int naluType(byte[] nalu) {
        return nalu[0] & 0x1F;
    }

    @Override
    protected boolean isKeyFrameNalu(int type) {
        return type == 5;
    }

    @Override
    protected boolean handleNonVcl(int type, byte[] nalu) {
        switch (type) {
            case 7:
                if (storeSps(nalu)) {
                    emitParamSets();
                }
                return true;
            case 8:
                if (storePps(nalu)) {
                    emitParamSets();
                }
                return true;
            case 9: // AUD
                return true;
            default:
                return false;
        }
    }

    private boolean storeSps(byte[] nalu) {
        int id = parseSpsId(nalu);
        byte[] old = spsById.get(id);
        if (old != null && Arrays.equals(old, nalu)) {
            return false;
        }
        spsById.put(id, nalu);
        return true;
    }

    private boolean storePps(byte[] nalu) {
        int id = parsePpsId(nalu);
        byte[] old = ppsById.get(id);
        if (old != null && Arrays.equals(old, nalu)) {
            return false;
        }
        ppsById.put(id, nalu);
        return true;
    }

    private void emitParamSets() {
        if (spsById.isEmpty() || ppsById.isEmpty()) {
            return;
        }
        listener.onParameterSets(new ParamSets(CodecType.H264, List.of(),
                new ArrayList<>(spsById.values()), new ArrayList<>(ppsById.values())));
    }

    static int parseSpsId(byte[] nalu) {
        try {
            byte[] rbsp = BitReader.stripEmulationPrevention(Arrays.copyOfRange(nalu, 1, nalu.length));
            BitReader r = new BitReader(rbsp);
            r.skip(24); // profile_idc + constraint_flags + level_idc
            return r.ue();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    static int parsePpsId(byte[] nalu) {
        try {
            byte[] rbsp = BitReader.stripEmulationPrevention(Arrays.copyOfRange(nalu, 1, nalu.length));
            return new BitReader(rbsp).ue();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private static byte[] copy(ByteBuf buf, int index, int len) {
        byte[] b = new byte[len];
        buf.getBytes(index, b);
        return b;
    }
}
