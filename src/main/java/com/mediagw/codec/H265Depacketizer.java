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
 * H.265 RTP 载荷重组（RFC 7798）：单 NALU、AP(48)、FU(49)。
 * 缓存 VPS(32)/SPS(33)/PPS(34)，内容变化时回调 onParameterSets。
 */
public final class H265Depacketizer extends VideoDepacketizer {

    private final Map<Integer, byte[]> vpsById = new LinkedHashMap<>();
    private final Map<Integer, byte[]> spsById = new LinkedHashMap<>();
    private final Map<Integer, byte[]> ppsById = new LinkedHashMap<>();

    public H265Depacketizer(Listener listener) {
        super(CodecType.H265, listener);
    }

    /** 注入 SDP sprop-vps/sprop-sps/sprop-pps 解析出的初始参数集。 */
    public void seedParamSets(List<byte[]> vps, List<byte[]> sps, List<byte[]> pps) {
        boolean changed = false;
        if (vps != null) {
            for (byte[] v : vps) {
                changed |= store(vpsById, parseVpsId(v), v);
            }
        }
        if (sps != null) {
            for (byte[] s : sps) {
                changed |= store(spsById, parseSpsId(s), s);
            }
        }
        if (pps != null) {
            for (byte[] p : pps) {
                changed |= store(ppsById, parsePpsId(p), p);
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
        if (n < 2) {
            return;
        }
        int base = payload.readerIndex();
        int b0 = payload.getUnsignedByte(base);
        int b1 = payload.getUnsignedByte(base + 1);
        int type = (b0 >>> 1) & 0x3F;
        if (type <= 47) { // 单 NALU
            addNalu(copy(payload, base, n));
        } else if (type == 48) { // AP 聚合
            int idx = base + 2;
            int end = base + n;
            while (idx + 2 <= end) {
                int size = payload.getUnsignedShort(idx);
                idx += 2;
                if (size < 2 || idx + size > end) {
                    markCorrupted("invalid H265 AP aggregation");
                    return;
                }
                byte[] nalu = new byte[size];
                payload.getBytes(idx, nalu);
                idx += size;
                addNalu(nalu);
            }
        } else if (type == 49) { // FU
            if (n < 3) {
                markCorrupted("truncated H265 FU");
                return;
            }
            int fuHeader = payload.getUnsignedByte(base + 2);
            boolean start = (fuHeader & 0x80) != 0;
            boolean end = (fuHeader & 0x40) != 0;
            int fuType = fuHeader & 0x3F;
            if (start) {
                fuStart(new byte[]{(byte) ((b0 & 0x81) | (fuType << 1)), (byte) b1});
            }
            fuFragment(payload, base + 3, n - 3);
            if (end) {
                fuEnd();
            }
        }
        // type 50 (PACI) 暂不支持，忽略
    }

    @Override
    protected int naluType(byte[] nalu) {
        return (nalu[0] >>> 1) & 0x3F;
    }

    @Override
    protected boolean isKeyFrameNalu(int type) {
        // IRAP：BLA_W_LP(16) .. CRA_NUT(21)，其中 IDR_W_RADL(19)/IDR_N_LP(20) 最常见
        return type >= 16 && type <= 21;
    }

    @Override
    protected boolean handleNonVcl(int type, byte[] nalu) {
        switch (type) {
            case 32:
                if (store(vpsById, parseVpsId(nalu), nalu)) {
                    emitParamSets();
                }
                return true;
            case 33:
                if (store(spsById, parseSpsId(nalu), nalu)) {
                    emitParamSets();
                }
                return true;
            case 34:
                if (store(ppsById, parsePpsId(nalu), nalu)) {
                    emitParamSets();
                }
                return true;
            case 35: // AUD
            case 36: // EOS
            case 37: // EOB
                return true;
            default:
                return false;
        }
    }

    private static boolean store(Map<Integer, byte[]> map, int id, byte[] nalu) {
        byte[] old = map.get(id);
        if (old != null && Arrays.equals(old, nalu)) {
            return false;
        }
        map.put(id, nalu);
        return true;
    }

    private void emitParamSets() {
        if (spsById.isEmpty() || ppsById.isEmpty()) {
            return;
        }
        listener.onParameterSets(new ParamSets(CodecType.H265,
                new ArrayList<>(vpsById.values()),
                new ArrayList<>(spsById.values()),
                new ArrayList<>(ppsById.values())));
    }

    static int parseVpsId(byte[] nalu) {
        try {
            byte[] rbsp = BitReader.stripEmulationPrevention(Arrays.copyOfRange(nalu, 2, nalu.length));
            return new BitReader(rbsp).u(4);
        } catch (RuntimeException e) {
            return 0;
        }
    }

    static int parseSpsId(byte[] nalu) {
        try {
            return HevcSpsParser.parse(nalu).spsId;
        } catch (RuntimeException e) {
            return 0;
        }
    }

    static int parsePpsId(byte[] nalu) {
        try {
            byte[] rbsp = BitReader.stripEmulationPrevention(Arrays.copyOfRange(nalu, 2, nalu.length));
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
