package com.mediagw.codec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 视频参数集缓存。H.264 含 SPS/PPS，H.265 含 VPS/SPS/PPS。
 * 不可变对象，参数集变化时整体替换。
 */
public final class ParamSets {

    private final CodecType codec;
    private final List<byte[]> vps;
    private final List<byte[]> sps;
    private final List<byte[]> pps;
    private final String signature;

    public ParamSets(CodecType codec, List<byte[]> vps, List<byte[]> sps, List<byte[]> pps) {
        this.codec = codec;
        this.vps = copy(vps);
        this.sps = copy(sps);
        this.pps = copy(pps);
        this.signature = buildSignature();
    }

    private static List<byte[]> copy(List<byte[]> src) {
        List<byte[]> out = new ArrayList<>();
        if (src != null) {
            for (byte[] b : src) {
                out.add(Arrays.copyOf(b, b.length));
            }
        }
        return List.copyOf(out);
    }

    private String buildSignature() {
        StringBuilder sb = new StringBuilder(codec.name());
        appendHash(sb, vps);
        appendHash(sb, sps);
        appendHash(sb, pps);
        return sb.toString();
    }

    private static void appendHash(StringBuilder sb, List<byte[]> list) {
        sb.append('|');
        for (byte[] b : list) {
            sb.append(b.length).append(':').append(Arrays.hashCode(b)).append(',');
        }
    }

    /** 内容签名，用于检测参数集是否变化。 */
    public String signature() {
        return signature;
    }

    public CodecType codec() {
        return codec;
    }

    public List<byte[]> vps() {
        return vps;
    }

    public List<byte[]> sps() {
        return sps;
    }

    public List<byte[]> pps() {
        return pps;
    }

    /** 是否已具备生成解码配置记录的最低条件（SPS + PPS）。 */
    public boolean isComplete() {
        return !sps.isEmpty() && !pps.isEmpty();
    }
}
