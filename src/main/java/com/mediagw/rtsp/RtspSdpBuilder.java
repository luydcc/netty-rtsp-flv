package com.mediagw.rtsp;

import com.mediagw.codec.CodecType;
import com.mediagw.codec.HevcSpsParser;
import com.mediagw.codec.ParamSets;

import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * 构建网关作为 RTSP 服务端时 DESCRIBE 应答的 SDP（仅一路视频轨，无音频）。
 * 参数集以 sprop 形式带外声明，同时在关键帧前带内重复发送，两者互补。
 */
public final class RtspSdpBuilder {

    /** 动态载荷类型：H.264/H.265 均无静态类型，固定用 96。 */
    public static final int PAYLOAD_TYPE = 96;
    /** 视频轨控制 URL 后缀（相对 Content-Base 解析）。 */
    public static final String TRACK_CONTROL = "trackID=0";

    private RtspSdpBuilder() {
    }

    /**
     * @param streamId 流标识，作为会话名
     * @param codec    视频编码
     * @param ps       参数集（SPS/PPS 必须齐备）
     */
    public static String build(String streamId, CodecType codec, ParamSets ps) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("v=0\r\n")
                .append("o=- ").append(System.currentTimeMillis()).append(" 1 IN IP4 0.0.0.0\r\n")
                .append("s=").append(streamId).append("\r\n")
                .append("c=IN IP4 0.0.0.0\r\n")
                .append("t=0 0\r\n")
                .append("a=tool:netty-rtsp-flv\r\n")
                .append("a=control:*\r\n")
                .append("m=video 0 RTP/AVP ").append(PAYLOAD_TYPE).append("\r\n")
                .append("a=rtpmap:").append(PAYLOAD_TYPE).append(' ')
                .append(codec == CodecType.H265 ? "H265" : "H264").append("/90000\r\n");
        String fmtp = codec == CodecType.H265 ? hevcFmtp(ps) : avcFmtp(ps);
        if (!fmtp.isEmpty()) {
            sb.append("a=fmtp:").append(PAYLOAD_TYPE).append(' ').append(fmtp).append("\r\n");
        }
        sb.append("a=control:").append(TRACK_CONTROL).append("\r\n");
        return sb.toString();
    }

    /** H.264：packetization-mode=1（支持 FU-A），profile-level-id 取 SPS 前三个字节。 */
    private static String avcFmtp(ParamSets ps) {
        StringBuilder sb = new StringBuilder(128);
        sb.append("packetization-mode=1");
        byte[] sps = first(ps.sps());
        if (sps != null && sps.length >= 4) {
            sb.append(";profile-level-id=").append(String.format(Locale.ROOT, "%02X%02X%02X",
                    sps[1], sps[2], sps[3]));
        }
        String sprop = join(ps.sps(), ps.pps());
        if (!sprop.isEmpty()) {
            sb.append(";sprop-parameter-sets=").append(sprop);
        }
        return sb.toString();
    }

    /** H.265（RFC 7798）：profile/tier/level 与 interop-constraints 取自首个 SPS。 */
    private static String hevcFmtp(ParamSets ps) {
        StringBuilder sb = new StringBuilder(256);
        byte[] sps = first(ps.sps());
        if (sps != null) {
            try {
                HevcSpsParser.Info info = HevcSpsParser.parse(sps);
                sb.append("profile-space=").append(info.profileSpace)
                        .append(";profile-id=").append(info.profileIdc)
                        .append(";tier-flag=").append(info.tierFlag)
                        .append(";level-id=").append(info.levelIdc)
                        .append(";interop-constraints=").append(hex(info.constraintFlags));
            } catch (RuntimeException ignored) {
                // SPS 解析失败时只带 sprop，播放端仍可自行解析
            }
        }
        appendSprop(sb, "sprop-vps", ps.vps());
        appendSprop(sb, "sprop-sps", ps.sps());
        appendSprop(sb, "sprop-pps", ps.pps());
        return sb.toString();
    }

    /** 只声明首个参数集（SDP 体积可控），其余同类参数集靠关键帧前带内发送。 */
    private static void appendSprop(StringBuilder sb, String name, List<byte[]> list) {
        if (list.isEmpty()) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(';');
        }
        sb.append(name).append('=').append(b64(list.get(0)));
    }

    /** 多个参数集用逗号分隔（sprop-parameter-sets 的标准写法）。 */
    private static String join(List<byte[]> sps, List<byte[]> pps) {
        StringBuilder sb = new StringBuilder(128);
        for (byte[] b : sps) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(b64(b));
        }
        for (byte[] b : pps) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(b64(b));
        }
        return sb.toString();
    }

    private static byte[] first(List<byte[]> list) {
        return list.isEmpty() ? null : list.get(0);
    }

    private static String b64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    private static String hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(String.format(Locale.ROOT, "%02X", b));
        }
        return sb.toString();
    }
}
