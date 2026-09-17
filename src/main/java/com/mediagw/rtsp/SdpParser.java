package com.mediagw.rtsp;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** SDP 解析器：提取视频轨的 payload type、编码类型、control 与 sprop 参数集。 */
public final class SdpParser {

    private SdpParser() {
    }

    public static SdpInfo parse(String sdp, String contentBase) {
        List<MediaSection> sections = new ArrayList<>();
        String sessionControl = null;

        MediaSection cur = null;
        String[] lines = sdp.split("\r\n|\n|\r");
        for (String raw : lines) {
            String line = raw.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("m=")) {
                cur = new MediaSection();
                parseMLine(line.substring(2), cur);
                sections.add(cur);
                continue;
            }
            if (!line.startsWith("a=")) {
                continue;
            }
            String attr = line.substring(2);
            if (cur == null) {
                if (attr.startsWith("control:")) {
                    sessionControl = attr.substring("control:".length()).trim();
                }
                continue;
            }
            if (attr.startsWith("rtpmap:")) {
                // rtpmap:96 H264/90000
                String rest = attr.substring("rtpmap:".length()).trim();
                int sp = rest.indexOf(' ');
                if (sp > 0) {
                    try {
                        int pt = Integer.parseInt(rest.substring(0, sp).trim());
                        String enc = rest.substring(sp + 1).trim();
                        int slash = enc.indexOf('/');
                        if (slash > 0) {
                            enc = enc.substring(0, slash);
                        }
                        cur.setRtpmap(pt, enc);
                    } catch (NumberFormatException ignored) {
                    }
                }
            } else if (attr.startsWith("fmtp:")) {
                // fmtp:96 packetization-mode=1;sprop-parameter-sets=...
                String rest = attr.substring("fmtp:".length()).trim();
                int sp = rest.indexOf(' ');
                if (sp > 0) {
                    try {
                        int pt = Integer.parseInt(rest.substring(0, sp).trim());
                        cur.setFmtp(pt, rest.substring(sp + 1));
                    } catch (NumberFormatException ignored) {
                    }
                }
            } else if (attr.startsWith("control:")) {
                cur.control = attr.substring("control:".length()).trim();
            }
        }
        List<SdpInfo.Track> tracks = new ArrayList<>(sections.size());
        for (MediaSection s : sections) {
            tracks.add(s.toTrack());
        }
        return new SdpInfo(contentBase, sessionControl, tracks);
    }

    private static void parseMLine(String value, MediaSection section) {
        // m=video 0 RTP/AVP 96 97
        String[] parts = value.trim().split("\\s+");
        if (parts.length >= 1) {
            section.media = parts[0];
        }
        for (int i = 3; i < parts.length; i++) {
            try {
                section.payloadTypes.add(Integer.parseInt(parts[i]));
            } catch (NumberFormatException ignored) {
            }
        }
    }

    /** m= 行的解析中间状态。 */
    private static final class MediaSection {
        String media = "";
        final List<Integer> payloadTypes = new ArrayList<>();
        int payloadType = -1;
        String encoding;
        String control;
        int packetizationMode = 1;
        final List<byte[]> vps = new ArrayList<>();
        final List<byte[]> sps = new ArrayList<>();
        final List<byte[]> pps = new ArrayList<>();
        private String fmtpParams;

        void setRtpmap(int pt, String enc) {
            if (payloadTypes.contains(pt)) {
                payloadType = pt;
                encoding = enc;
            } else if (payloadType < 0) {
                payloadType = pt;
                encoding = enc;
            }
        }

        void setFmtp(int pt, String params) {
            if (payloadType >= 0 && pt != payloadType && payloadTypes.contains(payloadType)) {
                return;
            }
            if (payloadType < 0) {
                payloadType = pt;
            }
            fmtpParams = params;
            parseFmtp(params);
        }

        private void parseFmtp(String params) {
            for (String kv : params.split(";")) {
                String item = kv.trim();
                if (item.isEmpty()) {
                    continue;
                }
                int eq = item.indexOf('=');
                if (eq <= 0) {
                    continue;
                }
                String key = item.substring(0, eq).trim().toLowerCase();
                String value = item.substring(eq + 1).trim();
                switch (key) {
                    case "packetization-mode":
                        try {
                            packetizationMode = Integer.parseInt(value);
                        } catch (NumberFormatException ignored) {
                        }
                        break;
                    case "sprop-parameter-sets":
                        // H.264：逗号分隔的 base64，SPS(type=7) 与 PPS(type=8)
                        for (byte[] nalu : decodeList(value)) {
                            int type = nalu.length > 0 ? nalu[0] & 0x1F : -1;
                            if (type == 7) {
                                sps.add(nalu);
                            } else if (type == 8) {
                                pps.add(nalu);
                            }
                        }
                        break;
                    case "sprop-vps":
                        vps.addAll(decodeList(value));
                        break;
                    case "sprop-sps":
                        sps.addAll(decodeList(value));
                        break;
                    case "sprop-pps":
                        pps.addAll(decodeList(value));
                        break;
                    default:
                        break;
                }
            }
        }

        SdpInfo.Track toTrack() {
            return new SdpInfo.Track(media, payloadType, encoding, control,
                    packetizationMode, List.copyOf(vps), List.copyOf(sps), List.copyOf(pps));
        }
    }

    private static List<byte[]> decodeList(String value) {
        List<byte[]> out = new ArrayList<>();
        for (String part : value.split(",")) {
            String b64 = part.trim();
            if (b64.isEmpty()) {
                continue;
            }
            try {
                out.add(Base64.getMimeDecoder().decode(b64));
            } catch (IllegalArgumentException ignored) {
            }
        }
        return out;
    }
}
