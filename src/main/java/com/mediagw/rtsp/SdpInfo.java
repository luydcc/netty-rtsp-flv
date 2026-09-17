package com.mediagw.rtsp;

import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;

import java.util.List;

/** SDP 描述信息：会话级 control 与媒体轨。 */
public final class SdpInfo {

    private final String contentBase;
    private final String sessionControl;
    private final List<Track> tracks;

    SdpInfo(String contentBase, String sessionControl, List<Track> tracks) {
        this.contentBase = contentBase;
        this.sessionControl = sessionControl;
        this.tracks = tracks;
    }

    public String contentBase() {
        return contentBase;
    }

    public String sessionControl() {
        return sessionControl;
    }

    public List<Track> tracks() {
        return tracks;
    }

    /** 第一个受支持的视频轨（H.264/H.265），无则返回 null。 */
    public Track firstVideo() {
        for (Track t : tracks) {
            if (t.isSupportedVideo()) {
                return t;
            }
        }
        return null;
    }

    /** 媒体轨。 */
    public static final class Track {
        private final String media;
        private final int payloadType;
        private final String encoding;
        private final String control;
        private final int packetizationMode;
        private final List<byte[]> vps;
        private final List<byte[]> sps;
        private final List<byte[]> pps;

        Track(String media, int payloadType, String encoding, String control,
              int packetizationMode, List<byte[]> vps, List<byte[]> sps, List<byte[]> pps) {
            this.media = media;
            this.payloadType = payloadType;
            this.encoding = encoding;
            this.control = control;
            this.packetizationMode = packetizationMode;
            this.vps = vps;
            this.sps = sps;
            this.pps = pps;
        }

        public String media() {
            return media;
        }

        public int payloadType() {
            return payloadType;
        }

        public String encoding() {
            return encoding;
        }

        public String control() {
            return control;
        }

        public int packetizationMode() {
            return packetizationMode;
        }

        public CodecType codec() {
            return CodecType.fromRtpMap(encoding);
        }

        public boolean isSupportedVideo() {
            return "video".equalsIgnoreCase(media) && codec() != null;
        }

        /** 由 sprop 参数集构造 ParamSets；无 sprop 时返回不完整的 ParamSets。 */
        public ParamSets toParamSets() {
            return new ParamSets(codec(), vps, sps, pps);
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
    }
}
