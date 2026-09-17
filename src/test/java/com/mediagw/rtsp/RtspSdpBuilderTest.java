package com.mediagw.rtsp;

import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.testutil.TestMedia;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;

import static com.mediagw.testutil.TestMedia.H264_PPS;
import static com.mediagw.testutil.TestMedia.H264_SPS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 转发 SDP 生成测试，并用 {@link SdpParser} 回环校验（拉流侧解析同一份 SDP）。 */
class RtspSdpBuilderTest {

    private static String b64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    private static ParamSets h264Params() {
        return new ParamSets(CodecType.H264, List.of(), List.of(H264_SPS), List.of(H264_PPS));
    }

    private static ParamSets h265Params() {
        return new ParamSets(CodecType.H265, List.of(TestMedia.hevcVps()),
                List.of(TestMedia.hevcSps()), List.of(TestMedia.hevcPps()));
    }

    @Test
    void buildsH264Sdp() {
        String sdp = RtspSdpBuilder.build("cam1", CodecType.H264, h264Params());

        assertTrue(sdp.startsWith("v=0\r\n"), sdp);
        assertTrue(sdp.contains("\r\ns=cam1\r\n"), sdp);
        assertTrue(sdp.contains("\r\nt=0 0\r\n"), sdp);
        assertTrue(sdp.contains("\r\na=control:*\r\n"), sdp);
        assertTrue(sdp.contains("\r\nm=video 0 RTP/AVP 96\r\n"), sdp);
        assertTrue(sdp.contains("\r\na=rtpmap:96 H264/90000\r\n"), sdp);
        assertTrue(sdp.contains("a=fmtp:96 packetization-mode=1;profile-level-id=42C01F;sprop-parameter-sets="
                + b64(H264_SPS) + "," + b64(H264_PPS) + "\r\n"), sdp);
        assertTrue(sdp.endsWith("a=control:" + RtspSdpBuilder.TRACK_CONTROL + "\r\n"), sdp);
        assertFalse(sdp.contains("\r\n\r\n"), "SDP 行以单个 CRLF 分隔");
    }

    @Test
    void buildsH265Sdp() {
        ParamSets ps = h265Params();
        String sdp = RtspSdpBuilder.build("cam265", CodecType.H265, ps);

        assertTrue(sdp.contains("\r\na=rtpmap:96 H265/90000\r\n"), sdp);
        // profile/tier/level 与约束字节取自合成 SPS（Main、level 120、constraint[0]=0x90）
        assertTrue(sdp.contains("a=fmtp:96 profile-space=0;profile-id=1;tier-flag=0;level-id=120;"
                + "interop-constraints=900000000000;"), sdp);
        assertTrue(sdp.contains(";sprop-vps=" + b64(TestMedia.hevcVps())), sdp);
        assertTrue(sdp.contains(";sprop-sps=" + b64(TestMedia.hevcSps())), sdp);
        assertTrue(sdp.contains(";sprop-pps=" + b64(TestMedia.hevcPps())), sdp);
    }

    @Test
    void h264DeclaresAllParamSetsCommaSeparated() {
        byte[] sps2 = H264_SPS.clone();
        sps2[4] = (byte) (sps2[4] + 1);
        ParamSets ps = new ParamSets(CodecType.H264, List.of(),
                List.of(H264_SPS, sps2), List.of(H264_PPS));
        String sdp = RtspSdpBuilder.build("cam1", CodecType.H264, ps);

        assertTrue(sdp.contains("sprop-parameter-sets="
                + b64(H264_SPS) + "," + b64(sps2) + "," + b64(H264_PPS) + "\r\n"), sdp);
    }

    @Test
    void h265DeclaresOnlyFirstParamSetOfEachKind() {
        byte[] sps = TestMedia.hevcSps();
        byte[] sps2 = sps.clone();
        sps2[sps2.length - 1] = (byte) (sps2[sps2.length - 1] ^ 0xFF);
        ParamSets ps = new ParamSets(CodecType.H265, List.of(TestMedia.hevcVps()),
                List.of(sps, sps2), List.of(TestMedia.hevcPps()));
        String sdp = RtspSdpBuilder.build("cam265", CodecType.H265, ps);

        assertTrue(sdp.contains(";sprop-sps=" + b64(sps) + ";"), sdp);
        assertFalse(sdp.contains(b64(sps2)), sdp);
    }

    @Test
    void missingParamSetsStillYieldValidFmtp() {
        String sdp = RtspSdpBuilder.build("cam1", CodecType.H264,
                new ParamSets(CodecType.H264, List.of(), List.of(), List.of()));

        assertTrue(sdp.contains("a=fmtp:96 packetization-mode=1\r\n"), sdp);
        assertFalse(sdp.contains("profile-level-id"), sdp);
        assertFalse(sdp.contains("sprop-parameter-sets"), sdp);
    }

    @Test
    void h264SdpRoundTripsThroughSdpParser() {
        String sdp = RtspSdpBuilder.build("cam1", CodecType.H264, h264Params());
        SdpInfo info = SdpParser.parse(sdp, "rtsp://127.0.0.1:8554/live/cam1/");

        SdpInfo.Track video = info.firstVideo();
        assertNotNull(video);
        assertEquals(CodecType.H264, video.codec());
        assertEquals(RtspSdpBuilder.PAYLOAD_TYPE, video.payloadType());
        assertEquals(RtspSdpBuilder.TRACK_CONTROL, video.control());
        assertEquals(1, video.packetizationMode());
        assertArrayEquals(H264_SPS, video.sps().get(0));
        assertArrayEquals(H264_PPS, video.pps().get(0));
        assertTrue(video.toParamSets().isComplete());
    }

    @Test
    void h265SdpRoundTripsThroughSdpParser() {
        ParamSets ps = h265Params();
        String sdp = RtspSdpBuilder.build("cam265", CodecType.H265, ps);
        SdpInfo info = SdpParser.parse(sdp, null);

        SdpInfo.Track video = info.firstVideo();
        assertNotNull(video);
        assertEquals(CodecType.H265, video.codec());
        assertArrayEquals(TestMedia.hevcVps(), video.vps().get(0));
        assertArrayEquals(TestMedia.hevcSps(), video.sps().get(0));
        assertArrayEquals(TestMedia.hevcPps(), video.pps().get(0));
        assertEquals(ps.signature(), video.toParamSets().signature());
    }
}
