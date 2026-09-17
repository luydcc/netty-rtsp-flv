package com.mediagw.rtsp;

import com.mediagw.codec.CodecType;
import com.mediagw.testutil.TestMedia;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static com.mediagw.testutil.TestMedia.H264_PPS;
import static com.mediagw.testutil.TestMedia.H264_SPS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SdpParserTest {

    private static String b64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    @Test
    void parsesH264Sdp() {
        String sdp = String.join("\r\n",
                "v=0",
                "o=- 1700000000 1 IN IP4 192.168.1.10",
                "s=IPCam",
                "c=IN IP4 0.0.0.0",
                "t=0 0",
                "a=control:*",
                "m=video 0 RTP/AVP 96",
                "a=rtpmap:96 H264/90000",
                "a=fmtp:96 packetization-mode=1;profile-level-id=42C01F;sprop-parameter-sets="
                        + b64(H264_SPS) + "," + b64(H264_PPS),
                "a=control:trackID=1",
                "m=audio 0 RTP/AVP 97",
                "a=rtpmap:97 MPEG4-GENERIC/44100",
                "a=control:trackID=2",
                "");
        SdpInfo info = SdpParser.parse(sdp, "rtsp://192.168.1.10:554/live/");

        assertEquals("*", info.sessionControl());
        assertEquals(2, info.tracks().size());

        SdpInfo.Track video = info.firstVideo();
        assertNotNull(video);
        assertEquals("video", video.media());
        assertEquals(96, video.payloadType());
        assertEquals("H264", video.encoding());
        assertEquals(CodecType.H264, video.codec());
        assertEquals("trackID=1", video.control());
        assertEquals(1, video.packetizationMode());
        assertEquals(1, video.sps().size());
        assertEquals(1, video.pps().size());
        assertArrayEquals(H264_SPS, video.sps().get(0));
        assertArrayEquals(H264_PPS, video.pps().get(0));
        assertTrue(video.isSupportedVideo());
        assertTrue(video.toParamSets().isComplete());

        SdpInfo.Track audio = info.tracks().get(1);
        assertFalse(audio.isSupportedVideo());
    }

    @Test
    void parsesH265SdpWithSpropVpsSpsPps() {
        byte[] vps = TestMedia.hevcVps();
        byte[] sps = TestMedia.hevcSps();
        byte[] pps = TestMedia.hevcPps();
        String sdp = String.join("\r\n",
                "v=0",
                "o=- 1 1 IN IP4 0.0.0.0",
                "s=HEVC cam",
                "t=0 0",
                "m=video 0 RTP/AVP 96",
                "a=rtpmap:96 H265/90000",
                "a=fmtp:96 sprop-vps=" + b64(vps) + "; sprop-sps=" + b64(sps) + "; sprop-pps=" + b64(pps),
                "a=control:track1",
                "");
        SdpInfo info = SdpParser.parse(sdp, null);
        SdpInfo.Track video = info.firstVideo();
        assertNotNull(video);
        assertEquals(CodecType.H265, video.codec());
        assertArrayEquals(vps, video.vps().get(0));
        assertArrayEquals(sps, video.sps().get(0));
        assertArrayEquals(pps, video.pps().get(0));
        assertTrue(video.toParamSets().isComplete());
        assertEquals(CodecType.H265, video.toParamSets().codec());
    }

    @Test
    void noVideoTrackYieldsNullFirstVideo() {
        String sdp = String.join("\r\n",
                "v=0",
                "o=- 1 1 IN IP4 0.0.0.0",
                "s=audio only",
                "t=0 0",
                "m=audio 0 RTP/AVP 97",
                "a=rtpmap:97 PCMA/8000",
                "a=control:track1",
                "");
        SdpInfo info = SdpParser.parse(sdp, null);
        assertNull(info.firstVideo());
        assertEquals(1, info.tracks().size());
    }

    @Test
    void unknownVideoCodecNotSupported() {
        String sdp = String.join("\r\n",
                "v=0",
                "m=video 0 RTP/AVP 96",
                "a=rtpmap:96 MP4V-ES/90000",
                "a=control:track1",
                "");
        assertNull(SdpParser.parse(sdp, null).firstVideo());
    }
}
