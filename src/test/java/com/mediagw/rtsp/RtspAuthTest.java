package com.mediagw.rtsp;

import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RtspAuthTest {

    @Test
    void basicHeader() {
        RtspAuth auth = RtspAuth.fromChallenge("Basic realm=\"IPCamera\"", "user", "pass");
        assertEquals(RtspAuth.Scheme.BASIC, auth.scheme());
        assertEquals("Basic " + Base64.getEncoder().encodeToString("user:pass".getBytes()),
                auth.authorizationHeader("DESCRIBE", "rtsp://host/path"));
    }

    @Test
    void nullChallengeFallsBackToBasic() {
        RtspAuth auth = RtspAuth.fromChallenge(null, "user", "pass");
        assertEquals(RtspAuth.Scheme.BASIC, auth.scheme());
        assertTrue(auth.authorizationHeader("OPTIONS", "*").startsWith("Basic "));
    }

    @Test
    void noUsernameMeansNoAuth() {
        RtspAuth auth = RtspAuth.fromChallenge("Digest realm=\"r\", nonce=\"n\"", "", "p");
        assertEquals(RtspAuth.Scheme.NONE, auth.scheme());
        assertFalse(auth.hasCredentials());
        assertNull(auth.authorizationHeader("DESCRIBE", "rtsp://h/"));
    }

    /** RFC 2069 风格（无 qop）：response = md5(HA1:nonce:HA2)，HA1/HA2 用公开向量常数验证。 */
    @Test
    void digestWithoutQopKnownAnswer() {
        RtspAuth auth = RtspAuth.fromChallenge(
                "Digest realm=\"testrealm@host.com\", nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\"",
                "Mufasa", "Circle Of Life");
        assertEquals(RtspAuth.Scheme.DIGEST, auth.scheme());
        String h = auth.authorizationHeader("GET", "/dir/index.html");
        assertTrue(h.startsWith("Digest "), h);
        assertTrue(h.contains("username=\"Mufasa\""), h);
        assertTrue(h.contains("realm=\"testrealm@host.com\""), h);
        assertTrue(h.contains("uri=\"/dir/index.html\""), h);
        // 公开向量：HA1=md5(user:realm:pass), HA2=md5(method:uri)
        assertEquals("939e7578ed9e3c518a452acee763bce9",
                RtspAuth.md5Hex("Mufasa:testrealm@host.com:Circle Of Life"));
        assertEquals("39aff3a2bab6126f332b942af96d3366", RtspAuth.md5Hex("GET:/dir/index.html"));
        String expected = RtspAuth.md5Hex("939e7578ed9e3c518a452acee763bce9"
                + ":dcd98b7102dd2f0e8b11d0f600bfb0c093"
                + ":39aff3a2bab6126f332b942af96d3366");
        assertTrue(h.contains("response=\"" + expected + "\""), h);
        assertFalse(h.contains("qop="), h);
    }

    @Test
    void digestWithQopPicksAuthAndCountsNc() {
        RtspAuth auth = RtspAuth.fromChallenge(
                "Digest realm=\"cam\", nonce=\"abc123\", qop=\"auth,auth-int\", opaque=\"opq456\"",
                "admin", "secret");
        String h = auth.authorizationHeader("DESCRIBE", "rtsp://cam/live");

        assertTrue(h.contains("qop=auth,") || h.contains("qop=auth "), h);
        assertFalse(h.contains("auth-int"), h);
        assertTrue(h.contains("nc=00000001"), h);
        assertTrue(h.contains("opaque=\"opq456\""), h);

        String cnonce = group(h, "cnonce=\"([^\"]+)\"");
        String nc = group(h, "nc=([0-9a-fA-F]+)");
        String response = group(h, "response=\"([^\"]+)\"");
        String ha1 = RtspAuth.md5Hex("admin:cam:secret");
        String ha2 = RtspAuth.md5Hex("DESCRIBE:rtsp://cam/live");
        assertEquals(RtspAuth.md5Hex(ha1 + ":abc123:" + nc + ":" + cnonce + ":auth:" + ha2), response);

        // 第二次调用 nc 递增
        String h2 = auth.authorizationHeader("SETUP", "rtsp://cam/live/trackID=1");
        assertTrue(h2.contains("nc=00000002"), h2);
    }

    private static String group(String s, String regex) {
        Matcher m = Pattern.compile(regex).matcher(s);
        assertTrue(m.find(), "missing " + regex + " in: " + s);
        return m.group(1);
    }

    @Test
    void paramExtraction() {
        String challenge = "Digest realm=\"IP Camera(72)\", nonce=\"ff  ee\", qop=\"auth\"";
        assertEquals("IP Camera(72)", RtspAuth.param(challenge, "realm"));
        assertEquals("ff  ee", RtspAuth.param(challenge, "nonce"));
        assertEquals("auth", RtspAuth.param(challenge, "qop"));
        assertNull(RtspAuth.param(challenge, "opaque"));
    }
}
