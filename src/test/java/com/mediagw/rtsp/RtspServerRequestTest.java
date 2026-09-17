package com.mediagw.rtsp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 服务端侧 RTSP 请求解析测试。 */
class RtspServerRequestTest {

    @Test
    void parsesRequestLineAndHeaders() {
        RtspServerRequest req = RtspServerRequest.parse(
                "DESCRIBE rtsp://10.0.0.1:8554/live/cam1 RTSP/1.0\r\n"
                        + "CSeq: 2\r\n"
                        + "Accept: application/sdp\r\n"
                        + "User-Agent: VLC/3.0\r\n\r\n", "");

        assertEquals("DESCRIBE", req.method());
        assertEquals("rtsp://10.0.0.1:8554/live/cam1", req.uri());
        assertEquals("2", req.header("CSeq"));
        assertEquals("2", req.header("cseq")); // 头名大小写不敏感
        assertEquals("application/sdp", req.header("Accept"));
        assertEquals("", req.body());
        assertEquals(3, req.headers().size());
        assertEquals("DESCRIBE rtsp://10.0.0.1:8554/live/cam1", req.toString());
    }

    @Test
    void methodIsUpperCased() {
        RtspServerRequest req = RtspServerRequest.parse("options rtsp://h/live/cam1 RTSP/1.0\r\nCSeq: 1\r\n\r\n", "");
        assertEquals("OPTIONS", req.method());
    }

    @Test
    void keepsBody() {
        RtspServerRequest req = RtspServerRequest.parse(
                "SET_PARAMETER rtsp://h/live/cam1 RTSP/1.0\r\nCSeq: 5\r\nContent-Type: text/parameters\r\n\r\n",
                "volume: 50\r\n");
        assertEquals("volume: 50\r\n", req.body());
    }

    @Test
    void pathStripsAuthorityAndQuery() {
        assertEquals("/live/cam1", RtspServerRequest.parse(
                "DESCRIBE rtsp://10.0.0.1:8554/live/cam1?token=x RTSP/1.0\r\n\r\n", "").path());
        // 无路径的 authority
        assertEquals("/", RtspServerRequest.parse("OPTIONS rtsp://10.0.0.1:8554 RTSP/1.0\r\n\r\n", "").path());
        // 相对 URI（客户端基于 Content-Base 解析后可能只发控制后缀）
        assertEquals("trackID=0", RtspServerRequest.parse(
                "SETUP trackID=0 RTSP/1.0\r\n\r\n", "").path());
        assertEquals("trackID=0", RtspServerRequest.parse("SETUP trackID=0 RTSP/1.0\r\n\r\n", "").uri());
    }

    @Test
    void queryReadsParameters() {
        RtspServerRequest req = RtspServerRequest.parse(
                "DESCRIBE rtsp://h/live/cam1?token=sekrit&x=1 RTSP/1.0\r\n\r\n", "");
        assertEquals("sekrit", req.query("token"));
        assertEquals("1", req.query("x"));
        assertNull(req.query("missing"));
        assertNull(RtspServerRequest.parse("OPTIONS rtsp://h/live/cam1 RTSP/1.0\r\n\r\n", "").query("token"));
    }

    @Test
    void rejectsMalformedRequestLine() {
        assertThrows(RtspException.class, () -> RtspServerRequest.parse("", ""));
        assertThrows(RtspException.class, () -> RtspServerRequest.parse("GARBAGE\r\n\r\n", ""));
        // 响应不是请求
        assertThrows(RtspException.class, () -> RtspServerRequest.parse("RTSP/1.0 200 OK\r\n\r\n", ""));
    }

    @Test
    void malformedRequestLineMessageIsBrief() {
        // 非 RTSP 客户端（或二进制探测）连上转发端口时，异常消息不能把整段乱码带进日志
        String garbage = "\u0001\u001f" + "\ufffd".repeat(500);
        RtspException e = assertThrows(RtspException.class,
                () -> RtspServerRequest.parse(garbage + "\r\n\r\n", ""));
        String msg = e.getMessage();
        assertTrue(msg.length() < 120, msg);
        assertTrue(msg.contains("\\x01"), msg);
        assertTrue(msg.contains("...(502 chars)"), msg);
        assertFalse(msg.contains("\r") || msg.contains("\n"), msg);
    }

    @Test
    void ignoresHeaderLinesWithoutColon() {
        RtspServerRequest req = RtspServerRequest.parse("PLAY rtsp://h/live/cam1 RTSP/1.0\r\nCSeq:7\r\nbroken\r\n\r\n", "");
        assertEquals("7", req.header("CSeq")); // 值两侧空白被裁掉
        assertTrue(req.headers().size() == 1);
    }
}
