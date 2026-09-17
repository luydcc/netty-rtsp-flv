package com.mediagw.e2e;

import com.mediagw.testutil.TestMedia;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 最小 RTSP over TCP 模拟服务端（联调/端到端测试用）：
 * 响应 OPTIONS / DESCRIBE / SETUP / PLAY / GET_PARAMETER / TEARDOWN，
 * PLAY 后在 interleaved channel 0 推送合成帧（25fps，GOP=50）：
 * <ul>
 *   <li>URI 含 "265" → H.265 单 NALU 帧（IDR_W_RADL / TRAIL_R）；</li>
 *   <li>否则 → H.264 FU-A 分片帧（IDR / 非 IDR slice）。</li>
 * </ul>
 * 用法：{@code java -cp app.jar:test-classes com.mediagw.e2e.FakeRtspServer [port]}
 */
public final class FakeRtspServer {

    private static final int PT = 96;
    private static final int SSRC = 0x0BADF00D;

    private final int port;
    private volatile boolean running = true;
    private ServerSocket server;

    public FakeRtspServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        server = new ServerSocket(port);
        Thread t = new Thread(this::acceptLoop, "fake-rtsp-accept");
        t.setDaemon(true);
        t.start();
    }

    public void stop() throws IOException {
        running = false;
        if (server != null) {
            server.close();
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = server.accept();
                Thread ct = new Thread(() -> handle(s), "fake-rtsp-conn");
                ct.setDaemon(true);
                ct.start();
            } catch (IOException e) {
                return; // server 关闭
            }
        }
    }

    private void handle(Socket s) {
        try (s) {
            s.setTcpNoDelay(true);
            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();
            String session = "FAKE0SESSION1";
            while (true) {
                String[] req = readRequest(in);
                if (req == null) {
                    return; // 对端关闭
                }
                String method = req[0];
                String uri = req[1];
                int cseq = parseCSeq(req[2]);
                System.out.println("[fake-rtsp] " + method + " " + uri + " CSeq=" + cseq);
                System.out.flush();
                switch (method) {
                    case "OPTIONS" -> writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                            + "\r\nPublic: OPTIONS, DESCRIBE, SETUP, PLAY, TEARDOWN, GET_PARAMETER\r\n\r\n");
                    case "DESCRIBE" -> {
                        byte[] sdp = buildSdp(uri).getBytes(StandardCharsets.UTF_8);
                        writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                                + "\r\nContent-Base: " + contentBase(uri)
                                + "\r\nContent-Type: application/sdp\r\nContent-Length: " + sdp.length + "\r\n\r\n");
                        synchronized (out) {
                            out.write(sdp);
                            out.flush();
                        }
                    }
                    case "SETUP" -> writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                            + "\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1;ssrc="
                            + String.format("%08X", SSRC)
                            + "\r\nSession: " + session + "\r\n\r\n");
                    case "PLAY" -> {
                        writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                                + "\r\nSession: " + session + "\r\nRange: npt=0.000-\r\n\r\n");
                        boolean h265 = uri.contains("265");
                        Thread sender = new Thread(() -> sendLoop(out, h265), "fake-rtsp-sender");
                        sender.setDaemon(true);
                        sender.start();
                    }
                    case "GET_PARAMETER" -> writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                            + "\r\nSession: " + session + "\r\n\r\n");
                    case "TEARDOWN" -> {
                        writeText(out, "RTSP/1.0 200 OK\r\nCSeq: " + cseq
                                + "\r\nSession: " + session + "\r\n\r\n");
                        return; // 关闭连接，sender 随 IOException 退出
                    }
                    default -> writeText(out, "RTSP/1.0 501 Not Implemented\r\nCSeq: " + cseq + "\r\n\r\n");
                }
            }
        } catch (IOException ignored) {
            // 连接断开
        }
    }

    // ===== 媒体推送 =====

    private void sendLoop(OutputStream out, boolean h265) {
        int seq = 0;
        long ts = 0;
        int frameNo = 0;
        try {
            while (running) {
                boolean key = frameNo % 50 == 0;
                if (h265) {
                    byte[] nalu = key ? synthetic(19, 300, frameNo) : synthetic(1, 150, frameNo);
                    writeInterleaved(out, 0, rtpPacket(seq++, ts, true, nalu));
                } else {
                    byte[] nalu = key ? h264Nalu(0x65, 800, frameNo) : h264Nalu(0x41, 150, frameNo);
                    seq = sendFuA(out, nalu, seq, ts);
                }
                ts += 3600; // 90kHz / 25fps
                frameNo++;
                Thread.sleep(40);
            }
        } catch (IOException | InterruptedException | RuntimeException ignored) {
            // 连接关闭即停止
        }
    }

    /** FU-A 分片发送一个 H.264 NALU，返回新的 seq。 */
    private int sendFuA(OutputStream out, byte[] nalu, int seq, long ts) throws IOException {
        int chunk = 200;
        for (int off = 1; off < nalu.length; off += chunk) {
            int len = Math.min(chunk, nalu.length - off);
            boolean start = off == 1;
            boolean end = off + len >= nalu.length;
            byte[] payload = new byte[2 + len];
            payload[0] = (byte) ((nalu[0] & 0xE0) | 28);
            payload[1] = (byte) ((start ? 0x80 : 0) | (end ? 0x40 : 0) | (nalu[0] & 0x1F));
            System.arraycopy(nalu, off, payload, 2, len);
            writeInterleaved(out, 0, rtpPacket(seq++, ts, end, payload));
        }
        return seq;
    }

    private static byte[] h264Nalu(int firstByte, int size, int frameNo) {
        byte[] nalu = new byte[size];
        nalu[0] = (byte) firstByte;
        for (int i = 1; i < size; i++) {
            nalu[i] = (byte) (i * 31 + frameNo);
        }
        return nalu;
    }

    /** 合成 H.265 NALU：2 字节 header（type<<1）+ 载荷。 */
    private static byte[] synthetic(int type, int size, int frameNo) {
        byte[] rbsp = new byte[size];
        for (int i = 0; i < size; i++) {
            rbsp[i] = (byte) (i * 17 + frameNo + type);
        }
        return TestMedia.hevcNalu(type, rbsp);
    }

    private static byte[] rtpPacket(int seq, long ts, boolean marker, byte[] payload) {
        byte[] p = new byte[12 + payload.length];
        p[0] = (byte) 0x80;
        p[1] = (byte) ((marker ? 0x80 : 0) | PT);
        p[2] = (byte) (seq >>> 8);
        p[3] = (byte) seq;
        p[4] = (byte) (ts >>> 24);
        p[5] = (byte) (ts >>> 16);
        p[6] = (byte) (ts >>> 8);
        p[7] = (byte) ts;
        p[8] = (byte) (SSRC >>> 24);
        p[9] = (byte) (SSRC >>> 16);
        p[10] = (byte) (SSRC >>> 8);
        p[11] = (byte) SSRC;
        System.arraycopy(payload, 0, p, 12, payload.length);
        return p;
    }

    private static void writeInterleaved(OutputStream out, int channel, byte[] data) throws IOException {
        synchronized (out) {
            out.write('$');
            out.write(channel);
            out.write((data.length >>> 8) & 0xFF);
            out.write(data.length & 0xFF);
            out.write(data);
            out.flush();
        }
    }

    private static void writeText(OutputStream out, String text) throws IOException {
        synchronized (out) {
            out.write(text.getBytes(StandardCharsets.US_ASCII));
            out.flush();
        }
    }

    // ===== 请求解析与 SDP =====

    /** 读到 \r\n\r\n 为止；返回 [method, uri, 原始文本]，连接关闭返回 null。 */
    private static String[] readRequest(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int state = 0;
        while (true) {
            int b = in.read();
            if (b < 0) {
                return null;
            }
            buf.write(b);
            state = switch (state) {
                case 0 -> b == '\r' ? 1 : 0;
                case 1 -> b == '\n' ? 2 : 0;
                case 2 -> b == '\r' ? 3 : 0;
                case 3 -> b == '\n' ? 4 : 0;
                default -> 0;
            };
            if (state == 4) {
                break;
            }
        }
        String text = buf.toString(StandardCharsets.US_ASCII);
        String[] first = text.split("\r\n", 2)[0].split(" ");
        return new String[]{first[0], first.length > 1 ? first[1] : "", text};
    }

    private static int parseCSeq(String requestText) {
        for (String line : requestText.split("\r\n")) {
            if (line.toLowerCase().startsWith("cseq:")) {
                try {
                    return Integer.parseInt(line.substring(5).trim());
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }

    private static String stripQuery(String uri) {
        int q = uri.indexOf('?');
        return q >= 0 ? uri.substring(0, q) : uri;
    }

    /** DESCRIBE 的 Content-Base：绝对 URI 原样保留，相对路径补全为服务端地址。 */
    private String contentBase(String uri) {
        String u = stripQuery(uri);
        if (!u.startsWith("rtsp://")) {
            u = "rtsp://127.0.0.1:" + port + (u.startsWith("/") ? u : "/" + u);
        }
        return u.endsWith("/") ? u : u + "/";
    }

    private String buildSdp(String uri) {
        String base = "v=0\r\n"
                + "o=- 1 1 IN IP4 127.0.0.1\r\n"
                + "s=FakeCam\r\n"
                + "c=IN IP4 0.0.0.0\r\n"
                + "t=0 0\r\n"
                + "a=control:*\r\n";
        if (uri.contains("265")) {
            return base
                    + "m=video 0 RTP/AVP " + PT + "\r\n"
                    + "a=rtpmap:" + PT + " H265/90000\r\n"
                    + "a=fmtp:" + PT + " sprop-vps=" + b64(TestMedia.hevcVps())
                    + ";sprop-sps=" + b64(TestMedia.hevcSps())
                    + ";sprop-pps=" + b64(TestMedia.hevcPps()) + "\r\n"
                    + "a=control:trackID=1\r\n";
        }
        return base
                + "m=video 0 RTP/AVP " + PT + "\r\n"
                + "a=rtpmap:" + PT + " H264/90000\r\n"
                + "a=fmtp:" + PT + " packetization-mode=1;sprop-parameter-sets="
                + b64(TestMedia.H264_SPS) + "," + b64(TestMedia.H264_PPS) + "\r\n"
                + "a=control:trackID=1\r\n";
    }

    private static String b64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8554;
        FakeRtspServer server = new FakeRtspServer(port);
        server.start();
        System.out.println("FakeRtspServer listening on " + port + " (H.264: /test, H.265: URI 含 265)");
        Thread.currentThread().join();
    }
}
