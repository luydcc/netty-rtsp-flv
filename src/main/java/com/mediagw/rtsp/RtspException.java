package com.mediagw.rtsp;

/** RTSP 协议或流程异常。 */
public class RtspException extends RuntimeException {

    /** 异常消息中携带对端原文时的最大字符数。 */
    private static final int BRIEF_MAX = 64;

    public RtspException(String message) {
        super(message);
    }

    public RtspException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 把对端原始报文安全地放进异常消息：截断到 {@value #BRIEF_MAX} 字符，控制字符转义为
     * {@code \xNN}，非 ASCII 字节（按 US-ASCII 解码后为 U+FFFD）替换为 {@code ?}。
     * 对端返回二进制数据（连错端口、TLS 握手、裸 RTP 等）时，避免日志被整段乱码刷屏。
     */
    static String brief(String raw) {
        if (raw == null || raw.isEmpty()) {
            return "<empty>";
        }
        int len = Math.min(raw.length(), BRIEF_MAX);
        StringBuilder sb = new StringBuilder(len + 16);
        for (int i = 0; i < len; i++) {
            char c = raw.charAt(i);
            if (c >= 0x20 && c < 0x7F) {
                sb.append(c);
            } else if (c <= 0xFF) {
                sb.append(String.format("\\x%02X", (int) c));
            } else {
                sb.append('?');
            }
        }
        if (raw.length() > len) {
            sb.append("...(").append(raw.length()).append(" chars)");
        }
        return sb.toString();
    }
}
