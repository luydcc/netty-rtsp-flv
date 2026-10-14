package com.mediagw.gb28181;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SIP 报文的轻量模型：覆盖 GB28181 平台侧所需的最小字段集，
 * 支持解析设备上行的 REGISTER / MESSAGE 请求，并构造 401 / 200 响应报文。
 */
public final class SipMessage {

    /** RFC 3261 紧凑头到标准头名称的映射。 */
    private static final Map<String, String> COMPACT_HEADERS = Map.of(
            "i", "Call-ID", "f", "From", "t", "To", "v", "Via", "m", "Contact",
            "c", "Content-Type", "l", "Content-Length", "e", "CSeq");

    /** 响应 Date 头格式（RFC 1123），GB28181 设备依赖它校时。 */
    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH);

    private final boolean request;
    private final String method;
    private final String uri;
    private final int statusCode;
    private final String reason;
    private final Map<String, String> headers;
    private final String body;

    private SipMessage(boolean request, String method, String uri, int statusCode,
                       String reason, Map<String, String> headers, String body) {
        this.request = request;
        this.method = method;
        this.uri = uri;
        this.statusCode = statusCode;
        this.reason = reason;
        this.headers = headers;
        this.body = body;
    }

    /** 解析整条报文（头 + body，按第一个空行分隔），UDP 整包场景使用。 */
    public static SipMessage parse(String whole) {
        int sep = whole.indexOf("\r\n\r\n");
        int sepLen = 4;
        if (sep < 0) {
            sep = whole.indexOf("\n\n");
            sepLen = 2;
        }
        if (sep < 0) {
            return parse(whole, "");
        }
        return parse(whole.substring(0, sep + sepLen), whole.substring(sep + sepLen));
    }

    /** 解析报文：headerBlock 为头部块（含结尾空行），body 为消息体。 */
    public static SipMessage parse(String headerBlock, String body) {
        String[] lines = headerBlock.split("\r?\n");
        Map<String, String> headers = new LinkedHashMap<>();
        String startLine = lines.length > 0 ? lines[0].trim() : "";
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i];
            if (line.isBlank()) {
                continue;
            }
            // 头值折行：以空白开头的行追加到上一个头
            if (Character.isWhitespace(line.charAt(0)) && !headers.isEmpty()) {
                String last = headers.keySet().iterator().next();
                headers.put(last, headers.get(last) + " " + line.trim());
                continue;
            }
            int idx = line.indexOf(':');
            if (idx <= 0) {
                continue;
            }
            String key = canonical(line.substring(0, idx).trim());
            headers.putIfAbsent(key, line.substring(idx + 1).trim());
        }
        if (startLine.startsWith("SIP/2.0")) {
            // 响应行：SIP/2.0 <code> <reason>
            String[] parts = startLine.split("\\s+", 3);
            int code = parts.length > 1 ? parseInt(parts[1], 0) : 0;
            String reason = parts.length > 2 ? parts[2] : "";
            return new SipMessage(false, "", "", code, reason, headers, body);
        }
        // 请求行：METHOD <uri> SIP/2.0
        String[] parts = startLine.split("\\s+");
        String method = parts.length > 0 ? parts[0] : "";
        String uri = parts.length > 1 ? parts[1] : "";
        return new SipMessage(true, method, uri, 0, "", headers, body);
    }

    // ---- 字段访问 ----

    public boolean isRequest() { return request; }
    public String method() { return method; }
    public String uri() { return uri; }
    public int statusCode() { return statusCode; }
    public String body() { return body; }

    public String header(String name) { return headers.get(name); }

    /** Expires 头的整数值，缺失或非法返回 0（0 表示注销）。 */
    public int expires() {
        String v = headers.get("Expires");
        return v == null ? 0 : Math.max(0, parseInt(v, 0));
    }

    /** CSeq 中的方法名：{@code "1 REGISTER"} 返回 {@code REGISTER}。 */
    public String cseqMethod() {
        String v = headers.get("CSeq");
        if (v == null) {
            return "";
        }
        String[] parts = v.trim().split("\\s+");
        return parts.length > 0 ? parts[parts.length - 1] : "";
    }

    /** From 头中的设备国标编码（第一个 sip: 与 @ 之间的 user 部分）。 */
    public String fromUser() {
        return sipUser(headers.get("From"));
    }

    // ---- 响应构造 ----

    /**
     * 构造对本请求的响应报文：自动回填 Via（按需补 received/rport）、From、
     * To（补服务器 tag）、Call-ID 与 CSeq，并携带 Date 头供设备校时。
     *
     * @param extraHeader 附加头（如 WWW-Authenticate / Contact / Expires），行尾需自带 CRLF，可为 null
     * @param remoteIp    请求来源 IP，用于补 Via 的 received/rport，可为 null
     */
    public String toResponse(int code, String reason, String extraHeader, String remoteIp, int remotePort) {
        StringBuilder sb = new StringBuilder(320);
        sb.append("SIP/2.0 ").append(code).append(' ').append(reason).append("\r\n");
        sb.append("Via: ").append(viaWithReceived(remoteIp, remotePort)).append("\r\n");
        appendHeader(sb, "From");
        String to = headers.get("To");
        if (to != null) {
            sb.append("To: ").append(to.contains("tag=") ? to : to + ";tag=" + randomToken()).append("\r\n");
        }
        appendHeader(sb, "Call-ID");
        appendHeader(sb, "CSeq");
        if (extraHeader != null && !extraHeader.isEmpty()) {
            sb.append(extraHeader);
        }
        sb.append("Date: ").append(DATE_FORMAT.format(ZonedDateTime.now(ZoneOffset.UTC))).append("\r\n");
        sb.append("User-Agent: netty-rtsp-flv\r\n");
        sb.append("Content-Length: 0\r\n\r\n");
        return sb.toString();
    }

    // ---- 内部工具 ----

    /** RFC 3581：请求 Via 带 rport 时，响应补齐 rport 与 received 值。 */
    private String viaWithReceived(String remoteIp, int remotePort) {
        String via = headers.get("Via");
        if (via == null) {
            return "";
        }
        if (remoteIp == null) {
            return via;
        }
        String lower = via.toLowerCase(Locale.ROOT);
        if (lower.contains(";rport=") || !lower.contains("rport")) {
            return via;
        }
        return via.replaceFirst(";rport(?!\\s*=)", ";rport=" + remotePort + ";received=" + remoteIp);
    }

    private void appendHeader(StringBuilder sb, String name) {
        String v = headers.get(name);
        if (v != null) {
            sb.append(name).append(": ").append(v).append("\r\n");
        }
    }

    /** 从形如 {@code <sip:ID@domain>;tag=x} 的地址头提取 user 部分。 */
    static String sipUser(String addr) {
        if (addr == null) {
            return "";
        }
        int start = addr.indexOf("sip:") + 4;
        if (start < 4 || start >= addr.length()) {
            return "";
        }
        int end = addr.length();
        for (char stop : new char[]{'@', ';', '>', '?'}) {
            int i = addr.indexOf(stop, start);
            if (i >= 0 && i < end) {
                end = i;
            }
        }
        return addr.substring(start, end);
    }

    /** 头名称规范化：紧凑头还原为标准头，常见头统一大小写形式便于读取。 */
    private static String canonical(String name) {
        String compact = COMPACT_HEADERS.get(name);
        if (compact != null) {
            return compact;
        }
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "call-id" -> "Call-ID";
            case "cseq" -> "CSeq";
            case "via" -> "Via";
            case "from" -> "From";
            case "to" -> "To";
            case "contact" -> "Contact";
            case "content-type" -> "Content-Type";
            case "content-length" -> "Content-Length";
            case "expires" -> "Expires";
            case "authorization" -> "Authorization";
            case "www-authenticate" -> "WWW-Authenticate";
            default -> name;
        };
    }

    private static int parseInt(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static String randomToken() {
        return String.format("%08x", ThreadLocalRandom.current().nextInt());
    }
}
