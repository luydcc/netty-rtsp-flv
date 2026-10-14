package com.mediagw.util;

/**
 * 流地址解析：不能直接用 {@code java.net.URI}——设备密码常含 {@code #}、{@code ?} 等 URI 保留字符，
 * 按 RFC 语义会被当作 fragment/query 分隔符，导致 host 与 path 被截断（如
 * {@code rtsp://admin:Mdcz#2026@10.22.88.148:554/Streaming/Channels/101}）。
 * 这里按 {@code scheme://authority/path} 手工切分：authority 取 {@code ://} 后到下一个 {@code /} 之前，
 * 其中的 {@code #}、{@code ?} 归属 userinfo；path 段则原样保留 query 与 fragment。
 * userinfo 中的百分号转义仍按规范解码（{@code %23} -> {@code #}），两种写法都能用。
 */
public record StreamUrl(String scheme, String user, String password, String host, int port,
                        boolean portExplicit, String pathAndQuery) {

    /** 解析失败（缺 scheme、缺 host、host/端口含非法字符）返回 null，不抛异常。 */
    public static StreamUrl parse(String raw) {
        if (raw == null) {
            return null;
        }
        String url = raw.trim();
        int sep = url.indexOf("://");
        if (sep <= 0) {
            return null;
        }
        String scheme = url.substring(0, sep);
        if (!validScheme(scheme)) {
            return null;
        }
        String rest = url.substring(sep + 3);
        int slash = rest.indexOf('/');
        String authority = slash < 0 ? rest : rest.substring(0, slash);
        String path = slash < 0 ? "" : rest.substring(slash);

        String userInfo = null;
        String hostPort = authority;
        int at = authority.lastIndexOf('@');
        if (at >= 0) {
            userInfo = authority.substring(0, at);
            hostPort = authority.substring(at + 1);
            if (userInfo.isEmpty() || hostPort.isEmpty()) {
                return null;
            }
        }
        String host;
        int port = -1;
        boolean portExplicit = false;
        if (hostPort.startsWith("[")) { // IPv6 字面量
            int end = hostPort.indexOf(']');
            if (end < 0) {
                return null;
            }
            host = hostPort.substring(0, end + 1);
            String tail = hostPort.substring(end + 1);
            if (!tail.isEmpty()) {
                if (tail.charAt(0) != ':') {
                    return null;
                }
                port = parsePort(tail.substring(1));
                if (port < 0) {
                    return null;
                }
                portExplicit = true;
            }
        } else {
            int colon = hostPort.indexOf(':');
            if (colon < 0) {
                host = hostPort;
            } else {
                host = hostPort.substring(0, colon);
                port = parsePort(hostPort.substring(colon + 1));
                if (port < 0) {
                    return null;
                }
                portExplicit = true;
            }
        }
        if (!validHost(host)) {
            return null;
        }
        String user = null;
        String password = null;
        if (userInfo != null) {
            String decoded = decodePercent(userInfo);
            int colon = decoded.indexOf(':');
            if (colon >= 0) {
                user = decoded.substring(0, colon);
                password = decoded.substring(colon + 1);
            } else {
                user = decoded;
            }
        }
        return new StreamUrl(scheme.toLowerCase(), user, password, host, port, portExplicit, path);
    }

    /** 该 scheme 是否按 http/https 处理。 */
    public boolean http() {
        return "http".equals(scheme) || "https".equals(scheme);
    }

    public boolean https() {
        return "https".equals(scheme);
    }

    /** 解析结果是否是一个可直接拉流的地址（rtsp / http / https）。 */
    public boolean pullable() {
        return "rtsp".equals(scheme) || http();
    }

    /**
     * 从取流接口的 JSON 响应（如 {@code {"url":"rtsp://host/path"}}）中提取真实流地址。
     * 不引入 JSON 库：按约定双引号内不会再出现双引号，因此成对取引号内的内容逐个尝试解析，
     * 返回第一个可拉流的地址（字段名、其它字符串值因不含 {@code ://} 自然被跳过）。
     *
     * @return 流地址原文；未找到返回 null
     */
    public static String extractStreamUrl(String json) {
        if (json == null) {
            return null;
        }
        int from = 0;
        while (true) {
            int open = json.indexOf('"', from);
            if (open < 0) {
                return null;
            }
            int close = json.indexOf('"', open + 1);
            if (close < 0) {
                return null; // 引号不成对，后面不可能再有完整值
            }
            String candidate = json.substring(open + 1, close).trim();
            if (candidate.contains("\\/")) {
                // 部分平台（如 PHP json_encode）会把 / 转义成 \/
                candidate = candidate.replace("\\/", "/");
            }
            StreamUrl parsed = parse(candidate);
            if (parsed != null && parsed.pullable()) {
                return candidate;
            }
            from = close + 1;
        }
    }

    /** 重建请求 URL（RTSP 请求行与 Host 用）：仅当原始地址显式带端口时才保留端口。 */
    public String requestUrl() {
        String target = host;
        if (portExplicit) {
            target = host + ":" + port;
        }
        String path = pathAndQuery.isEmpty() ? "/" : pathAndQuery;
        return scheme + "://" + target + path;
    }

    // ===== 字符合法性 =====

    private static boolean validScheme(String scheme) {
        if (scheme.isEmpty() || !Character.isLetter(scheme.charAt(0))) {
            return false;
        }
        for (int i = 1; i < scheme.length(); i++) {
            char c = scheme.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '+' && c != '-' && c != '.') {
                return false;
            }
        }
        return true;
    }

    /** host 允许字母、数字、{@code - . _ :}，以及 IPv6 的方括号；含空格等字符即判非法。 */
    private static boolean validHost(String host) {
        if (host.isEmpty()) {
            return false;
        }
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            boolean ok = Character.isLetterOrDigit(c) || c == '-' || c == '.' || c == '_'
                    || c == ':' || c == '[' || c == ']';
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    /** 端口：仅数字且落在 1..65535，非法返回 -1。 */
    private static int parsePort(String text) {
        if (text.isEmpty() || text.length() > 5) {
            return -1;
        }
        int v = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            v = v * 10 + (c - '0');
        }
        return v >= 1 && v <= 65535 ? v : -1;
    }

    /** 只解码 {@code %XX} 十六进制转义，其余字符原样保留（密码里的裸 {@code %} 不会被误处理）。 */
    private static String decodePercent(String s) {
        int first = s.indexOf('%');
        if (first < 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        sb.append(s, 0, first);
        int i = first;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() && hex(s.charAt(i + 1)) >= 0 && hex(s.charAt(i + 2)) >= 0) {
                sb.append((char) (hex(s.charAt(i + 1)) << 4 | hex(s.charAt(i + 2))));
                i += 3;
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    private static int hex(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }
}
