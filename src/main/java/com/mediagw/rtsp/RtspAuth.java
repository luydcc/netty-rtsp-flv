package com.mediagw.rtsp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

/** RTSP 认证：支持 Basic 与 Digest（MD5，qop=auth 或无 qop）。 */
public final class RtspAuth {

    public enum Scheme { NONE, BASIC, DIGEST }

    private final Scheme scheme;
    private final String username;
    private final String password;
    private final String realm;
    private final String nonce;
    private final String opaque;
    private final String qop;
    private int nc;

    private RtspAuth(Scheme scheme, String username, String password,
                     String realm, String nonce, String opaque, String qop) {
        this.scheme = scheme;
        this.username = username;
        this.password = password;
        this.realm = realm;
        this.nonce = nonce;
        this.opaque = opaque;
        this.qop = qop;
    }

    public static RtspAuth none() {
        return new RtspAuth(Scheme.NONE, null, null, null, null, null, null);
    }

    /**
     * 依据 401 响应的 WWW-Authenticate 头与用户凭据构建认证器。
     *
     * @param challenge WWW-Authenticate 头值；为 null 时使用 Basic（若有凭据）
     */
    public static RtspAuth fromChallenge(String challenge, String username, String password) {
        if (username == null || username.isEmpty()) {
            return none();
        }
        if (challenge == null) {
            return new RtspAuth(Scheme.BASIC, username, password, null, null, null, null);
        }
        String lower = challenge.toLowerCase(Locale.ROOT);
        if (lower.startsWith("digest")) {
            String realm = param(challenge, "realm");
            String nonce = param(challenge, "nonce");
            String opaque = param(challenge, "opaque");
            String qop = param(challenge, "qop");
            if (qop != null) {
                // qop 可能是 "auth,auth-int"，优先取 auth
                for (String token : qop.split(",")) {
                    if (token.trim().equalsIgnoreCase("auth")) {
                        qop = "auth";
                        break;
                    }
                }
            }
            return new RtspAuth(Scheme.DIGEST, username, password, realm, nonce, opaque, qop);
        }
        if (lower.startsWith("basic")) {
            return new RtspAuth(Scheme.BASIC, username, password, null, null, null, null);
        }
        return none();
    }

    public boolean hasCredentials() {
        return username != null && !username.isEmpty();
    }

    public Scheme scheme() {
        return scheme;
    }

    /** 生成 Authorization 头值。 */
    public String authorizationHeader(String method, String uri) {
        switch (scheme) {
            case BASIC:
                return "Basic " + base64(username + ":" + password);
            case DIGEST:
                return digest(method, uri);
            default:
                return null;
        }
    }

    private String digest(String method, String uri) {
        String ha1 = md5Hex(username + ":" + realm + ":" + password);
        String ha2 = md5Hex(method + ":" + uri);
        String response;
        StringBuilder sb = new StringBuilder("Digest ");
        sb.append("username=\"").append(username).append("\", ");
        sb.append("realm=\"").append(realm).append("\", ");
        sb.append("nonce=\"").append(nonce).append("\", ");
        sb.append("uri=\"").append(uri).append("\", ");
        if (qop != null && !qop.isEmpty()) {
            nc++;
            String ncStr = String.format("%08x", nc);
            String cnonce = randomCnonce();
            response = md5Hex(ha1 + ":" + nonce + ":" + ncStr + ":" + cnonce + ":" + qop + ":" + ha2);
            sb.append("qop=").append(qop).append(", ");
            sb.append("nc=").append(ncStr).append(", ");
            sb.append("cnonce=\"").append(cnonce).append("\", ");
        } else {
            response = md5Hex(ha1 + ":" + nonce + ":" + ha2);
        }
        sb.append("response=\"").append(response).append("\"");
        if (opaque != null) {
            sb.append(", opaque=\"").append(opaque).append("\"");
        }
        return sb.toString();
    }

    private static String randomCnonce() {
        byte[] b = new byte[8];
        ThreadLocalRandom.current().nextBytes(b);
        return hex(b);
    }

    /** 从形如 {@code key="value"} 或 {@code key=value} 的挑战串中提取参数值。 */
    static String param(String challenge, String key) {
        String lower = challenge.toLowerCase(Locale.ROOT);
        int idx = lower.indexOf(key.toLowerCase(Locale.ROOT) + "=");
        if (idx < 0) {
            return null;
        }
        int valStart = idx + key.length() + 1;
        if (valStart >= challenge.length()) {
            return null;
        }
        if (challenge.charAt(valStart) == '"') {
            int end = challenge.indexOf('"', valStart + 1);
            if (end < 0) {
                return null;
            }
            return challenge.substring(valStart + 1, end);
        }
        int end = challenge.indexOf(',', valStart);
        if (end < 0) {
            end = challenge.length();
        }
        return challenge.substring(valStart, end).trim();
    }

    private static String base64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String md5Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return hex(md.digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 unavailable", e);
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16));
            sb.append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }
}
