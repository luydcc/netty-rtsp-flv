package com.mediagw.e2e;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * HTTP-FLV 端到端校验器（联调用，非单元测试）。
 * <pre>
 * 用法:
 *   flv   模式: HttpFlvVerifier http://host:port/live/id.flv [秒数，默认6]
 *   status模式: HttpFlvVerifier &lt;url&gt; status &lt;期望状态码&gt; [方法，默认GET]
 * 退出码 0=PASS 1=FAIL 2=用法错误
 * </pre>
 * flv 模式校验：响应 200 + Content-Type video/x-flv，随后复用 {@link WsFlvVerifier#verifyFlv}
 * 校验字节流结构（两种播放协议下发同一份 FLV，验收标准一致）。
 */
public final class HttpFlvVerifier {

    private static final int POLL_MS = 50;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: HttpFlvVerifier <http-url> [seconds | status <code> [method]]");
            System.exit(2);
        }
        String url = args[0];
        if (args.length >= 2 && "status".equals(args[1])) {
            int expect = Integer.parseInt(args[2]);
            String method = args.length >= 4 ? args[3] : "GET";
            System.exit(status(url, expect, method));
        }
        int seconds = args.length >= 2 ? Integer.parseInt(args[1]) : 6;
        System.exit(pull(url, seconds));
    }

    /** flv 模式：拉取指定秒数后校验 FLV 结构。 */
    private static int pull(String url, int seconds) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET().build();
        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());

        out("status=" + resp.statusCode() + " contentType=" + resp.headers().firstValue("Content-Type").orElse("-")
                + " transferEncoding=" + resp.headers().firstValue("Transfer-Encoding").orElse("-"));
        if (resp.statusCode() != 200) {
            try (InputStream in = resp.body()) {
                out("body=" + new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
            }
            return fail("expected 200, got " + resp.statusCode());
        }
        if (!"video/x-flv".equalsIgnoreCase(resp.headers().firstValue("Content-Type").orElse(""))) {
            return fail("bad Content-Type: " + resp.headers().firstValue("Content-Type").orElse("-"));
        }

        ByteArrayOutputStream data = new ByteArrayOutputStream();
        byte[] buf = new byte[64 * 1024];
        try (InputStream in = resp.body()) {
            long deadline = System.currentTimeMillis() + seconds * 1000L;
            while (System.currentTimeMillis() < deadline) {
                if (in.available() <= 0) {
                    Thread.sleep(POLL_MS);
                    continue;
                }
                int n = in.read(buf);
                if (n < 0) {
                    return fail("server closed stream after " + data.size() + " bytes");
                }
                data.write(buf, 0, n);
            }
        }
        byte[] all = data.toByteArray();
        out("bytes=" + all.length);
        return WsFlvVerifier.verifyFlv(all) ? pass("http-flv stream") : 1;
    }

    /** status 模式：只校验状态码（405/400/403/401/404/503 等错误分支）。 */
    private static int status(String url, int expect, String method) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .method(method, HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(10))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());

        out(method + " " + url + " -> " + resp.statusCode() + " \"" + resp.body() + "\"");
        return resp.statusCode() == expect
                ? pass(method + " -> " + expect)
                : fail("expected " + expect + ", got " + resp.statusCode());
    }

    private static void out(String msg) {
        System.out.println(msg);
        System.out.flush();
    }

    private static int pass(String what) {
        out("PASS " + what);
        return 0;
    }

    private static int fail(String msg) {
        out("FAIL: " + msg);
        return 1;
    }

    private HttpFlvVerifier() {
    }
}
