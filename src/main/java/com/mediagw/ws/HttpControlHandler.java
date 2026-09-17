package com.mediagw.ws;

import com.mediagw.config.GatewayConfig;
import com.mediagw.session.StreamSessionManager;
import com.mediagw.util.JsonBuilder;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP 控制通道：{@code /stats} 与 {@code /api/streams} 监控 JSON、
 * {@code /api/streams} POST 临时添加流、{@code /} 测试页与 {@code /streams.html} 看板；
 * {@code /live*} 的播放请求透传给下游 HttpFlvHandler / WebSocketServerProtocolHandler。
 */
public final class HttpControlHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(HttpControlHandler.class);

    /** 静态资源白名单：URL 路径 -> (classpath 资源名, Content-Type)。精确匹配，防路径穿越。 */
    private static final Map<String, String[]> STATIC_FILES = Map.of(
            "/jessibuca.js", new String[]{"/web/jessibuca.js", "application/javascript; charset=utf-8"},
            "/decoder.js", new String[]{"/web/decoder.js", "application/javascript; charset=utf-8"},
            "/decoder.wasm", new String[]{"/web/decoder.wasm", "application/wasm"},
            "/index.html", new String[]{"/web/index.html", "text/html; charset=utf-8"},
            "/streams.html", new String[]{"/web/streams.html", "text/html; charset=utf-8"}
    );

    private final StreamSessionManager manager;
    private final GatewayConfig config;
    private final Map<String, byte[]> resourceCache = new ConcurrentHashMap<>();

    public HttpControlHandler(StreamSessionManager manager, GatewayConfig config) {
        this.manager = manager;
        this.config = config;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ctx.fireChannelRead(msg);
            return;
        }
        String path = new QueryStringDecoder(req.uri()).path();
        if (path.startsWith("/live")) {
            ctx.fireChannelRead(req); // 交给 HTTP-FLV / WebSocket 处理，勿释放
            return;
        }
        try {
            switch (path) {
                case "/stats" -> sendJson(ctx, req, HttpResponseStatus.OK, manager.statsJson());
                case "/api/streams" -> handleStreamsApi(ctx, req);
                case "/" -> sendStatic(ctx, req, "/index.html");
                default -> {
                    if (STATIC_FILES.containsKey(path)) {
                        sendStatic(ctx, req, path);
                    } else {
                        sendText(ctx, req, HttpResponseStatus.NOT_FOUND);
                    }
                }
            }
        } finally {
            req.release();
        }
    }

    // ===== /api/streams =====

    private void handleStreamsApi(ChannelHandlerContext ctx, FullHttpRequest req) {
        if (!authorized(req)) {
            log.warn("unauthorized /api/streams {} from {}", req.method(), ctx.channel().remoteAddress());
            sendJson(ctx, req, HttpResponseStatus.UNAUTHORIZED,
                    "{\"ok\":false,\"error\":\"token 校验失败\"}");
            return;
        }
        if (HttpMethod.GET.equals(req.method())) {
            sendJson(ctx, req, HttpResponseStatus.OK, manager.streamsJson());
        } else if (HttpMethod.POST.equals(req.method())) {
            addStream(ctx, req);
        } else {
            sendText(ctx, req, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    private void addStream(ChannelHandlerContext ctx, FullHttpRequest req) {
        String body = req.content().toString(StandardCharsets.UTF_8);
        String id;
        String url;
        try {
            id = firstField(body, "streamId", "id");
            url = firstField(body, "url", "rtspUrl");
        } catch (RuntimeException e) {
            sendJson(ctx, req, HttpResponseStatus.BAD_REQUEST,
                    "{\"ok\":false,\"error\":\"请求体不是合法 JSON\"}");
            return;
        }
        if (id == null || url == null) {
            sendJson(ctx, req, HttpResponseStatus.BAD_REQUEST,
                    "{\"ok\":false,\"error\":\"请求体需为 JSON 且包含 streamId 与 url 字段\"}");
            return;
        }
        StreamSessionManager.AddResult result = manager.addDynamicStream(id, url);
        HttpResponseStatus status = result.ok() ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST;
        String json = "{\"ok\":" + result.ok()
                + ",\"streamId\":" + JsonBuilder.str(result.streamId())
                + ",\"message\":" + JsonBuilder.str(result.message()) + "}";
        sendJson(ctx, req, status, json);
    }

    /** 配置了 auth.token 时，控制接口与播放鉴权使用同一 token（查询参数 token）。 */
    private boolean authorized(FullHttpRequest req) {
        String expected = config.authToken();
        if (expected == null || expected.isEmpty()) {
            return true;
        }
        List<String> tokens = new QueryStringDecoder(req.uri()).parameters().get("token");
        return tokens != null && expected.equals(tokens.getFirst());
    }

    private static String firstField(String json, String... keys) {
        for (String key : keys) {
            String v = jsonField(json, key);
            if (v != null) {
                return v;
            }
        }
        return null;
    }

    /** 从扁平 JSON 对象中取出字符串字段值；字段不存在或不是字符串返回 null。 */
    static String jsonField(String json, String key) {
        if (json == null) {
            return null;
        }
        String needle = "\"" + key + "\"";
        for (int i = json.indexOf(needle); i >= 0; i = json.indexOf(needle, i + 1)) {
            int p = i + needle.length();
            while (p < json.length() && Character.isWhitespace(json.charAt(p))) {
                p++;
            }
            if (p >= json.length() || json.charAt(p) != ':') {
                continue;
            }
            do {
                p++;
            } while (p < json.length() && Character.isWhitespace(json.charAt(p)));
            return p < json.length() && json.charAt(p) == '"' ? readString(json, p) : null;
        }
        return null;
    }

    /** 从起始引号处解析 JSON 字符串字面量；未闭合返回 null。 */
    private static String readString(String json, int quote) {
        StringBuilder sb = new StringBuilder();
        for (int i = quote + 1; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') {
                return sb.toString();
            }
            if (c != '\\' || i + 1 >= json.length()) {
                sb.append(c);
                continue;
            }
            char esc = json.charAt(++i);
            switch (esc) {
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'u' -> {
                    if (i + 4 >= json.length()) {
                        throw new IllegalArgumentException("truncated \\u escape");
                    }
                    sb.append((char) Integer.parseInt(json.substring(i + 1, i + 5), 16));
                    i += 4;
                }
                default -> sb.append(esc);
            }
        }
        return null;
    }

    // ===== 响应输出 =====

    private void sendJson(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status, String json) {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=utf-8");
        resp.headers().set(HttpHeaderNames.CACHE_CONTROL, HttpHeaderValues.NO_STORE);
        finish(ctx, req, resp, body.length);
    }

    private void sendStatic(ChannelHandlerContext ctx, FullHttpRequest req, String path) {
        byte[] body = loadResource(path);
        if (body == null) {
            sendText(ctx, req, HttpResponseStatus.NOT_FOUND);
            return;
        }
        FullHttpResponse resp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(body));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, STATIC_FILES.get(path)[1]);
        finish(ctx, req, resp, body.length);
    }

    private void sendText(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status) {
        byte[] body = (status.code() + " " + status.reasonPhrase()).getBytes(StandardCharsets.UTF_8);
        FullHttpResponse resp = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
        finish(ctx, req, resp, body.length);
    }

    private void finish(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse resp, int len) {
        resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, len);
        boolean keepAlive = HttpUtil.isKeepAlive(req);
        if (keepAlive) {
            resp.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(resp);
        } else {
            ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /** 按白名单从 classpath 加载静态资源并缓存；不存在返回 null。 */
    private byte[] loadResource(String path) {
        return resourceCache.computeIfAbsent(path, p -> {
            String[] meta = STATIC_FILES.get(p);
            if (meta == null) {
                return null;
            }
            try (InputStream in = getClass().getResourceAsStream(meta[0])) {
                if (in == null) {
                    log.warn("classpath resource {} not found", meta[0]);
                    return null;
                }
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) {
                    out.write(buf, 0, n);
                }
                return out.toByteArray();
            } catch (IOException e) {
                log.warn("failed to load resource {}", meta[0], e);
                return null;
            }
        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("HTTP control handler error", cause);
        ctx.close();
    }
}
