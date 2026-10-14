package com.mediagw;

import com.mediagw.config.GatewayConfig;
import com.mediagw.gb28181.Gb28181Server;
import com.mediagw.gb28181.Gb28181Service;
import com.mediagw.rtsp.RtspForwardServer;
import com.mediagw.session.StreamSessionManager;
import com.mediagw.ws.FlvWsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 基于 Netty 的 RTSP 流媒体网关入口：拉取 RTSP 后对外提供
 * FLV over WebSocket、HTTP-FLV 与 RTSP 转发三种播放方式。
 * 用法：java -jar netty-rtsp-flv.jar [config.properties]
 */
public final class MediaGatewayApp {
    private static final Logger log = LoggerFactory.getLogger(MediaGatewayApp.class);
    public static void main(String[] args) throws Exception {
        String cfgPath = args.length > 0 ? args[0] : "config.properties";
        GatewayConfig config = GatewayConfig.load(cfgPath);

        StreamSessionManager manager = new StreamSessionManager(config);
        // GB28181 业务门面（内存通道存储/点播/收流），启用时接入 WS 服务的 HTTP 层
        Gb28181Service gbService = config.gb28181Port() > 0 ? new Gb28181Service(config) : null;
        FlvWsServer server = new FlvWsServer(config, manager, gbService);
        server.start();

        RtspForwardServer forwardServer = new RtspForwardServer(config, manager);
        if(config.rtspForwardPort()>0) {
            forwardServer.start();
        }

        Gb28181Server gbServer = new Gb28181Server(config, gbService);
        if(config.gb28181Port()>0) {
            gbServer.start();
        }

        String token = config.authToken().isEmpty() ? "" : "?token=***";
        log.info("==== netty-rtsp-flv gateway started ====");
        log.info("WebSocket : ws://127.0.0.1:{}/live/{{streamId}}.flv{}", config.port(), token);
        log.info("HTTP-FLV  : http://127.0.0.1:{}/live/{{streamId}}.flv{}", config.port(), token);
        if (forwardServer.isRunning()) {
            log.info("RTSP out  : rtsp://127.0.0.1:{}/live/{{streamId}}{}", config.rtspForwardPort(), token);
        }
        if (gbServer.isRunning()) {
            log.info("GB28181   : sip://0.0.0.0:{} (udp+tcp, serverId={}, 注册/心跳/目录 -> 内存通道存储)",
                    config.gb28181Port(), config.gb28181ServerId());
            log.info("GB Channels: http://127.0.0.1:{}/gb_channels.html (点播 FLV: /gb/live/{{channelId}}.flv{})",
                    config.port(), token);
        }
        log.info("Test page : http://127.0.0.1:{}/", config.port());
        log.info("Dashboard : http://127.0.0.1:{}/streams.html", config.port());
        log.info("Stream API: http://127.0.0.1:{}/api/streams (GET|POST, POST添加的临时流仅存内存，重启消失)",
                config.port());
        log.info("Stats     : http://127.0.0.1:{}/stats", config.port());
        log.info("Idle stop : 无客户端观看 {}s 后自动停止拉流", config.closeDelaySec());
        if (config.streams().isEmpty()) {
            log.warn("no streams configured! add 'stream.<id>=<rtsp url>' to {} or POST /api/streams at runtime",
                    cfgPath);
        }else{
            log.info("Total {} streams configured", config.streams().size());
        }
        for (Map.Entry<String, String> e : config.streams().entrySet()) {
            log.info("  stream '{}' -> {}", e.getKey(), GatewayConfig.sanitizeUrl(e.getValue()));
        }
        // 所有工作线程均为 daemon，main 须驻留至 JVM 收到退出信号
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down gateway...");
            gbServer.stop();
            if (gbService != null) {
                gbService.shutdown();
            }
            forwardServer.stop();
            server.stop();
            manager.shutdown();
            log.info("gateway stopped");
            shutdownLatch.countDown();
        }, "gateway-shutdown"));
        shutdownLatch.await();
    }
}
