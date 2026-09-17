# netty-rtsp-flv

基于 Netty 的 RTSP 流媒体网关：拉取 RTSP 后零转码（仅重封装）分发为 **FLV over WebSocket** 与 **HTTP-FLV**，并可作为 RTSP 服务端把流**转发**给 VLC / ffplay。支持 H.264（标准 FLV）与 H.265（Enhanced FLV），多客户端共享同一路 RTSP 拉流，配合 Jessibuca 播放器实现浏览器低延迟播放。

## 特性

- **零转码重封装**：RTP 拆包 → NALU 重组 → FLV Tag 封装，CPU 开销极低
- **H.264 / H.265**：H.264 输出标准 FLV（avcC SequenceHeader）；H.265 输出 Enhanced FLV（FourCC `hvc1` + hvcC）
- **三种播放协议**：WS-FLV 与 HTTP-FLV 共用端口与路径、下发同一份 FLV 字节流，可混用；另有独立的 RTSP 转发端口
- **RTSP 拉流 over TCP**：interleaved 模式，无需 UDP 端口；支持 Basic / Digest 认证
- **RTSP 转发**：网关作为 RTSP 服务端，将已拉取的帧重新打包为 RTP（H.264 Single/FU-A、H.265 Single/FU），支持 TCP interleaved 与 UDP，关键帧前带内补发 VPS/SPS/PPS
- **一路拉流多路分发**：同一 streamId 的多个客户端共享一个 RTSP 会话（单次启动保证）
- **新客户端秒开**：连接后立即下发 FLV Header + 最新参数集 + 最近 GOP 缓存（RTSP 转发侧下发 RTP GOP 快照）
- **延迟关闭**：最后一个订阅者断开后等待 30s（可配）再关闭 RTSP，期间有人重连则取消关闭
- **慢消费者隔离**：写缓冲不可写时丢弃非关键帧，连续 3 个关键帧写不进则踢除该客户端，不影响其他人
- **断线重连**：指数退避（1s→30s），重连后时间轴连续（不回跳）
- **参数集动态更新**：SPS/PPS/VPS 变化时自动重新广播 SequenceHeader
- **跨域访问**：HTTP 接口与 WebSocket 握手均支持跨域（含 OPTIONS 预检），来源白名单可配
- **容量限制**：限制同时拉流路数与临时流数量，临时流超限按最久未使用（LRU）自动淘汰
- **监控**：`/stats` 输出每路流的帧率、字节数、订阅者（FLV / RTSP 分列）、丢帧、踢除、重连等指标

## 构建

要求 JDK 17+、Maven 3.6+：

```bash
mvn package        # 产物 target/netty-rtsp-flv.jar（fat jar）
```

## 运行

```bash
java -jar target/netty-rtsp-flv.jar [config.properties]
```

默认读取当前目录 `config.properties`。
