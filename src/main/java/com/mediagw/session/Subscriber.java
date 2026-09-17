package com.mediagw.session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * 单个 FLV 订阅者（WebSocket 或 HTTP-FLV）。
 * BOOTSTRAPPING：正在发送首屏缓存（FLV Header + 配置帧 + GOP），期间实时帧进入 pending 队列；
 * ACTIVE：直接接收实时广播；REMOVED：已移除。
 * 除 channel()/id() 外，所有方法必须持有 StreamSession 锁调用。
 */
final class Subscriber {

    enum Phase { BOOTSTRAPPING, ACTIVE, REMOVED }

    private final Channel channel;
    private final FlvTransport transport;
    private final long maxPendingBytes;
    private final ArrayDeque<ByteBuf> pending = new ArrayDeque<>();
    private final long subscribedAtMs = System.currentTimeMillis();

    private Phase phase = Phase.BOOTSTRAPPING;
    private long pendingBytes;
    private volatile long bootstrappedAtMs = -1;

    int notWritableKeyStreak;
    long droppedFrames;

    Subscriber(Channel channel, long maxPendingBytes, FlvTransport transport) {
        this.channel = channel;
        this.maxPendingBytes = maxPendingBytes;
        this.transport = transport;
    }

    Channel channel() {
        return channel;
    }

    FlvTransport transport() {
        return transport;
    }

    String id() {
        return channel.id().asShortText();
    }

    /** 按传输方式封装并写出一段 FLV 数据（不 flush），数据所有权移交 pipeline。 */
    void write(ByteBuf data) {
        channel.write(transport.frame(data));
    }

    /** 按传输方式封装并写出 + flush 一段 FLV 数据，数据所有权移交 pipeline。 */
    void writeAndFlush(ByteBuf data) {
        channel.writeAndFlush(transport.frame(data));
    }

    boolean isActivePhase() {
        return phase == Phase.ACTIVE;
    }

    /**
     * BOOTSTRAPPING 期间入队实时帧，入队数据所有权（已 retain）移交本对象。
     *
     * @return false 表示超出积压上限，已释放队列并关闭 Channel
     */
    boolean queue(ByteBuf retained) {
        if (phase != Phase.BOOTSTRAPPING) {
            retained.release();
            return phase == Phase.ACTIVE;
        }
        pending.addLast(retained);
        pendingBytes += retained.readableBytes();
        if (pendingBytes > maxPendingBytes) {
            releasePending();
            channel.close();
            return false;
        }
        return true;
    }

    /** 切换到 ACTIVE 并取出 pending 队列（所有权移交调用方）。 */
    List<ByteBuf> activateAndDrain() {
        phase = Phase.ACTIVE;
        List<ByteBuf> out = new ArrayList<>(pending);
        pending.clear();
        pendingBytes = 0;
        return out;
    }

    void markBootstrapped() {
        bootstrappedAtMs = System.currentTimeMillis();
    }

    long subscribedAtMs() {
        return subscribedAtMs;
    }

    long bootstrappedAtMs() {
        return bootstrappedAtMs;
    }

    /** 幂等移除；首次调用清理 pending 队列。@return 仅首次返回 true */
    boolean markRemoved() {
        if (phase == Phase.REMOVED) {
            return false;
        }
        phase = Phase.REMOVED;
        releasePending();
        return true;
    }

    private void releasePending() {
        for (ByteBuf b : pending) {
            b.release();
        }
        pending.clear();
        pendingBytes = 0;
    }
}
