package com.mediagw.session;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * 最近 GOP 缓存：保存从最近一个关键帧起的所有 FLV Tag（含 PreviousTagSize），
 * 用于新订阅者首屏加速。超过容量上限时停止缓存当前 GOP（等待下一个关键帧重新开始）。
 * 非线程安全：调用方（StreamSession）必须持有会话锁。
 */
final class GopCache {

    private final long maxBytes;
    private final List<ByteBuf> tags = new ArrayList<>();
    private long totalBytes;
    private boolean overflowed;

    GopCache(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /** 追加一个 FLV Tag；关键帧开启新 GOP（丢弃旧 GOP）。 */
    void add(ByteBuf flvTag, boolean keyFrame) {
        if (keyFrame) {
            clear();
            overflowed = false;
        }
        if (overflowed) {
            return;
        }
        int size = flvTag.readableBytes();
        if (totalBytes + size > maxBytes) {
            overflowed = true;
            return;
        }
        tags.add(flvTag.retainedDuplicate());
        totalBytes += size;
    }

    /** 输出所有缓存 Tag 的 retainedDuplicate，所有权移交调用方。 */
    void snapshotTo(List<ByteBuf> out) {
        for (ByteBuf t : tags) {
            out.add(t.retainedDuplicate());
        }
    }

    long bytes() {
        return totalBytes;
    }

    int tagCount() {
        return tags.size();
    }

    void clear() {
        for (ByteBuf t : tags) {
            t.release();
        }
        tags.clear();
        totalBytes = 0;
    }
}
