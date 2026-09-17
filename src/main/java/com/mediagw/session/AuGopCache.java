package com.mediagw.session;

import com.mediagw.codec.AccessUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * 最近 GOP 的访问单元缓存：保存从最近一个关键帧起的所有帧（仅持有引用，不复制 NALU），
 * 用于 RTSP 转发等非 FLV 输出协议的首屏加速。超过容量上限时停止缓存当前 GOP。
 * 非线程安全：调用方（StreamSession）必须持有会话锁。
 */
final class AuGopCache {

    private final long maxBytes;
    private final List<AccessUnit> units = new ArrayList<>();
    private long totalBytes;
    private boolean overflowed;

    AuGopCache(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    /** 追加一个访问单元；关键帧开启新 GOP（丢弃旧 GOP）。 */
    void add(AccessUnit au) {
        if (au.keyFrame()) {
            units.clear();
            totalBytes = 0;
            overflowed = false;
        }
        if (overflowed) {
            return;
        }
        int size = sizeOf(au);
        if (totalBytes + size > maxBytes) {
            overflowed = true;
            return;
        }
        units.add(au);
        totalBytes += size;
    }

    /** 输出缓存的全部访问单元（引用，不复制）。 */
    void snapshotTo(List<AccessUnit> out) {
        out.addAll(units);
    }

    long bytes() {
        return totalBytes;
    }

    int size() {
        return units.size();
    }

    void clear() {
        units.clear();
        totalBytes = 0;
        overflowed = false;
    }

    private static int sizeOf(AccessUnit au) {
        int size = 0;
        for (byte[] nalu : au.nalus()) {
            size += nalu.length;
        }
        return size;
    }
}
