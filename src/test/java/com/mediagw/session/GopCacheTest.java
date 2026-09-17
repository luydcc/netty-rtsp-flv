package com.mediagw.session;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GopCacheTest {

    /** 填充指定字节、指定长度的 Tag。 */
    private static ByteBuf tag(int id, int size) {
        byte[] data = new byte[size];
        java.util.Arrays.fill(data, (byte) id);
        return Unpooled.wrappedBuffer(data);
    }

    @Test
    void keyframeStartsNewGop() {
        GopCache cache = new GopCache(10_000);
        ByteBuf t1 = tag(1, 100);
        ByteBuf t2 = tag(2, 100);
        cache.add(t1, true);
        cache.add(t2, false);
        assertEquals(2, cache.tagCount());
        assertEquals(200, cache.bytes());

        ByteBuf key = tag(4, 100);
        cache.add(key, true); // 新关键帧 → 旧 GOP 丢弃
        assertEquals(1, cache.tagCount());
        assertEquals(100, cache.bytes());

        List<ByteBuf> snap = new ArrayList<>();
        cache.snapshotTo(snap);
        assertEquals(1, snap.size());
        assertEquals(100, snap.get(0).readableBytes());
        assertEquals(4, snap.get(0).getByte(0));

        for (ByteBuf b : snap) {
            b.release();
        }
        cache.clear();
        t1.release();
        t2.release();
        key.release();
        assertEquals(0, cache.tagCount());
    }

    @Test
    void storedTagsSurviveCallerRelease() {
        GopCache cache = new GopCache(10_000);
        ByteBuf t = tag(7, 50);
        cache.add(t, true); // 内部 retainedDuplicate → refCnt=2
        t.release();        // 调用方释放自己的引用 → 缓存仍持有 1 个引用
        assertEquals(1, t.refCnt());

        List<ByteBuf> snap = new ArrayList<>();
        cache.snapshotTo(snap);
        assertEquals(50, snap.get(0).readableBytes());
        assertEquals(7, snap.get(0).getByte(0));
        for (ByteBuf b : snap) {
            b.release();
        }
        cache.clear();
    }

    @Test
    void overflowStopsCachingUntilNextKeyframe() {
        GopCache cache = new GopCache(100);
        ByteBuf k1 = tag(1, 60);
        cache.add(k1, true);
        assertEquals(1, cache.tagCount());

        ByteBuf big = tag(2, 50);
        cache.add(big, false); // 60+50 > 100 → 溢出，不缓存
        assertEquals(1, cache.tagCount());

        ByteBuf small = tag(3, 10);
        cache.add(small, false); // 溢出状态持续，跳过
        assertEquals(1, cache.tagCount());

        ByteBuf k2 = tag(4, 20);
        cache.add(k2, true); // 新关键帧恢复缓存
        assertEquals(1, cache.tagCount());
        assertEquals(20, cache.bytes());

        List<ByteBuf> snap = new ArrayList<>();
        cache.snapshotTo(snap);
        assertEquals(4, snap.get(0).getByte(0));
        for (ByteBuf b : snap) {
            b.release();
        }
        cache.clear();
        k1.release();
        big.release();
        small.release();
        k2.release();
        assertEquals(0, cache.bytes());
    }
}
