package com.mediagw.flv;

import io.netty.buffer.ByteBuf;

/**
 * 上游裸流响应体解析器：把连续字节流按协议切块后回调上层，内部自行累积不完整数据。
 * 两种实现按响应体前缀自动选择：
 * <ul>
 *   <li>JT/T 1078 视频帧流（{@code Jt1078FrameParser}，需重组 H.264 后再封装 FLV）；</li>
 *   <li>标准 FLV 裸流（{@link FlvPassthroughParser}，零转码按 Tag 透传）。</li>
 * </ul>
 * 非线程安全，须在单一收流线程内使用。
 */
public interface StreamBodyParser {

    /** 追加一段流数据，切出的完整块立即回调上层。入参为拷贝读取，所有权仍归调用方。 */
    void feed(ByteBuf in);

    /** 结束解析并释放内部缓冲。 */
    void close();

    /** 解析已不可恢复（如 Tag 边界错乱），调用方应断流交由上层重连。 */
    default boolean isBroken() {
        return false;
    }

    /** 已解析输出的视频帧数（FLV 透传按视频 Tag 计）。 */
    long frameCount();

    /** 累计喂入的字节数。 */
    long byteCount();
}
