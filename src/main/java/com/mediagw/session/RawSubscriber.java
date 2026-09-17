package com.mediagw.session;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;

import java.util.List;

/**
 * 原始帧订阅者：接收参数集与访问单元，用于 RTSP 转发等非 FLV 输出协议。
 * 与 FLV 订阅者共享同一路 RTSP 拉流与会话生命周期（首个订阅者触发拉流，
 * 最后一个订阅者离开后延迟关闭）。
 *
 * <p>线程约定：
 * <ul>
 *   <li>{@link #onGopSnapshot} 与 {@link #onAccessUnit} 在会话锁内回调，实现方不得阻塞，
 *       只应做投递（如提交到自身 Channel 的 EventLoop），以保证与后续实时帧的顺序；</li>
 *   <li>{@link #onStreamInfo} 与 {@link #onStreamClosed} 可能在任意线程回调，实现方自行切换线程。</li>
 * </ul>
 */
public interface RawSubscriber {

    /** 订阅者标识（日志用）。 */
    String id();

    /** 连接是否仍然存活；返回 false 时会话将其移除。 */
    boolean isActive();

    /** 参数集首次就绪或发生变化，可据此生成 SDP 与带内参数集。 */
    void onStreamInfo(CodecType codec, ParamSets params);

    /** 开始播放时的最近 GOP 快照（元素不可变，可直接引用）。 */
    void onGopSnapshot(List<AccessUnit> gop);

    /** 一个实时访问单元。 */
    void onAccessUnit(AccessUnit au);

    /** 会话终止：拉流启动失败、上游不可恢复或网关停机。 */
    void onStreamClosed();
}
