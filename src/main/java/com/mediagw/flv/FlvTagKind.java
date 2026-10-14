package com.mediagw.flv;

/** FLV Tag 分类（透传解析用）：只读取 Tag 头部用于分类与首屏缓存，不改写载荷。 */
public enum FlvTagKind {

    /** 视频解码配置（AVC/HEVC sequence header）。 */
    VIDEO_CONFIG,
    /** 视频关键帧。 */
    VIDEO_KEYFRAME,
    /** 视频非关键帧。 */
    VIDEO_FRAME,
    /** AAC 解码配置（AudioSpecificConfig）。 */
    AUDIO_CONFIG,
    /** 音频数据。 */
    AUDIO_FRAME,
    /** 脚本/保留类型等无关 Tag。 */
    OTHER;

    public boolean video() {
        return this == VIDEO_CONFIG || this == VIDEO_KEYFRAME || this == VIDEO_FRAME;
    }

    public boolean audio() {
        return this == AUDIO_CONFIG || this == AUDIO_FRAME;
    }

    /** 解码配置 Tag：新订阅者首屏必须先于 GOP 下发。 */
    public boolean config() {
        return this == VIDEO_CONFIG || this == AUDIO_CONFIG;
    }
}
