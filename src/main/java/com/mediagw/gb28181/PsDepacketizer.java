package com.mediagw.gb28181;

import com.mediagw.codec.AccessUnit;
import com.mediagw.codec.CodecType;
import com.mediagw.codec.ParamSets;
import com.mediagw.codec.VideoDepacketizer;
import com.mediagw.rtp.RtpPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GB28181 媒体流解复用：RTP 载荷为 MPEG-PS 复用流，本类将其拆为
 * H.264 / H.265 访问单元，输出接口与 RTSP 管线一致（{@link VideoDepacketizer.Listener}），
 * 可直接对接 {@link com.mediagw.flv.FlvMuxer}。
 *
 * <p>单遍前向扫描状态机：按 00 00 01 起始码识别包类型——pack(0xBA)/system(0xBB) 跳过，
 * PSM(0xBC) 依 ISO 布局解析 elementary_stream 表识别编码（0x1B=H.264，0x24=H.265），
 * 视频 PES(0xE0~0xEF) 依 packet_length 精确界定（0 时以下一起始码为界），取出 ES 按
 * Annex-B 分 NAL、剥离参数集后组成一个访问单元。因 Annex-B 有防竞争起始码，
 * PES 内部的 00 00 01 不会被误当作顶层包边界。
 *
 * <p>非线程安全，须在单一收流 EventLoop 线程内使用；缓冲超 {@value #MAX_BUFFER}
 * 字节仍无法推进视为垃圾流整体重置。
 */
public final class PsDepacketizer {

    /** 防垃圾流：PS 累积缓冲上限。 */
    private static final int MAX_BUFFER = 8 * 1024 * 1024;

    private final VideoDepacketizer.Listener listener;
    private final ByteBufAllocator alloc;
    private ByteBuf buf;

    private CodecType codec;
    private long lastPts = -1;

    /**
     * 当前访问单元累加器。真实设备常把一帧拆成多个 PES（每 RTP 分片一个），
     * 故一帧 = 若干 PES 的 NAL 并集；PS 的 pack(0xBA)/PSM(0xBC) 复现标志新 AU 开始，
     * 配合 RTP marker 与超时兜底在此处封口产出。
     */
    private final List<byte[]> curNalus = new ArrayList<>();
    private boolean curKey;
    private long curPts = -1;
    private long lastVideoAt;
    /** 单个 AU 缓冲 NAL 上限，超限视为缺边界垃圾流强吐，避免无界增长。 */
    private static final int MAX_AU_NALUS = 4096;
    /** 收流空闲该毫秒数视为一帧结束（marker/pack 均缺失时的兜底）。 */
    private static final int AU_IDLE_MS = 200;

    // 诊断计数（收流→出帧链路卡点定位用）
    private long rtpPackets;
    private long rtpBytes;
    private long auCount;

    // 参数集缓存（内容变化才回调 onParameterSets）
    private final List<byte[]> vps = new ArrayList<>();
    private final List<byte[]> sps = new ArrayList<>();
    private final List<byte[]> pps = new ArrayList<>();
    private String paramSignature = "";

    public PsDepacketizer(VideoDepacketizer.Listener listener, ByteBufAllocator alloc) {
        this.listener = listener;
        this.alloc = alloc;
        this.buf = alloc.buffer(64 * 1024);
    }

    public CodecType codec() {
        return codec;
    }

    /** 诊断用：已产出访问单元数 / 已收 RTP 包数与字节数 / 当前 PS 滞留缓冲。 */
    public long accessUnits() {
        return auCount;
    }

    public long rtpPacketCount() {
        return rtpPackets;
    }

    public long rtpByteCount() {
        return rtpBytes;
    }

    public int bufferedBytes() {
        return buf == null ? 0 : buf.readableBytes();
    }

    /** 输入一个 RTP 包（payload 为 PS 流分片）。 */
    public void onRtpPacket(RtpPacket p) {
        // 兜底：上一帧后收流空闲超阈值仍未封口（marker 不可靠且帧间无 pack 的设备），先结算
        if (System.currentTimeMillis() - lastVideoAt > AU_IDLE_MS) {
            flushAu();
        }
        rtpPackets++;
        rtpBytes += p.payload().readableBytes();
        buf.writeBytes(p.payload());
        try {
            drain();
        } catch (RuntimeException e) {
            listener.onError("PS parse error: " + e);
            compactReset();
        }
        // 一帧跨多个 RTP 包时，中间分片 marker=0；末包 marker=1 收尾当前 AU
        if (p.marker()) {
            flushAu();
        }
    }

    public void close() {
        if (buf != null) {
            buf.release();
            buf = null;
        }
    }

    // ===== 单遍扫描 =====

    private void drain() {
        while (true) {
            // 已消费区域惰性回收（本循环每轮重新取 readerIndex，压缩安全）
            if (buf.readerIndex() > 128 * 1024) {
                buf.discardSomeReadBytes();
            }
            int avail = buf.readableBytes();
            if (avail < 4) {
                return;
            }
            int base = buf.readerIndex();
            // 对齐到起始码：丢弃起始码之前的对齐/垃圾字节
            int sc = findStartCode(base, avail);
            if (sc < 0) {
                // 无起始码：丢弃全部但保留末尾 2 字节（可能是跨包的 00 00 前缀）
                buf.skipBytes(Math.max(0, avail - 2));
                guardOverflow();
                return;
            }
            if (sc > base) {
                buf.skipBytes(sc - base);
                base = buf.readerIndex();
                avail = buf.readableBytes();
            }
            int sid = buf.getUnsignedByte(base + 3);
            if (sid == 0xBA) {                 // pack header —— 新访问单元边界
                flushAu();
                if (avail < 14) {
                    return;
                }
                int total = 14 + (buf.getUnsignedByte(base + 13) & 0x07);
                if (avail < total) {
                    return;
                }
                buf.skipBytes(total);
            } else if (sid == 0xBB || sid == 0xBC) { // system header / PSM
                if (avail < 6) {
                    return;
                }
                int total = 6 + buf.getUnsignedShort(base + 4);
                if (avail < total) {
                    return;
                }
                if (sid == 0xBC) {
                    flushAu(); // PSM 复现亦视为新 AU 起点（部分设备每帧前置 PSM）
                    parsePsm(base, total);
                }
                buf.skipBytes(total);
            } else if (sid == 0xBE || sid == 0xBF) { // padding_stream / private_stream_2
                int need = sid == 0xBE ? 6 : 8;
                if (avail < need) {
                    return;
                }
                int len = sid == 0xBE ? buf.getUnsignedShort(base + 4) & 0x3FFF
                        : (int) (buf.getUnsignedInt(base + 4) & 0x7FFF);
                int total = need + len;
                if (avail < total) {
                    return;
                }
                buf.skipBytes(total);
            } else if (sid == 0xBD || (sid >= 0xC0 && sid <= 0xDF)          // 私有流/音频/视频 PES
                    || (sid >= 0xE0 && sid <= 0xEF)) {                       // 统一按 packet_length 消费，
                if (avail < 6) {                                             // 避免逐字节重同步误吞载荷内的起始码
                    return;
                }
                int pktLen = buf.getUnsignedShort(base + 4);
                int total = pktLen > 0 ? 6 + pktLen : nextBoundaryLength(base, avail);
                if (total < 0 || avail < total) {
                    guardOverflow();
                    return; // 需要更多数据
                }
                if (sid >= 0xE0) {
                    parseVideoPes(base, total);
                }
                buf.skipBytes(total);
            } else {
                buf.skipBytes(1); // 未知起始码，前进一字节重同步
            }
            guardOverflow();
        }
    }

    /**
     * packet_length=0 的 PES：设限扫到下一个<b>顶层</b>包起始码为止。
     * ES 内 Annex-B 的 00 00 01 也合法存在（后跟 NAL 头字节），不能作边界，
     * 只认 pack/PSM/PES 等流 ID；未找到返回 -1 等待更多数据。
     */
    private int nextBoundaryLength(int base, int avail) {
        int from = base + 6;
        int end = base + avail;
        while (from + 4 <= end) {
            int next = findStartCode(from, end - from);
            if (next < 0 || next + 3 >= end) {
                return -1;
            }
            if (isTopLevelSid(buf.getUnsignedByte(next + 3))) {
                return next - base;
            }
            from = next + 3; // ES 内部的 Annex-B 起始码，跳过继续找
        }
        return -1;
    }

    /** 顶层包流 ID：pack/system/PSM/私有/填充/音视频 PES。视频 ES 内的 NAL 头
     * 字节因 forbidden_zero_bit=0 恒小于 0x80，不会与 0xE0~0xEF 碰撞。 */
    private static boolean isTopLevelSid(int sid) {
        return sid == 0xBA || sid == 0xBB || sid == 0xBC || sid == 0xBD
                || sid == 0xBE || sid == 0xBF
                || (sid >= 0xC0 && sid <= 0xDF) || (sid >= 0xE0 && sid <= 0xEF);
    }

    /** 在 [from, from+len) 内查找 00 00 01，返回绝对下标，找不到 -1。 */
    private int findStartCode(int from, int len) {
        int end = from + len - 2;
        for (int i = from; i < end; i++) {
            if (buf.getByte(i) == 0 && buf.getByte(i + 1) == 0 && buf.getByte(i + 2) == 1) {
                return i;
            }
        }
        return -1;
    }

    private void guardOverflow() {
        if (buf.readableBytes() > MAX_BUFFER) {
            listener.onError("PS buffer overflow (" + MAX_BUFFER + " bytes), stream reset");
            compactReset();
        }
    }

    private void compactReset() {
        curNalus.clear();
        curKey = false;
        curPts = -1;
        buf.release();
        buf = alloc.buffer(64 * 1024);
    }

    /**
     * PSM：识别视频 ES 条目。各设备 PSM 字段偏移不尽一致，严格定长遍历不可靠
     * （PCR 首字节 0x20~0x2F 会冒充 H.265 的 stream_type 0x24 造成误标），
     * 改用签名扫描：stream_type(0x1B/0x24) 后紧跟视频流 ID(0xE0~0xEF)
     * 且条目长度高位字节合法，才认定编码。
     */
    private void parsePsm(int base, int total) {
        int end = base + total - 4;
        for (int q = base + 6; q < end; q++) {
            int streamType = buf.getUnsignedByte(q);
            if (streamType != 0x1B && streamType != 0x24) {
                continue;
            }
            int sid = buf.getUnsignedByte(q + 1);
            if (sid < 0xE0 || sid > 0xEF || (buf.getUnsignedByte(q + 2) & 0xF0) != 0) {
                continue; // 后随非视频流 ID，视为 PCR/描述符数据巧合
            }
            codec = streamType == 0x1B ? CodecType.H264 : CodecType.H265;
            return;
        }
    }

    /** 视频 PES：解析可选头取 pts，ES 数据交给 Annex-B 分帧（累加进当前 AU）。 */
    private void parseVideoPes(int start, int len) {
        int esStart = start + 6; // 跳过起始码(4) + packet_length(2)
        int esEnd = start + len;
        if (esEnd - esStart < 3) {
            return;
        }
        int flags1 = buf.getUnsignedByte(esStart);
        boolean hasPts = false;
        if ((flags1 & 0xC0) == 0x80) { // '10' 可选头标记
            int flags2 = buf.getUnsignedByte(esStart + 1);
            int hdrLen = buf.getUnsignedByte(esStart + 2);
            if ((flags2 & 0x80) != 0 && hdrLen >= 5 && esStart + 3 + 5 <= esEnd) {
                lastPts = readPts(esStart + 3);
                hasPts = true;
            }
            esStart += 3 + hdrLen;
        }
        if (esStart < esEnd) {
            handleEs(esStart, esEnd, hasPts ? lastPts : -1);
        }
    }

    /** 5 字节 MPEG 时间戳（33 位 pts，90kHz）。 */
    private long readPts(int p) {
        long b0 = buf.getUnsignedByte(p);
        long b1 = buf.getUnsignedByte(p + 1);
        long b2 = buf.getUnsignedByte(p + 2);
        long b3 = buf.getUnsignedByte(p + 3);
        long b4 = buf.getUnsignedByte(p + 4);
        return ((b0 >> 1) & 0x07) << 30
                | ((b1 << 8 | b2) >> 1) << 15
                | ((b3 << 8 | b4) >> 1);
    }

    // ===== Annex-B ES → 访问单元 =====

    private void handleEs(int from, int to, long pts) {
        boolean sawVideo = false;
        int p = from;
        while (p < to) {
            int sc = annexBStartCodeAt(p, to);
            if (sc < 0) {
                break;
            }
            int nalStart = sc + 3;
            if (nalStart < to && buf.getByte(nalStart) == 0) {
                nalStart++; // 00 00 00 01 形式
            }
            int next = annexBStartCodeAt(nalStart, to);
            int nalEnd = next < 0 ? to : next;
            if (nalEnd > nalStart) {
                byte[] nalu = new byte[nalEnd - nalStart];
                buf.getBytes(nalStart, nalu);
                int type = nalType(nalu);
                if (type >= 0) {
                    if (codec == null) {
                        codec = probeCodec(nalu);
                        type = nalType(nalu); // 探测可能改变类型解释
                    }
                    boolean h264 = codec == CodecType.H264;
                    if ((h264 && type == 7) || (!h264 && type == 33)) {
                        if (replace(sps, nalu)) {
                            emitParamSets();
                        }
                        sawVideo = true;
                    } else if ((h264 && type == 8) || (!h264 && type == 34)) {
                        if (replace(pps, nalu)) {
                            emitParamSets();
                        }
                        sawVideo = true;
                    } else if (!h264 && type == 32) {
                        if (replace(vps, nalu)) {
                            emitParamSets();
                        }
                        sawVideo = true;
                    } else if (isVcl(type)) {
                        curNalus.add(nalu);
                        curKey |= isKeyFrame(type);
                        sawVideo = true;
                    }
                    // AUD / SEI / 其余非 VCL NAL 不进帧数据
                }
            }
            p = nalEnd > p ? nalEnd : p + 1;
        }
        if (sawVideo) {
            if (curPts < 0 && pts >= 0) {
                curPts = pts; // 以本 AU 首个（关键帧）PES 的 pts 为准
            }
            lastVideoAt = System.currentTimeMillis();
            if (curNalus.size() >= MAX_AU_NALUS) {
                flushAu(); // 缺边界兜底，避免无界增长
            }
        }
    }

    /** 封口产出当前访问单元（若含 VCL NAL）。 */
    private void flushAu() {
        if (!curNalus.isEmpty()) {
            auCount++;
            long ts = curPts >= 0 ? curPts : (lastPts >= 0 ? lastPts : 0);
            listener.onAccessUnit(new AccessUnit(ts & 0xFFFFFFFFL, new ArrayList<>(curNalus), curKey));
        }
        curNalus.clear();
        curKey = false;
        curPts = -1;
    }

    private int annexBStartCodeAt(int i, int to) {
        for (; i + 2 < to; i++) {
            if (buf.getByte(i) == 0 && buf.getByte(i + 1) == 0 && buf.getByte(i + 2) == 1) {
                return i;
            }
        }
        return -1;
    }

    private int nalType(byte[] nalu) {
        if (nalu.length < 2) {
            return -1;
        }
        return codec == CodecType.H265 ? (nalu[0] >>> 1) & 0x3F : nalu[0] & 0x1F;
    }

    private boolean isVcl(int type) {
        return codec == CodecType.H265 ? type <= 31 : type >= 1 && type <= 5;
    }

    private boolean isKeyFrame(int type) {
        return codec == CodecType.H265 ? type >= 16 && type <= 21 : type == 5;
    }

    /** 编码未知时按 NAL 头探测：优先参数集/关键帧类型域（H264 5/7/8/9 vs H265 32~35）。 */
    private static CodecType probeCodec(byte[] nalu) {
        int b0 = nalu[0] & 0xFF;
        int t264 = b0 & 0x1F;
        int t265 = (b0 >>> 1) & 0x3F;
        if (t264 == 5 || t264 == 7 || t264 == 8 || t264 == 9) {
            return CodecType.H264;
        }
        if (t265 >= 32 && t265 <= 35) {
            return CodecType.H265;
        }
        return CodecType.H264; // 兜底按 H.264，后续 PSM 可纠正
    }

    /** 覆盖式替换单个参数集，内容不同返回 true。 */
    private static boolean replace(List<byte[]> list, byte[] nalu) {
        if (!list.isEmpty() && Arrays.equals(list.getFirst(), nalu)) {
            return false;
        }
        list.clear();
        list.add(nalu);
        return true;
    }

    private void emitParamSets() {
        ParamSets ps = new ParamSets(codec, vps, sps, pps);
        if (ps.signature().equals(paramSignature)) {
            return;
        }
        paramSignature = ps.signature();
        if (ps.isComplete()) {
            listener.onParameterSets(ps);
        }
    }
}
