package com.mediagw.util;

/** 简单比特流读取器，用于解析 H.264/H.265 SPS 等 NALU RBSP。 */
public final class BitReader {

    private final byte[] data;
    private int bitPos;

    public BitReader(byte[] data) {
        this.data = data;
    }

    public int u(int n) {
        int v = 0;
        for (int i = 0; i < n; i++) {
            v = (v << 1) | bit();
        }
        return v;
    }

    public long uLong(int n) {
        long v = 0;
        for (int i = 0; i < n; i++) {
            v = (v << 1) | bit();
        }
        return v;
    }

    /** Exp-Golomb 无符号编码。 */
    public int ue() {
        int zeros = 0;
        while (zeros < 31 && bit() == 0) {
            zeros++;
        }
        if (zeros == 0) {
            return 0;
        }
        return ((1 << zeros) - 1) + u(zeros);
    }

    public int bit() {
        int byteIdx = bitPos >>> 3;
        int bitIdx = bitPos & 7;
        bitPos++;
        if (byteIdx >= data.length) {
            return 0;
        }
        return (data[byteIdx] >>> (7 - bitIdx)) & 1;
    }

    public void skip(int n) {
        bitPos += n;
    }

    public int bitsRead() {
        return bitPos;
    }

    /** 去除防竞争字节（00 00 03 -> 00 00）。 */
    public static byte[] stripEmulationPrevention(byte[] raw) {
        int zeros = 0;
        int count = 0;
        for (byte value : raw) {
            int b = value & 0xFF;
            if (zeros >= 2 && b == 0x03) {
                count++;
                zeros = 0;
            } else if (b == 0x00) {
                zeros++;
            } else {
                zeros = 0;
            }
        }
        if (count == 0) {
            return raw;
        }
        byte[] out = new byte[raw.length - count];
        zeros = 0;
        int j = 0;
        for (byte value : raw) {
            int b = value & 0xFF;
            if (zeros >= 2 && b == 0x03) {
                zeros = 0;
                continue;
            }
            out[j++] = value;
            if (b == 0x00) {
                zeros++;
            } else {
                zeros = 0;
            }
        }
        return out;
    }
}
