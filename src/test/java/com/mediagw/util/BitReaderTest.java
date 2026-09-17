package com.mediagw.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BitReaderTest {

    @Test
    void stripEmulationPreventionRemoves03() {
        assertArrayEquals(new byte[]{0, 0, 0}, BitReader.stripEmulationPrevention(new byte[]{0, 0, 3, 0}));
        assertArrayEquals(new byte[]{0, 0, 3, 1}, BitReader.stripEmulationPrevention(new byte[]{0, 0, 3, 3, 1}));
        assertArrayEquals(new byte[]{1, 2, 0, 0, 4}, BitReader.stripEmulationPrevention(new byte[]{1, 2, 0, 0, 3, 4}));
    }

    @Test
    void readsFixedWidthFields() {
        // bits: 0110 0110 1001 1100 ...
        BitReader r = new BitReader(new byte[]{0x66, (byte) 0x9C});
        assertEquals(0b01, r.u(2));
        assertEquals(0b1001, r.u(4));
        assertEquals(0b10100, r.u(5));
        assertEquals(0b11100, r.u(5));
    }

    @Test
    void readsUlong32() {
        BitReader r = new BitReader(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
        assertEquals(0xDEADBEEFL, r.uLong(32));
    }

    @Test
    void readsExpGolomb() {
        // 0->"1" 1->"010" 2->"011" 3->"00100"，拼接 = 1010 0110 0100 0000
        BitReader r = new BitReader(new byte[]{(byte) 0xA6, 0x40});
        assertEquals(0, r.ue());
        assertEquals(1, r.ue());
        assertEquals(2, r.ue());
        assertEquals(3, r.ue());
    }

    @Test
    void skipAdvancesPosition() {
        BitReader r = new BitReader(new byte[]{(byte) 0xFF, 0x00});
        r.skip(8);
        assertEquals(0, r.u(8));
    }
}
