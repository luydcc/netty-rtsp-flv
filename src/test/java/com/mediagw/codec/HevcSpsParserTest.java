package com.mediagw.codec;

import com.mediagw.testutil.TestMedia;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HevcSpsParserTest {

    @Test
    void parsesSyntheticSps() {
        HevcSpsParser.Info info = HevcSpsParser.parse(TestMedia.hevcSps());
        assertEquals(0, info.spsId);
        assertEquals(0, info.profileSpace);
        assertEquals(0, info.tierFlag);
        assertEquals(1, info.profileIdc);
        assertEquals(0x60000000L, info.profileCompatFlags);
        assertArrayEquals(new byte[]{(byte) 0x90, 0, 0, 0, 0, 0}, info.constraintFlags);
        assertEquals(120, info.levelIdc);
        assertEquals(1, info.chromaFormatIdc);
        assertEquals(0, info.bitDepthLumaMinus8);
        assertEquals(0, info.bitDepthChromaMinus8);
        assertEquals(0, info.maxSubLayersMinus1);
        assertTrue(info.temporalIdNesting);
        assertEquals(1, info.numTemporalLayers());
    }

    @Test
    void rejectsTooShortNalu() {
        assertThrows(IllegalArgumentException.class, () -> HevcSpsParser.parse(new byte[]{0x42, 0x01}));
        assertThrows(IllegalArgumentException.class, () -> HevcSpsParser.parse(null));
    }
}
