package com.mediagw.ws;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpControlHandlerTest {

    /** 反斜杠常量：避免源码里的 {@code \\u} 被 javac 当作 Unicode 转义提前解码。 */
    private static final char BS = '\\';

    @Test
    void extractsStringFields() {
        String json = "{\"streamId\":\"cam9\",\"url\":\"rtsp://10.0.0.5:554/live\"}";
        assertEquals("cam9", HttpControlHandler.jsonField(json, "streamId"));
        assertEquals("rtsp://10.0.0.5:554/live", HttpControlHandler.jsonField(json, "url"));
    }

    @Test
    void toleratesWhitespaceAndFieldOrder() {
        String json = "{\n  \"url\" : \"rtsp://h/p\" ,\n  \"streamId\" :  \"cam9\"\n}";
        assertEquals("rtsp://h/p", HttpControlHandler.jsonField(json, "url"));
        assertEquals("cam9", HttpControlHandler.jsonField(json, "streamId"));
    }

    @Test
    void unescapesJsonStringEscapes() {
        String json = "{\"u\":\"a" + BS + "\"b" + BS + BS + "c" + BS + "nd"
                + BS + "u0041e" + BS + "/f\"}";
        assertEquals("a\"b\\c\ndAe/f", HttpControlHandler.jsonField(json, "u"));
    }

    @Test
    void returnsNullForMissingOrNonStringFields() {
        String json = "{\"streamId\":\"cam9\",\"count\":3,\"live\":true,\"nothing\":null}";
        assertNull(HttpControlHandler.jsonField(json, "url"));
        assertNull(HttpControlHandler.jsonField(json, "count"));
        assertNull(HttpControlHandler.jsonField(json, "live"));
        assertNull(HttpControlHandler.jsonField(json, "nothing"));
        assertNull(HttpControlHandler.jsonField(null, "streamId"));
        assertNull(HttpControlHandler.jsonField("", "streamId"));
    }

    @Test
    void doesNotMatchKeyAsSubstringOfAnotherKey() {
        String json = "{\"streamId\":\"cam9\"}";
        assertNull(HttpControlHandler.jsonField(json, "id"));
        assertNull(HttpControlHandler.jsonField(json, "treamId"));
    }

    @Test
    void returnsNullForMalformedInput() {
        assertNull(HttpControlHandler.jsonField("{\"url\":\"unterminated", "url"));
        assertNull(HttpControlHandler.jsonField("not json at all", "url"));
        assertNull(HttpControlHandler.jsonField("{\"url\"}", "url"));
    }

    @Test
    void rejectsBrokenUnicodeEscape() {
        // addStream 捕获 RuntimeException 后回 400
        assertThrows(NumberFormatException.class,
                () -> HttpControlHandler.jsonField("{\"url\":\"" + BS + "u00zz\"}", "url"));
    }

    @Test
    void ignoresKeyTextInsideOtherValues() {
        String json = "{\"note\":\"set " + BS + "\"url" + BS + "\" first\",\"url\":\"rtsp://h/p\"}";
        assertEquals("rtsp://h/p", HttpControlHandler.jsonField(json, "url"));
    }
}
