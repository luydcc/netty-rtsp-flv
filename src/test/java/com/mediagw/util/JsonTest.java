package com.mediagw.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonTest {

    @Test
    void quotesPlainStrings() {
        assertEquals("\"cam1\"", JsonBuilder.str("cam1"));
        assertEquals("\"\"", JsonBuilder.str(""));
        assertEquals("null", JsonBuilder.str(null));
    }

    @Test
    void escapesQuotesBackslashAndWhitespace() {
        assertEquals("\"a\\\"b\"", JsonBuilder.str("a\"b"));
        assertEquals("\"a\\\\b\"", JsonBuilder.str("a\\b"));
        assertEquals("\"a\\nb\"", JsonBuilder.str("a\nb"));
        assertEquals("\"a\\rb\"", JsonBuilder.str("a\rb"));
        assertEquals("\"a\\tb\"", JsonBuilder.str("a\tb"));
    }

    @Test
    void escapesControlCharactersAsUnicode() {
        assertEquals("\"a\\u0001b\"", JsonBuilder.str("a\u0001b"));
        assertEquals("\"\\u001f\"", JsonBuilder.str("\u001f"));
        // 0x20（空格）不需转义
        assertEquals("\"a b\"", JsonBuilder.str("a b"));
    }

    @Test
    void keepsNonAsciiCharacters() {
        assertEquals("\"临时流\"", JsonBuilder.str("临时流"));
    }
    @Test
    void jsonBuild() {
        JsonBuilder json=JsonBuilder.create(true).append("name","value")
                .child("child",true).end()
                .end();
        System.out.println(json.endJson());
    }
}
