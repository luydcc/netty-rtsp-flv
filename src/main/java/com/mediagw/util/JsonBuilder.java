package com.mediagw.util;

/** 极简 JSON 输出工具，仅覆盖本项目监控接口所需的字符串转义。 */
public final class JsonBuilder {
    private final StringBuilder temp;
    private final boolean[] objTag = new boolean[16];
    private final boolean[] firstTag = new boolean[16];
    private int depth = 0;
    public JsonBuilder(int capacity) {
        this(capacity,true);
    }
    public JsonBuilder(int capacity,boolean obj) {
        temp=new StringBuilder(capacity);
        this.objTag[depth] = obj;
        this.firstTag[depth] = true;
        temp.append(obj ? "{" : "[");
    }
    public JsonBuilder child(String key, boolean obj) {
        writeKey(key);
        this.depth++;
        this.objTag[depth] = obj;
        this.firstTag[depth] = true;
        temp.append(obj ? "{" : "[");
        return this;
    }

    public JsonBuilder append(String key, JsonBuilder value) {
        writeKey(key);
        temp.append(value == null ? "null" : value.temp);
        return this;
    }
    public JsonBuilder append(String key, String value) {
        writeKey(key);
        temp.append(str(value));
        return this;
    }
    public JsonBuilder append(String key, boolean value) {
        writeKey(key);
        temp.append(value);
        return this;
    }
    public JsonBuilder append(String key, long value) {
        writeKey(key);
        temp.append(value);
        return this;
    }
    public JsonBuilder append(String key, int value) {
        writeKey(key);
        temp.append(value);
        return this;
    }
    private void writeKey(String key) {
        if(!this.firstTag[depth]) {
            temp.append(',');
        }else{
            this.firstTag[depth]=false;
        }
        temp.append('"').append(key).append("\":");
    }
    public JsonBuilder append(String value) {
        if(!this.firstTag[depth]) {
            temp.append(',');
        }else{
            this.firstTag[depth]=false;
        }
        temp.append(str(value));
        return this;
    }
    public JsonBuilder end() {
        temp.append(this.objTag[depth] ? "}" : "]");
        this.depth--;
        return this;
    }
    public String endJson() {
        if(depth>=0) {
            this.end();
        }
        return temp.toString();
    }
    private static void warp(StringBuilder sb,String s) {
        if (s == null) {
            sb.append("null");
            return;
        }
        sb.append('"');
        for (int i = 0, len = s.length(); i < len; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
    public static JsonBuilder create(boolean obj) {
        return new JsonBuilder(1024,obj);
    }
    /** 编码为带引号的 JSON 字符串字面量；{@code null} 编码为字面量 null。 */
    public static String str(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        warp(sb, s);
        return sb.toString();
    }
    @Override
    public String toString() {
        return temp.toString();
    }
}
