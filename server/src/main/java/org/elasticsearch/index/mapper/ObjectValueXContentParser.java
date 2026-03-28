/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.List;
import java.util.Map;

/**
 * A synthetic XContentParser that wraps Java Objects (String, Long, Double, Boolean, List, Map, null)
 * so that FieldMapper.parse() can consume them without serializing to JSON bytes first.
 * <p>
 * For scalars and lists, tokens are produced directly from pre-built Entry arrays (zero allocation
 * beyond the parser itself). For maps, the value is serialized to JSON and parsed with a standard
 * XContentParser (correctness over speed — maps are rare in the row path).
 */
final class ObjectValueXContentParser extends AbstractXContentParser {

    private final Entry[] entries;
    private int position = -1;
    private boolean closed = false;

    private ObjectValueXContentParser(Entry[] entries) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.entries = entries;
    }

    /**
     * Creates an appropriate parser for any Java Object value.
     */
    static XContentParser forValue(Object value) throws IOException {
        if (value == null) {
            return RowValueXContentParser.forNullValue();
        }
        if (value instanceof String s) {
            return forScalar(Token.VALUE_STRING, s);
        }
        if (value instanceof Long l) {
            return forScalar(Token.VALUE_NUMBER, l);
        }
        if (value instanceof Integer i) {
            return forScalar(Token.VALUE_NUMBER, i);
        }
        if (value instanceof Double d) {
            return forScalar(Token.VALUE_NUMBER, d);
        }
        if (value instanceof Float f) {
            return forScalar(Token.VALUE_NUMBER, f);
        }
        if (value instanceof Boolean b) {
            return forScalar(b ? Token.VALUE_BOOLEAN : Token.VALUE_BOOLEAN, b);
        }
        if (value instanceof List<?> list) {
            return forList(list);
        }
        if (value instanceof Map<?, ?> map) {
            return forMap(map);
        }
        if (value instanceof byte[] bytes) {
            return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes);
        }
        // Fallback: treat as string
        return forScalar(Token.VALUE_STRING, value.toString());
    }

    private static ObjectValueXContentParser forScalar(Token token, Object value) {
        Entry[] entries = new Entry[] { new Entry(token, null, value), new Entry(null, null, null) };
        return new ObjectValueXContentParser(entries);
    }

    private static ObjectValueXContentParser forList(List<?> values) throws IOException {
        // Build Entry[] with START_ARRAY, flattened element entries, END_ARRAY
        // For nested lists/maps inside a list, we serialize to JSON as a fallback
        int count = values.size();
        Entry[] entries = new Entry[count + 3]; // START_ARRAY + elements + END_ARRAY + null sentinel
        entries[0] = new Entry(Token.START_ARRAY, null, null);
        for (int i = 0; i < count; i++) {
            Object v = values.get(i);
            entries[i + 1] = scalarEntry(v);
        }
        entries[count + 1] = new Entry(Token.END_ARRAY, null, null);
        entries[count + 2] = new Entry(null, null, null);
        return new ObjectValueXContentParser(entries);
    }

    private static Entry scalarEntry(Object value) {
        if (value == null) {
            return new Entry(Token.VALUE_NULL, null, null);
        }
        if (value instanceof String) {
            return new Entry(Token.VALUE_STRING, null, value);
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Double || value instanceof Float) {
            return new Entry(Token.VALUE_NUMBER, null, value);
        }
        if (value instanceof Boolean) {
            return new Entry(Token.VALUE_BOOLEAN, null, value);
        }
        // Fallback for complex elements: treat as string
        return new Entry(Token.VALUE_STRING, null, value.toString());
    }

    @SuppressWarnings("unchecked")
    private static XContentParser forMap(Map<?, ?> map) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.map((Map<String, ?>) map);
        byte[] bytes = org.elasticsearch.common.bytes.BytesReference.bytes(builder).toBytesRef().bytes;
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes);
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;
        position++;
        if (position >= entries.length) return null;
        Entry entry = entries[position];
        return entry != null ? entry.token : null;
    }

    @Override
    public Token currentToken() {
        if (position < 0 || position >= entries.length) return null;
        Entry entry = entries[position];
        return entry != null ? entry.token : null;
    }

    @Override
    public String currentName() throws IOException {
        if (currentToken() == Token.FIELD_NAME) {
            return entries[position].name;
        }
        if (position > 0 && entries[position - 1] != null && entries[position - 1].token == Token.FIELD_NAME) {
            return entries[position - 1].name;
        }
        return null;
    }

    @Override
    public void skipChildren() throws IOException {
        if (currentToken() == Token.START_OBJECT || currentToken() == Token.START_ARRAY) {
            int depth = 1;
            while (depth > 0) {
                Token t = nextToken();
                if (t == null) break;
                if (t == Token.START_OBJECT || t == Token.START_ARRAY) depth++;
                else if (t == Token.END_OBJECT || t == Token.END_ARRAY) depth--;
            }
        }
    }

    @Override
    public String text() throws IOException {
        Entry entry = currentEntry();
        if (entry == null) return null;
        if (entry.token == Token.FIELD_NAME && entry.name != null) {
            return entry.name;
        }
        if (entry.value != null) {
            return entry.value.toString();
        }
        return null;
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        String t = text();
        return t != null ? CharBuffer.wrap(t) : null;
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public char[] textCharacters() throws IOException {
        String t = text();
        return t != null ? t.toCharArray() : new char[0];
    }

    @Override
    public int textLength() throws IOException {
        String t = text();
        return t != null ? t.length() : 0;
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    @Override
    public Number numberValue() throws IOException {
        Entry entry = currentEntry();
        if (entry != null && entry.value instanceof Number n) {
            return n;
        }
        return null;
    }

    @Override
    public NumberType numberType() throws IOException {
        Entry entry = currentEntry();
        if (entry != null && entry.value instanceof Double) return NumberType.DOUBLE;
        if (entry != null && entry.value instanceof Float) return NumberType.FLOAT;
        if (entry != null && entry.value instanceof Long) return NumberType.LONG;
        if (entry != null && entry.value instanceof Integer) return NumberType.INT;
        return null;
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        Entry entry = currentEntry();
        if (entry != null && entry.value instanceof Boolean b) return b;
        return false;
    }

    @Override
    protected short doShortValue() throws IOException {
        Number n = numberValue();
        return n != null ? n.shortValue() : 0;
    }

    @Override
    protected int doIntValue() throws IOException {
        Number n = numberValue();
        return n != null ? n.intValue() : 0;
    }

    @Override
    protected long doLongValue() throws IOException {
        Number n = numberValue();
        return n != null ? n.longValue() : 0;
    }

    @Override
    protected float doFloatValue() throws IOException {
        Number n = numberValue();
        return n != null ? n.floatValue() : 0;
    }

    @Override
    protected double doDoubleValue() throws IOException {
        Number n = numberValue();
        return n != null ? n.doubleValue() : 0;
    }

    @Override
    public Object objectText() throws IOException {
        Entry entry = currentEntry();
        if (entry != null && entry.value != null) return entry.value;
        return text();
    }

    @Override
    public Object objectBytes() throws IOException {
        return objectText();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return null;
    }

    @Override
    public XContentLocation getTokenLocation() {
        return XContentLocation.UNKNOWN;
    }

    @Override
    public XContentLocation getCurrentLocation() {
        return XContentLocation.UNKNOWN;
    }

    @Override
    public XContentType contentType() {
        return XContentType.JSON;
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {}

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    private Entry currentEntry() {
        if (position >= 0 && position < entries.length) {
            return entries[position];
        }
        return null;
    }

    private record Entry(Token token, String name, Object value) {}
}
