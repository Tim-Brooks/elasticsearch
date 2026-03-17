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
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * A synthetic XContentParser that replays a pre-built token sequence for compound field types
 * (e.g., aggregate_metric_double, histogram). Instead of serializing sub-field values to JSON
 * and parsing them back, this parser produces the token stream directly from typed values.
 */
final class CompoundFieldXContentParser extends AbstractXContentParser {

    private final Entry[] entries;
    private int position = -1;
    private boolean closed = false;

    private CompoundFieldXContentParser(Entry[] entries) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.entries = entries;
    }

    /**
     * Creates a parser for an aggregate_metric_double field with sum and value_count sub-fields.
     * Produces: START_OBJECT → FIELD_NAME("sum") → VALUE_NUMBER(sum) → FIELD_NAME("value_count") → VALUE_NUMBER(valueCount) → END_OBJECT
     */
    static CompoundFieldXContentParser forAggregateMetricDouble(double sum, long valueCount) {
        Entry[] entries = new Entry[] {
            new Entry(Token.START_OBJECT, null, null),
            new Entry(Token.FIELD_NAME, "sum", null),
            new Entry(Token.VALUE_NUMBER, null, sum),
            new Entry(Token.FIELD_NAME, "value_count", null),
            new Entry(Token.VALUE_NUMBER, null, valueCount),
            new Entry(Token.END_OBJECT, null, null), };
        return new CompoundFieldXContentParser(entries);
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;
        position++;
        if (position >= entries.length) {
            return null;
        }
        return entries[position].token;
    }

    @Override
    public Token currentToken() {
        if (position < 0 || position >= entries.length) return null;
        return entries[position].token;
    }

    @Override
    public String currentName() throws IOException {
        if (currentToken() == Token.FIELD_NAME) {
            return entries[position].name;
        }
        // After reading a value, currentName() should return the field name
        if (position > 0 && entries[position - 1].token == Token.FIELD_NAME) {
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
        if (entry != null && entry.name != null && currentToken() == Token.FIELD_NAME) {
            return entry.name;
        }
        if (entry != null && entry.value != null) {
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
        if (entry != null && entry.value instanceof Double) {
            return NumberType.DOUBLE;
        }
        if (entry != null && entry.value instanceof Long) {
            return NumberType.LONG;
        }
        if (entry != null && entry.value instanceof Integer) {
            return NumberType.INT;
        }
        return null;
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
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
        return text();
    }

    @Override
    public Object objectBytes() throws IOException {
        return text();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return null;
    }

    @Override
    public XContentLocation getTokenLocation() {
        return new XContentLocation(0, 0);
    }

    @Override
    public XContentLocation getCurrentLocation() {
        return new XContentLocation(0, 0);
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
