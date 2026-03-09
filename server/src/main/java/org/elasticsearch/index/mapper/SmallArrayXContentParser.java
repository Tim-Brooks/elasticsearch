/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.RowType;
import org.elasticsearch.action.bulk.SmallArrayReader;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentString.UTF8Bytes;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

/**
 * An XContentParser that presents a compact typed array ({@link SmallArrayReader}) as a
 * sequence of tokens: START_ARRAY, element values, END_ARRAY.
 */
final class SmallArrayXContentParser extends AbstractXContentParser {

    private final SmallArrayReader reader;

    private enum State {
        BEFORE_ARRAY,
        IN_ARRAY,
        AFTER_ARRAY,
        DONE
    }

    private State state = State.BEFORE_ARRAY;
    private Token currentToken;
    private boolean closed;

    SmallArrayXContentParser(SmallArrayReader reader) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.reader = reader;
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;
        switch (state) {
            case BEFORE_ARRAY -> {
                state = State.IN_ARRAY;
                currentToken = Token.START_ARRAY;
                return currentToken;
            }
            case IN_ARRAY -> {
                if (reader.next()) {
                    currentToken = elementToken();
                    return currentToken;
                } else {
                    state = State.AFTER_ARRAY;
                    currentToken = Token.END_ARRAY;
                    return currentToken;
                }
            }
            case AFTER_ARRAY -> {
                state = State.DONE;
                currentToken = null;
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    private Token elementToken() {
        byte type = reader.type();
        return switch (type) {
            case RowType.NULL -> Token.VALUE_NULL;
            case RowType.TRUE, RowType.FALSE -> Token.VALUE_BOOLEAN;
            case RowType.LONG, RowType.DOUBLE -> Token.VALUE_NUMBER;
            case RowType.STRING -> Token.VALUE_STRING;
            default -> throw new IllegalStateException("Unsupported element type in small array: " + RowType.name(type));
        };
    }

    @Override
    public Token currentToken() {
        return currentToken;
    }

    @Override
    public String currentName() throws IOException {
        return null;
    }

    @Override
    public void skipChildren() throws IOException {
        // no-op — array elements are all leaves
    }

    @Override
    public String text() throws IOException {
        byte type = reader.type();
        return switch (type) {
            case RowType.STRING -> reader.stringValue();
            case RowType.LONG -> Long.toString(reader.longValue());
            case RowType.DOUBLE -> Double.toString(reader.doubleValue());
            case RowType.TRUE -> "true";
            case RowType.FALSE -> "false";
            default -> null;
        };
    }

    @Override
    public XContentString optimizedText() throws IOException {
        if (reader.type() == RowType.STRING) {
            return new Text(new UTF8Bytes(reader.stringBytes(), reader.stringOffset(), reader.stringLength()));
        }
        return new Text(text());
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
        return switch (reader.type()) {
            case RowType.LONG -> reader.longValue();
            case RowType.DOUBLE -> reader.doubleValue();
            default -> null;
        };
    }

    @Override
    public NumberType numberType() throws IOException {
        return switch (reader.type()) {
            case RowType.LONG -> NumberType.LONG;
            case RowType.DOUBLE -> NumberType.DOUBLE;
            default -> null;
        };
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return reader.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return (short) reader.longValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return (int) reader.longValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return reader.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        if (reader.type() == RowType.DOUBLE) {
            return (float) reader.doubleValue();
        }
        return (float) reader.longValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        if (reader.type() == RowType.DOUBLE) {
            return reader.doubleValue();
        }
        return reader.longValue();
    }

    @Override
    public Object objectText() throws IOException {
        return objectBytes();
    }

    @Override
    public Object objectBytes() throws IOException {
        Token token = currentToken();
        if (token == Token.VALUE_STRING) return text();
        if (token == Token.VALUE_NUMBER) return numberValue();
        if (token == Token.VALUE_BOOLEAN) return booleanValue();
        if (token == Token.VALUE_NULL) return null;
        return null;
    }

    @Override
    public byte[] binaryValue() throws IOException {
        if (reader.type() == RowType.STRING) {
            return reader.stringValue().getBytes(StandardCharsets.UTF_8);
        }
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
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        // no-op
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }
}
