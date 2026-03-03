/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.DocBatchRowReader;
import org.elasticsearch.action.bulk.RowType;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * An XContentParser adapter that presents a single field value from a {@link DocBatchRowReader}
 * as an XContentParser. Parallel to {@link ColumnValueXContentParser} but for row-oriented data.
 *
 * <p>Two modes of operation:</p>
 * <ul>
 *   <li><b>Leaf scalar</b>: Presents a single typed value (long, double, string, boolean) as a one-token parser.</li>
 *   <li><b>Binary delegate</b>: Wraps raw bytes (for arrays/binary) in a standard XContentParser.</li>
 * </ul>
 */
public class RowValueXContentParser extends AbstractXContentParser {

    /**
     * Create a parser for a leaf scalar value from a row column.
     */
    public static RowValueXContentParser forLeafValue(DocBatchRowReader reader, int col) {
        return new RowValueXContentParser(reader, col);
    }

    /**
     * Create a parser that delegates to a standard XContentParser wrapping raw binary (array/nested) data.
     */
    public static XContentParser forBinary(DocBatchRowReader reader, int col, XContentType xContentType) throws IOException {
        byte[] bytes = reader.getBinaryValue(col);
        if (bytes == null || bytes.length == 0) {
            return ColumnValueXContentParser.forNullValue();
        }
        return xContentType.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, new java.io.ByteArrayInputStream(bytes));
    }

    private final DocBatchRowReader reader;
    private final int col;
    private Token currentToken;
    private boolean closed;

    private RowValueXContentParser(DocBatchRowReader reader, int col) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.reader = reader;
        this.col = col;
        this.currentToken = null;
        this.closed = false;
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;

        if (currentToken == null) {
            currentToken = leafToken();
            return currentToken;
        }
        currentToken = null;
        return null;
    }

    private Token leafToken() {
        byte baseType = reader.getBaseType(col);
        return switch (baseType) {
            case RowType.NULL -> Token.VALUE_NULL;
            case RowType.TRUE, RowType.FALSE -> Token.VALUE_BOOLEAN;
            case RowType.LONG -> Token.VALUE_NUMBER;
            case RowType.DOUBLE -> Token.VALUE_NUMBER;
            case RowType.STRING -> Token.VALUE_STRING;
            case RowType.BINARY, RowType.ARRAY -> Token.VALUE_EMBEDDED_OBJECT;
            default -> throw new IllegalStateException("Unsupported row type: " + RowType.name(reader.getTypeByte(col)));
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
        // no-op for single value
    }

    @Override
    public String text() throws IOException {
        byte baseType = reader.getBaseType(col);
        return switch (baseType) {
            case RowType.STRING -> reader.getStringValue(col);
            case RowType.LONG -> Long.toString(reader.getLongValue(col));
            case RowType.DOUBLE -> Double.toString(reader.getDoubleValue(col));
            case RowType.TRUE -> "true";
            case RowType.FALSE -> "false";
            default -> null;
        };
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
        byte baseType = reader.getBaseType(col);
        return switch (baseType) {
            case RowType.LONG -> reader.getLongValue(col);
            case RowType.DOUBLE -> reader.getDoubleValue(col);
            default -> null;
        };
    }

    @Override
    public NumberType numberType() throws IOException {
        byte baseType = reader.getBaseType(col);
        return switch (baseType) {
            case RowType.LONG -> NumberType.LONG;
            case RowType.DOUBLE -> NumberType.DOUBLE;
            default -> null;
        };
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return reader.getBooleanValue(col);
    }

    @Override
    protected short doShortValue() throws IOException {
        return (short) reader.getLongValue(col);
    }

    @Override
    protected int doIntValue() throws IOException {
        return (int) reader.getLongValue(col);
    }

    @Override
    protected long doLongValue() throws IOException {
        return reader.getLongValue(col);
    }

    @Override
    protected float doFloatValue() throws IOException {
        byte baseType = reader.getBaseType(col);
        if (baseType == RowType.DOUBLE) {
            return (float) reader.getDoubleValue(col);
        }
        return (float) reader.getLongValue(col);
    }

    @Override
    protected double doDoubleValue() throws IOException {
        byte baseType = reader.getBaseType(col);
        if (baseType == RowType.DOUBLE) {
            return reader.getDoubleValue(col);
        }
        return reader.getLongValue(col);
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
        if (token == Token.VALUE_EMBEDDED_OBJECT) return binaryValue();
        return null;
    }

    @Override
    public byte[] binaryValue() throws IOException {
        byte baseType = reader.getBaseType(col);
        if (baseType == RowType.BINARY || baseType == RowType.ARRAY) {
            return reader.getBinaryValue(col);
        }
        return null;
    }

    @Override
    public XContentLocation getTokenLocation() {
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
