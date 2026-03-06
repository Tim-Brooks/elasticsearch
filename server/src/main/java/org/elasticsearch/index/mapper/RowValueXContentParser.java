/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.DocBatchRowIterator;
import org.elasticsearch.action.bulk.RowType;
import org.elasticsearch.action.bulk.SmallArrayReader;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

/**
 * An XContentParser adapter that presents a single field value from a row-oriented batch
 * as an XContentParser.
 *
 * <p>Two modes of operation:</p>
 * <ul>
 *   <li><b>Leaf scalar</b>: Presents a single typed value (long, double, string, boolean) as a one-token parser.</li>
 *   <li><b>Binary delegate</b>: Wraps raw bytes (for arrays/binary) in a standard XContentParser.</li>
 * </ul>
 */
public class RowValueXContentParser extends AbstractXContentParser {

    /**
     * Create a parser for a null value. Returns VALUE_NULL on the first nextToken() call and null thereafter.
     */
    public static RowValueXContentParser forNullValue() {
        return new RowValueXContentParser();
    }

    /**
     * Create a parser for a leaf scalar value using a {@link DocBatchRowIterator} positioned at the target column.
     * The iterator's cursor must remain stable while this parser is in use.
     */
    public static RowValueXContentParser forLeafValue(DocBatchRowIterator iterator) {
        return new RowValueXContentParser(iterator);
    }

    /**
     * Create a parser that delegates to a standard XContentParser wrapping raw binary (xcontent array/nested) data,
     * using a {@link DocBatchRowIterator} positioned at the target column.
     */
    public static XContentParser forBinary(DocBatchRowIterator iterator, XContentType xContentType) throws IOException {
        byte[] bytes = iterator.binaryValue();
        if (bytes == null || bytes.length == 0) {
            return forNullValue();
        }
        return xContentType.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, new java.io.ByteArrayInputStream(bytes));
    }

    /**
     * Create a parser that presents a compact typed array (from a {@link SmallArrayReader}) as
     * START_ARRAY, element tokens, END_ARRAY.
     */
    public static XContentParser forSmallArray(DocBatchRowIterator iterator) {
        return new SmallArrayXContentParser(iterator.smallArrayReader());
    }

    private final DocBatchRowIterator iterator;

    private Token currentToken;
    private boolean closed;

    private RowValueXContentParser(DocBatchRowIterator iterator) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.iterator = iterator;
        this.currentToken = null;
        this.closed = false;
    }

    private RowValueXContentParser() {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        this.iterator = null;
        this.currentToken = null;
        this.closed = false;
    }

    private byte baseType() {
        return iterator.baseType();
    }

    private boolean isNullMode() {
        return iterator == null;
    }

    @Override
    public Token nextToken() throws IOException {
        if (closed) return null;

        if (currentToken == null) {
            currentToken = isNullMode() ? Token.VALUE_NULL : leafToken();
            return currentToken;
        }
        currentToken = null;
        return null;
    }

    private Token leafToken() {
        byte baseType = baseType();
        return switch (baseType) {
            case RowType.NULL -> Token.VALUE_NULL;
            case RowType.TRUE, RowType.FALSE -> Token.VALUE_BOOLEAN;
            case RowType.LONG -> Token.VALUE_NUMBER;
            case RowType.DOUBLE -> Token.VALUE_NUMBER;
            case RowType.STRING -> Token.VALUE_STRING;
            case RowType.BINARY, RowType.ARRAY, RowType.XCONTENT_ARRAY -> Token.VALUE_EMBEDDED_OBJECT;
            default -> throw new IllegalStateException("Unsupported row type: " + RowType.name(iterator.typeByte()));
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
        return switch (baseType()) {
            case RowType.STRING -> iterator.stringValue();
            case RowType.LONG -> Long.toString(iterator.longValue());
            case RowType.DOUBLE -> Double.toString(iterator.doubleValue());
            case RowType.TRUE -> "true";
            case RowType.FALSE -> "false";
            default -> null;
        };
    }

    @Override
    public XContentString optimizedText() throws IOException {
        if (baseType() == RowType.STRING) {
            return new Text(iterator.stringUTF8Bytes());
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
        return switch (baseType()) {
            case RowType.LONG -> iterator.longValue();
            case RowType.DOUBLE -> iterator.doubleValue();
            default -> null;
        };
    }

    @Override
    public NumberType numberType() throws IOException {
        byte baseType = baseType();
        return switch (baseType) {
            case RowType.LONG -> NumberType.LONG;
            case RowType.DOUBLE -> NumberType.DOUBLE;
            default -> null;
        };
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return iterator.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return (short) iterator.longValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return (int) iterator.longValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return iterator.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        if (baseType() == RowType.DOUBLE) {
            return (float) iterator.doubleValue();
        }
        return (float) iterator.longValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        if (baseType() == RowType.DOUBLE) {
            return iterator.doubleValue();
        }
        return iterator.longValue();
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
        byte baseType = baseType();
        if (baseType == RowType.BINARY || baseType == RowType.ARRAY || baseType == RowType.XCONTENT_ARRAY) {
            return iterator.binaryValue();
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
