/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.ByteUtils;
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
import java.io.UncheckedIOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A forward-only iterator over columns in a row-oriented document batch row.
 * Tracks the fixed-section offset incrementally as columns are advanced.
 *
 * <p>Also implements {@link XContentParser} so it can be used directly as a parser
 * for scalar leaf values without allocating a separate wrapper. Call {@link #resetParser()}
 * before each leaf field to reset the parser state; the iterator's current column
 * position determines which value is read.
 *
 * <p>Usage:
 * <pre>
 * DocBatchRowIterator it = batch.getRowIterator(docIndex);
 * while (it.next()) {
 *     if (it.isNull()) continue;
 *     // For scalar leaves, use the iterator directly as an XContentParser:
 *     it.resetParser();
 *     it.nextToken();
 *     fieldMapper.parse(context);
 * }
 * </pre>
 */
public final class DocBatchRowIterator extends AbstractXContentParser {

    private final StreamInput typeData;
    private final BytesReference fixedData;
    private final BytesReference varData;
    private final boolean fixedDataHasArray;
    private final boolean varDataHasArray;
    private final int rowColumnCount;

    // Column cursor state
    private int col = -1;
    private byte typeByte;
    private byte baseType;
    private int fixedOffset = 0;

    // XContentParser state
    private Token currentToken;
    private boolean parserClosed;

    DocBatchRowIterator(BytesReference data, int rowColumnCount, int fixedSectionOffset, int varSectionOffset) {
        super(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, RestApiVersion.current());
        try {
            this.typeData = data.slice(DocBatchRowReader.TYPE_BYTES_OFFSET, fixedSectionOffset - DocBatchRowReader.TYPE_BYTES_OFFSET)
                .streamInput();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.fixedData = data.slice(fixedSectionOffset, varSectionOffset - fixedSectionOffset);
        this.varData = data.slice(varSectionOffset, data.length() - varSectionOffset);
        this.fixedDataHasArray = fixedData.hasArray();
        this.varDataHasArray = varData.hasArray();
        this.rowColumnCount = rowColumnCount;
    }

    // ---- Column iteration ----

    /**
     * Advances to the next column. Returns {@code false} when all columns have been visited.
     */
    public boolean next() {
        if (col >= 0) {
            fixedOffset += RowType.fixedSize(typeByte);
        }
        col++;
        if (col >= rowColumnCount) {
            return false;
        }
        try {
            typeByte = typeData.readByte();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        baseType = RowType.baseType(typeByte);
        return true;
    }

    public int column() {
        return col;
    }

    public byte baseType() {
        return baseType;
    }

    public boolean isNull() {
        return baseType == RowType.NULL;
    }

    public boolean rowBooleanValue() {
        if (baseType == RowType.TRUE) return true;
        if (baseType == RowType.FALSE) return false;
        throw new IllegalStateException("Column " + col + " is not a boolean, type=" + RowType.name(typeByte));
    }

    public long rowLongValue() {
        return readFixedLong();
    }

    public double rowDoubleValue() {
        return Double.longBitsToDouble(rowLongValue());
    }

    public String stringValue() {
        long packed = readFixedLong();
        int varOffset = (int) (packed >>> 32);
        int varLength = (int) packed;
        BytesRef bytesRef = getBytesRef(varOffset, varLength);
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8);
    }

    public XContentString.UTF8Bytes stringUTF8Bytes() {
        long packed = readFixedLong();
        int varOffset = (int) (packed >>> 32);
        int varLength = (int) packed;
        BytesRef bytesRef = getBytesRef(varOffset, varLength);
        return new XContentString.UTF8Bytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    public byte[] rowBinaryValue() {
        long packed = readFixedLong();
        int varOffset = (int) (packed >>> 32);
        int varLength = (int) packed;
        BytesRef bytesRef = getBytesRef(varOffset, varLength);
        byte[] result = new byte[varLength];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, result, 0, varLength);
        return result;
    }

    /**
     * Returns a {@link SmallArrayReader} for the current column (must be ARRAY type).
     */
    public SmallArrayReader smallArrayReader() {
        return new SmallArrayReader(rowBinaryValue());
    }

    private long readFixedLong() {
        if (fixedDataHasArray) {
            return ByteUtils.readLongBE(fixedData.array(), fixedData.arrayOffset() + fixedOffset);
        }
        return fixedData.getLong(fixedOffset);
    }

    private BytesRef getBytesRef(int varOffset, int varLength) {
        if (varDataHasArray) {
            return new BytesRef(varData.array(), varData.arrayOffset() + varOffset, varLength);
        }
        return varData.slice(varOffset, varLength).toBytesRef();
    }

    // ---- XContentParser implementation for scalar leaf values ----

    /**
     * Resets the parser state so this iterator can be reused as an XContentParser for
     * the next leaf value. The iterator must already be positioned at the target column.
     */
    public void resetParser() {
        this.currentToken = null;
        this.parserClosed = false;
    }

    @Override
    public Token nextToken() throws IOException {
        if (parserClosed) return null;

        if (currentToken == null) {
            currentToken = leafToken();
            return currentToken;
        }
        currentToken = null;
        return null;
    }

    private Token leafToken() {
        return switch (baseType) {
            case RowType.NULL -> Token.VALUE_NULL;
            case RowType.TRUE, RowType.FALSE -> Token.VALUE_BOOLEAN;
            case RowType.LONG -> Token.VALUE_NUMBER;
            case RowType.DOUBLE -> Token.VALUE_NUMBER;
            case RowType.STRING -> Token.VALUE_STRING;
            case RowType.BINARY, RowType.ARRAY, RowType.XCONTENT_ARRAY -> Token.VALUE_EMBEDDED_OBJECT;
            default -> throw new IllegalStateException("Unsupported row type: " + RowType.name(typeByte));
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
        return switch (baseType) {
            case RowType.STRING -> stringValue();
            case RowType.LONG -> Long.toString(rowLongValue());
            case RowType.DOUBLE -> Double.toString(rowDoubleValue());
            case RowType.TRUE -> "true";
            case RowType.FALSE -> "false";
            default -> null;
        };
    }

    @Override
    public XContentString optimizedText() throws IOException {
        if (baseType == RowType.STRING) {
            return new Text(stringUTF8Bytes());
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
    public boolean emptyText() throws IOException {
        XContentString xContentString = optimizedTextOrNull();
        return xContentString == null || xContentString.isMoreCharsThan(0) == false;
    }

    @Override
    public int textOffset() throws IOException {
        return 0;
    }

    @Override
    public Number numberValue() throws IOException {
        return switch (baseType) {
            case RowType.LONG -> rowLongValue();
            case RowType.DOUBLE -> rowDoubleValue();
            default -> null;
        };
    }

    @Override
    public NumberType numberType() throws IOException {
        return switch (baseType) {
            case RowType.LONG -> NumberType.LONG;
            case RowType.DOUBLE -> NumberType.DOUBLE;
            default -> null;
        };
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        return rowBooleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        return (short) rowLongValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        return (int) rowLongValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        return rowLongValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        if (baseType == RowType.DOUBLE) {
            return (float) rowDoubleValue();
        }
        return (float) rowLongValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        if (baseType == RowType.DOUBLE) {
            return rowDoubleValue();
        }
        return rowLongValue();
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
        if (baseType == RowType.BINARY || baseType == RowType.ARRAY || baseType == RowType.XCONTENT_ARRAY) {
            return rowBinaryValue();
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
        return parserClosed;
    }

    @Override
    public void close() throws IOException {
        parserClosed = true;
        typeData.close();
    }
}
