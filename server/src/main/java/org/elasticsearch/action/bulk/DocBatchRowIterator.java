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
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;

/**
 * A forward-only iterator over columns in a row-oriented document batch row.
 * Tracks the fixed-section offset incrementally as columns are advanced.
 *
 * <p>Usage:
 * <pre>
 * DocBatchRowIterator it = batch.getRowIterator(docIndex);
 * while (it.next()) {
 *     if (it.isNull()) continue;
 *     // read values via it.longValue(), it.stringRawOffset(), etc.
 * }
 * </pre>
 */
public final class DocBatchRowIterator {

    private final BytesReference data;
    private final int typeBytesOffset;
    private final int varSectionOffset;
    private final int rowColumnCount;

    // Cursor state
    private int col;
    private byte typeByte;
    private byte baseType;
    private int fixedOffset;

    DocBatchRowIterator(BytesReference data, int rowOffset, int rowColumnCount, int fixedSectionOffset, int varSectionOffset) {
        this.data = data;
        this.typeBytesOffset = rowOffset + 4;
        this.varSectionOffset = varSectionOffset;
        this.rowColumnCount = rowColumnCount;
        this.col = -1;
        this.fixedOffset = fixedSectionOffset;
    }

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
        typeByte = data.get(typeBytesOffset + col);
        baseType = RowType.baseType(typeByte);
        return true;
    }

    public int column() {
        return col;
    }

    public byte typeByte() {
        return typeByte;
    }

    public byte baseType() {
        return baseType;
    }

    public boolean isNull() {
        return baseType == RowType.NULL;
    }

    public boolean isFromObject() {
        return RowType.isFromObject(typeByte);
    }

    public boolean booleanValue() {
        if (baseType == RowType.TRUE) return true;
        if (baseType == RowType.FALSE) return false;
        throw new IllegalStateException("Column " + col + " is not a boolean, type=" + RowType.name(typeByte));
    }

    public long longValue() {
        return data.getLong(fixedOffset);
    }

    public double doubleValue() {
        return Double.longBitsToDouble(longValue());
    }

    public String stringValue() {
        int varOffset = data.getInt(fixedOffset);
        int varLength = data.getInt(fixedOffset + 4);
        BytesRef bytesRef = data.slice(varSectionOffset + varOffset, varLength).toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8);
    }

    public XContentString.UTF8Bytes stringUTF8Bytes() {
        int varOffset = data.getInt(fixedOffset);
        int varLength = data.getInt(fixedOffset + 4);
        BytesRef bytesRef = data.slice(varSectionOffset + varOffset, varLength).toBytesRef();
        return new XContentString.UTF8Bytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
    }

    public byte[] binaryValue() {
        int varOffset = data.getInt(fixedOffset);
        int varLength = data.getInt(fixedOffset + 4);
        BytesRef bytesRef = data.slice(varSectionOffset + varOffset, varLength).toBytesRef();
        byte[] result = new byte[varLength];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, result, 0, varLength);
        return result;
    }
}
