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

import java.nio.charset.StandardCharsets;

/**
 * Zero-copy reader for a single row in a row-oriented document batch.
 *
 * <p>Row layout:
 * <pre>
 * row_column_count(4) | type_bytes[row_column_count] | fixed_section | var_section
 * </pre>
 */
public final class DocBatchRowReader {

    private final BytesReference data;
    private final int rowOffset;
    private final int rowLength;
    private final DocBatchSchema schema;
    private final int rowColumnCount;
    private final int typeBytesOffset;
    private final int fixedSectionOffset;
    private final int varSectionOffset;

    public DocBatchRowReader(BytesReference data, int rowOffset, int rowLength, DocBatchSchema schema) {
        this.data = data;
        this.rowOffset = rowOffset;
        this.rowLength = rowLength;
        this.schema = schema;
        this.rowColumnCount = data.getInt(rowOffset);
        this.typeBytesOffset = rowOffset + 4;
        this.fixedSectionOffset = typeBytesOffset + rowColumnCount;

        // Compute var section offset by summing fixed sizes of all columns
        int fixedTotal = 0;
        for (int col = 0; col < rowColumnCount; col++) {
            fixedTotal += RowType.fixedSize(data.get(typeBytesOffset + col));
        }
        this.varSectionOffset = fixedSectionOffset + fixedTotal;
    }

    public int columnCount() {
        return rowColumnCount;
    }

    public byte getTypeByte(int col) {
        if (col >= rowColumnCount) {
            return RowType.NULL;
        }
        return data.get(typeBytesOffset + col);
    }

    public byte getBaseType(int col) {
        return RowType.baseType(getTypeByte(col));
    }

    public boolean isFromObject(int col) {
        return RowType.isFromObject(getTypeByte(col));
    }

    public boolean isNull(int col) {
        return getBaseType(col) == RowType.NULL;
    }

    public boolean getBooleanValue(int col) {
        byte base = getBaseType(col);
        if (base == RowType.TRUE) return true;
        if (base == RowType.FALSE) return false;
        throw new IllegalStateException("Column " + col + " is not a boolean, type=" + RowType.name(getTypeByte(col)));
    }

    public long getLongValue(int col) {
        int offset = computeFixedOffset(col);
        return data.getLong(offset);
    }

    public double getDoubleValue(int col) {
        int offset = computeFixedOffset(col);
        long bits = data.getLong(offset);
        return Double.longBitsToDouble(bits);
    }

    public String getStringValue(int col) {
        int offset = computeFixedOffset(col);
        int varOffset = data.getInt(offset);
        int varLength = data.getInt(offset + 4);
        BytesRef bytesRef = data.slice(varSectionOffset + varOffset, varLength).toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8);
    }

    /**
     * Returns the raw UTF-8 bytes for a string column without copying or decoding.
     * The returned array is shared — callers must not modify it.
     */
    public BytesReference data() {
        return data;
    }

    /**
     * Returns the absolute offset within {@link #data} for the string value at the given column.
     */
    public int getStringRawOffset(int col) {
        int offset = computeFixedOffset(col);
        int varOffset = data.getInt(offset);
        return varSectionOffset + varOffset;
    }

    /**
     * Returns the byte length of the string value at the given column.
     */
    public int getStringRawLength(int col) {
        int offset = computeFixedOffset(col);
        return data.getInt(offset + 4);
    }

    public byte[] getBinaryValue(int col) {
        int offset = computeFixedOffset(col);
        int varOffset = data.getInt(offset);
        int varLength = data.getInt(offset + 4);
        BytesRef bytesRef = data.slice(varSectionOffset + varOffset, varLength).toBytesRef();
        byte[] result = new byte[varLength];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, result, 0, varLength);
        return result;
    }

    public byte[] getArrayValue(int col) {
        return getBinaryValue(col);
    }

    /**
     * Iterates over all non-null fields in this row.
     */
    public void forEachField(FieldConsumer consumer) {
        for (int col = 0; col < rowColumnCount; col++) {
            byte typeByte = data.get(typeBytesOffset + col);
            if (RowType.baseType(typeByte) != RowType.NULL) {
                consumer.accept(col, typeByte);
            }
        }
    }

    /**
     * Creates a forward-only iterator over this row's columns.
     * The iterator tracks the fixed-section offset incrementally, avoiding O(col)
     * recomputation on each value access.
     */
    public DocBatchRowIterator iterator() {
        return new DocBatchRowIterator(data, rowOffset, rowColumnCount, fixedSectionOffset, varSectionOffset);
    }

    @FunctionalInterface
    public interface FieldConsumer {
        void accept(int columnIndex, byte typeByte);
    }

    /**
     * Computes the byte offset in the backing array for the fixed-section entry of column {@code col}.
     * Sums FIXED_SIZE for columns 0..col-1.
     */
    private int computeFixedOffset(int col) {
        int offset = fixedSectionOffset;
        for (int i = 0; i < col; i++) {
            offset += RowType.fixedSize(data.get(typeBytesOffset + i));
        }
        return offset;
    }
}
