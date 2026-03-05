/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * A forward-only iterator over columns in a row-oriented document batch row.
 * Tracks the fixed-section offset incrementally as columns are advanced,
 * avoiding the O(col) recomputation that {@link DocBatchRowReader#computeFixedOffset} performs.
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

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private final byte[] data;
    private final int typeBytesOffset;
    private final int varSectionOffset;
    private final int rowColumnCount;

    // Cursor state
    private int col;
    private byte typeByte;
    private byte baseType;
    private int fixedOffset;

    DocBatchRowIterator(byte[] data, int rowOffset, int rowColumnCount, int fixedSectionOffset, int varSectionOffset) {
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
        typeByte = data[typeBytesOffset + col];
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
        return (long) LONG_HANDLE.get(data, fixedOffset);
    }

    public double doubleValue() {
        return Double.longBitsToDouble((long) LONG_HANDLE.get(data, fixedOffset));
    }

    public String stringValue() {
        int varOffset = (int) INT_HANDLE.get(data, fixedOffset);
        int varLength = (int) INT_HANDLE.get(data, fixedOffset + 4);
        return new String(data, varSectionOffset + varOffset, varLength, StandardCharsets.UTF_8);
    }

    /**
     * Returns the backing byte array containing string data. Callers must not modify it.
     */
    public byte[] stringRawBytes() {
        return data;
    }

    /**
     * Returns the absolute offset within {@link #stringRawBytes()} for the current column's string value.
     */
    public int stringRawOffset() {
        int varOffset = (int) INT_HANDLE.get(data, fixedOffset);
        return varSectionOffset + varOffset;
    }

    /**
     * Returns the byte length of the current column's string value.
     */
    public int stringRawLength() {
        return (int) INT_HANDLE.get(data, fixedOffset + 4);
    }

    public byte[] binaryValue() {
        int varOffset = (int) INT_HANDLE.get(data, fixedOffset);
        int varLength = (int) INT_HANDLE.get(data, fixedOffset + 4);
        byte[] result = new byte[varLength];
        System.arraycopy(data, varSectionOffset + varOffset, result, 0, varLength);
        return result;
    }
}
