/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;

import java.nio.charset.StandardCharsets;

/**
 * Zero-copy reader for a single row in EIRF format.
 *
 * <p>Row layout:
 * <pre>
 * column_count(u16) | type_bytes[column_count] | fixed_section | var_section
 * </pre>
 */
public final class EirfRowReader {

    static final int TYPE_BYTES_OFFSET = 2; // u16 column_count

    private final EirfSchema schema;
    private final BytesReference rowData;
    private final int rowColumnCount;
    private final int fixedSectionOffset;
    private final int varSectionOffset;

    public EirfRowReader(BytesReference rowData, EirfSchema schema) {
        this.rowData = rowData;
        this.schema = schema;
        this.rowColumnCount = EirfBatch.readU16(rowData, 0);
        this.fixedSectionOffset = TYPE_BYTES_OFFSET + rowColumnCount;

        int fixedTotal = 0;
        for (int col = 0; col < rowColumnCount; col++) {
            fixedTotal += EirfType.fixedSize(rowData.get(TYPE_BYTES_OFFSET + col));
        }
        this.varSectionOffset = fixedSectionOffset + fixedTotal;
    }

    public int columnCount() {
        return rowColumnCount;
    }

    public EirfSchema schema() {
        return schema;
    }

    public byte getTypeByte(int col) {
        if (col >= rowColumnCount) {
            return EirfType.NULL;
        }
        return rowData.get(TYPE_BYTES_OFFSET + col);
    }

    public boolean isNull(int col) {
        return getTypeByte(col) == EirfType.NULL;
    }

    public boolean getBooleanValue(int col) {
        byte type = getTypeByte(col);
        if (type == EirfType.TRUE) return true;
        if (type == EirfType.FALSE) return false;
        throw new IllegalStateException("Column " + col + " is not a boolean, type=" + EirfType.name(type));
    }

    public int getIntValue(int col) {
        int offset = computeFixedOffset(col);
        return readIntBE(offset);
    }

    public float getFloatValue(int col) {
        int offset = computeFixedOffset(col);
        return Float.intBitsToFloat(readIntBE(offset));
    }

    public long getLongValue(int col) {
        int offset = computeFixedOffset(col);
        return readLongBE(offset);
    }

    public double getDoubleValue(int col) {
        int offset = computeFixedOffset(col);
        long bits = readLongBE(offset);
        return Double.longBitsToDouble(bits);
    }

    public String getStringValue(int col) {
        int[] varSlice = getVarSlice(col);
        BytesRef bytesRef = rowData.slice(varSectionOffset + varSlice[0], varSlice[1]).toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8);
    }

    public byte[] getBinaryValue(int col) {
        int[] varSlice = getVarSlice(col);
        BytesRef bytesRef = rowData.slice(varSectionOffset + varSlice[0], varSlice[1]).toBytesRef();
        byte[] result = new byte[varSlice[1]];
        System.arraycopy(bytesRef.bytes, bytesRef.offset, result, 0, varSlice[1]);
        return result;
    }

    public byte[] getXContentValue(int col) {
        return getBinaryValue(col);
    }

    public byte[] getArrayValue(int col) {
        return getBinaryValue(col);
    }

    /**
     * Reads the var offset and length from the fixed section for the given column.
     * Handles both small (u16|u16) and large (u32|u32) variants.
     * Returns [varOffset, varLength].
     */
    private int[] getVarSlice(int col) {
        int offset = computeFixedOffset(col);
        byte type = getTypeByte(col);
        if (EirfType.isSmallVariable(type)) {
            int varOffset = EirfBatch.readU16(rowData, offset);
            int varLength = EirfBatch.readU16(rowData, offset + 2);
            return new int[] { varOffset, varLength };
        } else {
            int varOffset = rowData.getInt(offset);
            int varLength = rowData.getInt(offset + 4);
            return new int[] { varOffset, varLength };
        }
    }

    private int computeFixedOffset(int col) {
        int offset = fixedSectionOffset;
        for (int i = 0; i < col; i++) {
            offset += EirfType.fixedSize(rowData.get(TYPE_BYTES_OFFSET + i));
        }
        return offset;
    }

    // TODO: Add to BytesReference
    private long readLongBE(int offset) {
        long hi = rowData.getInt(offset) & 0xFFFFFFFFL;
        long lo = rowData.getInt(offset + 4) & 0xFFFFFFFFL;
        return (hi << 32) | lo;
    }

    private int readIntBE(int offset) {
        return rowData.getInt(offset);
    }
}
