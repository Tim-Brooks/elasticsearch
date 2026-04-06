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
 * row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes[column_count] | fixed_section | var_section
 * </pre>
 */
public final class EirfRowReader {

    private final EirfSchema schema;
    private final BytesReference rowData;
    private final boolean smallRow;
    private final int rowColumnCount;
    private final int typeBytesOffset;
    private final int fixedSectionOffset;
    private final int varSectionOffset;

    public EirfRowReader(BytesReference rowData, EirfSchema schema) {
        this.rowData = rowData;
        this.schema = schema;

        // Parse row_flags (u8 at offset 0)
        byte rowFlags = rowData.get(0);
        this.smallRow = (rowFlags & 0x01) != 0;

        // Parse column_count (u16 LE at offset 1)
        this.rowColumnCount = EirfBatch.readU16LE(rowData, 1);

        // Parse var_offset (u16 LE or i32 LE at offset 3)
        if (smallRow) {
            this.varSectionOffset = EirfBatch.readU16LE(rowData, 3);
            this.typeBytesOffset = 5; // 1 + 2 + 2
        } else {
            this.varSectionOffset = rowData.getIntLE(3);
            this.typeBytesOffset = 7; // 1 + 2 + 4
        }
        this.fixedSectionOffset = typeBytesOffset + rowColumnCount;
    }

    public int columnCount() {
        return rowColumnCount;
    }

    public boolean isSmallRow() {
        return smallRow;
    }

    public EirfSchema schema() {
        return schema;
    }

    public byte getTypeByte(int col) {
        if (col >= rowColumnCount) {
            return EirfType.NULL;
        }
        return rowData.get(typeBytesOffset + col);
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
        return rowData.getIntLE(offset);
    }

    public float getFloatValue(int col) {
        int offset = computeFixedOffset(col);
        return Float.intBitsToFloat(rowData.getIntLE(offset));
    }

    public long getLongValue(int col) {
        int offset = computeFixedOffset(col);
        return rowData.getLongLE(offset);
    }

    public double getDoubleValue(int col) {
        int offset = computeFixedOffset(col);
        return Double.longBitsToDouble(rowData.getLongLE(offset));
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

    public byte[] getKeyValueBytes(int col) {
        return getBinaryValue(col);
    }

    public byte[] getArrayValue(int col) {
        return getBinaryValue(col);
    }

    /**
     * Reads the var offset and length from the fixed section for the given column.
     * Handles both small (u16|u16 LE) and large (i32|i32 LE) variants.
     * Returns [varOffset, varLength].
     */
    private int[] getVarSlice(int col) {
        int offset = computeFixedOffset(col);
        if (smallRow) {
            int varOffset = EirfBatch.readU16LE(rowData, offset);
            int varLength = EirfBatch.readU16LE(rowData, offset + 2);
            return new int[] { varOffset, varLength };
        } else {
            int varOffset = rowData.getIntLE(offset);
            int varLength = rowData.getIntLE(offset + 4);
            return new int[] { varOffset, varLength };
        }
    }

    private int computeFixedOffset(int col) {
        int offset = fixedSectionOffset;
        for (int i = 0; i < col; i++) {
            offset += EirfType.fixedSize(rowData.get(typeBytesOffset + i), smallRow);
        }
        return offset;
    }
}
