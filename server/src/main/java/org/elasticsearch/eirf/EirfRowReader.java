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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * Zero-copy reader for a single row in EIRF format.
 *
 * <p>Row layout:
 * <pre>
 * row_flags(u8) | column_count(u16) | var_offset(u16 or i32) | type_bytes[column_count] | fixed_section | var_section
 * </pre>
 */
public final class EirfRowReader {

    private static final int ROW_FLAGS_OFFSET = 0;
    private static final int ROW_COLUMN_COUNT_OFFSET = ROW_FLAGS_OFFSET + 1;
    // The offset to read the var_offset
    private static final int ROW_VAR_SECTION_OFFSET_OFFSET = ROW_COLUMN_COUNT_OFFSET + 2;

    // Var section offset is u16 so + 2
    private static final int SMALL_ROW_TYPE_BYTES_OFFSET = ROW_VAR_SECTION_OFFSET_OFFSET + 2;
    // Var section offset is i32 so + 4
    private static final int ROW_TYPE_BYTES_OFFSET = ROW_VAR_SECTION_OFFSET_OFFSET + 4;

    private final EirfSchema schema;
    // The row's full bytes, kept for the slow path (non-array-backed BytesReference)
    private final BytesReference rowData;
    // When rowData is array-backed (the common case — rows fit comfortably in a page)
    private final byte[] rowBytes;
    private final int rowArrayOffset;
    private final boolean smallRow;
    private final int rowColumnCount;
    private final int typeBytesOffset;
    private final int fixedSectionOffset;
    private final int varSectionOffset;

    // Forward-biased cursor over the fixed section. cursorLeaf is the column index whose
    // fixed-section slot starts at cursorOffset; cursorLeaf == -1 means "before column 0",
    // in which case cursorOffset == fixedSectionOffset. Sequential access is O(1) per leaf;
    // a backward seek transparently rewinds and re-walks forward.
    private int cursorLeaf;
    private int cursorOffset;

    public EirfRowReader(BytesReference rowData, EirfSchema schema) {
        this.rowData = rowData;
        this.schema = schema;
        if (rowData.hasArray()) {
            this.rowBytes = rowData.array();
            this.rowArrayOffset = rowData.arrayOffset();
        } else {
            this.rowBytes = null;
            this.rowArrayOffset = 0;
        }

        // TODO: Could consider packing all these reads into one and unpacking the values.
        byte rowFlags = readByte(ROW_FLAGS_OFFSET);
        this.smallRow = (rowFlags & 0x01) != 0;
        this.rowColumnCount = EirfBatch.readU16LE(rowData, ROW_COLUMN_COUNT_OFFSET);

        if (smallRow) {
            this.varSectionOffset = EirfBatch.readU16LE(rowData, ROW_VAR_SECTION_OFFSET_OFFSET);
            this.typeBytesOffset = SMALL_ROW_TYPE_BYTES_OFFSET;
        } else {
            this.varSectionOffset = readIntLE(ROW_VAR_SECTION_OFFSET_OFFSET);
            this.typeBytesOffset = ROW_TYPE_BYTES_OFFSET;
        }
        this.fixedSectionOffset = typeBytesOffset + rowColumnCount;
        this.cursorLeaf = -1;
        this.cursorOffset = fixedSectionOffset;
    }

    private byte readByte(int idx) {
        return rowBytes != null ? rowBytes[rowArrayOffset + idx] : rowData.get(idx);
    }

    private int readIntLE(int idx) {
        return rowBytes != null ? ByteUtils.readIntLE(rowBytes, rowArrayOffset + idx) : rowData.getIntLE(idx);
    }

    private long readLongLE(int idx) {
        return rowBytes != null ? ByteUtils.readLongLE(rowBytes, rowArrayOffset + idx) : rowData.getLongLE(idx);
    }

    /**
     * Rewinds the fixed-section cursor to before column 0. Call this between passes that read
     * the row in column-index order (e.g. between {@code rowToSource} and per-row mapper parsing
     * inside {@code ShardBatchMapper}) so the next sequential pass avoids the backward fallback.
     */
    public void resetCursor() {
        this.cursorLeaf = -1;
        this.cursorOffset = fixedSectionOffset;
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
            return EirfType.ABSENT;
        }
        return readByte(typeBytesOffset + col);
    }

    public boolean isAbsent(int col) {
        return getTypeByte(col) == EirfType.ABSENT;
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
        int offset = seekFixedOffset(col);
        return readIntLE(offset);
    }

    public float getFloatValue(int col) {
        int offset = seekFixedOffset(col);
        return Float.intBitsToFloat(readIntLE(offset));
    }

    public long getLongValue(int col) {
        int offset = seekFixedOffset(col);
        return readLongLE(offset);
    }

    public double getDoubleValue(int col) {
        int offset = seekFixedOffset(col);
        return Double.longBitsToDouble(readLongLE(offset));
    }

    public Text getStringValue(int col) {
        BytesRef ref = getVarBytesRef(col);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    public BytesRef getBinaryValue(int col) {
        return getVarBytesRef(col);
    }

    public EirfKeyValueReader getKeyValue(int col) {
        BytesRef ref = getVarBytesRef(col);
        return new EirfKeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    public EirfArrayReader getArrayValue(int col) {
        boolean fixed = getTypeByte(col) == EirfType.FIXED_ARRAY;
        BytesRef ref = getVarBytesRef(col);
        return new EirfArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    private BytesRef getVarBytesRef(int col) {
        long packed = readVarRef(col);
        int varOffset = varRefOffset(packed);
        int varLength = varRefLength(packed);
        if (rowBytes != null) {
            return new BytesRef(rowBytes, rowArrayOffset + varSectionOffset + varOffset, varLength);
        }
        return rowData.slice(varSectionOffset + varOffset, varLength).toBytesRef();
    }

    /**
     * Reads the var offset and length from the fixed section for the given column.
     * Small row: reads one LE int containing two packed u16 (offset in low 16, length in high 16).
     * Large row: reads one LE long containing two packed i32 (offset in low 32, length in high 32).
     * Returns a packed long: offset in lower 32 bits, length in upper 32 bits.
     */
    private long readVarRef(int col) {
        int offset = seekFixedOffset(col);
        if (smallRow) {
            // Two u16 LE = one i32 LE: low 16 bits = var offset, high 16 bits = var length
            int packed = readIntLE(offset);
            return (long) (packed & 0xFFFF) | ((long) (packed >>> 16) << 32);
        } else {
            // Two i32 LE = one i64 LE: low 32 bits = var offset, high 32 bits = var length
            return readLongLE(offset);
        }
    }

    private static int varRefOffset(long packed) {
        return (int) packed;
    }

    private static int varRefLength(long packed) {
        return (int) (packed >>> 32);
    }

    /**
     * Returns the absolute offset into {@code rowData} of column {@code col}'s fixed-section
     * slot, advancing or rewinding the cursor as needed.
     *
     * <p>Forward sequential access (each leaf visited once, in 0..N order) costs one type-byte
     * read and one add per leaf. Re-reading the same column is free. A backward seek rewinds
     * the cursor to the start of the fixed section and re-walks forward — this preserves
     * random-access correctness for callers that don't iterate monotonically.
     *
     * <p>State invariant: when {@code cursorLeaf >= 0}, {@code cursorOffset} is the start of
     * column {@code cursorLeaf}'s fixed-section slot. When {@code cursorLeaf == -1} (the
     * initial / reset state), {@code cursorOffset == fixedSectionOffset}, which is also
     * column 0's offset.
     */
    private int seekFixedOffset(int col) {
        if (col < cursorLeaf) {
            cursorLeaf = -1;
            cursorOffset = fixedSectionOffset;
        }
        while (cursorLeaf < col) {
            if (cursorLeaf >= 0) {
                cursorOffset += EirfType.fixedSize(readByte(typeBytesOffset + cursorLeaf), smallRow);
            }
            cursorLeaf++;
        }
        return cursorOffset;
    }
}
