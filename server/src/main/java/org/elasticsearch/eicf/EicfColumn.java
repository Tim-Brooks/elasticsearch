/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * A direct-access column view over an EICF column data blob.
 *
 * <p>Unlike the row-major {@link org.elasticsearch.eirf.EirfColumn}, this implementation
 * reads values directly from the column vector without scanning any row. Absent values are
 * tracked by a per-column bitset at the start of the column blob.
 *
 * <p>All getters are pure reads that do not advance any cursor state.
 */
public final class EicfColumn implements SourceColumn {

    private final int columnIndex;
    private final byte kind;
    /** The raw column data blob (kind-specific layout; see {@link EicfColumnKind}). */
    private final BytesReference data;
    private final int docCount;
    /** Number of bytes used by each bitset section: {@code ((docCount + 63) / 64) * 8}. */
    private final int bitsetBytes;

    /**
     * Pre-computed section offsets within {@code data} (depend only on kind + docCount):
     * <ul>
     *   <li>LONG/DOUBLE:   valuesOffset = bitsetBytes</li>
     *   <li>BOOL:          valueBitsOffset = bitsetBytes</li>
     *   <li>STRING/BINARY: offsetsOffset = bitsetBytes;  bytesOffset = bitsetBytes + (docCount+1)*4</li>
     *   <li>ARRAY:         typeVecOffset = bitsetBytes;  arrOffsetsOffset = typeVecOffset + docCount;
     *                      packedOffset  = arrOffsetsOffset + (docCount+1)*4</li>
     *   <li>NUMERIC_UNION: isDecimalOffset = bitsetBytes;  nuValuesOffset = 2*bitsetBytes</li>
     *   <li>UNION:         typeVecOffset = bitsetBytes;  unionOffsetsOffset = typeVecOffset + docCount;
     *                      denseOffset   = unionOffsetsOffset + (docCount+1)*4</li>
     * </ul>
     */
    private final int offA; // first section after absent bitset
    private final int offB; // second section (only for STRING/BINARY/ARRAY/NUMERIC_UNION/UNION)
    private final int offC; // third section (only for ARRAY/UNION)

    EicfColumn(int columnIndex, byte kind, BytesReference data, int docCount) {
        this.columnIndex = columnIndex;
        this.kind = kind;
        this.data = data;
        this.docCount = docCount;
        this.bitsetBytes = EicfColumnBuilder.bitsetBytes(docCount);

        // Pre-compute offsets
        this.offA = bitsetBytes;
        switch (kind) {
            case EicfColumnKind.LONG, EicfColumnKind.DOUBLE, EicfColumnKind.BOOL -> {
                offB = 0;
                offC = 0;
            }
            case EicfColumnKind.STRING, EicfColumnKind.BINARY -> {
                offB = offA + (docCount + 1) * 4;
                offC = 0;
            }
            case EicfColumnKind.ARRAY -> {
                offB = offA + docCount;                     // offsets start
                offC = offB + (docCount + 1) * 4;          // packed bytes start
            }
            case EicfColumnKind.NUMERIC_UNION -> {
                offB = bitsetBytes + bitsetBytes;           // values start
                offC = 0;
            }
            case EicfColumnKind.UNION -> {
                offB = offA + docCount;                     // offsets start
                offC = offB + (docCount + 1) * 4;          // dense values start
            }
            default -> throw new IllegalArgumentException("Unknown column kind: " + EicfColumnKind.name(kind));
        }
    }

    @Override
    public int columnIndex() {
        return columnIndex;
    }

    @Override
    public int docCount() {
        return docCount;
    }

    // -------------------------------------------------------------------------
    // Absent / null / type
    // -------------------------------------------------------------------------

    @Override
    public boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        if (kind == EicfColumnKind.UNION) {
            // The type vector carries ABSENT for absent rows — no separate bitset check needed.
            return data.get(offA + d) == EirfType.ABSENT;
        }
        return isBitSet(0, d);
    }

    @Override
    public boolean isNull(int d) {
        return getTypeByte(d) == EirfType.NULL;
    }

    @Override
    public byte getTypeByte(int d) {
        if (d < 0 || d >= docCount) {
            return EirfType.ABSENT;
        }
        return switch (kind) {
            case EicfColumnKind.LONG -> isBitSet(0, d) ? EirfType.ABSENT : EirfType.LONG;
            case EicfColumnKind.DOUBLE -> isBitSet(0, d) ? EirfType.ABSENT : EirfType.DOUBLE;
            case EicfColumnKind.BOOL -> {
                if (isBitSet(0, d)) yield EirfType.ABSENT;
                yield isBitSet(offA, d) ? EirfType.TRUE : EirfType.FALSE;
            }
            case EicfColumnKind.STRING -> isBitSet(0, d) ? EirfType.ABSENT : EirfType.STRING;
            case EicfColumnKind.BINARY -> isBitSet(0, d) ? EirfType.ABSENT : EirfType.BINARY;
            case EicfColumnKind.ARRAY -> {
                if (isBitSet(0, d)) yield EirfType.ABSENT;
                yield data.get(offA + d);
            }
            case EicfColumnKind.NUMERIC_UNION -> {
                if (isBitSet(0, d)) yield EirfType.ABSENT;
                yield isBitSet(offA, d) ? EirfType.DOUBLE : EirfType.LONG;
            }
            case EicfColumnKind.UNION -> data.get(offA + d); // ABSENT stored in type vector
            default -> throw new IllegalStateException("Unknown kind: " + EicfColumnKind.name(kind));
        };
    }

    // -------------------------------------------------------------------------
    // Typed value getters
    // -------------------------------------------------------------------------

    @Override
    public boolean getBooleanValue(int d) {
        return switch (kind) {
            case EicfColumnKind.BOOL -> isBitSet(offA, d);
            case EicfColumnKind.UNION -> {
                byte t = data.get(offA + d);
                if (t == EirfType.TRUE) yield true;
                if (t == EirfType.FALSE) yield false;
                throw new IllegalStateException("Column " + columnIndex + " doc " + d + " is not boolean, type=" + EirfType.name(t));
            }
            default -> throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " is not boolean");
        };
    }

    @Override
    public long getLongValue(int d) {
        return switch (kind) {
            case EicfColumnKind.LONG -> data.getLongLE(offA + d * 8);
            case EicfColumnKind.NUMERIC_UNION -> data.getLongLE(offB + d * 8);
            case EicfColumnKind.UNION -> {
                int off0 = data.getIntLE(offB + d * 4);
                yield data.getLongLE(offC + off0);
            }
            default -> throw new IllegalStateException(
                "Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no long values"
            );
        };
    }

    @Override
    public double getDoubleValue(int d) {
        return switch (kind) {
            case EicfColumnKind.DOUBLE -> Double.longBitsToDouble(data.getLongLE(offA + d * 8));
            case EicfColumnKind.NUMERIC_UNION -> Double.longBitsToDouble(data.getLongLE(offB + d * 8));
            case EicfColumnKind.UNION -> {
                int off0 = data.getIntLE(offB + d * 4);
                yield Double.longBitsToDouble(data.getLongLE(offC + off0));
            }
            default -> throw new IllegalStateException(
                "Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no double values"
            );
        };
    }

    /**
     * Narrows the stored long to an int. Throws if the value does not fit in {@code int} range.
     * Callers operating on an EICF batch should prefer {@link #getLongValue}.
     */
    @Override
    public int getIntValue(int d) {
        long val = getLongValue(d);
        if (val < Integer.MIN_VALUE || val > Integer.MAX_VALUE) {
            throw new ArithmeticException("Long value " + val + " does not fit in int for column " + columnIndex);
        }
        return (int) val;
    }

    /**
     * Narrows the stored double to a float. Callers operating on an EICF batch should prefer
     * {@link #getDoubleValue}.
     */
    @Override
    public float getFloatValue(int d) {
        return (float) getDoubleValue(d);
    }

    @Override
    public Text getStringValue(int d) {
        BytesRef ref = varRef(d);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    public BytesRef getBinaryValue(int d) {
        return varRef(d);
    }

    @Override
    public EirfArrayReader getArrayValue(int d) {
        BytesRef ref;
        boolean fixed;
        if (kind == EicfColumnKind.ARRAY) {
            fixed = data.get(offA + d) == EirfType.FIXED_ARRAY;
            int off0 = data.getIntLE(offB + d * 4);
            int off1 = data.getIntLE(offB + (d + 1) * 4);
            ref = data.slice(offC + off0, off1 - off0).toBytesRef();
        } else if (kind == EicfColumnKind.UNION) {
            byte t = data.get(offA + d);
            fixed = t == EirfType.FIXED_ARRAY;
            int off0 = data.getIntLE(offB + d * 4);
            int off1 = data.getIntLE(offB + (d + 1) * 4);
            ref = data.slice(offC + off0, off1 - off0).toBytesRef();
        } else {
            throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no array values");
        }
        return new EirfArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    @Override
    public EirfKeyValueReader getKeyValue(int d) {
        if (kind == EicfColumnKind.UNION) {
            int off0 = data.getIntLE(offB + d * 4);
            int off1 = data.getIntLE(offB + (d + 1) * 4);
            BytesRef ref = data.slice(offC + off0, off1 - off0).toBytesRef();
            return new EirfKeyValueReader(ref.bytes, ref.offset, ref.length);
        }
        throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no key-value values");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Reads the variable-length value for doc {@code d} (STRING/BINARY or UNION). */
    private BytesRef varRef(int d) {
        if (kind == EicfColumnKind.STRING || kind == EicfColumnKind.BINARY) {
            int off0 = data.getIntLE(offA + d * 4);
            int off1 = data.getIntLE(offA + (d + 1) * 4);
            return data.slice(offB + off0, off1 - off0).toBytesRef();
        } else if (kind == EicfColumnKind.UNION) {
            int off0 = data.getIntLE(offB + d * 4);
            int off1 = data.getIntLE(offB + (d + 1) * 4);
            return data.slice(offC + off0, off1 - off0).toBytesRef();
        }
        throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no variable-length value");
    }

    /**
     * Returns true if bit {@code d} is set in the bitset stored at {@code bitsetOffset} in
     * {@link #data}. Bitsets are serialised as little-endian longs: bit {@code d} is at word
     * {@code d/64}, bit-position {@code d%64} within that word.
     */
    private boolean isBitSet(int bitsetOffset, int d) {
        long word = data.getLongLE(bitsetOffset + (d / 64) * 8);
        return ((word >>> (d & 63)) & 1L) != 0;
    }
}
