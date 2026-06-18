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
 * A direct-access column view over an EICF column's four fields: an optional absent bitset, an
 * optional per-document type vector, an optional offset vector, and a data payload. Reads are
 * driven by {@link EicfColumnKind kind} together with which fields are present:
 * <ul>
 *   <li>The <b>absent bitset</b>, when present, marks absent documents (bit set = absent).</li>
 *   <li>The <b>type vector</b>, when present (ARRAY / UNION), gives the {@link EirfType} of each
 *       document; otherwise the per-document type is implied by {@code kind}.</li>
 *   <li>The <b>offset vector</b>, when present (STRING / BINARY / ARRAY / UNION), locates each
 *       document's value in {@code data}; otherwise values are fixed 8-byte slots (LONG / DOUBLE)
 *       or {@code data} is a value bitset (BOOL).</li>
 * </ul>
 *
 * <p>All getters are pure reads that do not advance any cursor state.
 */
public final class EicfColumn implements SourceColumn {

    private final int columnIndex;
    private final byte kind;
    private final int docCount;
    /** Absent bitset (LE longs, bit set = absent), or {@code null} if no document is absent. */
    private final BytesReference absent;
    /** Per-document {@link EirfType} vector, or {@code null} when implied by {@link #kind}. */
    private final BytesReference typeVector;
    /** {@code (docCount + 1)} LE i32 offsets into {@link #data}, or {@code null} for fixed/bitset kinds. */
    private final BytesReference offsets;
    /** The value payload (LONG/DOUBLE slots, UTF-8/binary/packed bytes, a value bitset, or dense union bytes). */
    private final BytesReference data;

    EicfColumn(
        int columnIndex,
        byte kind,
        int docCount,
        BytesReference absent,
        BytesReference typeVector,
        BytesReference offsets,
        BytesReference data
    ) {
        this.columnIndex = columnIndex;
        this.kind = kind;
        this.docCount = docCount;
        this.absent = absent;
        this.typeVector = typeVector;
        this.offsets = offsets;
        this.data = data;
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
    // Package-private field accessors (used by EicfLuceneColumns adapters)
    // -------------------------------------------------------------------------

    /** The column kind (see {@link EicfColumnKind}). */
    byte kind() {
        return kind;
    }

    /** The absent bitset, or {@code null} when every document is present (dense). */
    BytesReference absentBitset() {
        return absent;
    }

    /** The offset vector, or {@code null} for fixed-width (LONG/DOUBLE) and value-bitset (BOOL) kinds. */
    BytesReference offsets() {
        return offsets;
    }

    /** The value payload. */
    BytesReference data() {
        return data;
    }

    // -------------------------------------------------------------------------
    // Absent / null / type
    // -------------------------------------------------------------------------

    @Override
    public boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        return absent != null && EicfColumnBuilder.isBitSet(absent, 0, d);
    }

    @Override
    public boolean isNull(int d) {
        return getTypeByte(d) == EirfType.NULL;
    }

    @Override
    public byte getTypeByte(int d) {
        if (d < 0 || d >= docCount || isAbsent(d)) {
            return EirfType.ABSENT;
        }
        if (typeVector != null) {
            return typeVector.get(d);
        }
        return switch (kind) {
            case EicfColumnKind.LONG -> EirfType.LONG;
            case EicfColumnKind.DOUBLE -> EirfType.DOUBLE;
            case EicfColumnKind.STRING -> EirfType.STRING;
            case EicfColumnKind.BINARY -> EirfType.BINARY;
            case EicfColumnKind.BOOL -> EicfColumnBuilder.isBitSet(data, 0, d) ? EirfType.TRUE : EirfType.FALSE;
            default -> throw new IllegalStateException("Unexpected kind without type vector: " + EicfColumnKind.name(kind));
        };
    }

    // -------------------------------------------------------------------------
    // Typed value getters
    // -------------------------------------------------------------------------

    @Override
    public boolean getBooleanValue(int d) {
        return switch (kind) {
            case EicfColumnKind.BOOL -> EicfColumnBuilder.isBitSet(data, 0, d);
            case EicfColumnKind.UNION -> {
                byte t = typeVector.get(d);
                if (t == EirfType.TRUE) yield true;
                if (t == EirfType.FALSE) yield false;
                throw new IllegalStateException("Column " + columnIndex + " doc " + d + " is not boolean, type=" + EirfType.name(t));
            }
            default -> throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " is not boolean");
        };
    }

    @Override
    public long getLongValue(int d) {
        return data.getLongLE(valueOffset(d));
    }

    @Override
    public double getDoubleValue(int d) {
        return Double.longBitsToDouble(data.getLongLE(valueOffset(d)));
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
        if (kind != EicfColumnKind.ARRAY && kind != EicfColumnKind.UNION) {
            throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no array values");
        }
        boolean fixed = typeVector.get(d) == EirfType.FIXED_ARRAY;
        BytesRef ref = varRef(d);
        return new EirfArrayReader(ref.bytes, ref.offset, ref.length, fixed);
    }

    @Override
    public EirfKeyValueReader getKeyValue(int d) {
        if (kind != EicfColumnKind.UNION) {
            throw new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind) + " has no key-value values");
        }
        BytesRef ref = varRef(d);
        return new EirfKeyValueReader(ref.bytes, ref.offset, ref.length);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Byte offset of doc {@code d}'s fixed-width value: via the offset vector, else a fixed 8-byte slot. */
    private int valueOffset(int d) {
        return offsets != null ? offsets.getIntLE(d * 4) : d * 8;
    }

    /** Reads the variable-length value for doc {@code d} via the offset vector. */
    private BytesRef varRef(int d) {
        int off0 = offsets.getIntLE(d * 4);
        int off1 = offsets.getIntLE((d + 1) * 4);
        return data.slice(off0, off1 - off0).toBytesRef();
    }
}
