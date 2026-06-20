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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.xcontent.Text;

/**
 * A direct-access column view over a single EICF leaf column. Each physical column kind is a
 * distinct, {@code sealed} subtype that holds its data <b>unwrapped</b> into the primitive
 * representation it needs ({@code byte[]} / {@code int[]} / {@code long[]} and a {@link FixedBitSet}
 * absent set) rather than a chain of {@link org.elasticsearch.common.bytes.BytesReference}s. This
 * removes per-read indirection on the columnar indexing hot path and gives each kind a natural place
 * to expose specialized bulk accessors (consumed by {@link EicfLuceneColumns} when wrapping into the
 * Lucene column API).
 *
 * <p>The shared base carries identity ({@link #columnIndex()} / {@link #docCount()}) and the absent
 * set, and resolves {@link #getTypeByte}/{@link #isAbsent}/{@link #isNull} once. Typed value getters
 * default to throwing; each subtype overrides only the getters it supports. {@link #getIntValue} and
 * {@link #getFloatValue} are derived from {@link #getLongValue}/{@link #getDoubleValue}, so a subtype
 * that supports the 64-bit getter supports the narrowed one for free.
 *
 * <p>All getters are pure reads that do not advance any cursor state.
 */
public sealed abstract class EicfColumn implements SourceColumn permits EicfLongColumn, EicfDoubleColumn, EicfBoolColumn, EicfStringColumn,
    EicfBinaryColumn, EicfArrayColumn, EicfUnionColumn {

    final int columnIndex;
    final int docCount;
    /** Absent set (bit set = absent), or {@code null} when every document is present (dense). */
    final FixedBitSet absent;

    EicfColumn(int columnIndex, int docCount, FixedBitSet absent) {
        this.columnIndex = columnIndex;
        this.docCount = docCount;
        this.absent = absent;
    }

    @Override
    public final int columnIndex() {
        return columnIndex;
    }

    @Override
    public final int docCount() {
        return docCount;
    }

    /** The column kind (see {@link EicfColumnKind}). */
    abstract byte kind();

    /** The absent set (bit set = absent), or {@code null} when the column is dense. */
    final FixedBitSet absentBits() {
        return absent;
    }

    /** {@code true} when no document is absent (the absent set is {@code null}). */
    final boolean dense() {
        return absent == null;
    }

    /**
     * Builds the typed column view for {@code col}, dispatching on its {@link EicfColumnKind kind} and
     * unwrapping each present field's {@link BytesReference} into the primitive representation the
     * subtype keeps ({@code byte[]} / {@code int[]} / {@code long[]} and a {@link FixedBitSet}). This
     * one-time cost per column removes per-read {@code BytesReference} indirection.
     */
    static EicfColumn from(int columnIndex, EicfColumnData col) {
        int docCount = col.docCount();
        FixedBitSet absent = toFixedBitSet(col.absentBitset(), docCount);
        return switch (col.kind()) {
            case EicfColumnKind.LONG -> {
                BytesRef d = col.data().toBytesRef();
                yield new EicfLongColumn(columnIndex, docCount, absent, d.bytes, d.offset);
            }
            case EicfColumnKind.DOUBLE -> {
                BytesRef d = col.data().toBytesRef();
                yield new EicfDoubleColumn(columnIndex, docCount, absent, d.bytes, d.offset);
            }
            case EicfColumnKind.BOOL -> new EicfBoolColumn(columnIndex, docCount, absent, toBitsetWords(col.data(), docCount));
            case EicfColumnKind.STRING -> {
                BytesRef d = col.data().toBytesRef();
                yield new EicfStringColumn(columnIndex, docCount, absent, d.bytes, d.offset, toOffsets(col.offsets(), docCount));
            }
            case EicfColumnKind.BINARY -> {
                BytesRef d = col.data().toBytesRef();
                yield new EicfBinaryColumn(columnIndex, docCount, absent, d.bytes, d.offset, toOffsets(col.offsets(), docCount));
            }
            case EicfColumnKind.ARRAY -> {
                BytesRef d = col.data().toBytesRef();
                BytesRef tv = col.typeVector().toBytesRef();
                yield new EicfArrayColumn(
                    columnIndex,
                    docCount,
                    absent,
                    tv.bytes,
                    tv.offset,
                    toOffsets(col.offsets(), docCount),
                    d.bytes,
                    d.offset
                );
            }
            case EicfColumnKind.UNION -> {
                BytesRef d = col.data().toBytesRef();
                BytesRef tv = col.typeVector().toBytesRef();
                yield new EicfUnionColumn(
                    columnIndex,
                    docCount,
                    absent,
                    tv.bytes,
                    tv.offset,
                    toOffsets(col.offsets(), docCount),
                    d.bytes,
                    d.offset
                );
            }
            default -> throw new IllegalStateException("Unknown EICF column kind: " + EicfColumnKind.name(col.kind()));
        };
    }

    /** Materializes an absent bitset ({@code null} = dense) into a {@link FixedBitSet} over its LE-long words. */
    private static FixedBitSet toFixedBitSet(BytesReference ref, int docCount) {
        if (ref == null) {
            return null;
        }
        int words = EicfColumnBuilder.bitsetBytes(docCount) / 8;
        long[] bits = new long[words];
        for (int w = 0; w < words; w++) {
            bits[w] = ref.getLongLE(w * 8);
        }
        return new FixedBitSet(bits, words * 64);
    }

    /** Materializes a value bitset (BOOL data) into LE-long words; tolerates an empty/short payload (all false). */
    private static long[] toBitsetWords(BytesReference ref, int docCount) {
        int words = EicfColumnBuilder.bitsetBytes(docCount) / 8;
        long[] bits = new long[words];
        int len = ref.length();
        for (int w = 0; w < words; w++) {
            if (w * 8 + 8 <= len) {
                bits[w] = ref.getLongLE(w * 8);
            }
        }
        return bits;
    }

    /** Materializes the {@code (docCount + 1)} LE i32 offset vector into an {@code int[]}. */
    private static int[] toOffsets(BytesReference ref, int docCount) {
        int[] offsets = new int[docCount + 1];
        for (int i = 0; i <= docCount; i++) {
            offsets[i] = ref.getIntLE(i * 4);
        }
        return offsets;
    }

    @Override
    public final boolean isAbsent(int d) {
        if (d < 0 || d >= docCount) {
            return true;
        }
        return absent != null && absent.get(d);
    }

    @Override
    public final byte getTypeByte(int d) {
        if (d < 0 || d >= docCount || isAbsent(d)) {
            return EirfType.ABSENT;
        }
        return typeByteForPresent(d);
    }

    /** The {@link EirfType} byte for document {@code d}, which is known to be present (non-absent). */
    abstract byte typeByteForPresent(int d);

    @Override
    public final boolean isNull(int d) {
        return getTypeByte(d) == EirfType.NULL;
    }

    // -------------------------------------------------------------------------
    // Typed value getters — default to throwing; subtypes override what they support.
    // -------------------------------------------------------------------------

    @Override
    public boolean getBooleanValue(int d) {
        throw notA("boolean");
    }

    @Override
    public long getLongValue(int d) {
        throw notA("long");
    }

    @Override
    public double getDoubleValue(int d) {
        throw notA("double");
    }

    /**
     * Narrows {@link #getLongValue} to an {@code int}. Throws if the value does not fit in {@code int}
     * range. Callers operating on an EICF batch should prefer {@link #getLongValue}.
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
     * Narrows {@link #getDoubleValue} to a {@code float}. Callers operating on an EICF batch should
     * prefer {@link #getDoubleValue}.
     */
    @Override
    public float getFloatValue(int d) {
        return (float) getDoubleValue(d);
    }

    @Override
    public Text getStringValue(int d) {
        throw notA("string");
    }

    @Override
    public BytesRef getBinaryValue(int d) {
        throw notA("binary");
    }

    @Override
    public EirfArrayReader getArrayValue(int d) {
        throw notA("array");
    }

    @Override
    public EirfKeyValueReader getKeyValue(int d) {
        throw notA("key-value");
    }

    private IllegalStateException notA(String what) {
        return new IllegalStateException("Column " + columnIndex + " kind=" + EicfColumnKind.name(kind()) + " has no " + what + " values");
    }
}
