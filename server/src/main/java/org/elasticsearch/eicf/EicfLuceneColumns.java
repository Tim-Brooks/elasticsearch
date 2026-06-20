/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.sourcebatch.SourceColumn;

/**
 * Adapts typed EICF columns — and plain in-memory arrays (used for engine/metadata columns) — to
 * Lucene's column-oriented batch indexing API ({@link org.apache.lucene.document.column}). An EICF
 * column carries no field name or {@link IndexableFieldType}, so the caller supplies both.
 *
 * <p>Dispatch is on the concrete {@link EicfColumn} subtype, and the adapters read the column's
 * already-unwrapped primitives ({@code byte[]} / {@code int[]} and a {@link FixedBitSet} absent set)
 * directly — no {@link org.elasticsearch.common.bytes.BytesReference} indirection on the read path:
 * <ul>
 *   <li>{@link EicfLongColumn} / {@link EicfDoubleColumn} → {@link LongColumn}.</li>
 *   <li>{@link EicfStringColumn} / {@link EicfBinaryColumn} → {@link BinaryColumn}.</li>
 * </ul>
 * Other kinds (BOOL, ARRAY, UNION) are not yet adaptable and throw.
 *
 * <p>The numeric interpretation of a long column can be selected explicitly with the
 * {@link LongColumn.NumericKind} overloads: an integral kind (INT/LONG) emits the EICF long value
 * unchanged, while a decimal kind interprets the EICF double bits and emits the sortable encoding
 * Lucene expects (FLOAT via {@link NumericUtils#floatToSortableInt}, DOUBLE via
 * {@link NumericUtils#doubleToSortableLong}).
 *
 * <p>The {@code arrayLongColumn}/{@code arrayBinaryColumn} factories wrap plain Java arrays (no EICF
 * backing) so the bulk-indexing path can build metadata columns (_id, _source, _seq_no, ...) and
 * fill mutable seq-no/version values from the engine.
 */
public final class EicfLuceneColumns {

    private EicfLuceneColumns() {}

    /**
     * Adapts {@code column} to the matching Lucene {@link Column} subtype, dispatching on its concrete
     * type.
     *
     * @throws UnsupportedOperationException if the column's kind is not yet adaptable
     */
    public static Column of(EicfColumn column, String name, IndexableFieldType fieldType) {
        if (column instanceof EicfLongColumn || column instanceof EicfDoubleColumn) {
            return longColumn(column, name, fieldType);
        }
        if (column instanceof EicfStringColumn || column instanceof EicfBinaryColumn) {
            return binaryColumn(column, name, fieldType);
        }
        throw new UnsupportedOperationException(
            "EICF column kind " + EicfColumnKind.name(column.kind()) + " is not adaptable to a Lucene column"
        );
    }

    /**
     * Adapts an {@link EicfLongColumn} or {@link EicfDoubleColumn} to a {@link LongColumn}, deriving the
     * numeric kind from the column type (long → LONG, double → DOUBLE).
     */
    public static LongColumn longColumn(EicfColumn column, String name, IndexableFieldType fieldType) {
        if (column instanceof EicfDoubleColumn) {
            return longColumn(column, name, fieldType, LongColumn.NumericKind.DOUBLE);
        }
        if (column instanceof EicfLongColumn) {
            return longColumn(column, name, fieldType, LongColumn.NumericKind.LONG);
        }
        throw new IllegalArgumentException("longColumn requires a LONG or DOUBLE column, got " + EicfColumnKind.name(column.kind()));
    }

    /**
     * Adapts a numeric EICF column to a {@link LongColumn} with an explicit {@link LongColumn.NumericKind}.
     * Both {@link EicfLongColumn} and {@link EicfDoubleColumn} expose their raw 8-byte slot buffer; the
     * kind drives how each slot is encoded for Lucene (see {@link #encode}).
     */
    public static LongColumn longColumn(EicfColumn column, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        final byte[] bytes;
        final int base;
        if (column instanceof EicfLongColumn l) {
            bytes = l.valueBytes();
            base = l.valueBase();
        } else if (column instanceof EicfDoubleColumn d) {
            bytes = d.valueBytes();
            base = d.valueBase();
        } else {
            throw new IllegalArgumentException("longColumn requires a LONG or DOUBLE column, got " + EicfColumnKind.name(column.kind()));
        }
        return new EicfLongColumnAdapter(name, fieldType, column.absentBits(), column.docCount(), bytes, base, kind);
    }

    /**
     * Adapts a {@link SourceColumn} to a numeric {@link LongColumn} with an explicit numeric kind.
     * The column must be EICF-backed and numeric; otherwise this throws to signal the caller should
     * fall back to the row-major path.
     */
    public static LongColumn longColumn(SourceColumn column, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        if (column instanceof EicfColumn ec) {
            return longColumn(ec, name, fieldType, kind);
        }
        throw new IllegalArgumentException("columnar numeric indexing requires an EICF column, got " + column.getClass().getSimpleName());
    }

    /** Adapts an {@link EicfStringColumn} or {@link EicfBinaryColumn} to a {@link BinaryColumn}. */
    public static BinaryColumn binaryColumn(EicfColumn column, String name, IndexableFieldType fieldType) {
        final byte[] data;
        final int dataBase;
        final int[] offsets;
        if (column instanceof EicfStringColumn s) {
            data = s.dataBytes();
            dataBase = s.dataBase();
            offsets = s.offsets();
        } else if (column instanceof EicfBinaryColumn b) {
            data = b.dataBytes();
            dataBase = b.dataBase();
            offsets = b.offsets();
        } else {
            throw new IllegalArgumentException(
                "binaryColumn requires a STRING or BINARY column, got " + EicfColumnKind.name(column.kind())
            );
        }
        return new EicfBinaryColumnAdapter(name, fieldType, column.absentBits(), column.docCount(), data, dataBase, offsets);
    }

    /**
     * A {@link LongColumn} backed by a plain {@code long[]} (no EICF column). The array may be mutated
     * by the caller (e.g. the engine filling seq-no/version) up until a cursor is requested. Always
     * {@link Column.Density#DENSE DENSE}; values are emitted unchanged (caller pre-encodes if needed).
     */
    public static LongColumn arrayLongColumn(long[] values, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
        return new ArrayLongColumn(values, name, fieldType, kind);
    }

    /**
     * A {@link BinaryColumn} backed by a {@code BytesRef[]} (no EICF column). A {@code null} entry marks
     * an absent document; the column is {@link Column.Density#DENSE DENSE} only when every entry is present.
     */
    public static BinaryColumn arrayBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
        return new ArrayBinaryColumn(values, name, fieldType);
    }

    /** Returns the next present (non-absent) batch-local doc-id strictly after {@code after}, or {@code docCount} if none. */
    private static int nextPresent(FixedBitSet absent, int docCount, int after) {
        int d = after + 1;
        if (absent != null) {
            while (d < docCount && absent.get(d)) {
                d++;
            }
        }
        return d;
    }

    /** Encodes an EICF raw 8-byte slot to the Lucene long value for the given numeric kind. */
    private static long encode(long raw, LongColumn.NumericKind kind) {
        return switch (kind) {
            case INT, LONG -> raw;
            case DOUBLE -> NumericUtils.doubleToSortableLong(Double.longBitsToDouble(raw));
            case FLOAT -> NumericUtils.floatToSortableInt((float) Double.longBitsToDouble(raw)) & 0xFFFF_FFFFL;
        };
    }

    // -------------------------------------------------------------------------
    // EICF-backed LongColumn adapter
    // -------------------------------------------------------------------------

    private static final class EicfLongColumnAdapter extends LongColumn {
        private final FixedBitSet absent;
        private final int docCount;
        private final byte[] bytes;
        private final int base;

        EicfLongColumnAdapter(
            String name,
            IndexableFieldType fieldType,
            FixedBitSet absent,
            int docCount,
            byte[] bytes,
            int base,
            NumericKind kind
        ) {
            super(name, fieldType, absent == null ? Density.DENSE : Density.SPARSE, kind);
            this.absent = absent;
            this.docCount = docCount;
            this.bytes = bytes;
            this.base = base;
        }

        @Override
        public LongTupleCursor tuples() {
            return new LongTupleCursor() {
                private int doc = -1;
                private long value;

                @Override
                public int nextDoc() {
                    final NumericKind kind = numericKind();

                    int next = nextPresent(absent, docCount, doc);
                    if (next >= docCount) {
                        doc = docCount;
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    doc = next;
                    value = encode(ByteUtils.readLongLE(bytes, base + next * 8), kind);
                    return next;
                }

                @Override
                public long longValue() {
                    return value;
                }
            };
        }

        @Override
        public LongValuesCursor values() {
            if (density() != Density.DENSE) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new DenseEicfLongCursor(bytes, base, docCount, numericKind());
        }
    }

    /** Dense bulk cursor over a LONG/DOUBLE column's contiguous 8-byte slots. */
    private static final class DenseEicfLongCursor extends LongValuesCursor {
        private final byte[] bytes;
        private final int base;
        private final LongColumn.NumericKind kind;
        private int pos;

        DenseEicfLongCursor(byte[] bytes, int base, int docCount, LongColumn.NumericKind kind) {
            super(docCount);
            this.bytes = bytes;
            this.base = base;
            this.kind = kind;
        }

        @Override
        public long nextLong() {
            if (pos >= size()) {
                throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
            }
            return encode(ByteUtils.readLongLE(bytes, base + pos++ * 8), kind);
        }

        @Override
        public void fillDocValues(long[] dst, int offset, int length) {
            if (pos + length > size()) {
                throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
            }
            for (int i = 0; i < length; i++) {
                dst[offset + i] = encode(ByteUtils.readLongLE(bytes, base + (pos + i) * 8), kind);
            }
            pos += length;
        }
    }

    // -------------------------------------------------------------------------
    // EICF-backed BinaryColumn adapter
    // -------------------------------------------------------------------------

    private static final class EicfBinaryColumnAdapter extends BinaryColumn {
        private final FixedBitSet absent;
        private final int docCount;
        private final byte[] data;
        private final int dataBase;
        private final int[] offsets;

        EicfBinaryColumnAdapter(
            String name,
            IndexableFieldType fieldType,
            FixedBitSet absent,
            int docCount,
            byte[] data,
            int dataBase,
            int[] offsets
        ) {
            super(name, fieldType, absent == null ? Density.DENSE : Density.SPARSE);
            this.absent = absent;
            this.docCount = docCount;
            this.data = data;
            this.dataBase = dataBase;
            this.offsets = offsets;
        }

        @Override
        public ObjectTupleCursor<BytesRef> tuples() {
            return new ObjectTupleCursor<>() {
                private final BytesRef scratch = new BytesRef();
                private int doc = -1;

                @Override
                public int nextDoc() {
                    int next = nextPresent(absent, docCount, doc);
                    if (next >= docCount) {
                        doc = docCount;
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    doc = next;
                    int off0 = offsets[next];
                    scratch.bytes = data;
                    scratch.offset = dataBase + off0;
                    scratch.length = offsets[next + 1] - off0;
                    return next;
                }

                @Override
                public BytesRef value() {
                    return scratch;
                }
            };
        }

        @Override
        public BytesRefValuesCursor values() {
            if (density() != Density.DENSE) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new DenseEicfBytesCursor(data, dataBase, offsets, docCount);
        }
    }

    /** Dense bulk cursor over a STRING/BINARY column's offset-delimited values. */
    private static final class DenseEicfBytesCursor extends BytesRefValuesCursor {
        private final byte[] data;
        private final int dataBase;
        private final int[] offsets;
        private final BytesRef scratch = new BytesRef();
        private int pos;

        DenseEicfBytesCursor(byte[] data, int dataBase, int[] offsets, int docCount) {
            super(docCount);
            this.data = data;
            this.dataBase = dataBase;
            this.offsets = offsets;
        }

        @Override
        public BytesRef nextValue() {
            if (pos >= size()) {
                throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
            }
            int off0 = offsets[pos];
            int off1 = offsets[pos + 1];
            pos++;
            scratch.bytes = data;
            scratch.offset = dataBase + off0;
            scratch.length = off1 - off0;
            return scratch;
        }

        @Override
        public void fillPackedPoints(byte[] dst, int offset, int length, int width) {
            if (pos + length > size()) {
                throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
            }
            for (int i = 0; i < length; i++) {
                int valueLen = offsets[pos + i + 1] - offsets[pos + i];
                if (valueLen != width) {
                    throw new IllegalArgumentException("dense point value has length=" + valueLen + " but should be " + width);
                }
            }
            int startByte = offsets[pos];
            System.arraycopy(data, dataBase + startByte, dst, offset, length * width);
            pos += length;
        }
    }

    // -------------------------------------------------------------------------
    // Array-backed columns (engine / metadata; no EICF backing)
    // -------------------------------------------------------------------------

    private static final class ArrayLongColumn extends LongColumn {
        private final long[] values;

        ArrayLongColumn(long[] values, String name, IndexableFieldType fieldType, NumericKind kind) {
            super(name, fieldType, Density.DENSE, kind);
            this.values = values;
        }

        @Override
        public LongTupleCursor tuples() {
            return new LongTupleCursor() {
                private int doc = -1;

                @Override
                public int nextDoc() {
                    return ++doc < values.length ? doc : DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public long longValue() {
                    return values[doc];
                }
            };
        }

        @Override
        public LongValuesCursor values() {
            return new LongValuesCursor(values.length) {
                private int pos;

                @Override
                public long nextLong() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextLong() called more than size()=" + size() + " times");
                    }
                    return values[pos++];
                }

                @Override
                public void fillDocValues(long[] dst, int offset, int length) {
                    if (pos + length > size()) {
                        throw new IllegalStateException("fill of " + length + " from pos " + pos + " exceeds size()=" + size());
                    }
                    System.arraycopy(values, pos, dst, offset, length);
                    pos += length;
                }
            };
        }
    }

    private static final class ArrayBinaryColumn extends BinaryColumn {
        private final BytesRef[] values;
        private final boolean dense;

        ArrayBinaryColumn(BytesRef[] values, String name, IndexableFieldType fieldType) {
            super(name, fieldType, allPresent(values) ? Density.DENSE : Density.SPARSE);
            this.values = values;
            this.dense = allPresent(values);
        }

        private static boolean allPresent(BytesRef[] values) {
            for (BytesRef v : values) {
                if (v == null) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public ObjectTupleCursor<BytesRef> tuples() {
            return new ObjectTupleCursor<>() {
                private int doc = -1;

                @Override
                public int nextDoc() {
                    int next = doc + 1;
                    while (next < values.length && values[next] == null) {
                        next++;
                    }
                    doc = next;
                    return next < values.length ? next : DocIdSetIterator.NO_MORE_DOCS;
                }

                @Override
                public BytesRef value() {
                    return values[doc];
                }
            };
        }

        @Override
        public BytesRefValuesCursor values() {
            if (dense == false) {
                return super.values(); // throws; never consulted for SPARSE columns
            }
            return new BytesRefValuesCursor(values.length) {
                private int pos;

                @Override
                public BytesRef nextValue() {
                    if (pos >= size()) {
                        throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
                    }
                    return values[pos++];
                }
            };
        }
    }
}
