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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceColumnCursor;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;

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

    private static final Logger logger = LogManager.getLogger(EicfLuceneColumns.class);

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

    /**
     * Converts an arbitrary {@link SourceColumn} — typically a heterogeneous {@code UNION} column, or a
     * numeric column whose physical kind does not match the field — into a homogeneous numeric Lucene
     * {@link LongColumn} of the requested {@code kind}.
     *
     * <p>The column is rebuilt one document at a time through an {@link EicfColumnBuilder}: raw numerics
     * are coerced to the target type (long↔double), strings are parsed best-effort, and every other type
     * — along with unparseable strings — is skipped, leaving that document absent. The rebuilt typed
     * {@link EicfColumn} is then wrapped via {@link #longColumn(EicfColumn, String, IndexableFieldType,
     * LongColumn.NumericKind)}, which already honors the absent bitset.
     *
     * <p>Explicit JSON nulls and empty strings are replaced with {@code nullValue}, mirroring the row-major
     * path in {@code NumberFieldMapper.value} (an empty string or a {@code VALUE_NULL} token there yields the
     * field's configured {@code null_value}). When {@code nullValue} is {@code null} (no {@code null_value}
     * configured) those documents are left absent, exactly as the row path produces no value. Documents whose
     * field is genuinely absent stay absent regardless of {@code nullValue}, since the row path never invokes
     * {@code value} for a missing field.
     *
     * <p>This is the POC fallback for columns the fast path cannot wrap directly; string parsing is
     * intentionally basic and can be made stricter (coerce/range checks) later.
     */
    public static LongColumn convertToNumeric(
        SourceColumn column,
        String name,
        IndexableFieldType fieldType,
        LongColumn.NumericKind kind,
        Number nullValue
    ) {
        final boolean wantDouble = kind == LongColumn.NumericKind.FLOAT || kind == LongColumn.NumericKind.DOUBLE;
        final int docCount = column.docCount();
        final EicfColumnBuilder builder = new EicfColumnBuilder();
        final SourceColumnCursor cursor = column.cursor();
        // Reused output holder for the UTF-8 long fast path, so the common case allocates nothing per value.
        final long[] scratch = new long[1];
        while (cursor.advance()) {
            switch (cursor.type()) {
                case EirfType.LONG -> {
                    if (wantDouble) {
                        builder.addDouble((double) cursor.longValue());
                    } else {
                        builder.addLong(cursor.longValue());
                    }
                }
                case EirfType.DOUBLE -> {
                    if (wantDouble) {
                        builder.addDouble(cursor.doubleValue());
                    } else {
                        builder.addLong((long) cursor.doubleValue());
                    }
                }
                case EirfType.STRING -> {
                    final Text text = cursor.stringValue();
                    final var utf8 = text.bytes();
                    if (utf8.length() == 0) {
                        // An empty string maps to the configured null_value, matching the row path.
                        addNullValue(builder, nullValue, wantDouble);
                    } else if (wantDouble) {
                        // Float/double target: java has no byte-level double parser, so decode the String once.
                        final String s = text.string();
                        try {
                            builder.addDouble(Double.parseDouble(s));
                        } catch (NumberFormatException e) {
                            logger.info("failed to parse string [{}] as number", s, e);
                            builder.addAbsent();
                        }
                    } else {
                        // Long target: parse the UTF-8 bytes directly. The common plain-integer case allocates
                        // no String. Only decimal/exponent inputs ("2.5", "1e3") and inputs the byte parser
                        // cannot handle exactly (overflow, non-ASCII digits) fall back to decoding the String.
                        switch (tryParseAsciiLong(utf8.bytes(), utf8.offset(), utf8.length(), scratch)) {
                            case PARSE_LONG -> builder.addLong(scratch[0]);
                            // coerce: truncate the decimal to long, matching the EirfType.DOUBLE case and the
                            // row path's coerce behavior.
                            case PARSE_DECIMAL -> addParsedStringAsLong(builder, text.string(), true);
                            // Overflow / signs-only / non-ASCII digits: defer to Long.parseLong for exact
                            // semantics (e.g. Unicode digits); a genuine failure leaves the document absent.
                            case PARSE_FALLBACK -> addParsedStringAsLong(builder, text.string(), false);
                            default -> throw new AssertionError("unexpected parse result");
                        }
                    }
                }
                // An explicit JSON null maps to the configured null_value, matching the row path.
                case EirfType.NULL -> addNullValue(builder, nullValue, wantDouble);
                // Genuinely absent documents, booleans, binary, arrays, and key-value objects have no
                // numeric interpretation in this POC; leave the document without a value.
                case EirfType.ABSENT, EirfType.TRUE, EirfType.FALSE, EirfType.BINARY, EirfType.UNION_ARRAY, EirfType.FIXED_ARRAY,
                    EirfType.KEY_VALUE -> builder.addAbsent();
                default -> throw new IllegalStateException(
                    "unexpected EIRF type [" + EirfType.name(cursor.type()) + "] in column " + column.columnIndex()
                );
            }
        }
        final EicfColumn converted = EicfColumn.from(column.columnIndex(), builder.finish(docCount));
        return longColumn(converted, name, fieldType, kind);
    }

    /**
     * Appends the field's {@code null_value} to {@code builder} as the target numeric type, or leaves the
     * document absent when no {@code null_value} is configured ({@code nullValue == null}). Mirrors
     * {@code NumberFieldMapper.value} returning {@code nullValue} (possibly {@code null}) for nulls and empty
     * strings.
     */
    private static void addNullValue(EicfColumnBuilder builder, Number nullValue, boolean wantDouble) {
        if (nullValue == null) {
            builder.addAbsent();
        } else if (wantDouble) {
            builder.addDouble(nullValue.doubleValue());
        } else {
            builder.addLong(nullValue.longValue());
        }
    }

    /** {@link #tryParseAsciiLong} parsed the whole input as a {@code long}; the value is in {@code out[0]}. */
    private static final int PARSE_LONG = 0;
    /** The input holds a decimal point or exponent; parse it as a double and truncate. */
    private static final int PARSE_DECIMAL = 1;
    /** The input is not a plain ASCII integer (overflow, signs-only, or non-ASCII digits); defer to {@code String}. */
    private static final int PARSE_FALLBACK = 2;

    /**
     * Parses {@code [off, off+len)} of {@code bytes} as a base-10 {@code long} directly from UTF-8, avoiding a
     * {@code String} allocation for the common case. Numeric characters are all single-byte ASCII, so no decoding
     * is required; any byte that is not an ASCII digit, sign, or decimal/exponent marker forces the
     * {@link #PARSE_FALLBACK} path so the caller can preserve {@link Long#parseLong}'s exact semantics (including
     * Unicode digits). Overflow likewise yields {@link #PARSE_FALLBACK} rather than a wrong (truncated) value.
     *
     * <p>Accumulation is done as a negative number (mirroring {@link Long#parseLong}) so that {@link Long#MIN_VALUE}
     * is representable and overflow is detected before it happens.
     *
     * @param out receives the parsed value when {@link #PARSE_LONG} is returned; untouched otherwise
     * @return one of {@link #PARSE_LONG}, {@link #PARSE_DECIMAL}, or {@link #PARSE_FALLBACK}
     */
    static int tryParseAsciiLong(byte[] bytes, int off, int len, long[] out) {
        final int end = off + len;
        int i = off;
        boolean negative = false;
        final byte first = bytes[i];
        if (first == '-' || first == '+') {
            negative = first == '-';
            i++;
            if (i == end) {
                return PARSE_FALLBACK; // a lone "+" or "-"
            }
        }
        final long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        final long multiplyLimit = limit / 10;
        long accumulator = 0; // accumulated as a negative number
        for (; i < end; i++) {
            final byte b = bytes[i];
            if (b == '.' || b == 'e' || b == 'E') {
                return PARSE_DECIMAL;
            }
            final int digit = b - '0';
            if (digit < 0 || digit > 9) {
                return PARSE_FALLBACK; // non-ASCII-digit byte (includes any byte >= 0x80, which is negative)
            }
            if (accumulator < multiplyLimit) {
                return PARSE_FALLBACK; // accumulator * 10 would overflow
            }
            accumulator *= 10;
            if (accumulator < limit + digit) {
                return PARSE_FALLBACK; // accumulator - digit would overflow
            }
            accumulator -= digit;
        }
        out[0] = negative ? accumulator : -accumulator;
        return PARSE_LONG;
    }

    /**
     * Slow path for a numeric string targeting a long column: parses {@code s} (as a double then truncated when
     * {@code viaDouble}, else directly as a long) and appends it, or logs and leaves the document absent when the
     * string is not parseable.
     */
    private static void addParsedStringAsLong(EicfColumnBuilder builder, String s, boolean viaDouble) {
        try {
            if (viaDouble) {
                builder.addLong((long) Double.parseDouble(s));
            } else {
                builder.addLong(Long.parseLong(s));
            }
        } catch (NumberFormatException e) {
            logger.info("failed to parse string [{}] as number", s, e);
            builder.addAbsent();
        }
    }

    /**
     * Returns a builder that assembles a numeric {@link LongColumn} one document at a time, for mappers
     * that must derive a new column rather than wrap an existing one (e.g. parsing date strings to
     * epoch values, or emitting a per-document value count). The caller adds exactly one value per
     * document in order — {@link LongColumnBuilder#addLong} for a present value or
     * {@link LongColumnBuilder#addAbsent} to leave the document without one — then calls
     * {@link LongColumnBuilder#build}. Skipped documents become the column's absent bitset.
     */
    public static LongColumnBuilder longColumnBuilder(
        int docCount,
        String name,
        IndexableFieldType fieldType,
        LongColumn.NumericKind kind
    ) {
        return new LongColumnBuilder(docCount, name, fieldType, kind);
    }

    /**
     * Accumulates per-document {@code long} values (or absences) and finishes them into a homogeneous
     * numeric {@link LongColumn} via an {@link EicfColumnBuilder}. One value must be added per document,
     * in document order, before {@link #build()} is called.
     */
    public static final class LongColumnBuilder {
        private final EicfColumnBuilder builder = new EicfColumnBuilder();
        private final int docCount;
        private final String name;
        private final IndexableFieldType fieldType;
        private final LongColumn.NumericKind kind;

        LongColumnBuilder(int docCount, String name, IndexableFieldType fieldType, LongColumn.NumericKind kind) {
            this.docCount = docCount;
            this.name = name;
            this.fieldType = fieldType;
            this.kind = kind;
        }

        /** Adds the value for the next document. */
        public void addLong(long value) {
            builder.addLong(value);
        }

        /** Leaves the next document without a value (it becomes absent in the resulting column). */
        public void addAbsent() {
            builder.addAbsent();
        }

        /** Finishes the accumulated values into a {@link LongColumn}. Call exactly once. */
        public LongColumn build() {
            // The synthetic column carries no schema identity, so the column index is irrelevant here.
            return longColumn(EicfColumn.from(0, builder.finish(docCount)), name, fieldType, kind);
        }
    }

    /**
     * Returns a builder that assembles a {@link BinaryColumn} one document at a time into a single
     * contiguous byte buffer plus an offsets array, for mappers that must derive new per-document binary
     * values rather than wrap an existing column (e.g. a {@code pattern_text} field emitting its computed
     * {@code template}/{@code args} sub-columns). Packing into one buffer keeps the column cache-friendly
     * on the indexing read path, unlike a {@code BytesRef[]} of independently allocated values. The caller
     * adds exactly one entry per document, in document order — {@link BinaryColumnBuilder#addString} /
     * {@link BinaryColumnBuilder#addBytesRef} for a present value or {@link BinaryColumnBuilder#addAbsent}
     * to leave the document without one — then calls {@link BinaryColumnBuilder#build}. Skipped documents
     * become the column's absent bitset.
     */
    public static BinaryColumnBuilder binaryColumnBuilder(int docCount, String name, IndexableFieldType fieldType) {
        return new BinaryColumnBuilder(docCount, name, fieldType);
    }

    /**
     * Accumulates per-document binary values (or absences) into a contiguous {@code byte[]} + {@code int[]}
     * offsets representation and finishes them into a {@link BinaryColumn}. One value must be added per
     * document, in document order, before {@link #build()} is called. String values are UTF-8 encoded
     * through a single reused {@link BytesRefBuilder}, so the common case allocates only the growing
     * backing buffers rather than a {@link BytesRef} per value.
     */
    public static final class BinaryColumnBuilder {
        private final int docCount;
        private final String name;
        private final IndexableFieldType fieldType;
        private final int[] offsets;
        private final BytesRefBuilder scratch = new BytesRefBuilder();
        private byte[] data;
        private int dataLen;
        private int doc;
        private FixedBitSet absent;
        private boolean anyPresent;

        BinaryColumnBuilder(int docCount, String name, IndexableFieldType fieldType) {
            this.docCount = docCount;
            this.name = name;
            this.fieldType = fieldType;
            this.offsets = new int[docCount + 1];
            this.data = new byte[Math.max(16, docCount * 8)];
        }

        /** Adds the UTF-8 encoding of {@code value} as the next document's value. */
        public void addString(String value) {
            scratch.copyChars(value);
            append(scratch.bytes(), 0, scratch.length());
        }

        /** Adds a copy of {@code value}'s bytes as the next document's value. */
        public void addBytesRef(BytesRef value) {
            append(value.bytes, value.offset, value.length);
        }

        /** Adds a copy of {@code bytes[off, off+len)} as the next document's value. */
        public void addBytes(byte[] bytes, int off, int len) {
            append(bytes, off, len);
        }

        private void append(byte[] bytes, int off, int len) {
            rawAppend(bytes, off, len);
            offsets[++doc] = dataLen;
            anyPresent = true;
        }

        // -- Direct-write primitives: compose one document's value in place (no intermediate buffer) --
        // Usage: appendVInt/appendBytes one or more times, then commitValue() exactly once.

        /** Appends a base-128 VInt directly into the current document's value (matches Lucene/ES VInt). */
        public void appendVInt(int value) {
            int v = value;
            while ((v & ~0x7F) != 0) {
                ensureCapacity(1);
                data[dataLen++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            ensureCapacity(1);
            data[dataLen++] = (byte) v;
        }

        /** Appends raw bytes directly into the current document's value. */
        public void appendBytes(byte[] bytes, int off, int len) {
            rawAppend(bytes, off, len);
        }

        /** Finishes the current document's value after one or more {@code append*} calls. */
        public void commitValue() {
            offsets[++doc] = dataLen;
            anyPresent = true;
        }

        private void rawAppend(byte[] bytes, int off, int len) {
            ensureCapacity(len);
            System.arraycopy(bytes, off, data, dataLen, len);
            dataLen += len;
        }

        private void ensureCapacity(int extra) {
            if (dataLen + extra > data.length) {
                data = ArrayUtil.grow(data, dataLen + extra);
            }
        }

        /** Leaves the next document without a value (it becomes absent in the resulting column). */
        public void addAbsent() {
            if (absent == null) {
                absent = new FixedBitSet(docCount);
            }
            absent.set(doc);
            offsets[doc + 1] = dataLen;
            doc++;
        }

        /** Whether every document so far has been left absent (the column would carry no values). */
        public boolean isEmpty() {
            return anyPresent == false;
        }

        /** Finishes the accumulated values into a {@link BinaryColumn}. Call exactly once. */
        public BinaryColumn build() {
            assert doc == docCount : "added [" + doc + "] documents but expected [" + docCount + "]";
            return new EicfBinaryColumnAdapter(name, fieldType, absent, docCount, data, 0, offsets);
        }
    }

    /**
     * Builds a {@code MultiValuedBinaryDocValuesField}-SeparateCount-compatible pair of columns for a
     * multi-valued binary doc-values field: a values {@link BinaryColumn} plus a companion counts
     * {@link LongColumn}. Per document the caller begins a doc ({@code startDoc}), adds zero or more
     * values, then ends it ({@code endDoc}); the builder sorts the values (unsigned byte order) and
     * encodes the document's binary value as the raw bytes when there is a single value (no length
     * prefix) or {@code [VInt len][bytes]…} when there are several — matching
     * {@code MultiValuedBinaryDocValuesField.SeparateCount#binaryValue()} — while the counts column
     * records the per-document value count (absent when the document has no values).
     *
     * <p>It deliberately does <b>not</b> deduplicate: callers that derive values from single-valued
     * (non-array) EICF leaf columns produce unique entries by construction (e.g. a flattened field's
     * {@code key\0value} entries). Values are copied into a reused arena and written straight into the
     * column's contiguous buffer, so the hot path allocates nothing per value.
     */
    public static SeparateCountColumnBuilder separateCountColumnBuilder(
        int docCount,
        String valuesName,
        IndexableFieldType valuesType,
        String countsName,
        IndexableFieldType countsType
    ) {
        return new SeparateCountColumnBuilder(docCount, valuesName, valuesType, countsName, countsType);
    }

    /** @see #separateCountColumnBuilder */
    public static final class SeparateCountColumnBuilder {
        private final BinaryColumnBuilder values;
        private final LongColumnBuilder counts;
        // Per-document scratch: values are copied into the arena, then sorted by index without moving bytes.
        private byte[] arena = new byte[64];
        private int arenaLen;
        private int[] entryOff = new int[8];
        private int[] entryLen = new int[8];
        private int[] order = new int[8];
        private int entryCount;

        SeparateCountColumnBuilder(
            int docCount,
            String valuesName,
            IndexableFieldType valuesType,
            String countsName,
            IndexableFieldType countsType
        ) {
            this.values = new BinaryColumnBuilder(docCount, valuesName, valuesType);
            this.counts = new LongColumnBuilder(docCount, countsName, countsType, LongColumn.NumericKind.LONG);
        }

        /** Begins accumulating values for the next document. */
        public void startDoc() {
            arenaLen = 0;
            entryCount = 0;
        }

        /** Adds one value for the current document. */
        public void addValue(byte[] bytes, int off, int len) {
            if (entryCount == entryOff.length) {
                entryOff = ArrayUtil.grow(entryOff, entryCount + 1);
                entryLen = ArrayUtil.grow(entryLen, entryCount + 1);
                order = ArrayUtil.grow(order, entryCount + 1);
            }
            if (arenaLen + len > arena.length) {
                arena = ArrayUtil.grow(arena, arenaLen + len);
            }
            System.arraycopy(bytes, off, arena, arenaLen, len);
            entryOff[entryCount] = arenaLen;
            entryLen[entryCount] = len;
            arenaLen += len;
            entryCount++;
        }

        /** Adds one value for the current document from a {@link BytesRef}. */
        public void addValue(BytesRef value) {
            addValue(value.bytes, value.offset, value.length);
        }

        /** Finishes the current document, encoding its sorted values into the values + counts columns. */
        public void endDoc() {
            if (entryCount == 0) {
                values.addAbsent();
                counts.addAbsent();
                return;
            }
            if (entryCount == 1) {
                values.addBytes(arena, entryOff[0], entryLen[0]);
            } else {
                sortEntries();
                for (int i = 0; i < entryCount; i++) {
                    final int e = order[i];
                    values.appendVInt(entryLen[e]);
                    values.appendBytes(arena, entryOff[e], entryLen[e]);
                }
                values.commitValue();
            }
            counts.addLong(entryCount);
        }

        public BinaryColumn buildValues() {
            return values.build();
        }

        public LongColumn buildCounts() {
            return counts.build();
        }

        // Insertion sort of entry indices by unsigned byte order; entry counts per document are small.
        private void sortEntries() {
            for (int i = 0; i < entryCount; i++) {
                order[i] = i;
            }
            for (int i = 1; i < entryCount; i++) {
                final int current = order[i];
                int j = i - 1;
                while (j >= 0 && compareEntries(order[j], current) > 0) {
                    order[j + 1] = order[j];
                    j--;
                }
                order[j + 1] = current;
            }
        }

        private int compareEntries(int a, int b) {
            final int aOff = entryOff[a];
            final int bOff = entryOff[b];
            final int len = Math.min(entryLen[a], entryLen[b]);
            for (int i = 0; i < len; i++) {
                final int cmp = (arena[aOff + i] & 0xFF) - (arena[bOff + i] & 0xFF);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return entryLen[a] - entryLen[b];
        }
    }

    /**
     * Adapts an arbitrary {@link SourceColumn} to a {@link BinaryColumn}. An {@link EicfStringColumn} or
     * {@link EicfBinaryColumn} is wrapped directly (fast path); any other column — typically a
     * heterogeneous {@code UNION} (e.g. a keyword field that received explicit nulls) or an all-absent
     * column — is converted document-by-document via {@link #convertToBinary}.
     */
    public static BinaryColumn toBinaryColumn(SourceColumn column, String name, IndexableFieldType fieldType) {
        if (column instanceof EicfStringColumn || column instanceof EicfBinaryColumn) {
            return binaryColumn((EicfColumn) column, name, fieldType);
        }
        return convertToBinary(column, name, fieldType);
    }

    /**
     * Converts an arbitrary {@link SourceColumn} into a {@link BinaryColumn} by materializing each
     * document's value: a {@code STRING} value is copied to a {@link BytesRef}, and every other type
     * (numbers, booleans, explicit nulls, arrays, key-value objects, and absences) leaves the document
     * absent. This is the POC fallback for keyword columns the fast path cannot wrap directly — chiefly
     * UNION columns produced when a keyword field receives a mix of strings and nulls — so such columns
     * no longer force a row-major fallback. String coercion of non-string scalars can be added later.
     */
    public static BinaryColumn convertToBinary(SourceColumn column, String name, IndexableFieldType fieldType) {
        final int docCount = column.docCount();
        final BytesRef[] values = new BytesRef[docCount];
        final SourceColumnCursor cursor = column.cursor();
        int doc = 0;
        while (cursor.advance()) {
            if (cursor.type() == EirfType.STRING) {
                final XContentString.UTF8Bytes utf8 = cursor.stringValue().bytes();
                values[doc] = new BytesRef(Arrays.copyOfRange(utf8.bytes(), utf8.offset(), utf8.offset() + utf8.length()));
            } else {
                values[doc] = null;
            }
            doc++;
        }
        return arrayBinaryColumn(values, name, fieldType);
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
