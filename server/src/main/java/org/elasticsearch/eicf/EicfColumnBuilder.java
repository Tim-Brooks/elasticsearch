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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

/**
 * Accumulates the per-document values of a single leaf column and serialises them into an
 * {@link EicfColumnData} (four optional fields — absent bitset, type vector, offset vector, and a
 * data payload) when {@link #finish(int)} is called.
 *
 * <p>This class is a thin <b>facade</b>: it dispatches each {@code add*} call to a dedicated
 * {@link TypedBuilder} for the column's current type. Values are written <b>directly</b> into a
 * {@link RecyclerBytesStreamOutput} as they arrive — no per-value boxing — and the auxiliary
 * vectors are materialized lazily, only for the kinds that need them.
 *
 * <p><b>Promotion.</b> The first non-absent value selects the typed builder. If a later value has a
 * different base type, or an explicit {@code null} arrives, the accumulated builder is
 * <b>promoted</b> to a {@link UnionBuilder} by replaying its values; the {@link UnionBuilder}
 * thereafter accepts any type. (There is no numeric-union: a long+double mix promotes straight to
 * {@link EicfColumnKind#UNION}.)
 *
 * <p>Usage: call exactly one {@code add*} method per document, in document order; call
 * {@link #addAbsent()} for documents where this column is not present.
 */
final class EicfColumnBuilder {

    /** Recycler backing the per-column data streams; pages are returned when a builder is finished or discarded. */
    private final Recycler<BytesRef> recycler;
    /** The active typed builder, or {@code null} until the first value (or {@link #finish}). */
    private TypedBuilder current;
    /**
     * Mirrors {@code current.kind()} for the active builder ({@link EicfColumnKind#NONE} while
     * {@code current == null}). Cached as a primitive on the facade so the per-value type check in
     * {@link #ensure} is a plain field compare on {@code this} — it never dereferences {@code current},
     * avoiding a megamorphic interface call on the hot path. Kept in sync at the two points where
     * {@code current} is (re)assigned for value accumulation: {@link #ensure} and {@link #promoteToUnion}.
     */
    private byte currentKind = EicfColumnKind.NONE;
    /** Absent documents seen before the first value, backfilled when a typed builder is created. */
    private int leadingAbsents;

    EicfColumnBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_8K_INSTANCE);
    }

    EicfColumnBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    void addAbsent() {
        if (current == null) {
            leadingAbsents++;
        } else {
            current.addAbsent();
        }
    }

    void addLong(long value) {
        ensure(EicfColumnKind.LONG);
        current.addLong(value);
    }

    void addDouble(double value) {
        ensure(EicfColumnKind.DOUBLE);
        current.addDouble(value);
    }

    void addBoolean(boolean value) {
        ensure(EicfColumnKind.BOOL);
        current.addBoolean(value);
    }

    /**
     * Adds a UTF-8 string value. The slice is written directly into the data stream; it is not
     * retained, so the backing buffer may be reused immediately after this call returns.
     */
    void addString(XContentString.UTF8Bytes utf8) {
        ensure(EicfColumnKind.STRING);
        current.addString(utf8);
    }

    /**
     * Adds a raw binary value. The slice is written directly into the data stream and is not
     * retained.
     */
    void addBinary(XContentString.UTF8Bytes bytes) {
        ensure(EicfColumnKind.BINARY);
        current.addBinary(bytes);
    }

    /**
     * Adds an array value. {@code arrayType} must be {@code EirfType.FIXED_ARRAY} or
     * {@code EirfType.UNION_ARRAY}. The {@code packed} bytes are written directly into the data
     * stream and are not retained.
     */
    void addArray(byte arrayType, byte[] packed) {
        assert arrayType == EirfType.FIXED_ARRAY || arrayType == EirfType.UNION_ARRAY : "arrayType must be FIXED_ARRAY or UNION_ARRAY";
        ensure(EicfColumnKind.ARRAY);
        current.addArray(arrayType, packed);
    }

    void addNull() {
        // An explicit null always forces a union column.
        promoteToUnion();
        current.addNull();
    }

    /**
     * Determines the column kind and serialises it. A column whose every document is absent (or an
     * empty column) finishes as {@link EicfColumnKind#LONG} with an all-absent bitset.
     */
    EicfColumnData finish(int docCount) {
        if (current == null) {
            FixedNumericBuilder allAbsent = new FixedNumericBuilder(EicfColumnKind.LONG, recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                allAbsent.addAbsent();
            }
            current = allAbsent;
        }
        return current.finish(docCount);
    }

    /**
     * Releases the active typed builder's data stream without producing a column, returning its pages to
     * the recycler. Safe to call on a builder that was already {@link #finish finished} (its pages have
     * been moved out, so the underlying {@code close()} is a no-op) and on a builder that never received a
     * value ({@code current == null}).
     */
    void discard() {
        if (current != null) {
            current.discard();
        }
    }

    private void ensure(byte kind) {
        if (current == null) {
            current = newTyped(kind, recycler);
            currentKind = kind;
            for (int i = 0; i < leadingAbsents; i++) {
                current.addAbsent();
            }
            leadingAbsents = 0;
        } else if (currentKind != kind && currentKind != EicfColumnKind.UNION) {
            promoteToUnion();
        }
    }

    private void promoteToUnion() {
        // currentKind is NONE while current == null, so this never short-circuits the leading-absents path.
        if (currentKind == EicfColumnKind.UNION) {
            return;
        }
        if (current != null) {
            // Ownership of the data stream transfers to the union; the old builder must not be discarded.
            current = current.promote(recycler);
        } else {
            UnionBuilder union = new UnionBuilder(recycler);
            for (int i = 0; i < leadingAbsents; i++) {
                union.addAbsent();
            }
            current = union;
        }
        currentKind = EicfColumnKind.UNION;
        leadingAbsents = 0;
    }

    private static TypedBuilder newTyped(byte kind, Recycler<BytesRef> recycler) {
        return switch (kind) {
            case EicfColumnKind.LONG, EicfColumnKind.DOUBLE -> new FixedNumericBuilder(kind, recycler);
            case EicfColumnKind.BOOL -> new BoolBuilder();
            case EicfColumnKind.STRING, EicfColumnKind.BINARY -> new VarBuilder(kind, recycler);
            case EicfColumnKind.ARRAY -> new ArrayBuilder(recycler);
            default -> throw new IllegalArgumentException("No typed builder for kind " + EicfColumnKind.name(kind));
        };
    }

    /**
     * A dedicated accumulator for one column kind. The facade guarantees that only the
     * type-appropriate {@code add*} methods are invoked on a given implementation; the unsupported
     * ones throw {@link AssertionError} via {@link BaseBuilder}.
     */
    private interface TypedBuilder {
        byte kind();

        void addLong(long value);

        void addDouble(double value);

        void addBoolean(boolean value);

        void addString(XContentString.UTF8Bytes utf8);

        void addBinary(XContentString.UTF8Bytes bytes);

        void addArray(byte arrayType, byte[] packed);

        void addNull();

        void addAbsent();

        /**
         * Promotes this builder to a terminal {@link UnionBuilder} by adopting its buffers, avoiding any
         * per-value re-encoding. The returned union continues this column's document sequence. Ownership of
         * the data stream transfers to the union, so the promoted builder must not be used or
         * {@link #discard discarded} afterwards.
         */
        UnionBuilder promote(Recycler<BytesRef> recycler);

        /** Serialises the accumulated column into its four-field form. */
        EicfColumnData finish(int docCount);

        /** Releases any held resources without producing a column (used for a promoted-away builder). */
        void discard();
    }

    private abstract static class BaseBuilder implements TypedBuilder {
        /** Number of {@code add*} calls so far (== current document index). */
        int count;
        /** Lazily created; bit set = absent. {@code null} while no document is absent. */
        FixedBitSet absent;

        /** Records the current document index as absent. Call before incrementing {@link #count}. */
        final void markAbsent() {
            absent = absent == null ? new FixedBitSet(Math.max(64, count + 1)) : FixedBitSet.ensureCapacity(absent, count + 1);
            absent.set(count);
        }

        final boolean isAbsentAt(int d) {
            return absent != null && absent.get(d);
        }

        final BytesReference absentRef(int docCount) {
            return absent == null ? null : bitsetToRef(absent, docCount);
        }

        @Override
        public void addLong(long value) {
            throw unsupported("long");
        }

        @Override
        public void addDouble(double value) {
            throw unsupported("double");
        }

        @Override
        public void addBoolean(boolean value) {
            throw unsupported("boolean");
        }

        @Override
        public void addString(XContentString.UTF8Bytes utf8) {
            throw unsupported("string");
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            throw unsupported("binary");
        }

        @Override
        public void addArray(byte arrayType, byte[] packed) {
            throw unsupported("array");
        }

        @Override
        public void addNull() {
            throw unsupported("null");
        }

        @Override
        public void discard() {}

        private AssertionError unsupported(String type) {
            return new AssertionError("column kind " + EicfColumnKind.name(kind()) + " cannot accept a " + type + " value");
        }
    }

    /** LONG / DOUBLE: 8-byte slots (LE), one per document; absent slots are written as zero. */
    private static final class FixedNumericBuilder extends BaseBuilder {

        private final byte kind;
        private final RecyclerBytesStreamOutput data;

        FixedNumericBuilder(byte kind, Recycler<BytesRef> recycler) {
            this.kind = kind;
            this.data = newStream(recycler);
        }

        @Override
        public byte kind() {
            return kind;
        }

        @Override
        public void addLong(long value) {
            writeLongLE(data, value);
            count++;
        }

        @Override
        public void addDouble(double value) {
            writeLongLE(data, Double.doubleToRawLongBits(value));
            count++;
        }

        @Override
        public void addAbsent() {
            markAbsent();
            writeLongLE(data, 0L);
            count++;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            // The numeric data is already 8 bytes per document (absent documents hold a zero slot); adopt it
            // verbatim. The offset vector indexes the slot starts; the union reads numerics at offsets[d] and
            // ignores the offset delta, so the retained absent slots are harmless (8 wasted bytes each).
            byte present = kind == EicfColumnKind.LONG ? EirfType.LONG : EirfType.DOUBLE;
            byte[] typeVec = new byte[count];
            int[] offsets = new int[count + 1];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? EirfType.ABSENT : present;
                offsets[i] = i * 8;
            }
            offsets[count] = count * 8;
            return new UnionBuilder(data, typeVec, offsets, count * 8, count, absent);
        }

        @Override
        public EicfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return new EicfColumnData(kind, docCount, absentRef(docCount), null, null, data.moveToBytesReference());
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    /** BOOL: a value bitset (bit set = true) as the data field. */
    private static final class BoolBuilder extends BaseBuilder {
        /** Lazily created; bit set = {@code true}. {@code null} while every value seen is {@code false}/absent. */
        private FixedBitSet values;

        @Override
        public byte kind() {
            return EicfColumnKind.BOOL;
        }

        @Override
        public void addBoolean(boolean value) {
            if (value) {
                values = values == null ? new FixedBitSet(Math.max(64, count + 1)) : FixedBitSet.ensureCapacity(values, count + 1);
                values.set(count);
            }
            count++;
        }

        @Override
        public void addAbsent() {
            markAbsent();
            count++;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            // Booleans carry no payload in a union (the value is the TRUE/FALSE type byte), so the union
            // starts with an empty data buffer and an all-zero offset vector.
            byte[] typeVec = new byte[count];
            for (int i = 0; i < count; i++) {
                if (isAbsentAt(i)) {
                    typeVec[i] = EirfType.ABSENT;
                } else {
                    typeVec[i] = (values != null && values.get(i)) ? EirfType.TRUE : EirfType.FALSE;
                }
            }
            return new UnionBuilder(newStream(recycler), typeVec, new int[count + 1], 0, count, absent);
        }

        @Override
        public EicfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            return new EicfColumnData(EicfColumnKind.BOOL, docCount, absentRef(docCount), null, null, bitsetToRef(values, docCount));
        }
    }

    /** STRING / BINARY: raw bytes plus an offset vector. */
    private static final class VarBuilder extends BaseBuilder {
        private final byte kind;
        private final RecyclerBytesStreamOutput data;
        private int[] offsets = new int[16];
        private int dataLen;

        VarBuilder(byte kind, Recycler<BytesRef> recycler) {
            this.kind = kind;
            this.data = newStream(recycler);
        }

        @Override
        public byte kind() {
            return kind;
        }

        @Override
        public void addString(XContentString.UTF8Bytes utf8) {
            addBytes(utf8);
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            addBytes(bytes);
        }

        private void addBytes(XContentString.UTF8Bytes value) {
            recordOffset();
            writeBytes(data, value.bytes(), value.offset(), value.length());
            dataLen += value.length();
            count++;
        }

        @Override
        public void addAbsent() {
            recordOffset();
            markAbsent();
            count++;
        }

        private void recordOffset() {
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            // The data buffer is already dense (absent documents wrote no payload) and the offset vector
            // already matches the union layout; adopt both and only synthesize the per-document type vector.
            byte present = kind == EicfColumnKind.STRING ? EirfType.STRING : EirfType.BINARY;
            byte[] typeVec = new byte[count];
            for (int i = 0; i < count; i++) {
                typeVec[i] = isAbsentAt(i) ? EirfType.ABSENT : present;
            }
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new UnionBuilder(data, typeVec, offsets, dataLen, count, absent);
        }

        @Override
        public EicfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            // The trailing offset (index docCount) closes the last value; grow if docCount lands exactly
            // on the current capacity (the per-add recordOffset only ensures capacity up to docCount).
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new EicfColumnData(
                kind,
                docCount,
                absentRef(docCount),
                null,
                intArrayToRef(offsets, docCount + 1),
                data.moveToBytesReference()
            );
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    /** ARRAY: packed bytes plus a per-document array-type vector and an offset vector. */
    private static final class ArrayBuilder extends BaseBuilder {
        private final RecyclerBytesStreamOutput data;
        private int[] offsets = new int[16];
        private byte[] typeVec = new byte[16];
        private int dataLen;

        ArrayBuilder(Recycler<BytesRef> recycler) {
            this.data = newStream(recycler);
        }

        @Override
        public byte kind() {
            return EicfColumnKind.ARRAY;
        }

        @Override
        public void addArray(byte arrayType, byte[] packed) {
            ensureCap();
            typeVec[count] = arrayType;
            offsets[count] = dataLen;
            writeBytes(data, packed, 0, packed.length);
            dataLen += packed.length;
            count++;
        }

        @Override
        public void addAbsent() {
            ensureCap();
            typeVec[count] = EirfType.ABSENT;
            offsets[count] = dataLen;
            markAbsent();
            count++;
        }

        private void ensureCap() {
            offsets = ensureIntCapacity(offsets, count + 1);
            typeVec = ensureByteCapacity(typeVec, count + 1);
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            // ARRAY already stores exactly the union representation: a dense data buffer, an offset vector,
            // and a per-document type vector (the arrayType for present documents, ABSENT for absent ones).
            // Adopt all three with no per-value work.
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new UnionBuilder(data, typeVec, offsets, dataLen, count, absent);
        }

        @Override
        public EicfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            // The trailing offset (index docCount) closes the last value; grow if docCount lands exactly
            // on the current capacity (the per-add recordOffset only ensures capacity up to docCount).
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new EicfColumnData(
                EicfColumnKind.ARRAY,
                docCount,
                absentRef(docCount),
                byteArrayToRef(typeVec, docCount),
                intArrayToRef(offsets, docCount + 1),
                data.moveToBytesReference()
            );
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    /** UNION: a per-document {@link EirfType} vector, an offset vector, and a dense value buffer. */
    private static final class UnionBuilder extends BaseBuilder {
        private final RecyclerBytesStreamOutput data;
        private int[] offsets = new int[16];
        private byte[] typeVec = new byte[16];
        private int dataLen;

        UnionBuilder(Recycler<BytesRef> recycler) {
            this.data = newStream(recycler);
        }

        /**
         * Adopts the buffers of a promoted typed builder. Ownership of {@code data} transfers to this union;
         * {@code typeVec} and {@code offsets} hold the already-built per-document vectors (their capacity is
         * grown lazily as further values arrive), and {@code count}/{@code absent} continue this column's
         * document sequence.
         */
        UnionBuilder(RecyclerBytesStreamOutput data, byte[] typeVec, int[] offsets, int dataLen, int count, FixedBitSet absent) {
            this.data = data;
            this.typeVec = typeVec;
            this.offsets = offsets;
            this.dataLen = dataLen;
            this.count = count;
            this.absent = absent;
        }

        @Override
        public byte kind() {
            return EicfColumnKind.UNION;
        }

        @Override
        public void addLong(long value) {
            prep(EirfType.LONG);
            writeLongLE(data, value);
            dataLen += 8;
            count++;
        }

        @Override
        public void addDouble(double value) {
            prep(EirfType.DOUBLE);
            writeLongLE(data, Double.doubleToRawLongBits(value));
            dataLen += 8;
            count++;
        }

        @Override
        public void addBoolean(boolean value) {
            prep(value ? EirfType.TRUE : EirfType.FALSE);
            count++;
        }

        @Override
        public void addString(XContentString.UTF8Bytes utf8) {
            prep(EirfType.STRING);
            writeBytes(data, utf8.bytes(), utf8.offset(), utf8.length());
            dataLen += utf8.length();
            count++;
        }

        @Override
        public void addBinary(XContentString.UTF8Bytes bytes) {
            prep(EirfType.BINARY);
            writeBytes(data, bytes.bytes(), bytes.offset(), bytes.length());
            dataLen += bytes.length();
            count++;
        }

        @Override
        public void addArray(byte arrayType, byte[] packed) {
            prep(arrayType);
            writeBytes(data, packed, 0, packed.length);
            dataLen += packed.length;
            count++;
        }

        @Override
        public void addNull() {
            prep(EirfType.NULL);
            count++;
        }

        @Override
        public void addAbsent() {
            prep(EirfType.ABSENT);
            markAbsent();
            count++;
        }

        /** Records the type byte and start offset for the current document (before its payload is written). */
        private void prep(byte type) {
            offsets = ensureIntCapacity(offsets, count + 1);
            typeVec = ensureByteCapacity(typeVec, count + 1);
            typeVec[count] = type;
            offsets[count] = dataLen;
        }

        @Override
        public UnionBuilder promote(Recycler<BytesRef> recycler) {
            throw new AssertionError("a union builder is terminal and is never promoted");
        }

        @Override
        public EicfColumnData finish(int docCount) {
            assert count == docCount : "builder count " + count + " != docCount " + docCount;
            // The trailing offset (index docCount) closes the last value; grow if docCount lands exactly
            // on the current capacity (the per-add recordOffset only ensures capacity up to docCount).
            offsets = ensureIntCapacity(offsets, count + 1);
            offsets[count] = dataLen;
            return new EicfColumnData(
                EicfColumnKind.UNION,
                docCount,
                absentRef(docCount),
                byteArrayToRef(typeVec, docCount),
                intArrayToRef(offsets, docCount + 1),
                data.moveToBytesReference()
            );
        }

        @Override
        public void discard() {
            data.close();
        }
    }

    private static RecyclerBytesStreamOutput newStream(Recycler<BytesRef> recycler) {
        return new RecyclerBytesStreamOutput(recycler);
    }

    private static void writeLongLE(RecyclerBytesStreamOutput out, long value) {
        try {
            out.writeLongLE(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e); // in-memory stream never actually performs IO
        }
    }

    private static void writeBytes(RecyclerBytesStreamOutput out, byte[] bytes, int offset, int length) {
        out.writeBytes(bytes, offset, length); // RecyclerBytesStreamOutput#writeBytes does not perform IO
    }

    private static int[] ensureIntCapacity(int[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }

    private static byte[] ensureByteCapacity(byte[] array, int minSize) {
        return array.length >= minSize ? array : Arrays.copyOf(array, Math.max(minSize, array.length * 2));
    }

    /** Size of a bitset in bytes for {@code docCount} bits. */
    static int bitsetBytes(int docCount) {
        return ((docCount + 63) / 64) * 8;
    }

    /** Sets bit {@code d} in a bitset stored at {@code bitsetOffset} within {@code buf} (LE longs). */
    static void setBit(byte[] buf, int bitsetOffset, int d) {
        int wordIdx = d / 64;
        int bitIdx = d & 63;
        int bytePos = bitsetOffset + wordIdx * 8 + bitIdx / 8;
        buf[bytePos] |= (byte) (1 << (bitIdx & 7));
    }

    /**
     * Returns true if bit {@code d} is set in the bitset stored at {@code bitsetOffset} in
     * {@code src}. Bitsets are serialised as little-endian longs: bit {@code d} is at word
     * {@code d/64}, bit-position {@code d%64}.
     */
    static boolean isBitSet(BytesReference src, int bitsetOffset, int d) {
        long word = src.getLongLE(bitsetOffset + (d / 64) * 8);
        return ((word >>> (d & 63)) & 1L) != 0;
    }

    /** Serialises {@code bs} (or an all-clear bitset when {@code bs == null}) to {@code bitsetBytes(docCount)} LE bytes. */
    static BytesReference bitsetToRef(FixedBitSet bs, int docCount) {
        int n = bitsetBytes(docCount);
        byte[] out = new byte[n];
        if (bs != null) {
            long[] words = bs.getBits();
            int wordCount = n / 8;
            for (int w = 0; w < wordCount; w++) {
                long value = w < words.length ? words[w] : 0L;
                ByteUtils.writeLongLE(value, out, w * 8);
            }
        }
        return new BytesArray(out);
    }

    private static BytesReference intArrayToRef(int[] values, int length) {
        byte[] out = new byte[length * 4];
        for (int i = 0; i < length; i++) {
            ByteUtils.writeIntLE(values[i], out, i * 4);
        }
        return new BytesArray(out);
    }

    private static BytesReference byteArrayToRef(byte[] values, int length) {
        return new BytesArray(values, 0, length);
    }
}
