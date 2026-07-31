/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Shared base for the variable-length columns (STRING and BINARY), whose values are a contiguous
 * {@code data} payload delimited by a {@code (docCount + 1)}-entry offset vector
 * ({@code [offsets[d], offsets[d + 1])} within {@code data}).
 */
abstract class AbstractVarColumn extends EscfColumn {

    final BytesReference data;
    final IntsRef offsets;

    AbstractVarColumn(int docCount, FixedBitSet validity, BytesReference data, IntsRef offsets) {
        super(docCount, validity);
        // We do not handle lifecycle here. Unwrap to reduce indirection.
        this.data = ReleasableBytesReference.unwrap(data);
        this.offsets = offsets;
        assert offsets.length == docCount + 1;
    }

    abstract AbstractVarColumn newSlice(int count, FixedBitSet sliceValidity, BytesReference sliceData, IntsRef sliceOffsets);

    /**
     * Returns a forward-only {@link ObjectTupleCursor}{@code <BytesRef>} positioned before the first
     * row of this column's window. Absent rows (clear bits in the {@link #validity} bitset) are skipped;
     * present rows are yielded in ascending order. The returned {@link BytesRef} is valid only until
     * the next {@link ObjectTupleCursor#nextDoc()} call.
     */
    @Override
    public final ObjectTupleCursor<BytesRef> bytesRefCursor() {
        return new BytesRefTupleCursor(this);
    }

    /**
     * Returns a dense {@link BytesRefValuesCursor} positioned before the first row of this column's
     * window. The column must be fully present ({@link #validity} {@code == null}); call this only on
     * dense columns. The returned {@link BytesRef} per {@link BytesRefValuesCursor#nextValue()} is
     * valid only until the next call to {@code nextValue()}.
     */
    final BytesRefValuesCursor bytesRefValuesCursor() {
        assert validity == null : "values cursor is only valid for dense (fully-present) columns";
        return new DenseBytesRefValuesCursor(docCount, this);
    }

    @Override
    final BytesRef getBinaryValue(int row) {
        int off = intAt(offsets, row);
        return data.slice(off, intAt(offsets, row + 1) - off).toBytesRef();
    }

    @Override
    final EscfColumn sliceInternal(int from, int count) {
        // data is kept full/shared; the slice is expressed by adjusting dataOffsets.offset.
        return newSlice(count, windowValidity(validity, from, count), data, sliceOffsets(offsets, from, count));
    }

    @Override
    final EscfColumnData toColumnData() {
        BytesReference newData = sliceData(offsets, data, docCount);
        int[] newOffsets = rebasedOffsets(offsets, docCount);
        return EscfColumnData.ofVarWidth(kind(), docCount, validity, newOffsets, newData);
    }

    private static final class BytesRefTupleCursor extends ObjectTupleCursor<BytesRef> {
        private final AbstractVarColumn column;
        private final DenseBytesRefValuesCursor values;
        private int row = -1;
        private BytesRef currentValue;

        BytesRefTupleCursor(AbstractVarColumn column) {
            this.column = column;
            this.values = new DenseBytesRefValuesCursor(column.docCount, column);
        }

        @Override
        public int nextDoc() {
            while (++row < column.docCount) {
                values.nextValue();
                if (column.isAbsent(row) == false) {
                    currentValue = values.stableValue();
                    return row;
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        @Override
        public BytesRef value() {
            return currentValue;
        }
    }

    private static final class DenseBytesRefValuesCursor extends BytesRefValuesCursor {

        private final BytesRefIterator iter;
        private final int[] offsets;
        private final BytesRef value = new BytesRef();
        private byte[] currentBytes = BytesRef.EMPTY_BYTES;
        private byte[] scratch = BytesRef.EMPTY_BYTES;
        private int currentBytesOffset;
        private int currentBytesEnd;
        private int nextOffsetIndex;
        private int valueOffset;
        private int pos;

        DenseBytesRefValuesCursor(int count, AbstractVarColumn column) {
            super(count);
            this.iter = sliceData(column.offsets, column.data, count).iterator();
            this.offsets = column.offsets.ints;
            this.nextOffsetIndex = column.offsets.offset + 1;
            this.valueOffset = offsets[column.offsets.offset];
        }

        private void nextChunk() {
            try {
                BytesRef chunk = iter.next();
                if (chunk == null) {
                    throw new IllegalStateException("variable-width column data exhausted before all values were read");
                }
                currentBytes = chunk.bytes;
                currentBytesOffset = chunk.offset;
                currentBytesEnd = chunk.offset + chunk.length;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private BytesRef readNextValue(int valueSize) {
            if (valueSize == 0) {
                value.bytes = BytesRef.EMPTY_BYTES;
                value.offset = 0;
                value.length = 0;
                return value;
            }
            if (currentBytesOffset >= currentBytesEnd) {
                nextChunk();
            }
            int remaining = currentBytesEnd - currentBytesOffset;
            if (valueSize <= remaining) {
                value.bytes = currentBytes;
                value.offset = currentBytesOffset;
                value.length = valueSize;
                currentBytesOffset += valueSize;
                return value;
            }

            scratch = ArrayUtil.growNoCopy(scratch, valueSize);
            int copied = 0;
            while (copied < valueSize) {
                if (currentBytesOffset >= currentBytesEnd) {
                    nextChunk();
                }
                int toCopy = Math.min(valueSize - copied, currentBytesEnd - currentBytesOffset);
                System.arraycopy(currentBytes, currentBytesOffset, scratch, copied, toCopy);
                currentBytesOffset += toCopy;
                copied += toCopy;
            }
            value.bytes = scratch;
            value.offset = 0;
            value.length = valueSize;
            return value;
        }

        private BytesRef stableValue() {
            if (value.length > 0 && value.bytes == scratch) {
                return BytesRef.deepCopyOf(value);
            }
            return value.clone();
        }

        @Override
        public BytesRef nextValue() {
            if (pos >= size()) {
                throw new IllegalStateException("nextValue() called more than size()=" + size() + " times");
            }
            int nextOffset = offsets[nextOffsetIndex++];
            int valueSize = nextOffset - valueOffset;
            valueOffset = nextOffset;
            pos++;
            return readNextValue(valueSize);
        }
    }
}
