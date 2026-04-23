/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;

/**
 * A dense {@link LongColumn} backed by a {@link BytesReference} containing packed little-endian
 * longs with no doc-id prefix: the i-th long is the value for batch-local doc-id {@code i}.
 * <p>
 * Every page length must be a multiple of {@link Long#BYTES} so that it contains only complete
 * long values. This is validated at construction time.
 */
public final class BytesRefDenseLongColumn extends LongColumn {

    private final BytesRef[] pages;
    private final int valueCount;

    public BytesRefDenseLongColumn(String name, IndexableFieldType fieldType, BytesReference data) {
        super(name, fieldType, Density.DENSE);
        this.pages = toPageArray(data);
        this.valueCount = countValues(pages);
    }

    @Override
    public LongTupleCursor tuples() {
        return new DenseLongTupleCursor(pages, valueCount);
    }

    @Override
    public LongValuesCursor values() {
        return new DenseLongValuesCursor(pages, valueCount);
    }

    static BytesRef[] toPageArray(BytesReference data) {
        BytesRef[] pages = BytesRefLongColumn.toPageArray(data);
        for (int i = 0; i < pages.length; i++) {
            if ((pages[i].length & 7) != 0) {
                throw new IllegalArgumentException("page [" + i + "] length [" + pages[i].length + "] is not a multiple of Long.BYTES");
            }
        }
        return pages;
    }

    private static int countValues(BytesRef[] pages) {
        long total = 0;
        for (BytesRef p : pages) {
            total += p.length;
        }
        return Math.toIntExact(total / Long.BYTES);
    }

    private static final class DenseLongTupleCursor extends LongTupleCursor {

        private final BytesRef[] pages;
        private final int valueCount;

        private int pageIndex;
        private byte[] currentPage;
        private int currentOffset;
        private int currentRemaining;

        private int valuesRead;
        private long currentValue;

        DenseLongTupleCursor(BytesRef[] pages, int valueCount) {
            this.pages = pages;
            this.valueCount = valueCount;
            if (pages.length > 0) {
                BytesRef first = pages[0];
                this.currentPage = first.bytes;
                this.currentOffset = first.offset;
                this.currentRemaining = first.length;
            }
        }

        @Override
        public int nextDoc() {
            if (valuesRead >= valueCount) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            if (currentRemaining == 0) {
                advancePage();
            }
            currentValue = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(currentPage, currentOffset);
            currentOffset += Long.BYTES;
            currentRemaining -= Long.BYTES;
            return valuesRead++;
        }

        @Override
        public long longValue() {
            return currentValue;
        }

        private void advancePage() {
            pageIndex++;
            BytesRef page = pages[pageIndex];
            currentPage = page.bytes;
            currentOffset = page.offset;
            currentRemaining = page.length;
        }
    }

    private static final class DenseLongValuesCursor extends LongValuesCursor {

        private final BytesRef[] pages;
        private final int valueCount;

        private int pageIndex;
        private byte[] currentPage;
        private int currentOffset;
        private int currentRemaining;

        private int valuesRead;

        DenseLongValuesCursor(BytesRef[] pages, int valueCount) {
            this.pages = pages;
            this.valueCount = valueCount;
            if (pages.length > 0) {
                BytesRef first = pages[0];
                this.currentPage = first.bytes;
                this.currentOffset = first.offset;
                this.currentRemaining = first.length;
            }
        }

        @Override
        public int size() {
            return valueCount;
        }

        @Override
        public long nextLong() {
            if (valuesRead >= valueCount) {
                throw new IllegalStateException("exhausted: cursor produced " + valueCount + " values");
            }
            if (currentRemaining == 0) {
                advancePage();
            }
            long value = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(currentPage, currentOffset);
            currentOffset += Long.BYTES;
            currentRemaining -= Long.BYTES;
            valuesRead++;
            return value;
        }

        private void advancePage() {
            pageIndex++;
            BytesRef page = pages[pageIndex];
            currentPage = page.bytes;
            currentOffset = page.offset;
            currentRemaining = page.length;
        }
    }
}
