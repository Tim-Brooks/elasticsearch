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
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A sparse {@link LongColumn} backed by a {@link BytesReference} whose entries are laid out
 * as 4-byte LE int (doc-id) + 8-byte LE long (value) = 12 bytes per entry.
 * <p>
 * Reading is allocation-free in the common case where an entry fits within a single page;
 * a small scratch buffer handles the rare page-boundary crossing case.
 */
public final class BytesRefLongColumn extends LongColumn {

    static final int ENTRY_SIZE = 12; // 4 (docId) + 8 (long)

    private final BytesRef[] pages;
    private final int entryCount;

    public BytesRefLongColumn(String name, IndexableFieldType fieldType, BytesReference data, int entryCount) {
        super(name, fieldType, Density.SPARSE);
        this.entryCount = entryCount;
        this.pages = toPageArray(data);
    }

    @Override
    public LongTupleCursor tuples() {
        return new SparseLongCursor(pages, entryCount);
    }

    static BytesRef[] toPageArray(BytesReference data) {
        int count = 0;
        try {
            var iter = data.iterator();
            while (iter.next() != null) {
                count++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        BytesRef[] pages = new BytesRef[count];
        try {
            var iter = data.iterator();
            BytesRef ref;
            int i = 0;
            while ((ref = iter.next()) != null) {
                pages[i++] = ref;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return pages;
    }

    private static final class SparseLongCursor extends LongTupleCursor {

        private final BytesRef[] pages;
        private final int entryCount;

        private int pageIndex;
        private byte[] currentPage;
        private int currentOffset;
        private int currentRemaining;

        private int entriesRead;
        private int currentDocId;
        private long currentValue;

        private final byte[] scratch = new byte[ENTRY_SIZE];

        SparseLongCursor(BytesRef[] pages, int entryCount) {
            this.pages = pages;
            this.entryCount = entryCount;
            if (pages.length > 0) {
                BytesRef first = pages[0];
                this.currentPage = first.bytes;
                this.currentOffset = first.offset;
                this.currentRemaining = first.length;
            }
        }

        @Override
        public int nextDoc() {
            if (entriesRead >= entryCount) {
                return DocIdSetIterator.NO_MORE_DOCS;
            }
            if (currentRemaining >= ENTRY_SIZE) {
                currentDocId = (int) ByteUtils.LITTLE_ENDIAN_INT.get(currentPage, currentOffset);
                currentValue = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(currentPage, currentOffset + 4);
                currentOffset += ENTRY_SIZE;
                currentRemaining -= ENTRY_SIZE;
            } else {
                readIntoScratch();
                currentDocId = (int) ByteUtils.LITTLE_ENDIAN_INT.get(scratch, 0);
                currentValue = (long) ByteUtils.LITTLE_ENDIAN_LONG.get(scratch, 4);
            }
            entriesRead++;
            return currentDocId;
        }

        @Override
        public long longValue() {
            return currentValue;
        }

        private void readIntoScratch() {
            int written = 0;
            while (written < ENTRY_SIZE) {
                if (currentRemaining == 0) {
                    advancePage();
                }
                int toCopy = Math.min(ENTRY_SIZE - written, currentRemaining);
                System.arraycopy(currentPage, currentOffset, scratch, written, toCopy);
                currentOffset += toCopy;
                currentRemaining -= toCopy;
                written += toCopy;
            }
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