/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BinaryTupleCursor;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;

/**
 * A {@link BinaryColumn} that reads from a {@link BytesReference} containing entries
 * in the format: 4-byte LE int (doc-id) + 4-byte LE int (length) + N bytes data.
 * <p>
 * Reading is allocation-free when a value fits within a single page. When a value
 * crosses a page boundary, a temporary byte array is allocated.
 */
public final class BytesRefBinaryColumn extends BinaryColumn {

    private static final int HEADER_SIZE = 8; // 4 (docId) + 4 (length)

    private final BytesRef[] pages;
    private final int entryCount;

    public BytesRefBinaryColumn(String name, IndexableFieldType fieldType, Density density, BytesReference data, int entryCount) {
        super(name, fieldType, density);
        this.entryCount = entryCount;
        this.pages = BytesRefLongColumn.toPageArray(data);
    }

    @Override
    public BinaryTupleCursor tuples() {
        return new BinaryCursor(pages, entryCount);
    }

    private static final class BinaryCursor extends BinaryTupleCursor {

        private final BytesRef[] pages;
        private final int entryCount;

        private int pageIndex;
        private byte[] currentPage;
        private int currentOffset;
        private int currentRemaining;

        private int entriesRead;
        private int currentDocId;
        private final BytesRef valueRef = new BytesRef();

        private final byte[] headerScratch = new byte[HEADER_SIZE];
        private byte[] valueScratch = new byte[0];

        BinaryCursor(BytesRef[] pages, int entryCount) {
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

            int length;
            if (currentRemaining >= HEADER_SIZE) {
                currentDocId = (int) ByteUtils.LITTLE_ENDIAN_INT.get(currentPage, currentOffset);
                length = (int) ByteUtils.LITTLE_ENDIAN_INT.get(currentPage, currentOffset + 4);
                currentOffset += HEADER_SIZE;
                currentRemaining -= HEADER_SIZE;
            } else {
                readBytes(headerScratch, 0, HEADER_SIZE);
                currentDocId = (int) ByteUtils.LITTLE_ENDIAN_INT.get(headerScratch, 0);
                length = (int) ByteUtils.LITTLE_ENDIAN_INT.get(headerScratch, 4);
            }

            if (length <= currentRemaining) {
                valueRef.bytes = currentPage;
                valueRef.offset = currentOffset;
                valueRef.length = length;
                currentOffset += length;
                currentRemaining -= length;
            } else {
                if (valueScratch.length < length) {
                    valueScratch = new byte[length];
                }
                readBytes(valueScratch, 0, length);
                valueRef.bytes = valueScratch;
                valueRef.offset = 0;
                valueRef.length = length;
            }

            entriesRead++;
            return currentDocId;
        }

        @Override
        public BytesRef binaryValue() {
            return valueRef;
        }

        private void readBytes(byte[] target, int targetOffset, int bytes) {
            int written = 0;
            while (written < bytes) {
                if (currentRemaining == 0) {
                    advancePage();
                }
                int toCopy = Math.min(bytes - written, currentRemaining);
                System.arraycopy(currentPage, currentOffset, target, targetOffset + written, toCopy);
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
