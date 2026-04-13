/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Writes column data for a single field across all documents in a batch. Backed by a
 * {@link RecyclerBytesStreamOutput} using pooled 16KB pages.
 * <p>
 * Long column entry layout: 4-byte LE int (doc-id) + 8-byte LE long (value) = 12 bytes.
 * Binary column entry layout: 4-byte LE int (doc-id) + 4-byte LE int (length) + N bytes data.
 */
public final class ColumnWriter implements Releasable {

    private final String fieldName;
    private final IndexableFieldType fieldType;
    private final boolean isLong;
    private final RecyclerBytesStreamOutput output;
    private int entryCount;

    public ColumnWriter(String fieldName, IndexableFieldType fieldType, boolean isLong, Recycler<BytesRef> recycler) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.isLong = isLong;
        this.output = new RecyclerBytesStreamOutput(recycler);
    }

    /**
     * Write a long entry (doc-id + value) to this column.
     */
    public void writeLong(int docId, long value) {
        BytesRef page = output.tryGetPageForWrite(12);
        if (page != null) {
            ByteUtils.LITTLE_ENDIAN_INT.set(page.bytes, page.offset, docId);
            ByteUtils.LITTLE_ENDIAN_LONG.set(page.bytes, page.offset + 4, value);
        } else {
            try {
                output.writeIntLE(docId);
                output.writeLongLE(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        entryCount++;
    }

    /**
     * Write a binary entry (doc-id + length + bytes) to this column.
     */
    public void writeBinary(int docId, BytesRef value) {
        writeBinary(docId, value.bytes, value.offset, value.length);
    }

    /**
     * Write a binary entry (doc-id + length + bytes) to this column.
     */
    public void writeBinary(int docId, byte[] bytes, int offset, int length) {
        BytesRef page = output.tryGetPageForWrite(8 + length);
        if (page != null) {
            ByteUtils.LITTLE_ENDIAN_INT.set(page.bytes, page.offset, docId);
            ByteUtils.LITTLE_ENDIAN_INT.set(page.bytes, page.offset + 4, length);
            System.arraycopy(bytes, offset, page.bytes, page.offset + 8, length);
        } else {
            try {
                output.writeIntLE(docId);
                output.writeIntLE(length);
                output.writeBytes(bytes, offset, length);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        entryCount++;
    }

    public String fieldName() {
        return fieldName;
    }

    public IndexableFieldType fieldType() {
        return fieldType;
    }

    public boolean isLong() {
        return isLong;
    }

    public int entryCount() {
        return entryCount;
    }

    /**
     * Seal this column and return the data as a {@link ReleasableBytesReference}.
     * The caller is responsible for releasing the returned reference.
     */
    public ReleasableBytesReference finish() {
        return output.moveToBytesReference();
    }

    @Override
    public void close() {
        output.close();
    }
}
