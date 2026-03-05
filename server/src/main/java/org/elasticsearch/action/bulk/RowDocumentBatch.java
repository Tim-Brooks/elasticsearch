/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable reader for a row-oriented document batch.
 *
 * <p>Binary layout (32-byte header):
 * <pre>
 * magic(4) version(4) flags(4) doc_count(4) schema_offset(4) doc_index_offset(4) data_offset(4) total_size(4)
 * [Schema]    column_count(4) + entries: name_length(4) + name_bytes
 * [Doc Index] entries[doc_count]: data_offset(4) + data_length(4)
 * [Row Data]  rows back-to-back
 * </pre>
 */
public final class RowDocumentBatch implements Releasable, Accountable {

    public static final int MAGIC = 0x45534452; // "ESDR"
    public static final int VERSION = 1;

    private final BytesReference data;
    private final Releasable releasable;
    private final int docCount;
    private final DocBatchSchema schema;
    private final int docIndexOffset;
    private final int dataOffset;

    public RowDocumentBatch(BytesReference data) {
        this(data, () -> {});
    }

    public RowDocumentBatch(BytesReference data, Releasable releasable) {
        this.data = data;
        this.releasable = releasable;

        // Parse header (32 bytes)
        int magic = data.getInt(0);
        if (magic != MAGIC) {
            throw new IllegalArgumentException(
                "Invalid magic: 0x" + Integer.toHexString(magic) + ", expected 0x" + Integer.toHexString(MAGIC)
            );
        }
        int version = data.getInt(4);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        // flags at offset 8 (reserved, ignored for now)
        this.docCount = data.getInt(12);
        int schemaOffset = data.getInt(16);
        this.docIndexOffset = data.getInt(20);
        this.dataOffset = data.getInt(24);
        // total_size at offset 28

        // Parse schema
        this.schema = parseSchema(data, schemaOffset);
    }

    private static DocBatchSchema parseSchema(BytesReference data, int offset) {
        int columnCount = data.getInt(offset);
        offset += 4;
        List<String> names = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            int nameLen = data.getInt(offset);
            offset += 4;
            BytesRef bytesRef = data.slice(offset, nameLen).toBytesRef();
            names.add(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8));
            offset += nameLen;
        }
        return new DocBatchSchema(names);
    }

    public int docCount() {
        return docCount;
    }

    public DocBatchSchema schema() {
        return schema;
    }

    public int columnCount() {
        return schema.columnCount();
    }

    public byte[] getRawData() {
        return BytesReference.toBytes(data);
    }

    public DocBatchRowReader getRowReader(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        int entryOffset = docIndexOffset + docIndex * 8;
        int rowDataOffset = data.getInt(entryOffset);
        int rowDataLength = data.getInt(entryOffset + 4);
        return new DocBatchRowReader(data, dataOffset + rowDataOffset, rowDataLength, schema);
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        return data.length() + 64; // estimate overhead
    }
}
