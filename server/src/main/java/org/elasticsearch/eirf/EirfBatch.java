/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable reader for an EIRF (Elastic Internal Row Format) batch.
 *
 * <p>Binary layout (32-byte header):
 * <pre>
 * magic(4) version(4) flags(4) doc_count(4) schema_offset(4) doc_index_offset(4) data_offset(4) total_size(4)
 * [Schema]    non_leaf_count(4) + entries + leaf_count(4) + entries
 * [Doc Index] entries[doc_count]: data_offset(4) + data_length(4)
 * [Row Data]  rows back-to-back
 * </pre>
 */
public final class EirfBatch implements Releasable, Accountable {

    public static final int MAGIC = 0x65697266; // "eirf"
    public static final int VERSION = 1;

    private final BytesReference data;
    private final Releasable releasable;
    private final int docCount;
    private final EirfSchema schema;
    private final int docIndexOffset;
    private final int dataOffset;

    public EirfBatch(BytesReference data) {
        this(data, () -> {});
    }

    public EirfBatch(BytesReference data, Releasable releasable) {
        this.data = data;
        this.releasable = releasable;

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
        this.docCount = data.getInt(12);
        int schemaOffset = data.getInt(16);
        this.docIndexOffset = data.getInt(20);
        this.dataOffset = data.getInt(24);

        this.schema = parseSchema(data, schemaOffset);
    }

    private static EirfSchema parseSchema(BytesReference data, int offset) {
        // Non-leaf fields (all u16)
        int nonLeafCount = readU16(data, offset);
        offset += 2;
        List<String> nonLeafNames = new ArrayList<>(nonLeafCount);
        int[] nonLeafParents = new int[nonLeafCount];
        for (int i = 0; i < nonLeafCount; i++) {
            nonLeafParents[i] = readU16(data, offset);
            offset += 2;
            int nameLen = readU16(data, offset);
            offset += 2;
            if (nameLen > 0) {
                BytesRef bytesRef = data.slice(offset, nameLen).toBytesRef();
                nonLeafNames.add(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8));
            } else {
                nonLeafNames.add("");
            }
            offset += nameLen;
        }

        // Leaf fields (all u16)
        int leafCount = readU16(data, offset);
        offset += 2;
        List<String> leafNames = new ArrayList<>(leafCount);
        int[] leafParents = new int[leafCount];
        for (int i = 0; i < leafCount; i++) {
            leafParents[i] = readU16(data, offset);
            offset += 2;
            int nameLen = readU16(data, offset);
            offset += 2;
            BytesRef bytesRef = data.slice(offset, nameLen).toBytesRef();
            leafNames.add(new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.UTF_8));
            offset += nameLen;
        }

        return new EirfSchema(nonLeafNames, nonLeafParents, leafNames, leafParents);
    }

    static int readU16(BytesReference data, int offset) {
        return ((data.get(offset) & 0xFF) << 8) | (data.get(offset + 1) & 0xFF);
    }

    public int docCount() {
        return docCount;
    }

    public EirfSchema schema() {
        return schema;
    }

    public int columnCount() {
        return schema.leafCount();
    }

    public EirfRowReader getRowReader(int docIndex) {
        if (docIndex < 0 || docIndex >= docCount) {
            throw new IndexOutOfBoundsException("docIndex " + docIndex + " out of range [0, " + docCount + ")");
        }
        int entryOffset = docIndexOffset + docIndex * 8;
        int rowDataOffset = data.getInt(entryOffset);
        int rowDataLength = data.getInt(entryOffset + 4);
        return new EirfRowReader(data.slice(dataOffset + rowDataOffset, rowDataLength), schema);
    }

    @Override
    public void close() {
        releasable.close();
    }

    @Override
    public long ramBytesUsed() {
        return data.length() + 64;
    }
}
