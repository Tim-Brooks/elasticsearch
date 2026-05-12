/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;

/**
 * Buffers per-doc value counts inline with the field's main write loop, then replays them
 * through a {@link DirectMonotonicWriter} on {@link #build} to produce the per-doc address
 * table. Replaces the post-write second iteration over the field's doc values that
 * historically built this table on the flush path when merge stats were unavailable.
 *
 * <p>Counts are stored as VInts in an in-memory {@link ByteBuffersDataOutput} (typically
 * 1 byte per single-valued doc, 1-2 bytes per typical multi-valued doc), so memory usage
 * stays compact.
 */
final class DeferredOffsetsAccumulator extends OffsetsAccumulatorBase {

    private final ByteBuffersDataOutput countBuffer = new ByteBuffersDataOutput();
    private int numDocs = 0;

    /**
     * Buffers one document's value count. Must be called once per document with at least one
     * value, in doc order.
     */
    @Override
    public void addDoc(int docValueCount) throws IOException {
        countBuffer.writeVInt(docValueCount);
        numDocs++;
    }

    /**
     * Replays the buffered counts through a {@link DirectMonotonicWriter} to build the per-doc
     * address table. The bytes written to {@code meta} and {@code data} match the historical
     * flush-path fallback exactly: {@code [start (long), blockShift (vint), monotonic meta...,
     * length (long)]} in {@code meta} and the monotonic writer's address bytes in {@code data}.
     *
     * @param meta                      meta output to write the address-table header to
     * @param data                      data output to write the encoded address bytes to
     * @param numDocsWithField          the total number of documents with a value (must equal
     *                                  the number of {@link #addDoc} calls)
     * @param directMonotonicBlockShift block shift for {@link DirectMonotonicWriter}
     */
    void build(IndexOutput meta, IndexOutput data, int numDocsWithField, int directMonotonicBlockShift) throws IOException {
        assert numDocs == numDocsWithField : "buffered " + numDocs + " docs but caller reported " + numDocsWithField;
        long start = data.getFilePointer();
        meta.writeLong(start);
        meta.writeVInt(directMonotonicBlockShift);

        DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, data, numDocsWithField + 1L, directMonotonicBlockShift);
        long addr = 0;
        writer.add(addr);
        var input = countBuffer.toDataInput();
        for (int i = 0; i < numDocs; i++) {
            addr += input.readVInt();
            writer.add(addr);
        }
        writer.finish();
        meta.writeLong(data.getFilePointer() - start);
    }

    @Override
    public void close() {
        // No external resources; the buffer is GCed with this instance.
    }
}
