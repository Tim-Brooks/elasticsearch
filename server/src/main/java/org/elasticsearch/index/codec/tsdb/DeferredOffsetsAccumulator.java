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
 * Buffers per-doc value counts during the encoding loop and replays them through
 * {@link DirectMonotonicWriter} after the total document count is known. This eliminates
 * the need for a separate iteration to build the address offset table for multi-valued
 * sorted-numeric fields on the flush path.
 *
 * <p>Counts are stored as VInts in a {@link ByteBuffersDataOutput} buffer to keep memory
 * usage compact (1 byte per single-valued doc, 1-2 bytes per typical multi-valued doc).
 */
public final class DeferredOffsetsAccumulator implements NumericFieldWriter.OffsetsConsumer {

    private final ByteBuffersDataOutput countBuffer = new ByteBuffersDataOutput();
    private long totalAddr = 0;
    private int numDocs = 0;

    @Override
    public void accept(int docValueCount) throws IOException {
        countBuffer.writeVInt(docValueCount);
        totalAddr += docValueCount;
        numDocs++;
    }

    /**
     * Returns true if any document had more than one value, indicating the offset table is needed.
     */
    boolean isMultiValued() {
        return totalAddr > numDocs;
    }

    /**
     * Replays the buffered counts through a {@link DirectMonotonicWriter} to build the address
     * offset table. Only call this when {@link #isMultiValued()} returns true.
     */
    void build(IndexOutput meta, IndexOutput data, int blockShift) throws IOException {
        long start = data.getFilePointer();
        meta.writeLong(start);
        meta.writeVInt(blockShift);

        final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, data, numDocs + 1L, blockShift);
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
}
