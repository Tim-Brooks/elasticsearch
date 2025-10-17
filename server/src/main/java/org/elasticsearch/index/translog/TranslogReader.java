/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * an immutable translog filereader
 */
public class TranslogReader extends BaseTranslogReader implements Closeable {
    protected final long length;
    private final int totalOperations;
    private final Checkpoint checkpoint;
    protected final AtomicBoolean closed = new AtomicBoolean(false);
    private final TranslogDurabilityLayer durabilityLayer;

    /**
     * Create a translog reader against the specified generation handle.
     *
     * @param checkpoint the translog checkpoint
     * @param generationHandle the generation handle to read from
     * @param header     the header of the translog file
     * @param durabilityLayer the durability layer for checkpoint operations
     */
    TranslogReader(
        final Checkpoint checkpoint,
        final TranslogDurabilityLayer.GenerationHandle generationHandle,
        final TranslogHeader header,
        final TranslogDurabilityLayer durabilityLayer
    ) {
        super(checkpoint.generation, generationHandle, header);
        this.length = checkpoint.offset;
        this.totalOperations = checkpoint.numOps;
        this.checkpoint = checkpoint;
        this.durabilityLayer = durabilityLayer;
    }

    /**
     * Closes current reader and creates new one with new checkpoint and trimmed above sequence number
     */
    TranslogReader closeIntoTrimmedReader(long aboveSeqNo) throws IOException {
        if (closed.compareAndSet(false, true)) {
            Closeable toCloseOnFailure = generationHandle;
            final TranslogReader newReader;
            try {
                if (aboveSeqNo < checkpoint.trimmedAboveSeqNo
                    || aboveSeqNo < checkpoint.maxSeqNo && checkpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    final Checkpoint newCheckpoint = new Checkpoint(
                        checkpoint.offset,
                        checkpoint.numOps,
                        checkpoint.generation,
                        checkpoint.minSeqNo,
                        checkpoint.maxSeqNo,
                        checkpoint.globalCheckpoint,
                        checkpoint.minTranslogGeneration,
                        aboveSeqNo
                    );
                    durabilityLayer.writeGenerationCheckpoint(checkpoint.generation, newCheckpoint);

                    // Reopen with new checkpoint
                    TranslogDurabilityLayer.GenerationHandle newHandle = durabilityLayer.openForRead(checkpoint.generation, newCheckpoint);
                    newReader = new TranslogReader(newCheckpoint, newHandle, header, durabilityLayer);
                } else {
                    // No trimming needed, reuse current checkpoint
                    TranslogDurabilityLayer.GenerationHandle newHandle = durabilityLayer.openForRead(checkpoint.generation, checkpoint);
                    newReader = new TranslogReader(checkpoint, newHandle, header, durabilityLayer);
                }
                toCloseOnFailure = null;
                return newReader;
            } finally {
                IOUtils.close(toCloseOnFailure);
            }
        } else {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }

    public long sizeInBytes() {
        return length;
    }

    public int totalOperations() {
        return totalOperations;
    }

    @Override
    final Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * reads an operation at the given position into the given buffer.
     */
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        if (position >= length) {
            throw new EOFException("read requested past EOF. pos [" + position + "] end: [" + length + "]");
        }
        if (position < getFirstOperationOffset()) {
            throw new IOException(
                "read requested before position of first ops. pos [" + position + "] first op on: [" + getFirstOperationOffset() + "]"
            );
        }
        generationHandle.read(position, buffer);
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            generationHandle.close();
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }

    protected void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException(toString() + " is already closed");
        }
    }

    /**
     * Opens a TranslogReader from the given generation handle.
     *
     * @param handle the generation handle to read from
     * @param durabilityLayer the durability layer for checkpoint operations
     * @param checkpoint the checkpoint for this generation
     * @return a new TranslogReader instance
     * @throws IOException if opening the reader fails
     */
    public static TranslogReader open(
        TranslogDurabilityLayer.GenerationHandle handle,
        TranslogDurabilityLayer durabilityLayer,
        Checkpoint checkpoint
    ) throws IOException {
        if (handle instanceof FileChannelDurabilityLayer.FileChannelGenerationHandle fileHandle) {
            return new TranslogReader(checkpoint, handle, fileHandle.getHeader(), durabilityLayer);
        }
        throw new IllegalArgumentException("Invalid handle type: " + handle.getClass());
    }
}
