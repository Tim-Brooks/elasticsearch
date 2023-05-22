/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class WriteLayer {

    private final ReleasableLock writeLock = new ReleasableLock(new ReentrantLock());
    private final DiskIoBufferPool diskIoBufferPool;
    private final FileChannel channel;
    private final FileChannel checkpointChannel;

    public WriteLayer(DiskIoBufferPool diskIoBufferPool ,final FileChannel channel, final FileChannel checkpointChannel) {
        this.diskIoBufferPool = diskIoBufferPool;
        this.channel = channel;
        this.checkpointChannel = checkpointChannel;
    }

    public void flush(final Checkpoint checkpoint, final ReleasableBytesReference toWrite) throws IOException {
        try (ReleasableLock toClose = writeLock.acquire()) {
            try {
                // Write ops will release operations.
                writeAndReleaseOps(toWrite);
                assert channel.position() == checkpoint.offset;
            } catch (final Exception ex) {
//                closeWithTragicEvent(ex);
                throw ex;
            }
        }
    }

    public void sync() {

    }

    private void writeAndReleaseOps(ReleasableBytesReference toWrite) throws IOException {
        try (ReleasableBytesReference toClose = toWrite) {
            assert writeLock.isHeldByCurrentThread();
            final int length = toWrite.length();
            if (length == 0) {
                return;
            }
            ByteBuffer ioBuffer = diskIoBufferPool.maybeGetDirectIOBuffer();
            if (ioBuffer == null) {
                // not using a direct buffer for writes from the current thread so just write without copying to the io buffer
                BytesRefIterator iterator = toWrite.iterator();
                BytesRef current;
                while ((current = iterator.next()) != null) {
                    Channels.writeToChannel(current.bytes, current.offset, current.length, channel);
                }
                return;
            }
            BytesRefIterator iterator = toWrite.iterator();
            BytesRef current;
            while ((current = iterator.next()) != null) {
                int currentBytesConsumed = 0;
                while (currentBytesConsumed != current.length) {
                    int nBytesToWrite = Math.min(current.length - currentBytesConsumed, ioBuffer.remaining());
                    ioBuffer.put(current.bytes, current.offset + currentBytesConsumed, nBytesToWrite);
                    currentBytesConsumed += nBytesToWrite;
                    if (ioBuffer.hasRemaining() == false) {
                        ioBuffer.flip();
                        writeToFile(ioBuffer);
                        ioBuffer.clear();
                    }
                }
            }
            ioBuffer.flip();
            writeToFile(ioBuffer);
        }
    }

    @SuppressForbidden(reason = "Channel#write")
    private void writeToFile(ByteBuffer ioBuffer) throws IOException {
        while (ioBuffer.remaining() > 0) {
            channel.write(ioBuffer);
        }
    }

    private static void writeCheckpoint(final FileChannel fileChannel, final Path checkpointFile, final Checkpoint checkpoint)
        throws IOException {
        Checkpoint.write(fileChannel, checkpointFile, checkpoint);
    }
}
