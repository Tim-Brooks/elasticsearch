/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import static org.elasticsearch.core.Strings.format;

/**
 * File-based implementation of TranslogDurabilityLayer using FileChannels.
 * This implementation wraps the existing file-based translog logic.
 */
public class FileChannelDurabilityLayer implements TranslogDurabilityLayer {

    private final Path location;
    private final ChannelFactory channelFactory;
    private final String translogUUID;
    private final boolean fsync;

    public FileChannelDurabilityLayer(Path location, ChannelFactory channelFactory, String translogUUID, boolean fsync) {
        this.location = location;
        this.channelFactory = channelFactory;
        this.translogUUID = translogUUID;
        this.fsync = fsync;
    }

    @Override
    public GenerationHandle openForRead(long generation, Checkpoint checkpoint) throws IOException {
        Path translogPath = location.resolve(Translog.getFilename(generation));
        FileChannel channel = channelFactory.open(translogPath, StandardOpenOption.READ);
        try {
            TranslogHeader header = TranslogHeader.read(translogUUID, translogPath, channel);
            FileChannelGenerationHandle handle = new FileChannelGenerationHandle(
                generation,
                channel,
                translogPath,
                header,
                checkpoint.offset,
                true
            );
            channel = null; // Don't close on success
            return handle;
        } finally {
            IOUtils.close(channel);
        }
    }

    @Override
    public GenerationHandle createForWrite(long generation, TranslogHeader header) throws IOException {
        Path translogPath = location.resolve(Translog.getFilename(generation));
        FileChannel channel = channelFactory.open(
            translogPath,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE_NEW
        );
        try {
            header.write(channel, fsync);
            FileChannelGenerationHandle handle = new FileChannelGenerationHandle(
                generation,
                channel,
                translogPath,
                header,
                header.sizeInBytes(),
                false
            );
            channel = null; // Don't close on success
            return handle;
        } finally {
            IOUtils.close(channel);
        }
    }

    @Override
    public GenerationHandle closeWriteIntoRead(GenerationHandle writeHandle, Checkpoint checkpoint) throws IOException {
        if (writeHandle instanceof FileChannelGenerationHandle fileHandle) {
            // Close the write channel
            fileHandle.close();
            // Reopen as read-only
            return openForRead(writeHandle.generation(), checkpoint);
        }
        throw new IllegalArgumentException("Invalid handle type: " + writeHandle.getClass());
    }

    @Override
    public Checkpoint readCheckpoint() throws IOException {
        return Checkpoint.read(location.resolve(Translog.CHECKPOINT_FILE_NAME));
    }

    @Override
    public void writeCheckpoint(Checkpoint checkpoint, boolean fsync) throws IOException {
        Path checkpointPath = location.resolve(Translog.CHECKPOINT_FILE_NAME);
        try (FileChannel channel = channelFactory.open(checkpointPath, StandardOpenOption.WRITE)) {
            Checkpoint.write(channel, checkpointPath, checkpoint, fsync);
        }
    }

    @Override
    public void writeGenerationCheckpoint(long generation, Checkpoint checkpoint) throws IOException {
        Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
        Checkpoint.write(channelFactory, checkpointPath, checkpoint, StandardOpenOption.WRITE);
    }

    @Override
    public Checkpoint readGenerationCheckpoint(long generation) throws IOException {
        return Checkpoint.read(location.resolve(Translog.getCommitCheckpointFileName(generation)));
    }

    @Override
    public void deleteGeneration(long generation) throws IOException {
        Path translogPath = location.resolve(Translog.getFilename(generation));
        Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
        IOUtils.deleteFilesIgnoringExceptions(translogPath, checkpointPath);
    }

    @Override
    public String location() {
        return location.toString();
    }

    @Override
    public void copyCheckpoint(String targetCheckpointPath, String sourceCheckpointPath) throws IOException {
        // Create temp file to copy checkpoint to - note it must be on the same FS otherwise atomic move won't work
        final Path tempFile = Files.createTempFile(location, Translog.TRANSLOG_FILE_PREFIX, Translog.CHECKPOINT_SUFFIX);
        boolean tempFileRenamed = false;

        try {
            // We first copy this into the temp-file and then fsync it followed by an atomic move into the target file
            // that way if we hit a disk-full here we are still in a consistent state.
            Files.copy(location.resolve(sourceCheckpointPath), tempFile, StandardCopyOption.REPLACE_EXISTING);
            IOUtils.fsync(tempFile, false);
            Path target = location.resolve(targetCheckpointPath);
            Files.move(tempFile, target, StandardCopyOption.ATOMIC_MOVE);
            tempFileRenamed = true;
            // We only fsync the directory the tempFile was already fsynced
            IOUtils.fsync(target.getParent(), true);
        } finally {
            if (tempFileRenamed == false) {
                try {
                    Files.delete(tempFile);
                } catch (IOException ex) {
                    // Log warning if needed
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        // Nothing to close at the layer level
    }

    /**
     * FileChannel-based implementation of GenerationHandle.
     */
    static class FileChannelGenerationHandle implements GenerationHandle {
        private final long generation;
        private final FileChannel channel;
        private final Path path;
        private final TranslogHeader header;
        private final long size;
        private final boolean readOnly;
        private volatile long cachedLastModifiedTime = -1;

        FileChannelGenerationHandle(
            long generation,
            FileChannel channel,
            Path path,
            TranslogHeader header,
            long size,
            boolean readOnly
        ) {
            this.generation = generation;
            this.channel = channel;
            this.path = path;
            this.header = header;
            this.size = size;
            this.readOnly = readOnly;
        }

        @Override
        public long generation() {
            return generation;
        }

        @Override
        public long position() throws IOException {
            if (readOnly) {
                return size;
            }
            return channel.position();
        }

        @Override
        public void write(ByteBuffer data) throws IOException {
            if (readOnly) {
                throw new IllegalStateException("Cannot write to read-only handle");
            }
            while (data.hasRemaining()) {
                channel.write(data);
            }
        }

        @Override
        public void read(long position, ByteBuffer target) throws IOException {
            Channels.readFromFileChannelWithEofException(channel, position, target);
        }

        @Override
        public void force(boolean metadata) throws IOException {
            channel.force(metadata);
        }

        @Override
        public long size() throws IOException {
            if (readOnly) {
                return size;
            }
            return channel.position();
        }

        @Override
        public String path() {
            return path.toString();
        }

        @Override
        public long lastModifiedTime() throws IOException {
            if (readOnly) {
                long cached = cachedLastModifiedTime;
                if (cached == -1) {
                    cached = Files.getLastModifiedTime(path).toMillis();
                    cachedLastModifiedTime = cached;
                }
                return cached;
            }
            return Files.getLastModifiedTime(path).toMillis();
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        TranslogHeader getHeader() {
            return header;
        }
    }
}