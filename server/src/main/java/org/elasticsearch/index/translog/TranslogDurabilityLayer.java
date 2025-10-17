/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstraction for translog durability - handles persistent storage of translog data.
 * This interface decouples the translog logic from the underlying storage mechanism,
 * allowing for different implementations (file-based, in-memory, remote storage, etc.).
 */
public interface TranslogDurabilityLayer extends Closeable {

    /**
     * A handle to a specific generation's data. This represents either a read-only
     * or writable view of a single translog generation file.
     */
    interface GenerationHandle extends Closeable {

        /**
         * Returns the generation number for this handle.
         */
        long generation();

        /**
         * Returns the current position (write offset) in this generation.
         * For read-only handles, this returns the total size.
         */
        long position() throws IOException;

        /**
         * Writes data from the buffer to this generation at the current position.
         * This is only valid for write handles.
         *
         * @param data the data to write
         * @throws IOException if an I/O error occurs
         */
        void write(ByteBuffer data) throws IOException;

        /**
         * Reads data from the specified position into the target buffer.
         * The buffer's position will be updated to reflect the bytes read.
         *
         * @param position the position to read from
         * @param target the buffer to read into
         * @throws IOException if an I/O error occurs
         */
        void read(long position, ByteBuffer target) throws IOException;

        /**
         * Forces any buffered data to be written to durable storage.
         *
         * @param metadata if true, also sync file metadata
         * @throws IOException if an I/O error occurs
         */
        void force(boolean metadata) throws IOException;

        /**
         * Returns the total size of this generation in bytes.
         */
        long size() throws IOException;

        /**
         * Returns the path to this generation's file (for diagnostic purposes).
         */
        String path();

        /**
         * Returns the last modified time of this generation in milliseconds since epoch.
         */
        long lastModifiedTime() throws IOException;
    }

    /**
     * Opens an existing generation for reading.
     *
     * @param generation the generation number to open
     * @param checkpoint the checkpoint for this generation
     * @return a read-only handle to the generation
     * @throws IOException if the generation cannot be opened
     */
    GenerationHandle openForRead(long generation, Checkpoint checkpoint) throws IOException;

    /**
     * Creates a new generation for writing with the given header.
     *
     * @param generation the generation number to create
     * @param header the translog header to write
     * @return a writable handle to the new generation
     * @throws IOException if the generation cannot be created
     */
    GenerationHandle createForWrite(long generation, TranslogHeader header) throws IOException;

    /**
     * Transitions a write handle to a read-only handle. This is typically called
     * when rolling to a new generation.
     *
     * @param writeHandle the write handle to transition
     * @param checkpoint the final checkpoint for this generation
     * @return a read-only handle to the same generation
     * @throws IOException if the transition fails
     */
    GenerationHandle closeWriteIntoRead(GenerationHandle writeHandle, Checkpoint checkpoint) throws IOException;

    /**
     * Reads the current checkpoint from the translog.ckp file.
     *
     * @return the current checkpoint
     * @throws IOException if the checkpoint cannot be read
     */
    Checkpoint readCheckpoint() throws IOException;

    /**
     * Writes the current checkpoint to the translog.ckp file.
     *
     * @param checkpoint the checkpoint to write
     * @param fsync whether to fsync the checkpoint file
     * @throws IOException if the checkpoint cannot be written
     */
    void writeCheckpoint(Checkpoint checkpoint, boolean fsync) throws IOException;

    /**
     * Writes a generation-specific checkpoint file (translog-{gen}.ckp).
     * This is used when rolling generations to preserve the final state of a generation.
     *
     * @param generation the generation number
     * @param checkpoint the checkpoint to write
     * @throws IOException if the checkpoint cannot be written
     */
    void writeGenerationCheckpoint(long generation, Checkpoint checkpoint) throws IOException;

    /**
     * Reads a generation-specific checkpoint file.
     *
     * @param generation the generation number
     * @return the checkpoint for that generation
     * @throws IOException if the checkpoint cannot be read
     */
    Checkpoint readGenerationCheckpoint(long generation) throws IOException;

    /**
     * Deletes a generation and its associated checkpoint file.
     *
     * @param generation the generation to delete
     * @throws IOException if the deletion fails
     */
    void deleteGeneration(long generation) throws IOException;

    /**
     * Returns the path to the translog directory.
     */
    String location();

    /**
     * Creates a temporary checkpoint file and atomically moves it to the target path.
     * This is used to ensure atomic checkpoint updates.
     *
     * @param targetCheckpointPath the target path for the checkpoint
     * @param sourceCheckpointPath the source checkpoint to copy
     * @throws IOException if the copy fails
     */
    void copyCheckpoint(String targetCheckpointPath, String sourceCheckpointPath) throws IOException;
}