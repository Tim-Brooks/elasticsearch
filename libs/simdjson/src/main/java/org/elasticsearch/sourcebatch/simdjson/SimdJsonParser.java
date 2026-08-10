/*
 * @notice
 *
 * Copyright 2021-2024 The simdjson-java contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Based on a modification of https://github.com/simdjson/simdjson-java,
 * licensed under the Apache License 2.0.
 */

package org.elasticsearch.sourcebatch.simdjson;

/**
 * DOM/tape JSON parser backed by simdjson's two-stage algorithm.
 *
 * <p><strong>Not thread-safe.</strong> Each thread must own its own instance.
 *
 * <p><strong>Memory:</strong> The default constructor allocates ~476 MiB. Always prefer
 * {@link #SimdJsonParser(int, int)} sized to the largest document you expect, and pool one
 * instance per thread.
 *
 * <p><strong>Input contract:</strong> {@code buffer.length - len >= 64} is ideal; if not,
 * the parser copies the document into an internal padded buffer (one extra allocation per call).
 *
 * <p><strong>Vector availability:</strong> Construction throws {@link IllegalStateException} if
 * {@code jdk.incubator.vector} is absent at runtime. Check {@link SimdJsonSupport#isAvailable()}
 * before constructing if you need a graceful fallback.
 */
public class SimdJsonParser {

    private static final int PADDING = 64;
    private static final int DEFAULT_CAPACITY = 34 * 1024 * 1024; // handle JSONs <= 34 MiB
    private static final int DEFAULT_MAX_DEPTH = 1024;

    private final StructuralIndexer indexer;
    private final BitIndexes bitIndexes;
    private final JsonIterator jsonIterator;
    private final byte[] paddedBuffer;

    /** Constructs a parser with {@code DEFAULT_CAPACITY} (34 MiB) and {@code DEFAULT_MAX_DEPTH} (1024). */
    public SimdJsonParser() {
        this(DEFAULT_CAPACITY, DEFAULT_MAX_DEPTH);
    }

    /**
     * Constructs a parser with the given document capacity and nesting depth limit.
     *
     * @param capacity maximum document size in bytes
     * @param maxDepth maximum nesting depth
     * @throws IllegalStateException if {@code jdk.incubator.vector} is not available
     */
    public SimdJsonParser(int capacity, int maxDepth) {
        // Trigger SimdJsonSupport static init before any vector class loads, establishing the
        // module read-edge via Module.addReads. This must happen before VectorUtils is touched.
        if (!SimdJsonSupport.VECTOR_AVAILABLE) {
            throw new IllegalStateException("jdk.incubator.vector is not available at runtime");
        }
        bitIndexes = new BitIndexes(capacity);
        byte[] stringBuffer = new byte[capacity];
        jsonIterator = new JsonIterator(bitIndexes, stringBuffer, capacity, maxDepth, PADDING);
        paddedBuffer = new byte[capacity];
        indexer = new StructuralIndexer(bitIndexes);
    }

    /**
     * Parses the first {@code len} bytes of {@code buffer} and returns the root {@link JsonValue}.
     *
     * <p>The returned value is only valid for the lifetime of this parse call; subsequent calls
     * overwrite the internal tape.
     *
     * @param buffer input bytes (UTF-8 JSON)
     * @param len    number of valid bytes in {@code buffer}
     * @return the root JSON value
     * @throws JsonParsingException if the input is not valid JSON or UTF-8
     */
    public JsonValue parse(byte[] buffer, int len) {
        byte[] padded = padIfNeeded(buffer, len);
        reset();
        stage1(padded, len);
        return jsonIterator.walkDocument(padded, len);
    }

    /**
     * Parses the first {@code len} bytes of {@code buffer} into the internal tape without
     * constructing a {@link JsonValue}. After this call the tape is accessible via {@link #tape()}
     * and string data via {@link #stringBuffer()}.
     *
     * <p>Intended for use by {@link SimdJsonXContentParser}, which walks the tape as a streaming
     * cursor rather than via the random-access {@link JsonValue} API.
     *
     * @param buffer input bytes (UTF-8 JSON)
     * @param len    number of valid bytes in {@code buffer}
     * @throws JsonParsingException if the input is not valid JSON or UTF-8
     */
    void parseToTape(byte[] buffer, int len) {
        byte[] padded = padIfNeeded(buffer, len);
        reset();
        stage1(padded, len);
        jsonIterator.walkToTape(padded, len);
    }

    /**
     * Returns the internal tape after a call to {@link #parse} or {@link #parseToTape}.
     * The tape is reused across calls; its contents are valid only until the next parse.
     */
    Tape tape() {
        return jsonIterator.tape();
    }

    /**
     * Returns the internal string buffer after a call to {@link #parse} or {@link #parseToTape}.
     * The buffer is reused across calls. String slices embedded in it are valid only until the
     * next parse. The SIMD copy loop in {@link StringParser} may write up to one vector width
     * (16–64 bytes) past the logical end of a string; never read beyond the length prefix.
     */
    byte[] stringBuffer() {
        return jsonIterator.stringBuffer();
    }

    private byte[] padIfNeeded(byte[] buffer, int len) {
        if (buffer.length - len < PADDING) {
            System.arraycopy(buffer, 0, paddedBuffer, 0, len);
            return paddedBuffer;
        }
        return buffer;
    }

    private void reset() {
        bitIndexes.reset();
        jsonIterator.reset();
    }

    private void stage1(byte[] buffer, int length) {
        Utf8Validator.validate(buffer, length);
        indexer.index(buffer, length);
    }
}
