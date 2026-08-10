/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.sourcebatch.simdjson.SimdJsonSupport;
import org.elasticsearch.sourcebatch.simdjson.SimdJsonXContentParser;

/**
 * Thread-local pool of {@link SimdJsonXContentParser} instances for use by {@link EscfEncoder}.
 *
 * <p>{@link SimdJsonXContentParser} is not thread-safe and allocates ~14 × {@code capacity} bytes
 * up front: two {@code byte[capacity]} arrays plus {@code int[capacity]} and {@code long[capacity]}.
 * At the chosen capacity of {@code 2 × MAX_DOC_BYTES} (32 KiB) that is roughly 450 KiB per thread,
 * which is acceptable for long-lived write-pool threads. A fresh {@link EscfEncoder} is created per
 * concrete index per bulk request, so the parser must not live on the encoder.
 *
 * <p>A scratch byte array of {@code MAX_DOC_BYTES + 64} bytes is pooled for the same reason:
 * {@link SimdJsonXContentParser#reset} needs the source bytes starting at offset 0, so any slice
 * whose {@code arrayOffset() != 0} is copied once into this buffer before parsing.
 *
 * <p>{@link #AVAILABLE} must be checked before calling {@link #parser()} — the parser constructor
 * throws {@link IllegalStateException} when {@code jdk.incubator.vector} is absent at runtime.
 */
final class SimdJsonPool {

    /** Documents larger than this threshold are handled by the Jackson parser. */
    static final int MAX_DOC_BYTES = 16 * 1024;

    /**
     * Parser capacity: 2x MAX_DOC_BYTES so the internal string buffer (capacity bytes) is large
     * enough even for pathological documents where every byte is a distinct single-character string
     * (each needing a 4-byte length prefix), plus headroom for the SIMD overshoot (up to 64 bytes).
     */
    private static final int CAPACITY = 2 * MAX_DOC_BYTES;

    /**
     * Nesting depth limit. The index-mapping default depth limit is 20; 64 leaves comfortable
     * headroom while keeping the per-thread depth-stack allocation negligible.
     */
    private static final int MAX_DEPTH = 64;

    /**
     * True when {@code jdk.incubator.vector} is available at runtime. When false, every
     * eligibility check in {@link EscfEncoder} short-circuits to the Jackson path without ever
     * touching the {@link ThreadLocal} (which would fail on construction).
     */
    static final boolean AVAILABLE = SimdJsonSupport.isAvailable();

    private static final ThreadLocal<SimdJsonXContentParser> PARSER = ThreadLocal.withInitial(
        () -> new SimdJsonXContentParser(CAPACITY, MAX_DEPTH)
    );

    /**
     * Scratch buffer: {@code MAX_DOC_BYTES + 64} bytes so that
     * {@link SimdJsonXContentParser#reset} never needs to allocate a padded copy internally.
     * (The parser requires {@code buffer.length - len >= 64}.)
     */
    private static final ThreadLocal<byte[]> SCRATCH = ThreadLocal.withInitial(() -> new byte[MAX_DOC_BYTES + 64]);

    private SimdJsonPool() {}

    /** Returns the thread-local parser instance. Only call when {@link #AVAILABLE} is true. */
    static SimdJsonXContentParser parser() {
        return PARSER.get();
    }

    /**
     * Returns the thread-local scratch buffer of length {@code MAX_DOC_BYTES + 64}.
     * Used to copy a non-zero-offset {@link org.elasticsearch.common.bytes.BytesReference} into
     * a zero-offset array before handing it to the parser.
     */
    static byte[] scratch() {
        return SCRATCH.get();
    }
}
