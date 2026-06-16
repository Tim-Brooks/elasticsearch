/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.sourcebatch.SourceBatch;

/**
 * Factory for creating {@link SourceBatch} instances from serialised bytes.
 *
 * <p>This is the single swap point for the batch format: all deserialisation and
 * reconstruction sites (wire transport, translog, tests) must go through
 * {@link #fromBytes(BytesReference, Releasable)} rather than constructing a concrete
 * implementation directly. Changing the format to column-major requires updating only this
 * class.
 */
public final class SourceBatches {

    private SourceBatches() {}

    /**
     * Reconstructs a {@link SourceBatch} from its raw serialised bytes.
     *
     * @param data       the serialised batch bytes (as produced by {@link SourceBatch#data()})
     * @param releasable called on {@link SourceBatch#close()} to release any underlying buffers;
     *                   pass {@code () -> {}} when the bytes are not reference-counted
     */
    public static SourceBatch fromBytes(BytesReference data, Releasable releasable) {
        return new EirfBatch(data, releasable);
    }
}
