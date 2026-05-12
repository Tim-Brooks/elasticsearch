/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import java.io.Closeable;
import java.io.IOException;

/**
 * Common base for the two per-doc value-count accumulators driven from the field-write loop in
 * {@link TSDBDocValuesBlockWriter}: {@link OffsetsAccumulator} (merge path; writes the address
 * table to a temp file as it goes) and {@link DeferredOffsetsAccumulator} (flush path; buffers
 * counts as VInts and builds the address table after the loop).
 *
 * <p>The class is {@code sealed} and exposes {@link #addDoc(int)} so the per-doc call site in
 * the block writer stays monomorphic-or-bimorphic — JIT can inline through a type guard instead
 * of falling back to indirect dispatch on a {@code @FunctionalInterface} callback.
 */
public abstract sealed class OffsetsAccumulatorBase implements Closeable permits OffsetsAccumulator, DeferredOffsetsAccumulator {

    /** Records one document's value count. Called once per document, in doc order. */
    public abstract void addDoc(int docValueCount) throws IOException;
}
