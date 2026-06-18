/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.document.column.ColumnBatch;

/**
 * Carries the Lucene {@link ColumnBatch} assembled for a {@link SourceBatch} so the engine can index
 * the whole batch through {@code IndexWriter#addBatch} instead of per-document {@code addDocuments}.
 *
 * <p>The mapper assembles one provider per chunk and attaches it to the chunk's {@link SourceBatch}
 * (see {@link SourceBatch#columnBatchProvider()}). The engine assigns {@code _seq_no}/
 * {@code _primary_term}/{@code _version} per document — after mapping — by calling the
 * {@code setXxx} methods, which write into the mutable arrays backing those metadata columns, before
 * requesting the assembled {@link ColumnBatch}.
 *
 * <p><b>First-cut limitation:</b> {@link #columnBatch(int, int)} only supports the full range
 * {@code [0, docCount())}; the engine takes the columnar path only when a whole chunk is processed as
 * a single sub-batch and otherwise falls back to the per-document path.
 */
public interface ColumnBatchProvider {

    /** Number of documents covered by this provider (the chunk size). */
    int docCount();

    /** Sets the assigned {@code _seq_no} for batch-local document {@code doc}. */
    void setSeqNo(int doc, long seqNo);

    /** Sets the assigned {@code _primary_term} for batch-local document {@code doc}. */
    void setPrimaryTerm(int doc, long primaryTerm);

    /** Sets the assigned {@code _version} for batch-local document {@code doc}. */
    void setVersion(int doc, long version);

    /**
     * Builds the Lucene {@link ColumnBatch} covering documents {@code [from, to)}. The first cut only
     * supports the full range {@code [0, docCount())}.
     *
     * @throws UnsupportedOperationException if {@code [from, to)} is not the full range
     */
    ColumnBatch columnBatch(int from, int to);
}
