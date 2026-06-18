/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.elasticsearch.sourcebatch.ColumnBatchProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates Lucene {@link Column}s for a fixed number of documents and assembles them into a
 * {@link ColumnBatch} for {@code IndexWriter#addBatch}. Field mappers attach their value column via
 * {@link FieldMapper#mapColumnBatch}; metadata mappers attach theirs via
 * {@link MetadataFieldMapper#mapMetadataColumns}.
 *
 * <p>The {@code _seq_no}/{@code _primary_term}/{@code _version} columns are backed by mutable
 * {@code long[]} arrays this builder owns: the metadata mappers register array-backed columns over
 * them up front (placeholder zeros), and the engine fills the assigned values per document — after
 * mapping — via {@link #setSeqNo}/{@link #setPrimaryTerm}/{@link #setVersion} just before requesting
 * the {@link ColumnBatch}.
 *
 * @see ColumnBatchProvider
 */
public final class ColumnBatchBuilder implements ColumnBatchProvider {

    private final int docCount;
    private final BatchDocumentParserContext[] contexts;
    private final List<Column> columns = new ArrayList<>();

    private long[] seqNo;
    private long[] primaryTerm;
    private long[] version;

    public ColumnBatchBuilder(int docCount, BatchDocumentParserContext[] contexts) {
        assert contexts.length == docCount : "contexts [" + contexts.length + "] must match docCount [" + docCount + "]";
        this.docCount = docCount;
        this.contexts = contexts;
    }

    /** The per-document parse contexts (one per batch-local doc), used by metadata mappers. */
    public BatchDocumentParserContext[] contexts() {
        return contexts;
    }

    /** Attaches a fully-assembled Lucene column covering all {@code docCount} documents. */
    public void addColumn(Column column) {
        columns.add(column);
    }

    /** Lazily allocates and returns the mutable {@code _seq_no} backing array (length {@code docCount}). */
    public long[] seqNoArray() {
        if (seqNo == null) {
            seqNo = new long[docCount];
        }
        return seqNo;
    }

    /** Lazily allocates and returns the mutable {@code _primary_term} backing array (length {@code docCount}). */
    public long[] primaryTermArray() {
        if (primaryTerm == null) {
            primaryTerm = new long[docCount];
        }
        return primaryTerm;
    }

    /** Lazily allocates and returns the mutable {@code _version} backing array (length {@code docCount}). */
    public long[] versionArray() {
        if (version == null) {
            version = new long[docCount];
        }
        return version;
    }

    @Override
    public int docCount() {
        return docCount;
    }

    @Override
    public void setSeqNo(int doc, long value) {
        seqNoArray()[doc] = value;
    }

    @Override
    public void setPrimaryTerm(int doc, long value) {
        primaryTermArray()[doc] = value;
    }

    @Override
    public void setVersion(int doc, long value) {
        versionArray()[doc] = value;
    }

    @Override
    public ColumnBatch columnBatch(int from, int to) {
        if (from != 0 || to != docCount) {
            // First cut: a chunk is indexed atomically as one addBatch; sub-range slicing is a follow-up.
            throw new UnsupportedOperationException(
                "ColumnBatchBuilder only supports the full range [0, " + docCount + "), got [" + from + ", " + to + ")"
            );
        }
        final List<Column> batchColumns = List.copyOf(columns);
        return new ColumnBatch() {
            @Override
            public int numDocs() {
                return docCount;
            }

            @Override
            public Iterable<Column> columns() {
                return batchColumns;
            }
        };
    }
}
