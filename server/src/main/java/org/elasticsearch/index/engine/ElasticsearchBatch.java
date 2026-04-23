/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * An Elasticsearch implementation of the Lucene {@link ColumnBatch} that wraps a set of
 * {@link Column}s backed by pooled byte pages. Implements {@link Releasable} to
 * release the underlying byte pages after {@code IndexWriter.addBatch()} returns.
 */
public final class ElasticsearchBatch extends ColumnBatch implements Releasable {

    private final int numDocs;
    private final List<Column> columns;
    private final Releasable resources;

    public ElasticsearchBatch(int numDocs, List<Column> columns, Releasable resources) {
        this.numDocs = numDocs;
        this.columns = columns;
        this.resources = resources;
    }

    @Override
    public int numDocs() {
        return numDocs;
    }

    @Override
    public Iterable<Column> columns() {
        return columns;
    }

    @Override
    public void close() {
        resources.close();
    }
}
