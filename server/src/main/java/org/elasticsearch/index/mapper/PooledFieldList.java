/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;

import java.util.AbstractList;
import java.util.Arrays;

/**
 * A {@link java.util.List} implementation backed by shared pooled object arrays from
 * {@link BatchLuceneDocuments}. Fields are appended via {@link #add(IndexableField)} during
 * document parsing. After all fields are added, {@link #freeze(LuceneDocument)} copies the
 * field references into an {@code IndexableField[]} and swaps the document's field list to
 * a standard {@link Arrays#asList} wrapper whose iterator the JIT can fully devirtualize.
 */
final class PooledFieldList extends AbstractList<IndexableField> {

    private final BatchLuceneDocuments pool;
    private final int startOffset;
    private int size;

    PooledFieldList(BatchLuceneDocuments pool, int startOffset) {
        this.pool = pool;
        this.startOffset = startOffset;
        this.size = 0;
    }

    @Override
    public boolean add(IndexableField field) {
        pool.addField(field);
        size++;
        return true;
    }

    /**
     * Copies the field references into a contiguous {@code IndexableField[]} and replaces the
     * document's field list with a standard {@link Arrays#asList} wrapper. This ensures Lucene
     * iterates fields via a well-known JDK iterator type, avoiding megamorphic dispatch at the
     * iteration call site.
     */
    void freeze(LuceneDocument doc) {
        IndexableField[] frozen = new IndexableField[size];
        int pageSize = pool.pageSize();
        int startPage = startOffset / pageSize;
        int endPage = size == 0 ? startPage : (startOffset + size - 1) / pageSize;
        if (startPage == endPage) {
            // Common case: single page — bulk copy from the page array
            System.arraycopy(pool.getPage(startPage), startOffset % pageSize, frozen, 0, size);
        } else {
            // Rare case: fields span pages
            int globalIndex = startOffset;
            for (int i = 0; i < size; i++) {
                frozen[i] = (IndexableField) pool.getPage(globalIndex / pageSize)[globalIndex % pageSize];
                globalIndex++;
            }
        }
        doc.setFields(Arrays.asList(frozen));
    }

    @Override
    public IndexableField get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        int globalIndex = startOffset + index;
        int pageSize = pool.pageSize();
        return (IndexableField) pool.getPage(globalIndex / pageSize)[globalIndex % pageSize];
    }

    @Override
    public int size() {
        return size;
    }
}
