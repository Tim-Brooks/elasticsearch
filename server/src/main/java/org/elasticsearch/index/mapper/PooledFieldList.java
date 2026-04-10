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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * A {@link java.util.List} implementation backed by shared pooled object arrays from
 * {@link BatchLuceneDocuments}. Fields are appended via {@link #add(IndexableField)} during
 * document parsing, then {@link #freeze()} is called to resolve the backing array for fast
 * iteration and random access with no per-element arithmetic.
 * <p>
 * If all fields reside within a single pool page (the common case for ~80-field documents),
 * freeze captures the page's array reference directly. If fields span pages, freeze copies
 * them into a dedicated array.
 */
final class PooledFieldList extends AbstractList<IndexableField> implements RandomAccess {

    private final BatchLuceneDocuments pool;
    private final int startOffset;
    private int size;

    // Resolved by freeze()
    private Object[] array;
    private int arrayOffset;

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
     * Resolves the backing array for fast iteration. Must be called after all fields have been
     * added and before the document is passed to Lucene's IndexWriter.
     */
    void freeze() {
        if (size == 0) {
            array = new Object[0];
            arrayOffset = 0;
            return;
        }
        int pageSize = pool.pageSize();
        int startPage = startOffset / pageSize;
        int endPage = (startOffset + size - 1) / pageSize;
        if (startPage == endPage) {
            // Common case: all fields fit in a single page — just reference the page array directly
            array = pool.getPage(startPage);
            arrayOffset = startOffset % pageSize;
        } else {
            // Rare case: fields span pages — copy into a dedicated contiguous array
            Object[] copy = new Object[size];
            for (int i = 0; i < size; i++) {
                int globalIndex = startOffset + i;
                int pageIndex = globalIndex / pageSize;
                int offsetInPage = globalIndex % pageSize;
                copy[i] = pool.getPage(pageIndex)[offsetInPage];
            }
            array = copy;
            arrayOffset = 0;
        }
    }

    @Override
    public IndexableField get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        assert array != null : "PooledFieldList must be frozen before access";
        return (IndexableField) array[arrayOffset + index];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<IndexableField> iterator() {
        assert array != null : "PooledFieldList must be frozen before iteration";
        return new Iterator<>() {
            private int cursor = arrayOffset;
            private final int end = arrayOffset + size;

            @Override
            public boolean hasNext() {
                return cursor < end;
            }

            @Override
            public IndexableField next() {
                if (cursor >= end) {
                    throw new NoSuchElementException();
                }
                return (IndexableField) array[cursor++];
            }
        };
    }
}
