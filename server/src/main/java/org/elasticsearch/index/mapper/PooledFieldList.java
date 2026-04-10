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

/**
 * A {@link java.util.List} implementation backed by shared pooled object arrays from
 * {@link BatchLuceneDocuments}. Fields are appended via {@link #add(IndexableField)} during
 * document parsing. After all fields are added, {@link #freeze()} resolves the backing page
 * array and offset for direct slice iteration with no per-element arithmetic.
 */
final class PooledFieldList extends AbstractList<IndexableField> {

    private final BatchLuceneDocuments pool;
    private final int startOffset;
    private int size;

    // Resolved by freeze() — direct reference to the page array and the offset within it
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
     * Resolves the backing page array for direct slice access. For the common case where all
     * fields fit in a single page, this just captures the page reference and offset — zero copy.
     * For the rare cross-page case, fields are copied into a dedicated array.
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
            array = pool.getPage(startPage);
            arrayOffset = startOffset % pageSize;
        } else {
            Object[] copy = new Object[size];
            int globalIndex = startOffset;
            for (int i = 0; i < size; i++) {
                copy[i] = pool.getPage(globalIndex / pageSize)[globalIndex % pageSize];
                globalIndex++;
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
        if (array != null) {
            return (IndexableField) array[arrayOffset + index];
        }
        int globalIndex = startOffset + index;
        int pageSize = pool.pageSize();
        return (IndexableField) pool.getPage(globalIndex / pageSize)[globalIndex % pageSize];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<IndexableField> iterator() {
        if (array != null) {
            return new ArraySliceIterator(array, arrayOffset, size);
        }
        return super.iterator();
    }

    /**
     * Named iterator class for iterating a slice of an Object[] as IndexableField.
     * Named (not anonymous) so the JIT can build a stable type profile and inline fully.
     */
    static final class ArraySliceIterator implements Iterator<IndexableField> {
        private final Object[] array;
        private final int end;
        private int cursor;

        ArraySliceIterator(Object[] array, int offset, int size) {
            this.array = array;
            this.cursor = offset;
            this.end = offset + size;
        }

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
    }
}
