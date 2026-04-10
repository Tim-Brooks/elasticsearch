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
 * {@link BatchLuceneDocuments}. Each instance represents a contiguous range of fields
 * starting at a global offset in the pool. Fields may span page boundaries transparently.
 */
final class PooledFieldList extends AbstractList<IndexableField> implements RandomAccess {

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

    @Override
    public IndexableField get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        return pool.getField(startOffset + index);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<IndexableField> iterator() {
        return new Iterator<>() {
            private int cursor = 0;

            @Override
            public boolean hasNext() {
                return cursor < size;
            }

            @Override
            public IndexableField next() {
                if (cursor >= size) {
                    throw new NoSuchElementException();
                }
                return pool.getField(startOffset + cursor++);
            }
        };
    }
}
