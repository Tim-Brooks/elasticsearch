/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.util.BytesRef;

/**
 * A thread-local pool of pre-allocated Lucene field instances for reuse across batch indexing
 * operations. Fields are checked out sequentially via cursors and returned to the pool when
 * {@link #reset()} is called (typically at the end of a batch).
 * <p>
 * If the pool is exhausted for a given field type, fresh instances are allocated as a fallback.
 */
final class FieldPool {

    static final int SORTED_NUMERIC_POOL_SIZE = 10_000;
    static final int NUMERIC_POOL_SIZE = 1_000;
    static final int KEYWORD_POOL_SIZE = 5_000;

    private final PooledSortedNumericDocValuesField[] sortedNumericPool;
    private int sortedNumericCursor;

    private final PooledNumericDocValuesField[] numericPool;
    private int numericCursor;

    private final PooledKeywordField[] keywordPool;
    private int keywordCursor;

    FieldPool() {
        sortedNumericPool = new PooledSortedNumericDocValuesField[SORTED_NUMERIC_POOL_SIZE];
        for (int i = 0; i < SORTED_NUMERIC_POOL_SIZE; i++) {
            sortedNumericPool[i] = new PooledSortedNumericDocValuesField();
        }
        numericPool = new PooledNumericDocValuesField[NUMERIC_POOL_SIZE];
        for (int i = 0; i < NUMERIC_POOL_SIZE; i++) {
            numericPool[i] = new PooledNumericDocValuesField();
        }
        keywordPool = new PooledKeywordField[KEYWORD_POOL_SIZE];
        for (int i = 0; i < KEYWORD_POOL_SIZE; i++) {
            keywordPool[i] = new PooledKeywordField();
        }
    }

    /**
     * Returns a sorted numeric doc values field with the given name and value, reusing
     * a pooled instance if available or allocating a fresh one if the pool is exhausted.
     */
    Field nextSortedNumeric(String name, long value) {
        if (sortedNumericCursor < sortedNumericPool.length) {
            PooledSortedNumericDocValuesField field = sortedNumericPool[sortedNumericCursor++];
            field.reset(name, value);
            return field;
        }
        return SortedNumericDocValuesField.indexedField(name, value);
    }

    /**
     * Returns a numeric doc values field with the given name and value, reusing
     * a pooled instance if available or allocating a fresh one if the pool is exhausted.
     */
    Field nextNumeric(String name, long value) {
        if (numericCursor < numericPool.length) {
            PooledNumericDocValuesField field = numericPool[numericCursor++];
            field.reset(name, value);
            return field;
        }
        return new org.apache.lucene.document.NumericDocValuesField(name, value);
    }

    /**
     * Returns a keyword field with the given name, value, and field type, reusing
     * a pooled instance if available or allocating a fresh one if the pool is exhausted.
     */
    Field nextKeyword(String name, BytesRef value, FieldType fieldType) {
        if (keywordCursor < keywordPool.length) {
            PooledKeywordField field = keywordPool[keywordCursor++];
            field.reset(name, value, fieldType);
            return field;
        }
        return new KeywordFieldMapper.KeywordField(name, value, fieldType);
    }

    /**
     * Resets all cursors so that pooled fields can be reused by the next batch.
     */
    void reset() {
        sortedNumericCursor = 0;
        numericCursor = 0;
        keywordCursor = 0;
    }
}
