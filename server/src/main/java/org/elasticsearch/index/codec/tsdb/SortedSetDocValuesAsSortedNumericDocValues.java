/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongsRef;

import java.io.IOException;

/**
 * Adapts {@link SortedSetDocValues} to {@link SortedNumericDocValues} by buffering ordinals
 * read from each document in {@link #nextDoc()}.
 */
final class SortedSetDocValuesAsSortedNumericDocValues extends SortedNumericDocValues {

    private final SortedSetDocValues values;

    private long[] ords = LongsRef.EMPTY_LONGS;
    private int i;
    private int docValueCount;

    SortedSetDocValuesAsSortedNumericDocValues(SortedSetDocValues values) {
        this.values = values;
    }

    @Override
    public long nextValue() {
        return ords[i++];
    }

    @Override
    public int docValueCount() {
        return docValueCount;
    }

    @Override
    public boolean advanceExact(int target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
        return values.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        int doc = values.nextDoc();
        if (doc != NO_MORE_DOCS) {
            docValueCount = values.docValueCount();
            ords = ArrayUtil.grow(ords, docValueCount);
            for (int j = 0; j < docValueCount; j++) {
                ords[j] = values.nextOrd();
            }
            i = 0;
        }
        return doc;
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return values.cost();
    }
}
