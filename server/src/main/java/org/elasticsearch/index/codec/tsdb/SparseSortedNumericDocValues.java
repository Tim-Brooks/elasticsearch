/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * {@link SortedNumericDocValues} for the sparse case where only some documents have values.
 * Document iteration is driven by an {@link IndexedDISI}; value positions are lazily resolved
 * from the DISI index via an address table.
 */
final class SparseSortedNumericDocValues extends SortedNumericDocValues {

    private final IndexedDISI disi;
    private final LongValues addresses;
    private final NumericValues values;

    private boolean set;
    private long start;
    private int count;

    SparseSortedNumericDocValues(IndexedDISI disi, LongValues addresses, NumericValues values) {
        this.disi = disi;
        this.addresses = addresses;
        this.values = values;
    }

    @Override
    public int nextDoc() throws IOException {
        set = false;
        return disi.nextDoc();
    }

    @Override
    public int docID() {
        return disi.docID();
    }

    @Override
    public long cost() {
        return disi.cost();
    }

    @Override
    public int advance(int target) throws IOException {
        set = false;
        return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        set = false;
        return disi.advanceExact(target);
    }

    @Override
    public long nextValue() throws IOException {
        set();
        return values.advance(start++);
    }

    @Override
    public int docValueCount() {
        set();
        return count;
    }

    @Override
    public int docIDRunEnd() throws IOException {
        return disi.docIDRunEnd();
    }

    private void set() {
        if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            long end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
        }
    }
}
