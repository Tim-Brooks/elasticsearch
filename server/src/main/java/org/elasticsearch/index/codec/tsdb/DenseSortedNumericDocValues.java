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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * {@link SortedNumericDocValues} for the dense case where all documents have at least one value.
 * Document iteration is a simple counter; value positions are resolved via an address table.
 */
final class DenseSortedNumericDocValues extends SortedNumericDocValues {

    private final int maxDoc;
    private final LongValues addresses;
    private final NumericValues values;

    private int doc = -1;
    private long start;
    private int count;

    DenseSortedNumericDocValues(int maxDoc, LongValues addresses, NumericValues values) {
        this.maxDoc = maxDoc;
        this.addresses = addresses;
        this.values = values;
    }

    @Override
    public int nextDoc() throws IOException {
        return advance(doc + 1);
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public long cost() {
        return maxDoc;
    }

    @Override
    public int advance(int target) throws IOException {
        if (target >= maxDoc) {
            return doc = DocIdSetIterator.NO_MORE_DOCS;
        }
        start = addresses.get(target);
        long end = addresses.get(target + 1L);
        count = (int) (end - start);
        return doc = target;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        start = addresses.get(target);
        long end = addresses.get(target + 1L);
        count = (int) (end - start);
        doc = target;
        return true;
    }

    @Override
    public long nextValue() throws IOException {
        return values.advance(start++);
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public int docIDRunEnd() {
        return maxDoc;
    }
}
