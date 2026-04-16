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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Accumulates skip index data as doc values are consumed, then serializes the skip index
 * to data and meta outputs. This replaces the old two-pass approach where skip index data
 * was computed in a separate iteration over all doc values.
 *
 * <p>Usage: create an instance, wrap doc values iterators via {@link #wrap}, let the field
 * writer consume values through the wrapper, then call {@link #writeSkipIndex} to flush.
 * If the field writer's encoding path doesn't iterate values (e.g. single-ordinal), use
 * {@link #buildFromValues} as a fallback.
 */
final class SkipIndexBuilder {

    private final int skipIndexIntervalSize;
    private final int skipIndexLevelShift;
    private final int skipIndexMaxLevel;
    private final int maxAccumulators;

    private final List<SkipAccumulator> accumulators = new ArrayList<>();
    private SkipAccumulator currentAccumulator;

    private final ByteBuffersDataOutput skipDataBuffer = new ByteBuffersDataOutput();
    private final IndexOutput skipDataOut = new ByteBuffersIndexOutput(skipDataBuffer, "skip-data-buffer", "skip-data-buffer");

    private long globalMaxValue = Long.MIN_VALUE;
    private long globalMinValue = Long.MAX_VALUE;
    private int globalDocCount = 0;
    private int maxDocId = -1;
    private boolean finished = false;

    SkipIndexBuilder(TSDBDocValuesFormatConfig formatConfig) {
        this.skipIndexIntervalSize = formatConfig.skipIndexIntervalSize();
        this.skipIndexLevelShift = formatConfig.skipIndexLevelShift();
        this.skipIndexMaxLevel = formatConfig.skipIndexMaxLevel();
        this.maxAccumulators = 1 << (skipIndexLevelShift * (skipIndexMaxLevel - 1));
    }

    /**
     * Called when the first value of a new document is about to be consumed.
     * Handles the isDone check on the previous accumulator and creates a new one if needed.
     */
    void onNewDoc(int docID, int docValueCount, long firstValue) throws IOException {
        if (currentAccumulator != null && currentAccumulator.isDone(skipIndexIntervalSize, docValueCount, firstValue, docID)) {
            globalMaxValue = Math.max(globalMaxValue, currentAccumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, currentAccumulator.minValue);
            globalDocCount += currentAccumulator.docCount;
            maxDocId = currentAccumulator.maxDocID;
            currentAccumulator = null;
            if (accumulators.size() == maxAccumulators) {
                writeLevels(accumulators);
                accumulators.clear();
            }
        }
        if (currentAccumulator == null) {
            currentAccumulator = new SkipAccumulator(docID);
            accumulators.add(currentAccumulator);
        }
        currentAccumulator.nextDoc(docID);
    }

    /**
     * Accumulate a single value for the current document.
     */
    void accumulate(long value) {
        currentAccumulator.accumulate(value);
    }

    /**
     * Finalize the last accumulator and flush remaining levels. Idempotent.
     */
    void finish() throws IOException {
        if (finished) {
            return;
        }
        finished = true;
        if (accumulators.isEmpty() == false) {
            globalMaxValue = Math.max(globalMaxValue, currentAccumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, currentAccumulator.minValue);
            globalDocCount += currentAccumulator.docCount;
            maxDocId = currentAccumulator.maxDocID;
            writeLevels(accumulators);
        }
    }

    /**
     * Returns true if no values were accumulated (e.g. the encoding path didn't iterate values).
     */
    boolean isEmpty() {
        return currentAccumulator == null && accumulators.isEmpty();
    }

    /**
     * Convenience method to iterate all values from the given doc values and accumulate skip data.
     * Used as a fallback when the field writer's encoding path doesn't iterate values (e.g. maxOrd == 1).
     */
    void buildFromValues(SortedNumericDocValues values) throws IOException {
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            long firstValue = values.nextValue();
            onNewDoc(doc, values.docValueCount(), firstValue);
            accumulate(firstValue);
            for (int i = 1, end = values.docValueCount(); i < end; ++i) {
                accumulate(values.nextValue());
            }
        }
        finish();
    }

    /**
     * Write the skip index data to the data output, then write skip index metadata to the meta output.
     */
    void writeSkipIndex(IndexOutput data, IndexOutput meta) throws IOException {
        finish();
        long start = data.getFilePointer();
        skipDataBuffer.copyTo(data);
        long length = data.getFilePointer() - start;
        meta.writeLong(start);
        meta.writeLong(length);
        assert globalDocCount == 0 || globalMaxValue >= globalMinValue;
        meta.writeLong(globalMaxValue);
        meta.writeLong(globalMinValue);
        assert globalDocCount <= maxDocId + 1;
        meta.writeInt(globalDocCount);
        meta.writeInt(maxDocId);
    }

    /**
     * Wrap the given doc values iterator so that values consumed from it are accumulated
     * into this builder's skip index data.
     */
    SortedNumericDocValues wrap(SortedNumericDocValues delegate) {
        return new SkipAccumulatingSortedNumericDocValues(delegate);
    }

    private void writeLevels(List<SkipAccumulator> accumulators) throws IOException {
        final List<List<SkipAccumulator>> accumulatorsLevels = new ArrayList<>(skipIndexMaxLevel);
        accumulatorsLevels.add(accumulators);
        for (int i = 0; i < skipIndexMaxLevel - 1; i++) {
            accumulatorsLevels.add(buildLevel(accumulatorsLevels.get(i)));
        }
        int totalAccumulators = accumulators.size();
        for (int index = 0; index < totalAccumulators; index++) {
            final int levels = getLevels(index, totalAccumulators);
            skipDataOut.writeByte((byte) levels);
            for (int level = levels - 1; level >= 0; level--) {
                final SkipAccumulator acc = accumulatorsLevels.get(level).get(index >> (skipIndexLevelShift * level));
                skipDataOut.writeInt(acc.maxDocID);
                skipDataOut.writeInt(acc.minDocID);
                skipDataOut.writeLong(acc.maxValue);
                skipDataOut.writeLong(acc.minValue);
                skipDataOut.writeInt(acc.docCount);
            }
        }
    }

    private List<SkipAccumulator> buildLevel(List<SkipAccumulator> accumulators) {
        final int levelSize = 1 << skipIndexLevelShift;
        final List<SkipAccumulator> collector = new ArrayList<>();
        for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {
            collector.add(SkipAccumulator.merge(accumulators, i, levelSize));
        }
        return collector;
    }

    private int getLevels(int index, int size) {
        if (Integer.numberOfTrailingZeros(index) >= skipIndexLevelShift) {
            final int left = size - index;
            for (int level = skipIndexMaxLevel - 1; level > 0; level--) {
                final int numberIntervals = 1 << (skipIndexLevelShift * level);
                if (left >= numberIntervals && index % numberIntervals == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }

    static class SkipAccumulator {
        int minDocID;
        int maxDocID;
        int docCount;
        long minValue;
        long maxValue;

        SkipAccumulator(int docID) {
            minDocID = docID;
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
            docCount = 0;
        }

        boolean isDone(int skipIndexIntervalSize, int valueCount, long nextValue, int nextDoc) {
            if (docCount < skipIndexIntervalSize) {
                return false;
            }
            return valueCount > 1 || minValue != maxValue || minValue != nextValue || docCount != nextDoc - minDocID;
        }

        void accumulate(long value) {
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
        }

        void accumulate(SkipAccumulator other) {
            assert minDocID <= other.minDocID && maxDocID < other.maxDocID;
            maxDocID = other.maxDocID;
            minValue = Math.min(minValue, other.minValue);
            maxValue = Math.max(maxValue, other.maxValue);
            docCount += other.docCount;
        }

        void nextDoc(int docID) {
            maxDocID = docID;
            ++docCount;
        }

        static SkipAccumulator merge(List<SkipAccumulator> list, int index, int length) {
            SkipAccumulator acc = new SkipAccumulator(list.get(index).minDocID);
            for (int i = 0; i < length; i++) {
                acc.accumulate(list.get(index + i));
            }
            return acc;
        }
    }

    /**
     * Wraps a {@link SortedNumericDocValues} to intercept value reads and feed them into
     * the skip index builder. Only {@link #nextValue()} triggers accumulation, so iteration
     * passes that only call {@link #nextDoc()} (e.g. stats-counting, address-writing) do not
     * produce any skip data.
     */
    private class SkipAccumulatingSortedNumericDocValues extends SortedNumericDocValues {
        private final SortedNumericDocValues delegate;
        private boolean firstValueForDoc;

        SkipAccumulatingSortedNumericDocValues(SortedNumericDocValues delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextDoc() throws IOException {
            int doc = delegate.nextDoc();
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                firstValueForDoc = true;
            }
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                firstValueForDoc = true;
            }
            return doc;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            boolean found = delegate.advanceExact(target);
            if (found) {
                firstValueForDoc = true;
            }
            return found;
        }

        @Override
        public long nextValue() throws IOException {
            long value = delegate.nextValue();
            if (firstValueForDoc) {
                firstValueForDoc = false;
                SkipIndexBuilder.this.onNewDoc(delegate.docID(), delegate.docValueCount(), value);
            }
            SkipIndexBuilder.this.accumulate(value);
            return value;
        }

        @Override
        public int docValueCount() {
            return delegate.docValueCount();
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }
}
