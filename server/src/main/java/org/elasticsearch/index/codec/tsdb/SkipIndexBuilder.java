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
 * Accumulates skip-index data as the field-write loop consumes values, then serializes the
 * skip-index data and meta on {@link #writeSkipIndex}. This replaces the historical two-pass
 * approach where the skip index was built by a separate iteration over the field's doc values
 * before the main encoding loop.
 *
 * <p>Usage:
 * <ol>
 *   <li>Construct one instance per field.</li>
 *   <li>Pass it into the field writer; the writer calls {@link #onNewDoc} for the first value
 *       of each doc and {@link #accumulate} for every value.</li>
 *   <li>If the writer's encoding strategy doesn't iterate values (e.g. {@code maxOrd == 1}),
 *       call {@link #buildFromValues} as a fallback.</li>
 *   <li>Call {@link #writeSkipIndex} to flush the accumulated data to the skip stream and the
 *       skip-index header to meta.</li>
 * </ol>
 *
 * <p>Skip data is buffered internally so the field's encoding loop can write to the data stream
 * without interleaving skip-index bytes. The buffered skip data is copied to the real skip
 * output exactly once by {@link #writeSkipIndex}.
 */
public final class SkipIndexBuilder {

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
    private int globalMaxValueCount = 0;
    private boolean finished = false;

    public SkipIndexBuilder(TSDBDocValuesFormatConfig formatConfig) {
        this.skipIndexIntervalSize = formatConfig.skipIndexIntervalSize();
        this.skipIndexLevelShift = formatConfig.skipIndexLevelShift();
        this.skipIndexMaxLevel = formatConfig.skipIndexMaxLevel();
        this.maxAccumulators = 1 << (skipIndexLevelShift * (skipIndexMaxLevel - 1));
    }

    /**
     * Notifies the builder that a new document is about to be consumed. The first value of
     * the document must be passed so the builder can decide whether to close out the current
     * skip-interval accumulator (see {@link SkipAccumulator#isDone}).
     */
    public void onNewDoc(int docID, int docValueCount, long firstValue) throws IOException {
        globalMaxValueCount = Math.max(globalMaxValueCount, docValueCount);
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
     * Accumulates one value into the current skip-interval accumulator. Must be called once
     * per value (including the first value passed to {@link #onNewDoc}).
     */
    public void accumulate(long value) {
        currentAccumulator.accumulate(value);
    }

    /**
     * Finalizes the trailing accumulator and flushes any remaining levels. Idempotent.
     */
    public void finish() throws IOException {
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
     * Returns true if {@link #onNewDoc} has not been called yet. Used by callers to decide
     * whether to drive {@link #buildFromValues} as a fallback when the field writer's
     * encoding path didn't iterate values per doc.
     */
    public boolean isEmpty() {
        return currentAccumulator == null && accumulators.isEmpty();
    }

    /**
     * Drives the builder against a {@link SortedNumericDocValues} iterator. Used as a fallback
     * when the field writer's encoding path doesn't iterate per-value (for example,
     * {@code maxOrd == 1}). Produces output byte-identical to the inline path.
     */
    public void buildFromValues(SortedNumericDocValues values) throws IOException {
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
     * Writes the buffered skip data to {@code skipOut} and the skip-index header to {@code meta}.
     * The header layout (start, length, max, min, docCount, maxDocId, maxValueCount) matches the
     * historical single-pass implementation byte-for-byte.
     */
    public void writeSkipIndex(IndexOutput skipOut, IndexOutput meta) throws IOException {
        finish();
        long start = skipOut.getFilePointer();
        skipDataBuffer.copyTo(skipOut);
        long length = skipOut.getFilePointer() - start;
        meta.writeLong(start);
        meta.writeLong(length);
        assert globalDocCount == 0 || globalMaxValue >= globalMinValue;
        meta.writeLong(globalMaxValue);
        meta.writeLong(globalMinValue);
        assert globalDocCount <= maxDocId + 1;
        meta.writeInt(globalDocCount);
        meta.writeInt(maxDocId);
        meta.writeInt(globalMaxValueCount);
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

    private static final class SkipAccumulator {
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
}
