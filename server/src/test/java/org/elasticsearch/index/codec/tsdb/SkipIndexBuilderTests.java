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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Byte-for-byte equivalence tests for {@link SkipIndexBuilder}. The oracle in this file
 * inlines the historical single-pass {@code writeSkipIndex} implementation that the
 * builder replaces.
 */
public class SkipIndexBuilderTests extends LuceneTestCase {

    public void testSmallFixedInput() throws IOException {
        TSDBDocValuesFormatConfig config = buildConfig(3, 4, 4096);
        Doc[] docs = new Doc[] { doc(0, 1L), doc(1, 5L), doc(2, 3L), doc(5, 100L) };
        assertMatchesOracle(config, docs);
    }

    public void testSingleValuePerDocConstant() throws IOException {
        TSDBDocValuesFormatConfig config = buildConfig(3, 4, 64);
        Doc[] docs = new Doc[500];
        for (int i = 0; i < docs.length; i++) {
            docs[i] = doc(i, 42L);
        }
        assertMatchesOracle(config, docs);
    }

    public void testMultiValuedDocs() throws IOException {
        TSDBDocValuesFormatConfig config = buildConfig(3, 4, 128);
        List<Doc> docs = new ArrayList<>();
        for (int d = 0, used = 0; used < 1000; d += 1 + (d % 3)) {
            int n = 1 + (used % 5);
            long[] vals = new long[n];
            for (int j = 0; j < n; j++) {
                vals[j] = used * 7L + j;
            }
            docs.add(new Doc(d, vals));
            used++;
        }
        assertMatchesOracle(config, docs.toArray(new Doc[0]));
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 25; iter++) {
            int levelShift = TestUtil.nextInt(random(), 2, 4);
            int maxLevel = TestUtil.nextInt(random(), 2, 4);
            int intervalSize = TestUtil.nextInt(random(), 4, 256);
            TSDBDocValuesFormatConfig config = buildConfig(levelShift, maxLevel, intervalSize);

            int numDocs = TestUtil.nextInt(random(), 1, 3000);
            Doc[] docs = new Doc[numDocs];
            int docId = 0;
            for (int i = 0; i < numDocs; i++) {
                docId += TestUtil.nextInt(random(), 1, 3);
                int valCount = TestUtil.nextInt(random(), 1, 6);
                long[] vals = new long[valCount];
                for (int j = 0; j < valCount; j++) {
                    vals[j] = random().nextLong();
                }
                docs[i] = new Doc(docId, vals);
            }
            assertMatchesOracle(config, docs);
        }
    }

    public void testEmpty() throws IOException {
        TSDBDocValuesFormatConfig config = buildConfig(3, 4, 4096);
        assertMatchesOracle(config, new Doc[0]);
    }

    public void testBuildFromValuesMatchesInline() throws IOException {
        // Drive SkipIndexBuilder twice with the same data — once inline (the production path)
        // and once via buildFromValues (the maxOrd==1 fallback path) — and assert byte equality.
        TSDBDocValuesFormatConfig config = buildConfig(3, 4, 256);
        Doc[] docs = randomDocs(500);

        // Inline path: drive onNewDoc + accumulate explicitly.
        SkipIndexBuilder inlineBuilder = new SkipIndexBuilder(config);
        for (Doc d : docs) {
            inlineBuilder.onNewDoc(d.docId, d.values.length, d.values[0]);
            for (long v : d.values) {
                inlineBuilder.accumulate(v);
            }
        }
        ByteBuffersDataOutput inlineSkip = new ByteBuffersDataOutput();
        ByteBuffersDataOutput inlineMeta = new ByteBuffersDataOutput();
        try (
            ByteBuffersIndexOutput skipOut = new ByteBuffersIndexOutput(inlineSkip, "s", "s");
            ByteBuffersIndexOutput metaOut = new ByteBuffersIndexOutput(inlineMeta, "m", "m")
        ) {
            inlineBuilder.writeSkipIndex(skipOut, metaOut);
        }

        // Fallback path: drive buildFromValues with a synthetic SortedNumericDocValues.
        SkipIndexBuilder fallbackBuilder = new SkipIndexBuilder(config);
        fallbackBuilder.buildFromValues(new ArrayBackedSortedNumeric(docs));
        ByteBuffersDataOutput fbSkip = new ByteBuffersDataOutput();
        ByteBuffersDataOutput fbMeta = new ByteBuffersDataOutput();
        try (
            ByteBuffersIndexOutput skipOut = new ByteBuffersIndexOutput(fbSkip, "s", "s");
            ByteBuffersIndexOutput metaOut = new ByteBuffersIndexOutput(fbMeta, "m", "m")
        ) {
            fallbackBuilder.writeSkipIndex(skipOut, metaOut);
        }

        assertArrayEquals("skip data must match between inline and fallback paths", inlineSkip.toArrayCopy(), fbSkip.toArrayCopy());
        assertArrayEquals("meta must match between inline and fallback paths", inlineMeta.toArrayCopy(), fbMeta.toArrayCopy());
    }

    /**
     * Drive both the builder (inline path) and the oracle (single-pass implementation that
     * SkipIndexBuilder replaces) on the same input and assert byte-for-byte equality of the
     * skip data and the meta header.
     */
    private static void assertMatchesOracle(TSDBDocValuesFormatConfig config, Doc[] docs) throws IOException {
        // Actual: drive the builder.
        SkipIndexBuilder builder = new SkipIndexBuilder(config);
        for (Doc d : docs) {
            builder.onNewDoc(d.docId, d.values.length, d.values[0]);
            for (long v : d.values) {
                builder.accumulate(v);
            }
        }
        ByteBuffersDataOutput actualSkip = new ByteBuffersDataOutput();
        ByteBuffersDataOutput actualMeta = new ByteBuffersDataOutput();
        try (
            ByteBuffersIndexOutput skipOut = new ByteBuffersIndexOutput(actualSkip, "s", "s");
            ByteBuffersIndexOutput metaOut = new ByteBuffersIndexOutput(actualMeta, "m", "m")
        ) {
            builder.writeSkipIndex(skipOut, metaOut);
        }

        // Oracle: historical single-pass implementation.
        ByteBuffersDataOutput oracleSkip = new ByteBuffersDataOutput();
        ByteBuffersDataOutput oracleMeta = new ByteBuffersDataOutput();
        try (
            ByteBuffersIndexOutput skipOut = new ByteBuffersIndexOutput(oracleSkip, "s", "s");
            ByteBuffersIndexOutput metaOut = new ByteBuffersIndexOutput(oracleMeta, "m", "m")
        ) {
            oracleWriteSkipIndex(config, new ArrayBackedSortedNumeric(docs), skipOut, metaOut);
        }

        assertArrayEquals("skip data byte mismatch", oracleSkip.toArrayCopy(), actualSkip.toArrayCopy());
        assertArrayEquals("meta byte mismatch", oracleMeta.toArrayCopy(), actualMeta.toArrayCopy());
    }

    /**
     * Verbatim port of the pre-SkipIndexBuilder logic from AbstractTSDBDocValuesConsumer.
     * Writes the skip-data bytes to {@code skipOut} and the skip-meta header (start, length,
     * max, min, docCount, maxDocId) to {@code meta}.
     */
    private static void oracleWriteSkipIndex(
        TSDBDocValuesFormatConfig config,
        SortedNumericDocValues values,
        IndexOutput skipOut,
        IndexOutput meta
    ) throws IOException {
        final long start = skipOut.getFilePointer();
        long globalMaxValue = Long.MIN_VALUE;
        long globalMinValue = Long.MAX_VALUE;
        int globalDocCount = 0;
        int maxDocId = -1;
        final List<OracleAcc> accumulators = new ArrayList<>();
        OracleAcc accumulator = null;
        final int maxAccumulators = 1 << (config.skipIndexLevelShift() * (config.skipIndexMaxLevel() - 1));
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            final long firstValue = values.nextValue();
            if (accumulator != null && accumulator.isDone(config.skipIndexIntervalSize(), values.docValueCount(), firstValue, doc)) {
                globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                globalDocCount += accumulator.docCount;
                maxDocId = accumulator.maxDocID;
                accumulator = null;
                if (accumulators.size() == maxAccumulators) {
                    oracleWriteLevels(config, accumulators, skipOut);
                    accumulators.clear();
                }
            }
            if (accumulator == null) {
                accumulator = new OracleAcc(doc);
                accumulators.add(accumulator);
            }
            accumulator.nextDoc(doc);
            accumulator.accumulate(firstValue);
            for (int i = 1, end = values.docValueCount(); i < end; ++i) {
                accumulator.accumulate(values.nextValue());
            }
        }
        if (accumulators.isEmpty() == false) {
            globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, accumulator.minValue);
            globalDocCount += accumulator.docCount;
            maxDocId = accumulator.maxDocID;
            oracleWriteLevels(config, accumulators, skipOut);
        }
        meta.writeLong(start);
        meta.writeLong(skipOut.getFilePointer() - start);
        meta.writeLong(globalMaxValue);
        meta.writeLong(globalMinValue);
        meta.writeInt(globalDocCount);
        meta.writeInt(maxDocId);
    }

    private static void oracleWriteLevels(TSDBDocValuesFormatConfig config, List<OracleAcc> accumulators, IndexOutput skipOut)
        throws IOException {
        final List<List<OracleAcc>> levels = new ArrayList<>(config.skipIndexMaxLevel());
        levels.add(accumulators);
        for (int i = 0; i < config.skipIndexMaxLevel() - 1; i++) {
            levels.add(oracleBuildLevel(config, levels.get(i)));
        }
        int total = accumulators.size();
        for (int index = 0; index < total; index++) {
            final int levelCount = oracleGetLevels(config, index, total);
            skipOut.writeByte((byte) levelCount);
            for (int level = levelCount - 1; level >= 0; level--) {
                OracleAcc acc = levels.get(level).get(index >> (config.skipIndexLevelShift() * level));
                skipOut.writeInt(acc.maxDocID);
                skipOut.writeInt(acc.minDocID);
                skipOut.writeLong(acc.maxValue);
                skipOut.writeLong(acc.minValue);
                skipOut.writeInt(acc.docCount);
            }
        }
    }

    private static List<OracleAcc> oracleBuildLevel(TSDBDocValuesFormatConfig config, List<OracleAcc> accumulators) {
        final int levelSize = 1 << config.skipIndexLevelShift();
        final List<OracleAcc> collector = new ArrayList<>();
        for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {
            collector.add(OracleAcc.merge(accumulators, i, levelSize));
        }
        return collector;
    }

    private static int oracleGetLevels(TSDBDocValuesFormatConfig config, int index, int size) {
        if (Integer.numberOfTrailingZeros(index) >= config.skipIndexLevelShift()) {
            int left = size - index;
            for (int level = config.skipIndexMaxLevel() - 1; level > 0; level--) {
                int numberIntervals = 1 << (config.skipIndexLevelShift() * level);
                if (left >= numberIntervals && index % numberIntervals == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }

    private static TSDBDocValuesFormatConfig buildConfig(int levelShift, int maxLevel, int intervalSize) {
        return new TSDBDocValuesFormatConfig(
            TSDBDocValuesFormatConfig.VERSION_CURRENT,
            new TSDBDocValuesFormatConfig.TermsDictConfig(15, 4, 9, 511),
            new TSDBDocValuesFormatConfig.SkipIndexConfig(levelShift, maxLevel, intervalSize),
            new TSDBDocValuesFormatConfig.NumericConfig(7, 7, 1),
            new TSDBDocValuesFormatConfig.BinaryConfig(0, 0, false, BinaryDVCompressionMode.NO_COMPRESS),
            16,
            false
        );
    }

    private static Doc[] randomDocs(int n) {
        Doc[] docs = new Doc[n];
        int docId = 0;
        for (int i = 0; i < n; i++) {
            docId += 1 + TestUtil.nextInt(random(), 0, 3);
            int count = 1 + TestUtil.nextInt(random(), 0, 4);
            long[] vals = new long[count];
            for (int j = 0; j < count; j++) {
                vals[j] = random().nextLong();
            }
            docs[i] = new Doc(docId, vals);
        }
        return docs;
    }

    private static Doc doc(int docId, long... values) {
        return new Doc(docId, values);
    }

    private record Doc(int docId, long[] values) {}

    /** Replays a Doc[] as a SortedNumericDocValues iterator for the oracle / buildFromValues. */
    private static final class ArrayBackedSortedNumeric extends SortedNumericDocValues {
        private final Doc[] docs;
        private int index = -1;
        private int valueIndex = 0;

        ArrayBackedSortedNumeric(Doc[] docs) {
            this.docs = docs;
        }

        @Override
        public int docID() {
            if (index == -1) return -1;
            if (index >= docs.length) return NO_MORE_DOCS;
            return docs[index].docId;
        }

        @Override
        public int nextDoc() {
            index++;
            valueIndex = 0;
            return docID();
        }

        @Override
        public int advance(int target) {
            do {
                index++;
            } while (index < docs.length && docs[index].docId < target);
            valueIndex = 0;
            return docID();
        }

        @Override
        public boolean advanceExact(int target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return docs.length;
        }

        @Override
        public int docValueCount() {
            return docs[index].values.length;
        }

        @Override
        public long nextValue() {
            return docs[index].values[valueIndex++];
        }
    }

    /** Oracle accumulator — verbatim copy of the historical SkipAccumulator. */
    private static final class OracleAcc {
        int minDocID;
        int maxDocID;
        int docCount;
        long minValue;
        long maxValue;

        OracleAcc(int docID) {
            minDocID = docID;
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
            docCount = 0;
        }

        boolean isDone(int intervalSize, int valueCount, long nextValue, int nextDoc) {
            if (docCount < intervalSize) {
                return false;
            }
            return valueCount > 1 || minValue != maxValue || minValue != nextValue || docCount != nextDoc - minDocID;
        }

        void accumulate(long value) {
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
        }

        void accumulate(OracleAcc other) {
            maxDocID = other.maxDocID;
            minValue = Math.min(minValue, other.minValue);
            maxValue = Math.max(maxValue, other.maxValue);
            docCount += other.docCount;
        }

        void nextDoc(int docID) {
            maxDocID = docID;
            ++docCount;
        }

        static OracleAcc merge(List<OracleAcc> list, int index, int length) {
            OracleAcc acc = new OracleAcc(list.get(index).minDocID);
            for (int i = 0; i < length; i++) {
                acc.accumulate(list.get(index + i));
            }
            return acc;
        }
    }
}
