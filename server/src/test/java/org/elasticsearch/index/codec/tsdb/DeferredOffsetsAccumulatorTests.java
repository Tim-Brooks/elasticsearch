/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import java.io.IOException;

public class DeferredOffsetsAccumulatorTests extends LuceneTestCase {

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    public void testSingleDoc() throws IOException {
        assertMatchesOracle(new int[] { 3 });
    }

    public void testMultipleDocs() throws IOException {
        assertMatchesOracle(new int[] { 1, 4, 2, 7, 3 });
    }

    public void testAllSingleValued() throws IOException {
        assertMatchesOracle(new int[] { 1, 1, 1, 1, 1, 1 });
    }

    public void testLargeValueCounts() throws IOException {
        assertMatchesOracle(new int[] { 10000, 20000, 30000 });
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 50; iter++) {
            int numDocs = TestUtil.nextInt(random(), 1, 500);
            int[] counts = new int[numDocs];
            for (int i = 0; i < numDocs; i++) {
                counts[i] = TestUtil.nextInt(random(), 1, 100);
            }
            assertMatchesOracle(counts);
        }
    }

    /**
     * Drives DeferredOffsetsAccumulator and compares the bytes it writes to meta + data
     * against the bytes produced by the historical flush-path fallback (AbstractTSDBDocValuesConsumer
     * pre-port lines 892-911), which constructed a DirectMonotonicWriter directly against
     * the meta + data outputs while re-iterating values.
     */
    private void assertMatchesOracle(int[] valueCounts) throws IOException {
        try (Directory dir = newDirectory()) {
            // Actual: drive DeferredOffsetsAccumulator.
            try (
                IndexOutput meta = dir.createOutput("actual-meta", IOContext.DEFAULT);
                IndexOutput data = dir.createOutput("actual-data", IOContext.DEFAULT);
                DeferredOffsetsAccumulator acc = new DeferredOffsetsAccumulator()
            ) {
                for (int c : valueCounts) {
                    acc.addDoc(c);
                }
                acc.build(meta, data, valueCounts.length, DIRECT_MONOTONIC_BLOCK_SHIFT);
            }

            // Oracle: replicate the pre-port direct-write flush-path fallback verbatim.
            try (
                IndexOutput meta = dir.createOutput("oracle-meta", IOContext.DEFAULT);
                IndexOutput data = dir.createOutput("oracle-data", IOContext.DEFAULT)
            ) {
                long start = data.getFilePointer();
                meta.writeLong(start);
                meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
                DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                    meta,
                    data,
                    valueCounts.length + 1L,
                    DIRECT_MONOTONIC_BLOCK_SHIFT
                );
                long addr = 0;
                writer.add(addr);
                for (int c : valueCounts) {
                    addr += c;
                    writer.add(addr);
                }
                writer.finish();
                meta.writeLong(data.getFilePointer() - start);
            }

            assertFilesEqual(dir, "actual-meta", "oracle-meta");
            assertFilesEqual(dir, "actual-data", "oracle-data");

            // Also verify the address table reads back to the expected offsets.
            long[] expectedOffsets = buildExpectedOffsets(valueCounts);
            long[] actualOffsets = readBackOffsets(dir, valueCounts.length);
            for (int i = 0; i < expectedOffsets.length; i++) {
                assertEquals("offset mismatch at index " + i, expectedOffsets[i], actualOffsets[i]);
            }

            for (String name : new String[] { "actual-meta", "actual-data", "oracle-meta", "oracle-data" }) {
                dir.deleteFile(name);
            }
        }
    }

    private static long[] buildExpectedOffsets(int[] valueCounts) {
        long[] offsets = new long[valueCounts.length + 1];
        for (int i = 0; i < valueCounts.length; i++) {
            offsets[i + 1] = offsets[i] + valueCounts[i];
        }
        return offsets;
    }

    private static long[] readBackOffsets(Directory dir, int numDocs) throws IOException {
        try (
            IndexInput meta = dir.openInput("actual-meta", IOContext.DEFAULT);
            IndexInput data = dir.openInput("actual-data", IOContext.DEFAULT)
        ) {
            long start = meta.readLong();
            int blockShift = meta.readVInt();
            DirectMonotonicReader.Meta addressesMeta = DirectMonotonicReader.loadMeta(meta, numDocs + 1, blockShift);
            long length = meta.readLong();
            DirectMonotonicReader reader = DirectMonotonicReader.getInstance(addressesMeta, data.randomAccessSlice(start, length));
            long[] result = new long[numDocs + 1];
            for (int i = 0; i <= numDocs; i++) {
                result[i] = reader.get(i);
            }
            return result;
        }
    }

    private static void assertFilesEqual(Directory dir, String a, String b) throws IOException {
        try (IndexInput ai = dir.openInput(a, IOContext.DEFAULT); IndexInput bi = dir.openInput(b, IOContext.DEFAULT)) {
            assertEquals("length mismatch for " + a + " vs " + b, ai.length(), bi.length());
            byte[] ab = new byte[(int) ai.length()];
            byte[] bb = new byte[(int) bi.length()];
            ai.readBytes(ab, 0, ab.length);
            bi.readBytes(bb, 0, bb.length);
            for (int i = 0; i < ab.length; i++) {
                if (ab[i] != bb[i]) {
                    fail("byte mismatch at offset " + i + " between " + a + " and " + b);
                }
            }
        }
    }
}
