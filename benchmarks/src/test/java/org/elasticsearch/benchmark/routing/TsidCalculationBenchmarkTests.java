/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.routing;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Self-test for {@link TsidCalculationBenchmark}, run in CI via {@code :benchmarks:check}.
 *
 * <p>The point is not merely that the benchmark runs: it asserts that all three arms produce
 * <em>identical</em> tsids for every parameter combination. If they diverged, the arms would not be
 * doing equivalent work and comparing their timings would be meaningless. It also re-checks
 * columnar/per-document parity over a corpus the unit tests do not generate — sparse dimension
 * subsets, array dimensions, and long rather than string columns.
 */
public class TsidCalculationBenchmarkTests extends ESTestCase {

    private final int docCount;
    private final int dimensionCount;
    private final TsidCalculationBenchmark.Shape shape;

    public TsidCalculationBenchmarkTests(int docCount, int dimensionCount, TsidCalculationBenchmark.Shape shape) {
        this.docCount = docCount;
        this.dimensionCount = dimensionCount;
        this.shape = shape;
    }

    public void testAllArmsAgree() throws Exception {
        TsidCalculationBenchmark bench = new TsidCalculationBenchmark();
        bench.docCount = docCount;
        bench.dimensionCount = dimensionCount;
        bench.shape = shape;
        bench.setup();
        try {
            BytesRef[] columnar = bench.columnarTsids();
            BytesRef[] perDocument = bench.perDocumentTsids();
            BytesRef[] encodeAndColumnar = bench.encodeAndColumnarTsids();

            assertEquals("one tsid per document", docCount, columnar.length);
            assertEquals("one tsid per document", docCount, perDocument.length);
            assertEquals("one tsid per document", docCount, encodeAndColumnar.length);

            for (int doc = 0; doc < docCount; doc++) {
                assertNotNull("null tsid at row " + doc, columnar[doc]);
                assertEquals("columnar vs per-document at row " + doc, perDocument[doc], columnar[doc]);
                assertEquals("columnar vs encode-and-columnar at row " + doc, columnar[doc], encodeAndColumnar[doc]);
            }
        } finally {
            bench.tearDown();
        }
    }

    /** Repeated invocations must be stable, since JMH calls each arm many times per trial. */
    public void testRepeatedInvocationIsStable() throws Exception {
        TsidCalculationBenchmark bench = new TsidCalculationBenchmark();
        bench.docCount = docCount;
        bench.dimensionCount = dimensionCount;
        bench.shape = shape;
        bench.setup();
        try {
            BytesRef[] first = bench.columnarTsids();
            for (int round = 0; round < 3; round++) {
                BytesRef[] repeat = bench.columnarTsids();
                for (int doc = 0; doc < docCount; doc++) {
                    assertEquals("round " + round + " diverged at row " + doc, first[doc], repeat[doc]);
                }
            }
        } finally {
            bench.tearDown();
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> parametersFactory() {
        List<Object[]> parameters = new ArrayList<>();
        for (String docCount : Utils.possibleValues(TsidCalculationBenchmark.class, "docCount")) {
            for (String dimensionCount : Utils.possibleValues(TsidCalculationBenchmark.class, "dimensionCount")) {
                for (String shape : Utils.possibleValues(TsidCalculationBenchmark.class, "shape")) {
                    parameters.add(
                        new Object[] {
                            Integer.parseInt(docCount),
                            Integer.parseInt(dimensionCount),
                            TsidCalculationBenchmark.Shape.valueOf(shape) }
                    );
                }
            }
        }
        return parameters;
    }
}
