/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ColumnarTsidCalculator;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Compares the two ways Elasticsearch can derive time series identifiers for a bulk request.
 *
 * <ul>
 *   <li>{@link #columnarTsids} — one column-major pass over an already-encoded batch. This is the
 *       marginal cost in batch indexing, where the batch is built for other reasons anyway, and is
 *       therefore the number the production decision turns on.</li>
 *   <li>{@link #perDocumentTsids} — parse each document's source and build its tsid on its own. The
 *       XContent parse is inherent to this path, not an artefact of the benchmark.</li>
 *   <li>{@link #encodeAndColumnarTsids} — encode the batch and then compute, for the case where the
 *       batch would not otherwise exist. Included so that "the columnar arm gets a free batch" is
 *       answered with a measurement rather than an argument.</li>
 * </ul>
 *
 * <p><b>Results are per batch of {@code docCount} documents, not per document.</b> JMH's
 * {@code @OperationsPerInvocation} cannot take a {@code @Param}, so divide by {@code docCount} before
 * comparing across batch sizes — otherwise the 1024-document rows look 64x worse than they are.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class TsidCalculationBenchmark {

    /** Shape of the generated dimension values, selecting which column kinds the scan has to handle. */
    public enum Shape {
        /** Every document carries every dimension as a string: dense STRING columns. The common case. */
        DENSE_STRING,
        /**
         * Every document carries every dimension as a long. LONG columns hash by pure arithmetic, so
         * this separates accumulator overhead from murmur3-over-bytes, which otherwise dominates.
         */
        DENSE_LONG,
        /** Each document carries roughly half the dimensions: sparse validity bitsets, skipped docs. */
        SPARSE_STRING,
        /** One dimension is a small array: the ARRAY column path and value-similarity dedup. */
        ARRAY_STRING
    }

    private static final String DIMENSION_PATTERN = "dim.*";
    private static final int ARRAY_LENGTH = 3;
    private static final int VALUE_LENGTH = 16;
    private static final long SEED = 42L;
    private static final char[] ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    // Do NOT make any field final (even if it is not annotated with @Param)! See also
    // http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java

    @Param({ "16", "1024" })
    public int docCount;

    @Param({ "4", "16" })
    public int dimensionCount;

    @Param({ "DENSE_STRING", "DENSE_LONG", "SPARSE_STRING", "ARRAY_STRING" })
    public Shape shape;

    private List<BytesReference> sources;
    private EscfBatch batch;
    private Predicate<String> isDimension;
    private IndexVersion indexVersion;
    private IndexRouting.ExtractFromSource.ForIndexDimensions strategy;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Utils.configureBenchmarkLogging();

        // A fixed seed so the corpus is identical run to run, and identical between the two arms.
        Random random = new Random(SEED);
        sources = new ArrayList<>(docCount);
        for (int doc = 0; doc < docCount; doc++) {
            sources.add(buildSource(doc, random));
        }

        indexVersion = IndexVersion.current();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), DIMENSION_PATTERN)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("bench").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        strategy = (IndexRouting.ExtractFromSource.ForIndexDimensions) IndexRouting.fromIndexMetadata(metadata);

        // The same matcher ForIndexDimensions builds internally from index.dimensions.
        isDimension = Regex.simpleMatcher(DIMENSION_PATTERN);

        // Encoded once and reused: computeTsids only reads the batch, taking a fresh present-doc
        // iterator and array reader per call.
        batch = EscfEncoder.encode(sources, XContentType.JSON);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (batch != null) {
            batch.close();
            batch = null;
        }
    }

    @Benchmark
    public BytesRef[] columnarTsids() {
        return ColumnarTsidCalculator.computeTsids(batch, isDimension, indexVersion);
    }

    @Benchmark
    public BytesRef[] perDocumentTsids() {
        BytesRef[] tsids = new BytesRef[sources.size()];
        for (int i = 0; i < tsids.length; i++) {
            tsids[i] = strategy.buildTsid(XContentType.JSON, sources.get(i));
        }
        return tsids;
    }

    @Benchmark
    public BytesRef[] encodeAndColumnarTsids() throws IOException {
        try (EscfBatch encoded = EscfEncoder.encode(sources, XContentType.JSON)) {
            return ColumnarTsidCalculator.computeTsids(encoded, isDimension, indexVersion);
        }
    }

    private BytesReference buildSource(int doc, Random random) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            switch (shape) {
                case DENSE_STRING -> {
                    for (int d = 0; d < dimensionCount; d++) {
                        builder.field(dimensionName(d), randomValue(random));
                    }
                }
                case DENSE_LONG -> {
                    for (int d = 0; d < dimensionCount; d++) {
                        builder.field(dimensionName(d), random.nextLong());
                    }
                }
                case SPARSE_STRING -> {
                    boolean anyPresent = false;
                    for (int d = 0; d < dimensionCount; d++) {
                        if (random.nextBoolean()) {
                            builder.field(dimensionName(d), randomValue(random));
                            anyPresent = true;
                        }
                    }
                    if (anyPresent == false) {
                        // A document with no dimensions at all makes both paths throw, so force one.
                        builder.field(dimensionName(0), randomValue(random));
                    }
                }
                case ARRAY_STRING -> {
                    builder.startArray(dimensionName(0));
                    for (int i = 0; i < ARRAY_LENGTH; i++) {
                        builder.value(randomValue(random));
                    }
                    builder.endArray();
                    for (int d = 1; d < dimensionCount; d++) {
                        builder.field(dimensionName(d), randomValue(random));
                    }
                }
            }
            // A non-dimension field, so the batch always holds a column the tsid scan must skip.
            builder.field("ts", doc);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private static String dimensionName(int dimension) {
        return "dim.f" + dimension;
    }

    private static String randomValue(Random random) {
        char[] value = new char[VALUE_LENGTH];
        for (int i = 0; i < value.length; i++) {
            value[i] = ALPHABET[random.nextInt(ALPHABET.length)];
        }
        return new String(value);
    }
}
