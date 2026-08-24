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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ColumnarTsidCalculator;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingExtractor;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
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
 * Compares the ways Elasticsearch can derive time series identifiers for a bulk request.
 *
 * <p><b>The end-to-end comparison</b> — both arms take documents and produce shard ids, setting the
 * tsid on every request, which is exactly what the coordinator needs:
 * <ul>
 *   <li>{@link #encodeThenColumnarRouting} — encode the whole batch, then one column-major pass to
 *       derive every tsid. Two passes over the data, the second reading typed columns.</li>
 *   <li>{@link #extractDuringEncodeRouting} — feed a {@link RoutingExtractor} into the encoder's
 *       parse so tsids fall out of the pass that was happening anyway, as
 *       {@code BulkBatchEncoders.tryEncodeAndRoute} does. One pass, but per document.</li>
 * </ul>
 * This is the real design question: is a second columnar pass cheaper than piggybacking on the parse?
 *
 * <p><b>Isolated tsid cost</b> — narrower arms that exclude encoding, useful for attributing any
 * difference above:
 * <ul>
 *   <li>{@link #columnarTsids} — the column-major pass alone, over a pre-built batch.</li>
 *   <li>{@link #perDocumentTsids} — parse each source again and build its tsid. This is the
 *       <em>non-batch</em> coordinator path; in batch indexing nobody parses twice, so do not read
 *       it as the batching baseline.</li>
 *   <li>{@link #encodeAndColumnarTsids} — encode plus the columnar pass, without the routing work.</li>
 * </ul>
 *
 * <p>The index is created with a single shard on purpose. The extractor path commits each row to the
 * partition of the shard it just computed, while the columnar path cannot know a shard id until the
 * batch exists; with one shard both produce exactly one partition, so the measurement isolates tsid
 * derivation instead of also measuring two different partitioning strategies.
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

    /**
     * Whether the last {@link #extractDuringEncodeRouting} invocation fell back to per-document
     * routing. Exposed so the self-test can assert the fallback fires only for array dimensions: if
     * the extractor rejected something unexpected, that arm would quietly measure the fallback while
     * still producing correct shard ids, and the comparison would be meaningless.
     */
    boolean usedFallback;

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
        IndexMetadata metadata = IndexMetadata.builder("bench").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
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

    @Benchmark
    public int[] encodeThenColumnarRouting() throws IOException {
        IndexRequest[] requests = newRequests();
        try (EscfEncoder encoder = new EscfEncoder()) {
            for (IndexRequest request : requests) {
                encoder.parseToScratch(request.indexSource().bytes(), XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            try (SourceBatch encoded = encoder.buildPartition(0)) {
                return strategy.indexShard(requests, encoded);
            }
        }
    }

    @Benchmark
    public int[] extractDuringEncodeRouting() throws IOException {
        IndexRequest[] requests = newRequests();
        int[] shardIds = new int[requests.length];
        usedFallback = false;
        try (EscfEncoder encoder = new EscfEncoder()) {
            RoutingExtractor extractor = strategy.newRoutingExtractor();
            try {
                for (int i = 0; i < requests.length; i++) {
                    extractor.reset();
                    encoder.parseToScratch(requests[i].indexSource().bytes(), XContentType.JSON, extractor);
                    shardIds[i] = extractor.computeShardId(requests[i]);
                    encoder.commitScratchTo(shardIds[i]);
                }
            } catch (Exception e) {
                // An array at a routing column makes the extractor throw. Production then abandons
                // the whole bulk's batch and re-routes every item from its inline source, so that is
                // what gets measured here rather than a partial result.
                usedFallback = true;
                for (int i = 0; i < requests.length; i++) {
                    shardIds[i] = strategy.indexShard(requests[i]);
                }
                return shardIds;
            }
            // Built and discarded: the caller needs the batch, so both arms must pay for it.
            encoder.buildPartition(0).close();
        }
        return shardIds;
    }

    /**
     * Fresh requests per invocation. Required, not incidental: {@code indexShard(IndexRequest[],
     * SourceBatch)} branches on whether the first request already carries a tsid, and both routing
     * arms set one, so reused requests would silently take the pre-set path on the second invocation.
     */
    private IndexRequest[] newRequests() {
        IndexRequest[] requests = new IndexRequest[sources.size()];
        for (int i = 0; i < requests.length; i++) {
            requests[i] = new IndexRequest("bench").source(sources.get(i), XContentType.JSON);
        }
        return requests;
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
