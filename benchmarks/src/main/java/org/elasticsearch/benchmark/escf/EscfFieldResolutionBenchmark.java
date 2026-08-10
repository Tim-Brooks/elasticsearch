/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.escf;

import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures the field-name → column-index resolution cost inside {@link EscfEncoder#parseToScratch}.
 *
 * <p>This is the hot path on which three flame-profile hotspots sit:
 * <ul>
 *   <li>{@code fnvHash} (4.55%) — byte-at-a-time FNV-1a in the SIMD parser's name intern cache</li>
 *   <li>{@code Arrays.mismatch} / {@code Arrays.equals} (1.55%) — key verification in the same cache</li>
 *   <li>{@code getOrDefault} on long/string fields (4.17% combined) — {@code SourceSchema.FieldLevel.append}
 *       via {@code EscfRowBuffer.addLeaf}</li>
 * </ul>
 *
 * <p><strong>Benchmark design.</strong> A fresh {@link EscfEncoder} is created per invocation, not
 * per {@code @Setup}. This mirrors the production lifecycle: {@code BulkBatchEncoders} allocates
 * one {@code EscfEncoder} per concrete index per bulk request. The schema (and the positional
 * prediction arrays added in Phase 2) are therefore cold at the start of each iteration, just as
 * they are in production. Using a per-{@code @Setup} encoder would pre-warm the schema and hide
 * exactly the allocation cost we are trying to measure.
 *
 * <p><strong>Corpus shapes.</strong>
 * <ul>
 *   <li>{@code clickbench_flat} — ~60 flat scalar fields, mostly numeric, identical schema every
 *       doc. Mirrors the real ClickBench field mix. This is the primary target: the numeric skew is
 *       why the {@code getOrDefault} for long fields outweighs the one for strings.</li>
 *   <li>{@code otel_nested} — nested objects ({@code resource}, {@code scope}, {@code attributes})
 *       so {@code startObject}/{@code appendNonLeaf} are exercised alongside leaf resolution.</li>
 *   <li>{@code heterogeneous} — rotating field subsets. Exercises the prediction-miss path to
 *       confirm Phase 2 degrades gracefully rather than thrashing.</li>
 * </ul>
 *
 * <p><strong>Parser path.</strong> The benchmark always runs with SIMD enabled (when available),
 * which is the production path. The absolute time per phase is the metric: a phase that eliminates
 * a hotspot will lower the number regardless of which parser produces the field names.
 *
 * <p><strong>Running.</strong>
 * <pre>{@code
 * cd benchmarks
 * ../gradlew run --args "org.elasticsearch.benchmark.escf.EscfFieldResolutionBenchmark \
 *   -rf json -rff build/jmh-result.json" | tee /tmp/bench/escf_field_resolution_baseline
 * }</pre>
 */
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class EscfFieldResolutionBenchmark {

    /** Number of documents encoded per benchmark invocation. */
    @Param({ "1000" })
    private int docCount;

    /** Seed for deterministic document generation. */
    @Param({ "1600172297" })
    private long seed;

    /**
     * Field layout shape. See class javadoc for corpus descriptions.
     */
    @Param({ "clickbench_flat", "otel_nested", "heterogeneous" })
    private String shape;

    private BytesReference[] docs;

    @Setup
    public void setUp() {
        Utils.configureBenchmarkLogging();
        Random random = new Random(seed);
        docs = new BytesReference[docCount];
        for (int i = 0; i < docCount; i++) {
            docs[i] = new BytesArray(generateDoc(random, shape, i));
        }
    }

    /**
     * Encodes all pre-generated documents through one fresh encoder instance. Returning the total
     * column count prevents JMH from dead-code-eliminating the parse calls.
     *
     * <p>A fresh encoder per invocation is deliberate — see class javadoc.
     */
    @Benchmark
    public int encodeDocuments() {
        try (EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            int columns = 0;
            for (BytesReference doc : docs) {
                encoder.parseToScratch(doc, XContentType.JSON, LeafSink.NO_OP);
                encoder.commitScratchTo(0);
            }
            try (EscfBatch batch = encoder.buildPartition(0)) {
                columns = batch.schema().leafCount();
            }
            return columns;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ------------------------------------------------------------------
    // Document generators
    // ------------------------------------------------------------------

    private static String generateDoc(Random random, String shape, int docIndex) {
        return switch (shape) {
            case "clickbench_flat" -> generateClickBenchFlat(random);
            case "otel_nested" -> generateOtelNested(random);
            case "heterogeneous" -> generateHeterogeneous(random, docIndex);
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    /**
     * Mirrors the ClickBench {@code hits} schema: ~60 scalar fields, predominantly long/int,
     * with a handful of strings. Field names and types are fixed — only values vary. This exercises
     * the warm-path (identical schema across all docs) for which Phase 2 (positional prediction)
     * provides the largest improvement.
     */
    private static String generateClickBenchFlat(Random random) {
        return """
            {
              "WatchID": %d, "JavaEnable": %d, "Title": "%s",
              "GoodEvent": %d, "EventTime": %d, "EventDate": %d,
              "CounterID": %d, "ClientIP": %d, "ClientIP6": "%s",
              "RegionID": %d, "UserID": %d,
              "CounterClass": %d, "OS": %d, "UserAgent": %d,
              "URL": "https://example.com/%s", "Referer": "https://ref.example.com/%s",
              "URLDomain": "example.com", "RefererDomain": "ref.example.com",
              "Refresh": %d, "IsRobot": %d, "RefererCategories": %d,
              "URLCategories": %d, "URLRegions": %d, "RefererRegions": %d,
              "ResolutionWidth": %d, "ResolutionHeight": %d, "ResolutionDepth": %d,
              "FlashMajor": %d, "FlashMinor": %d, "FlashMinor2": "%d",
              "NetMajor": %d, "NetMinor": %d, "UserAgentMajor": %d,
              "UserAgentMinor": %d, "CookieEnable": %d, "JavascriptEnable": %d,
              "IsMobile": %d, "MobilePhone": %d, "MobilePhoneModel": "%s",
              "Params": "", "IPNetworkID": %d,
              "TraficSourceID": %d, "SearchEngineID": %d,
              "SearchPhrase": "%s",
              "AdvEngineID": %d, "IsArtifical": %d, "WindowClientWidth": %d,
              "WindowClientHeight": %d, "ClientTimeZone": %d,
              "ClientEventTime": %d, "SilverlightVersion1": %d, "SilverlightVersion2": %d,
              "SilverlightVersion3": %d, "SilverlightVersion4": %d,
              "PageCharset": "UTF-8", "CodeVersion": %d, "IsLink": %d,
              "IsDownload": %d, "IsNotBounce": %d, "FUniqID": %d,
              "HID": %d, "IsOldCounter": %d, "IsEvent": %d,
              "IsParameter": %d, "DontCountHits": %d, "WithHash": %d,
              "HitColor": "W", "UTCEventTime": %d,
              "Age": %d, "Sex": %d, "Income": %d,
              "Interests": %d, "Robotness": %d, "GeneralInterests": %d,
              "RemoteIP": %d, "RemoteIP6": "%s",
              "WindowName": %d, "OpenerName": %d, "HistoryLength": %d,
              "BrowserLanguage": "en", "BrowserCountry": "US",
              "SocialNetwork": "", "SocialAction": "", "HTTPError": %d,
              "SendTiming": %d, "DNSTiming": %d, "ConnectTiming": %d,
              "ResponseStartTiming": %d, "ResponseEndTiming": %d,
              "FetchTiming": %d, "RedirectTiming": %d, "DOMInteractiveTiming": %d,
              "ContentLoadTiming": %d, "OnLoadTiming": %d,
              "RequestNum": %d, "RequestTry": %d,
              "NetErrorCode": %d, "SocialShareNetwork": "", "SocialSharePage": "",
              "ParamPrice": %d, "ParamOrderID": "", "ParamCurrency": "USD",
              "ParamCurrencyID": %d,
              "GoalsReached": %d, "OpenstatServiceName": "", "OpenstatCampaignID": "",
              "OpenstatAdID": "", "OpenstatSourceID": "",
              "UTMSource": "", "UTMMedium": "", "UTMCampaign": "", "UTMContent": "", "UTMTerm": "",
              "FromTag": "", "HasGCLID": %d, "RefererHash": %d, "URLHash": %d,
              "CLID": %d, "YCLID": %d, "ShareService": "", "ShareURL": "", "ShareTitle": ""
            }""".formatted(
            // Each %d / %s in insertion order
            random.nextLong(),
            random.nextInt(2),
            randomWord(random),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(19000),
            random.nextInt(100000),
            (long) (random.nextDouble() * 4_294_967_295L),
            "::1",
            random.nextInt(200000),
            random.nextLong(),
            random.nextInt(10),
            random.nextInt(255),
            random.nextInt(255),
            randomWord(random),
            randomWord(random),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(1000),
            random.nextInt(1000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(3840),
            random.nextInt(2160),
            random.nextInt(32),
            random.nextInt(33),
            random.nextInt(10),
            String.valueOf(random.nextInt(10)),
            random.nextInt(10),
            random.nextInt(10),
            random.nextInt(100),
            random.nextInt(100),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            randomWord(random),
            random.nextInt(1000000),
            random.nextInt(30),
            random.nextInt(100),
            randomWord(random),
            random.nextInt(10),
            random.nextInt(2),
            random.nextInt(3840),
            random.nextInt(2160),
            random.nextInt(720),
            random.nextLong(),
            random.nextInt(4),
            random.nextInt(4),
            random.nextInt(4000),
            random.nextInt(10000),
            random.nextInt(1000000),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(1000000),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextInt(2),
            random.nextLong(),
            random.nextInt(90),
            random.nextInt(2),
            random.nextInt(5),
            random.nextInt(10000),
            random.nextInt(10),
            random.nextInt(1000),
            (long) (random.nextDouble() * 4_294_967_295L),
            "::1",
            random.nextInt(1000),
            random.nextInt(1000),
            random.nextInt(100),
            random.nextInt(1000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100000),
            random.nextInt(100),
            random.nextInt(10),
            random.nextInt(10),
            random.nextLong(),
            random.nextInt(1000),
            random.nextInt(10),
            random.nextInt(2),
            random.nextLong(),
            random.nextLong(),
            random.nextInt(100),
            random.nextLong()
        );
    }

    /**
     * Mimics a flattened OTEL log record with nested {@code resource}, {@code scope}, and
     * {@code attributes} objects. Exercises {@code startObject}/{@code appendNonLeaf} as well as
     * leaf resolution, so the nested-object path is included in the measurement.
     */
    private static String generateOtelNested(Random random) {
        return """
            {
              "@timestamp": "2025-09-23T%02d:%02d:%02dZ",
              "resource": {
                "service.name": "%s",
                "service.version": "1.%d.0",
                "host.name": "host-%d",
                "deployment.environment": "%s"
              },
              "scope": {
                "name": "%s-logger",
                "version": "2.%d.0"
              },
              "severity_text": "%s",
              "severity_number": %d,
              "body": "%s",
              "trace_id": "%s",
              "span_id": "%s",
              "trace_flags": %d,
              "attributes": {
                "http.method": "%s",
                "http.status_code": %d,
                "http.url": "https://api.example.com/%s",
                "user.id": %d,
                "db.system": "postgresql",
                "db.statement": "SELECT * FROM %s WHERE id = %d"
              }
            }""".formatted(
            random.nextInt(24),
            random.nextInt(60),
            random.nextInt(60),
            randomService(random),
            random.nextInt(10),
            random.nextInt(100),
            randomEnv(random),
            randomService(random),
            random.nextInt(5),
            randomSeverity(random),
            random.nextInt(25),
            randomMessage(random),
            randomHex(random, 32),
            randomHex(random, 16),
            random.nextInt(2),
            randomMethod(random),
            random.nextInt(599) + 100,
            randomWord(random),
            random.nextLong(),
            randomWord(random),
            random.nextInt(10000)
        );
    }

    /**
     * Rotates between three distinct field subsets (A, B, C) across documents so that prediction
     * misses occur regularly. Validates that Phase 2's positional prediction degrades gracefully
     * rather than thrashing on schema variation.
     */
    private static String generateHeterogeneous(Random random, int docIndex) {
        return switch (docIndex % 3) {
            case 0 -> """
                {"type":"A","id":%d,"ts":%d,"val":%.4f,"label":"%s","active":%b,"count":%d}""".formatted(
                random.nextLong(),
                random.nextLong(),
                random.nextDouble(),
                randomWord(random),
                random.nextBoolean(),
                random.nextInt(10000)
            );
            case 1 -> """
                {"type":"B","uid":"%s","score":%.3f,"tags":%d,"region":"%s","retries":%d}""".formatted(
                randomWord(random),
                random.nextDouble() * 100,
                random.nextInt(50),
                randomWord(random),
                random.nextInt(5)
            );
            default -> """
                {"type":"C","key":%d,"name":"%s","bytes":%d,"ok":%b,"lat":%.2f,"code":%d}""".formatted(
                random.nextLong(),
                randomWord(random),
                random.nextLong(),
                random.nextBoolean(),
                random.nextDouble() * 1000,
                random.nextInt(600)
            );
        };
    }

    // ------------------------------------------------------------------
    // Value generators
    // ------------------------------------------------------------------

    private static final String[] WORDS = {
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa" };
    private static final String[] SERVICES = { "frontend", "backend", "gateway", "worker", "scheduler" };
    private static final String[] ENVS = { "prod", "staging", "dev", "qa" };
    private static final String[] SEVERITIES = { "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL" };
    private static final String[] MESSAGES_OTEL = {
        "Request processed",
        "Connection timeout",
        "Failed to place order",
        "Slow query detected",
        "Cache miss",
        "Auth succeeded" };
    private static final String[] METHODS = { "GET", "POST", "PUT", "DELETE", "PATCH" };

    private static String randomWord(Random r) {
        return WORDS[r.nextInt(WORDS.length)];
    }

    private static String randomService(Random r) {
        return SERVICES[r.nextInt(SERVICES.length)];
    }

    private static String randomEnv(Random r) {
        return ENVS[r.nextInt(ENVS.length)];
    }

    private static String randomSeverity(Random r) {
        return SEVERITIES[r.nextInt(SEVERITIES.length)];
    }

    private static String randomMessage(Random r) {
        return MESSAGES_OTEL[r.nextInt(MESSAGES_OTEL.length)];
    }

    private static String randomMethod(Random r) {
        return METHODS[r.nextInt(METHODS.length)];
    }

    private static String randomHex(Random r, int digits) {
        StringBuilder sb = new StringBuilder(digits);
        for (int i = 0; i < digits; i++) {
            sb.append(HEX_CHARS[r.nextInt(16)]);
        }
        return sb.toString();
    }

    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
}
