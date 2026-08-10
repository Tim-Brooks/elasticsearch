/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Differential tests for the SIMD JSON parser path in {@link EscfEncoder}.
 *
 * <p>Each test encodes the same document(s) through both the SIMD-enabled encoder and a
 * Jackson-only baseline (constructed via the package-private {@code allowSimd=false} constructor)
 * and asserts that the decoded row maps are identical. This validates that the SIMD path produces
 * equivalent results to the established Jackson path for the common scenarios relevant to the
 * macro benchmark — it is not expected to match Jackson for every JSON edge case (e.g. exotic
 * number formats or unicode escape sequences).
 *
 * <p>Ineligibility cases (doc size, composite source, non-zero offset, {@code passRawText} sinks)
 * assert that the SIMD encoder falls back correctly and still produces the right output.
 */
public class EscfEncoderSimdJsonTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Differential equality: SIMD vs Jackson for common scenarios
    // -----------------------------------------------------------------------

    public void testFlatScalars() throws IOException {
        assertSameOutput("""
            {"i":42,"l":10000000000,"d":1.5,"s":"hello","b":true,"f":false,"n":null}""");
    }

    public void testNestedObjects() throws IOException {
        assertSameOutput("""
            {"user":{"name":"alice","age":30},"status":"active"}""");
    }

    public void testDeepNesting() throws IOException {
        assertSameOutput("""
            {"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":1}}}}}}}}""");
    }

    public void testEmptyObject() throws IOException {
        assertSameOutput("""
            {"empty":{},"x":1}""");
    }

    public void testFixedLongArray() throws IOException {
        assertSameOutput("""
            {"vals":[1,2,3,4]}""");
    }

    public void testFixedDoubleArray() throws IOException {
        assertSameOutput("""
            {"vals":[1.5,2.5,-3.25]}""");
    }

    public void testFixedStringArray() throws IOException {
        assertSameOutput("""
            {"tags":["alpha","beta","gamma"]}""");
    }

    public void testArrayOfObjects() throws IOException {
        assertSameOutput("""
            {"items":[{"x":1},{"y":"two"}]}""");
    }

    public void testHeterogeneousArray() throws IOException {
        assertSameOutput("""
            {"mixed":[1,"two",true]}""");
    }

    public void testExplicitNull() throws IOException {
        assertSameOutput("""
            {"a":null,"b":5}""");
    }

    public void testEmptyArray() throws IOException {
        assertSameOutput("""
            {"empty":[],"x":1}""");
    }

    public void testBooleans() throws IOException {
        assertSameOutput("""
            {"t":true,"f":false}""");
    }

    /**
     * Multi-row batch: exercises the SIMD string buffer lifetime constraint — each document's
     * {@code reset()} overwrites the buffer, so strings must be copied into the column builder
     * (via {@code commitScratchTo}) before the next {@code reset()}. The caller does parse +
     * commit per document, so this is safe, but a regression would corrupt later rows.
     */
    public void testMultiRowBatchStringLifetime() throws IOException {
        assertSameOutput("""
            {"host":"server-alpha","service":"api","env":"prod"}""", """
            {"host":"server-beta","service":"worker","env":"staging"}""", """
            {"host":"server-gamma","service":"api","env":"prod"}""", """
            {"host":"server-delta","service":"db","env":"prod"}""");
    }

    /**
     * Diverse multi-row batch with absent fields, nested objects, arrays, and cross-row type
     * variation that promotes a column to UNION. Representative of real OTEL-shaped docs.
     */
    public void testOtelLogShapedDocs() throws IOException {
        assertSameOutput("""
            {"@timestamp":"2025-09-23T02:00:00Z","TraceId":"abc123","SpanId":"def456",\
            "TraceFlags":1,"SeverityText":"error","SeverityNumber":0,\
            "ServiceName":"frontend","Body":"Failed to place order",\
            "ResourceSchemaUrl":"","ScopeName":"node-logger","ScopeVersion":""}""", """
            {"@timestamp":"2025-09-23T02:01:00Z","TraceId":"aaa111","SpanId":"bbb222",\
            "TraceFlags":0,"SeverityText":"info","SeverityNumber":1,\
            "ServiceName":"backend","Body":"Request processed",\
            "ResourceSchemaUrl":"","ScopeName":"go-logger","ScopeVersion":"1.0"}""", """
            {"@timestamp":"2025-09-23T02:02:00Z","TraceId":"ccc333","SpanId":"ddd444",\
            "TraceFlags":1,"SeverityText":"warn","SeverityNumber":2,\
            "ServiceName":"frontend","Body":"Slow query"}""");
    }

    /**
     * Fields with varying types across rows (long in one doc, absent in another) — exercises
     * the union-promotion path in the column builder.
     */
    public void testHeterogeneousColumnsAcrossDocs() throws IOException {
        assertSameOutput("""
            {"a":1,"keep":true}""", """
            {"a":"text","keep":false}""", """
            {"keep":true}""");
    }

    /**
     * Same leaf name at the same traversal position but under different parent objects — the
     * positional prediction must check both name identity AND parent index, so "x" nested inside
     * "a" and "x" at the root are treated as distinct columns.
     */
    public void testSameNameDifferentParent() throws IOException {
        assertSameOutput("""
            {"a":{"x":1},"y":2}""", """
            {"x":10,"y":20}""", """
            {"a":{"x":3},"y":4}""");
    }

    /**
     * Field order permuted between documents — the positional prediction repairs on every
     * permuted row and must remain correct rather than assigning the wrong column index.
     */
    public void testFieldOrderPermuted() throws IOException {
        assertSameOutput("""
            {"a":1,"b":2,"c":3}""", """
            {"c":30,"a":10,"b":20}""", """
            {"b":200,"c":300,"a":100}""");
    }

    /**
     * A field absent in one document but present in the next — the prediction array grows
     * on the longer document and shrinks gracefully on the shorter one (fieldPos stops early).
     */
    public void testAbsentFieldBetweenDocs() throws IOException {
        assertSameOutput("""
            {"a":1,"b":2,"c":3}""", """
            {"a":10}""", """
            {"a":100,"b":200,"c":300}""");
    }

    /**
     * Rotating field sets across many rows — exercises repeated prediction repair and confirms
     * the prediction degrades gracefully (correct output on every permutation, not just the
     * first two documents).
     */
    public void testRotatingFieldSets() throws IOException {
        assertSameOutput("""
            {"x":1,"y":2}""", """
            {"y":20,"z":30}""", """
            {"z":300,"x":100}""", """
            {"x":1000,"y":2000}""", """
            {"y":20000,"z":30000}""", """
            {"z":300000,"x":100000}""");
    }

    /** Zero-offset contiguous source — direct array pass-through, no copy needed. */
    public void testZeroOffsetArrayBackedSource() throws IOException {
        byte[] json = "{\"k\":\"v\",\"n\":123}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BytesReference source = new BytesArray(json, 0, json.length);
        assertSameOutput(List.of(source));
    }

    /**
     * Non-zero array offset: the source bytes start partway into the backing array (common for bulk
     * body slices). Copied into the thread-local scratch buffer before parsing; SIMD still runs.
     */
    public void testNonZeroOffsetArrayBackedSource() throws IOException {
        byte[] padding = new byte[32];
        byte[] json = "{\"k\":\"v\",\"n\":42}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] combined = Arrays.copyOf(padding, padding.length + json.length);
        System.arraycopy(json, 0, combined, padding.length, json.length);
        BytesReference source = new BytesArray(combined, padding.length, json.length);
        assertSameOutput(List.of(source));
    }

    /**
     * Composite (multi-page) source — pages are walked and concatenated into the thread-local
     * scratch buffer before parsing; SIMD still runs.
     */
    public void testCompositeSource() throws IOException {
        byte[] part1 = "{\"k\":\"va".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] part2 = "lue\",\"n\":7}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BytesReference composite = CompositeBytesReference.of(new BytesArray(part1), new BytesArray(part2));
        assertSameOutput(List.of(composite));
    }

    // -----------------------------------------------------------------------
    // True fallback cases: SIMD is skipped, Jackson handles the document
    // -----------------------------------------------------------------------

    /**
     * Document just over the 16 KiB threshold: SIMD path is skipped (size check), falls back to
     * Jackson.
     */
    public void testLargeDocFallsBackToJackson() throws IOException {
        StringBuilder sb = new StringBuilder("{\"data\":\"");
        sb.append("x".repeat(SimdJsonPool.MAX_DOC_BYTES + 10));
        sb.append("\"}");
        String largeJson = sb.toString();
        assertSameOutput(largeJson);
    }

    /**
     * {@link LeafSink} with {@code passRawText() == true}: the SIMD path is skipped to avoid
     * reformatting numbers when computing routing hashes. The SIMD encoder still produces correct
     * output via the Jackson fallback.
     */
    public void testPassRawTextSinkFallsBack() throws IOException {
        String json = "{\"k\":\"v\",\"n\":99}";
        BytesReference source = new BytesArray(json);
        Recycler<BytesRef> recycler = newRecycler();

        LeafSink rawTextSink = new LeafSink() {
            @Override
            public boolean passRawText() {
                return true;
            }

            @Override
            public void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {}
        };

        // Encode with SIMD encoder (will fall back due to passRawText)
        try (EscfEncoder simdEncoder = new EscfEncoder(recycler, true)) {
            simdEncoder.parseToScratch(source, XContentType.JSON, rawTextSink);
            simdEncoder.commitScratchTo(0);
            try (EscfBatch batch = simdEncoder.buildPartition(0)) {
                Map<String, Object> actual = reconstruct(batch, 0);
                assertEquals(asMap(json), actual);
            }
        }
    }

    /**
     * Document containing a valid JSON unicode escape sequence: SIMD handles {@code \\uXXXX}
     * where all four hex digits are valid, producing the same output as Jackson.
     */
    public void testValidUnicodeEscape() throws IOException {
        // A = 'A'; all four hex digits are valid, so SIMD processes this directly.
        assertSameOutput("""
            {"name":"\\u0041lice","age":30}""");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Encodes each JSON string as a {@link BytesArray} and asserts SIMD ≡ Jackson. */
    private static void assertSameOutput(String... jsonDocs) throws IOException {
        List<BytesReference> sources = new ArrayList<>(jsonDocs.length);
        for (String doc : jsonDocs) {
            sources.add(new BytesArray(doc));
        }
        assertSameOutput(sources);
    }

    /**
     * Encodes {@code sources} through both the SIMD-enabled and the Jackson-only encoder and
     * asserts that every row's decoded source map is identical.
     */
    private static void assertSameOutput(List<BytesReference> sources) throws IOException {
        Recycler<BytesRef> recycler = newRecycler();

        try (EscfEncoder simdEncoder = new EscfEncoder(recycler, true); EscfEncoder jacksonEncoder = new EscfEncoder(recycler, false)) {
            for (BytesReference source : sources) {
                simdEncoder.addDocument(source, XContentType.JSON, 0);
                jacksonEncoder.addDocument(source, XContentType.JSON, 0);
            }

            try (EscfBatch simdBatch = simdEncoder.buildPartition(0); EscfBatch jacksonBatch = jacksonEncoder.buildPartition(0)) {
                assertEquals("doc count mismatch", jacksonBatch.docCount(), simdBatch.docCount());
                for (int i = 0; i < jacksonBatch.docCount(); i++) {
                    Map<String, Object> simdRow = reconstruct(simdBatch, i);
                    Map<String, Object> jacksonRow = reconstruct(jacksonBatch, i);
                    assertEquals("row " + i + " mismatch", jacksonRow, simdRow);
                }
            }
        }
    }

    private static Map<String, Object> reconstruct(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            EirfRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    private static Map<String, Object> asMap(String json) {
        return XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }

    private static Recycler<BytesRef> newRecycler() {
        return new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
    }
}
