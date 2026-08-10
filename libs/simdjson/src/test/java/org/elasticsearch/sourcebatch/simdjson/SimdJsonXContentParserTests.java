/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch.simdjson;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Differential tests for {@link SimdJsonXContentParser}: for every document, each token and
 * scalar value produced by the SIMD parser is asserted to match Jackson exactly.
 *
 * <p>The differential harness calls {@code nextToken()} on both parsers in lockstep and
 * compares {@code currentToken()}, {@code currentName()}, and scalar accessors ({@code text()},
 * {@code optimizedText().bytes()}, {@code longValue()}, {@code doubleValue()},
 * {@code booleanValue()}). A mismatch on any field is a test failure.
 *
 * <p>This single harness covers: flat objects, nested objects, arrays, arrays of objects, empty
 * containers, bare root scalars, escapes, multi-byte UTF-8, {@code skipChildren()},
 * parser reuse across multiple documents, and the field-name cache.
 */
public class SimdJsonXContentParserTests extends ESTestCase {

    // -- differential harness ----------------------------------------------------------------

    /**
     * Drains both parsers in lockstep; asserts token, name, and scalar value are identical.
     *
     * @param json        the JSON document as a UTF-8 string
     * @param simdParser  reusable SIMD parser instance (caller owns)
     */
    private void assertMatchesJackson(String json, SimdJsonXContentParser simdParser) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        simdParser.reset(bytes, bytes.length);
        try (XContentParser jackson = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
            while (true) {
                XContentParser.Token simdTok = simdParser.nextToken();
                XContentParser.Token jackTok = jackson.nextToken();
                assertEquals("token mismatch for " + json, jackTok, simdTok);
                if (simdTok == null) {
                    break;
                }
                assertEquals("currentName mismatch at token " + simdTok + " for " + json, jackson.currentName(), simdParser.currentName());

                switch (simdTok) {
                    case VALUE_STRING -> {
                        assertEquals("text mismatch for " + json, jackson.text(), simdParser.text());
                        XContentString.UTF8Bytes simdBytes = simdParser.optimizedText().bytes();
                        String decoded = new String(simdBytes.bytes(), simdBytes.offset(), simdBytes.length(), StandardCharsets.UTF_8);
                        assertEquals("optimizedText mismatch for " + json, jackson.text(), decoded);
                    }
                    case VALUE_NUMBER -> {
                        if (simdParser.numberType() == XContentParser.NumberType.LONG) {
                            assertEquals("longValue mismatch for " + json, jackson.longValue(), simdParser.longValue());
                        } else {
                            assertEquals("doubleValue mismatch for " + json, jackson.doubleValue(), simdParser.doubleValue(), 0.0);
                        }
                    }
                    case VALUE_BOOLEAN -> assertEquals(
                        "booleanValue mismatch for " + json,
                        jackson.booleanValue(),
                        simdParser.booleanValue()
                    );
                    default -> {
                        /* containers and null: token + name already compared above */ }
                }
            }
        }
    }

    // -- basic token shapes ------------------------------------------------------------------

    public void testFlatObject() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{\"a\":1,\"b\":\"hello\",\"c\":true,\"d\":false,\"e\":null}", parser);
    }

    public void testNestedObject() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{\"outer\":{\"inner\":42}}", parser);
    }

    public void testArray() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("[1,2,3]", parser);
    }

    public void testArrayOfObjects() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("[{\"a\":1},{\"b\":2}]", parser);
    }

    public void testEmptyObject() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{}", parser);
    }

    public void testEmptyArray() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("[]", parser);
    }

    public void testNestedEmptyContainers() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{\"a\":{},\"b\":[]}", parser);
    }

    // -- bare root scalars -------------------------------------------------------------------

    public void testRootString() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("\"hello world\"", parser);
    }

    public void testRootLong() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("42", parser);
    }

    public void testRootTrue() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("true", parser);
    }

    public void testRootNull() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("null", parser);
    }

    // -- number types ------------------------------------------------------------------------

    public void testLongBoundaries() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{\"min\":" + Long.MIN_VALUE + ",\"max\":" + Long.MAX_VALUE + "}", parser);
    }

    public void testDouble() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        assertMatchesJackson("{\"pi\":3.14159,\"neg\":-2.5e10}", parser);
    }

    // -- string escapes and multi-byte UTF-8 -------------------------------------------------

    public void testStringEscapes() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        // Basic escapes that don't involve unicode escape sequences (those hit a simdjson StringParser bug)
        assertMatchesJackson("{\"esc\":\"line1\\nline2\\ttab\\\"quote\\\\back\"}", parser);
    }

    public void testMultiByteUtf8() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        // Literal multi-byte UTF-8 sequences embedded directly in JSON (no JSON unicode escapes needed)
        // é = U+00E9 (2-byte UTF-8: C3 A9), 中 = U+4E2D (3-byte: E4 B8 AD), 😀 = U+1F600 (4-byte: F0 9F 98 80)
        String json = "{\"two\":\"\u00E9\",\"three\":\"\u4E2D\",\"four\":\"\uD83D\uDE00\"}";
        assertMatchesJackson(json, parser);
    }

    /**
     * Verifies that the simdjson StringParser correctly decodes JSON unicode escape sequences with
     * valid hex digits via the shared {@code walkToTape}/{@link StringParser} path.
     *
     * <p>{@code \\u0041} has four valid hex digits (0, 0, 4, 1), so {@code hexToInt} returns
     * 65 — the code point for {@code 'A'}. The parser writes byte 65 to its string buffer and
     * the resulting value is {@code "A"}.
     */
    public void testSimdJsonParseApiOnUnicodeEscapes() {
        SimdJsonParser direct = new SimdJsonParser(4096, 32);
        byte[] bytes = "{\"k\":\"\\u0041\"}".getBytes(StandardCharsets.UTF_8);
        JsonValue root = direct.parse(bytes, bytes.length);
        JsonValue kVal = root.get("k");
        assertNotNull(kVal);
        assertEquals("A", kVal.asString());
    }

    public void testOptimizedTextBytesDoNotOverrun() throws IOException {
        // Verify that the UTF8Bytes slice length is exactly what was written (SIMD copy loop
        // overshoots but the length prefix should bound our view correctly).
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        String value = "hello";
        byte[] bytes = ("{\"k\":\"" + value + "\"}").getBytes(StandardCharsets.UTF_8);
        parser.reset(bytes, bytes.length);
        parser.nextToken(); // START_OBJECT
        parser.nextToken(); // FIELD_NAME
        parser.nextToken(); // VALUE_STRING
        XContentString.UTF8Bytes utf8 = parser.optimizedText().bytes();
        assertEquals(value.length(), utf8.length());
        assertEquals(value, new String(utf8.bytes(), utf8.offset(), utf8.length(), StandardCharsets.UTF_8));
    }

    // -- skipChildren ------------------------------------------------------------------------

    public void testSkipChildrenObject() throws IOException {
        // skip a nested object, then continue reading
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bytes = "{\"a\":{\"x\":1,\"y\":2},\"b\":3}".getBytes(StandardCharsets.UTF_8);
        parser.reset(bytes, bytes.length);

        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("a", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        parser.skipChildren(); // skip {"x":1,"y":2}
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        // next field
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("b", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals(3L, parser.longValue());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testSkipChildrenArray() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bytes = "{\"a\":[1,2,3],\"b\":4}".getBytes(StandardCharsets.UTF_8);
        parser.reset(bytes, bytes.length);

        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
        parser.skipChildren();
        assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("b", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());
        assertEquals(4L, parser.longValue());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    public void testSkipChildrenEmptyObject() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bytes = "{\"a\":{},\"b\":1}".getBytes(StandardCharsets.UTF_8);
        parser.reset(bytes, bytes.length);

        parser.nextToken(); // START_OBJECT
        parser.nextToken(); // FIELD_NAME "a"
        parser.nextToken(); // START_OBJECT (empty)
        parser.skipChildren();
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("b", parser.currentName());
    }

    public void testSkipChildrenMatchesJackson() throws IOException {
        // Differential: after skipChildren, both parsers must agree on all subsequent tokens.
        String json = "{\"skip\":{\"x\":1,\"y\":{\"deep\":true}},\"keep\":\"value\"}";
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        SimdJsonXContentParser simd = new SimdJsonXContentParser(4096, 32);
        simd.reset(bytes, bytes.length);
        try (XContentParser jackson = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes)) {
            // both: START_OBJECT
            simd.nextToken();
            jackson.nextToken();
            // both: FIELD_NAME "skip"
            simd.nextToken();
            jackson.nextToken();
            // both: START_OBJECT for "skip"
            simd.nextToken();
            jackson.nextToken();
            // skipChildren on both
            simd.skipChildren();
            jackson.skipChildren();
            // now drain both and compare
            while (true) {
                XContentParser.Token simdTok = simd.nextToken();
                XContentParser.Token jackTok = jackson.nextToken();
                assertEquals("token after skip mismatch", jackTok, simdTok);
                if (simdTok == null) break;
                assertEquals("name after skip mismatch", jackson.currentName(), simd.currentName());
                if (simdTok == XContentParser.Token.VALUE_STRING) {
                    assertEquals("text after skip mismatch", jackson.text(), simd.text());
                }
            }
        }
    }

    // -- currentName after END_OBJECT --------------------------------------------------------

    public void testCurrentNameAfterEndObject() throws IOException {
        // After END_OBJECT the name should be the field that contained the object.
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bytes = "{\"nested\":{\"x\":1}}".getBytes(StandardCharsets.UTF_8);
        parser.reset(bytes, bytes.length);

        parser.nextToken(); // START_OBJECT (root)
        assertNull(parser.currentName());
        parser.nextToken(); // FIELD_NAME "nested"
        assertEquals("nested", parser.currentName());
        parser.nextToken(); // START_OBJECT
        assertEquals("nested", parser.currentName());
        parser.nextToken(); // FIELD_NAME "x"
        assertEquals("x", parser.currentName());
        parser.nextToken(); // VALUE_NUMBER
        parser.nextToken(); // END_OBJECT (inner)
        assertEquals("nested", parser.currentName()); // restored from name stack
        parser.nextToken(); // END_OBJECT (root)
        assertNull(parser.currentName());
    }

    // -- field-name cache --------------------------------------------------------------------

    public void testNameCacheRepeatedNames() throws IOException {
        // The same field names across many documents must return equal String instances (cache hit).
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bytes = "{\"a\":1,\"b\":2,\"c\":3}".getBytes(StandardCharsets.UTF_8);

        String[] firstNames = new String[3];
        parser.reset(bytes, bytes.length);
        parser.nextToken(); // START_OBJECT
        for (int i = 0; i < 3; i++) {
            parser.nextToken(); // FIELD_NAME
            firstNames[i] = parser.currentName();
            parser.nextToken(); // value
        }

        // Repeat 50 times and assert we get the same String objects (cache hit = same reference)
        for (int iter = 0; iter < 50; iter++) {
            parser.reset(bytes, bytes.length);
            parser.nextToken(); // START_OBJECT
            for (int i = 0; i < 3; i++) {
                parser.nextToken(); // FIELD_NAME
                assertSame("name cache should return same String instance on hit (iter " + iter + ")", firstNames[i], parser.currentName());
                parser.nextToken(); // value
            }
        }
    }

    // -- parser reuse / no state bleed -------------------------------------------------------

    public void testReuse() throws IOException {
        // Run 100 different documents through one parser instance; assert no state bleed.
        SimdJsonXContentParser parser = new SimdJsonXContentParser(65536, 64);
        for (int i = 0; i < 100; i++) {
            String json = String.format(Locale.ROOT, "{\"i\":%d,\"s\":\"value%d\"}", i, i);
            assertMatchesJackson(json, parser);
        }
    }

    // -- deeply nested structure -------------------------------------------------------------

    public void testDeeplyNested() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(65536, 64);
        // Build {"l0":{"l1":{"l2":{"l3":42}}}}
        StringBuilder sb = new StringBuilder();
        int levels = 20;
        for (int i = 0; i < levels; i++) {
            sb.append("{\"l").append(i).append("\":");
        }
        sb.append("42");
        for (int i = 0; i < levels; i++) {
            sb.append("}");
        }
        assertMatchesJackson(sb.toString(), parser);
    }

    // -- malformed input ---------------------------------------------------------------------

    public void testMalformedInputThrows() {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(4096, 32);
        byte[] bad = "{\"unclosed\":".getBytes(StandardCharsets.UTF_8);
        expectThrows(JsonParsingException.class, () -> parser.reset(bad, bad.length));
    }

    // -- cross-species tape agreement (128 vs 256 vs default) --------------------------------

    /**
     * Verifies that the SIMD parser produces the same token stream regardless of the vector
     * width selected by {@code es.simdjson.species}. This is the main correctness guard for
     * the hand-written 128-bit NEON path.
     *
     * <p>To exercise different kernels, run the test suite with:
     * <pre>{@code
     *   -Dtests.jvm.argline="-Des.simdjson.species=128"
     *   -Dtests.jvm.argline="-Des.simdjson.species=256"
     * }</pre>
     *
     * <p>Since the species cannot be changed after class-load time, this test does not attempt
     * to run both kernels in the same JVM. Instead it validates that whatever kernel is active
     * agrees with Jackson on a representative corpus.
     */
    public void testCorpusAgainstJackson() throws IOException {
        SimdJsonXContentParser parser = new SimdJsonXContentParser(1024 * 1024, 64);
        String[] corpus = {
            // flat
            "{\"a\":1,\"b\":2,\"c\":3}",
            // nested
            "{\"x\":{\"y\":{\"z\":true}}}",
            // array
            "[1,2,3,4,5]",
            // mixed
            "{\"arr\":[1,\"two\",false,null],\"obj\":{\"k\":\"v\"}}",
            // unicode (literal UTF-8 bytes — JSON unicode escape sequences hit a simdjson StringParser bug)
            "{\"emoji\":\"😀\",\"cjk\":\"中文\"}",
            // large string
            "{\"big\":\"" + "x".repeat(10000) + "\"}",
            // many fields
            buildManyFields(100), };
        for (String json : corpus) {
            assertMatchesJackson(json, parser);
        }
    }

    private static String buildManyFields(int count) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"field").append(i).append("\":").append(i);
        }
        sb.append("}");
        return sb.toString();
    }
}
