/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Tests adapting {@link EicfColumn}s to Lucene's {@code org.apache.lucene.document.column} API via
 * {@link EicfLuceneColumns}: tuple cursors, dense bulk values cursors with fast-path fills, density
 * reporting, sparse handling, and unsupported kinds.
 */
public class EicfLuceneColumnsTests extends ESTestCase {

    private static final IndexableFieldType NUMERIC_FIELD_TYPE = numericFieldType();
    private static final IndexableFieldType BINARY_FIELD_TYPE = binaryFieldType();

    private static IndexableFieldType numericFieldType() {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.NUMERIC);
        ft.freeze();
        return ft;
    }

    private static IndexableFieldType binaryFieldType() {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.BINARY);
        ft.freeze();
        return ft;
    }

    private static EicfColumn column(EicfBatch batch, int index) {
        return (EicfColumn) batch.column(index);
    }

    // -------------------------------------------------------------------------
    // LONG
    // -------------------------------------------------------------------------

    public void testLongColumnTuplesAndValues() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"v\":10}"), new BytesArray("{\"v\":20}"), new BytesArray("{\"v\":30}")),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.longColumn(column(batch, 0), "v", NUMERIC_FIELD_TYPE);
            assertEquals("v", col.name());
            assertEquals(LongColumn.NumericKind.LONG, col.numericKind());
            assertEquals(Column.Density.DENSE, col.density());

            // tuple cursor
            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(10L, tuples.longValue());
            assertEquals(1, tuples.nextDoc());
            assertEquals(20L, tuples.longValue());
            assertEquals(2, tuples.nextDoc());
            assertEquals(30L, tuples.longValue());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            // dense values cursor: one-by-one
            LongValuesCursor values = col.values();
            assertEquals(3, values.size());
            assertEquals(10L, values.nextLong());
            assertEquals(20L, values.nextLong());
            assertEquals(30L, values.nextLong());

            // dense values cursor: bulk fill fast path (fresh cursor)
            long[] dst = new long[3];
            col.values().fillDocValues(dst, 0, 3);
            assertArrayEquals(new long[] { 10L, 20L, 30L }, dst);
        }
    }

    public void testLongValuesOverrunThrows() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"v\":1}")), XContentType.JSON)) {
            LongValuesCursor values = EicfLuceneColumns.longColumn(column(batch, 0), "v", NUMERIC_FIELD_TYPE).values();
            assertEquals(1L, values.nextLong());
            expectThrows(IllegalStateException.class, values::nextLong);
        }
    }

    // -------------------------------------------------------------------------
    // DOUBLE
    // -------------------------------------------------------------------------

    public void testDoubleColumnSortableEncoding() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"v\":1.5}"), new BytesArray("{\"v\":-2.25}"), new BytesArray("{\"v\":3.0}")),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.longColumn(column(batch, 0), "v", NUMERIC_FIELD_TYPE);
            assertEquals(LongColumn.NumericKind.DOUBLE, col.numericKind());
            assertEquals(Column.Density.DENSE, col.density());

            double[] expected = { 1.5, -2.25, 3.0 };
            LongTupleCursor tuples = col.tuples();
            for (int i = 0; i < expected.length; i++) {
                assertEquals(i, tuples.nextDoc());
                assertEquals(NumericUtils.doubleToSortableLong(expected[i]), tuples.longValue());
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            long[] dst = new long[3];
            col.values().fillDocValues(dst, 0, 3);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(NumericUtils.doubleToSortableLong(expected[i]), dst[i]);
            }
        }
    }

    // -------------------------------------------------------------------------
    // STRING / BINARY
    // -------------------------------------------------------------------------

    public void testStringColumnTuplesAndValues() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"s\":\"alpha\"}"), new BytesArray("{\"s\":\"be\"}"), new BytesArray("{\"s\":\"gamma\"}")),
                XContentType.JSON
            )
        ) {
            BinaryColumn col = EicfLuceneColumns.binaryColumn(column(batch, 0), "s", BINARY_FIELD_TYPE);
            assertEquals(Column.Density.DENSE, col.density());

            String[] expected = { "alpha", "be", "gamma" };
            ObjectTupleCursor<BytesRef> tuples = col.tuples();
            for (int i = 0; i < expected.length; i++) {
                assertEquals(i, tuples.nextDoc());
                assertEquals(expected[i], tuples.value().utf8ToString());
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            BytesRefValuesCursor values = col.values();
            assertEquals(3, values.size());
            for (String s : expected) {
                assertEquals(s, values.nextValue().utf8ToString());
            }
        }
    }

    public void testBinaryFixedWidthPackedPointsFastPath() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"s\":\"aaaa\"}"), new BytesArray("{\"s\":\"bbbb\"}"), new BytesArray("{\"s\":\"cccc\"}")),
                XContentType.JSON
            )
        ) {
            BinaryColumn col = EicfLuceneColumns.binaryColumn(column(batch, 0), "s", BINARY_FIELD_TYPE);
            BytesRefValuesCursor values = col.values();

            byte[] dst = new byte[3 * 4];
            values.fillPackedPoints(dst, 0, 3, 4);
            assertArrayEquals("aaaabbbbcccc".getBytes(StandardCharsets.UTF_8), dst);
        }
    }

    public void testPackedPointsWrongWidthThrows() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"s\":\"abc\"}")), XContentType.JSON)) {
            BytesRefValuesCursor values = EicfLuceneColumns.binaryColumn(column(batch, 0), "s", BINARY_FIELD_TYPE).values();
            expectThrows(IllegalArgumentException.class, () -> values.fillPackedPoints(new byte[4], 0, 1, 4));
        }
    }

    // -------------------------------------------------------------------------
    // Sparse columns
    // -------------------------------------------------------------------------

    public void testSparseLongSkipsAbsentAndValuesThrows() throws IOException {
        // "v" is present in docs 0 and 2, absent in doc 1.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"a\":1,\"v\":5}"), new BytesArray("{\"a\":2}"), new BytesArray("{\"a\":3,\"v\":7}")),
                XContentType.JSON
            )
        ) {
            int vIndex = batch.schema().getFullPath(0).equals("v") ? 0 : 1;
            LongColumn col = EicfLuceneColumns.longColumn(column(batch, vIndex), "v", NUMERIC_FIELD_TYPE);
            assertEquals(Column.Density.SPARSE, col.density());

            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(5L, tuples.longValue());
            assertEquals("absent doc 1 is skipped", 2, tuples.nextDoc());
            assertEquals(7L, tuples.longValue());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            // values() is not available for a sparse column
            expectThrows(UnsupportedOperationException.class, col::values);
        }
    }

    public void testSparseBinarySkipsAbsent() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"a\":1,\"s\":\"x\"}"), new BytesArray("{\"a\":2}"), new BytesArray("{\"a\":3,\"s\":\"z\"}")),
                XContentType.JSON
            )
        ) {
            int sIndex = batch.schema().getFullPath(0).equals("s") ? 0 : 1;
            BinaryColumn col = EicfLuceneColumns.binaryColumn(column(batch, sIndex), "s", BINARY_FIELD_TYPE);
            assertEquals(Column.Density.SPARSE, col.density());

            ObjectTupleCursor<BytesRef> tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals("x", tuples.value().utf8ToString());
            assertEquals(2, tuples.nextDoc());
            assertEquals("z", tuples.value().utf8ToString());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            expectThrows(UnsupportedOperationException.class, col::values);
        }
    }

    // -------------------------------------------------------------------------
    // convertToNumeric: UNION / mismatched columns rebuilt into a typed numeric column
    // -------------------------------------------------------------------------

    public void testConvertUnionToLongSkipsUnconvertible() throws IOException {
        // A heterogeneous "v" column: long, numeric string, double, unparseable string, boolean, null.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(
                    new BytesArray("{\"v\":10}"),
                    new BytesArray("{\"v\":\"123\"}"),
                    new BytesArray("{\"v\":2.5}"),
                    new BytesArray("{\"v\":\"oops\"}"),
                    new BytesArray("{\"v\":true}"),
                    new BytesArray("{\"v\":null}")
                ),
                XContentType.JSON
            )
        ) {
            assertTrue("expected a UNION column", column(batch, 0) instanceof EicfUnionColumn);
            LongColumn col = EicfLuceneColumns.convertToNumeric(
                column(batch, 0),
                "v",
                NUMERIC_FIELD_TYPE,
                LongColumn.NumericKind.LONG,
                null
            );
            assertEquals(LongColumn.NumericKind.LONG, col.numericKind());
            assertEquals(Column.Density.SPARSE, col.density());

            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(10L, tuples.longValue());
            assertEquals(1, tuples.nextDoc());
            assertEquals("numeric string is parsed", 123L, tuples.longValue());
            assertEquals(2, tuples.nextDoc());
            assertEquals("double is truncated to long", 2L, tuples.longValue());
            assertEquals("unparseable string / boolean / null are skipped", DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

            // values() is not available for a sparse column
            expectThrows(UnsupportedOperationException.class, col::values);
        }
    }

    public void testConvertUnionToDoubleAppliesSortableEncoding() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(
                    new BytesArray("{\"v\":10}"),
                    new BytesArray("{\"v\":\"1.5\"}"),
                    new BytesArray("{\"v\":-2.25}"),
                    new BytesArray("{\"v\":\"nope\"}")
                ),
                XContentType.JSON
            )
        ) {
            assertTrue("expected a UNION column", column(batch, 0) instanceof EicfUnionColumn);
            LongColumn col = EicfLuceneColumns.convertToNumeric(
                column(batch, 0),
                "v",
                NUMERIC_FIELD_TYPE,
                LongColumn.NumericKind.DOUBLE,
                null
            );
            assertEquals(LongColumn.NumericKind.DOUBLE, col.numericKind());
            assertEquals(Column.Density.SPARSE, col.density());

            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(NumericUtils.doubleToSortableLong(10.0), tuples.longValue());
            assertEquals(1, tuples.nextDoc());
            assertEquals(NumericUtils.doubleToSortableLong(1.5), tuples.longValue());
            assertEquals(2, tuples.nextDoc());
            assertEquals(NumericUtils.doubleToSortableLong(-2.25), tuples.longValue());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());
        }
    }

    public void testConvertAllConvertibleIsDense() throws IOException {
        // long, numeric string, double — all convertible, so the rebuilt column is dense.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"v\":1}"), new BytesArray("{\"v\":\"2\"}"), new BytesArray("{\"v\":3.0}")),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.convertToNumeric(
                column(batch, 0),
                "v",
                NUMERIC_FIELD_TYPE,
                LongColumn.NumericKind.LONG,
                null
            );
            assertEquals(Column.Density.DENSE, col.density());

            LongValuesCursor values = col.values();
            assertEquals(3, values.size());
            assertEquals(1L, values.nextLong());
            assertEquals(2L, values.nextLong());
            assertEquals(3L, values.nextLong());
        }
    }

    // -------------------------------------------------------------------------
    // tryParseAsciiLong: UTF-8 byte-level long fast path
    // -------------------------------------------------------------------------

    /** Asserts the fast path parses {@code s} to {@code expected}, and that it agrees with {@link Long#parseLong}. */
    private static void assertParsesLong(String s, long expected) {
        long[] out = new long[1];
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        assertEquals("expected PARSE_LONG for [" + s + "]", 0 /* PARSE_LONG */, EicfLuceneColumns.tryParseAsciiLong(b, 0, b.length, out));
        assertEquals("value for [" + s + "]", expected, out[0]);
        assertEquals("must agree with Long.parseLong for [" + s + "]", Long.parseLong(s), out[0]);
    }

    private static int parseResult(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return EicfLuceneColumns.tryParseAsciiLong(b, 0, b.length, new long[1]);
    }

    public void testTryParseAsciiLongPlainIntegers() {
        assertParsesLong("0", 0L);
        assertParsesLong("7", 7L);
        assertParsesLong("-7", -7L);
        assertParsesLong("+7", 7L);
        assertParsesLong("007", 7L); // leading zeros, matching Long.parseLong
        assertParsesLong("123456789", 123456789L);
        assertParsesLong("-123456789", -123456789L);
    }

    public void testTryParseAsciiLongBoundaries() {
        assertParsesLong(Long.toString(Long.MAX_VALUE), Long.MAX_VALUE);
        assertParsesLong(Long.toString(Long.MIN_VALUE), Long.MIN_VALUE);
    }

    public void testTryParseAsciiLongOverflowFallsBack() {
        // One past the boundaries, and a clearly-too-long string: all must fall back rather than truncate.
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("9223372036854775808")); // Long.MAX_VALUE + 1
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("-9223372036854775809")); // Long.MIN_VALUE - 1
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("99999999999999999999999"));
    }

    public void testTryParseAsciiLongDecimalAndExponent() {
        assertEquals(1 /* PARSE_DECIMAL */, parseResult("2.5"));
        assertEquals(1 /* PARSE_DECIMAL */, parseResult("-3.9"));
        assertEquals(1 /* PARSE_DECIMAL */, parseResult("1e3"));
        assertEquals(1 /* PARSE_DECIMAL */, parseResult("1E3"));
        assertEquals(1 /* PARSE_DECIMAL */, parseResult("5.")); // trailing dot, like the row path's BigDecimal coercion
        assertEquals(1 /* PARSE_DECIMAL */, parseResult(".5"));
    }

    public void testTryParseAsciiLongMalformedFallsBack() {
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("+"));
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("-"));
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("oops"));
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("1_000")); // underscore not accepted by Long.parseLong either
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("12 "));
        // Non-ASCII (Arabic-Indic) digits: byte parser bails so the caller can defer to Long.parseLong's semantics.
        assertEquals(2 /* PARSE_FALLBACK */, parseResult("٠١"));
    }

    public void testTryParseAsciiLongHonoursOffsetAndLength() {
        // Parse only the "42" slice embedded in a larger buffer.
        byte[] b = "xx42yy".getBytes(StandardCharsets.UTF_8);
        long[] out = new long[1];
        assertEquals(0 /* PARSE_LONG */, EicfLuceneColumns.tryParseAsciiLong(b, 2, 2, out));
        assertEquals(42L, out[0]);
    }

    public void testConvertDecimalStringToLongTruncates() throws IOException {
        // Strings that hold a decimal/exponent are coerced to long by parsing as a double then truncating,
        // matching a real double value and the row path's coerce behavior — without a thrown exception.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(
                    new BytesArray("{\"v\":\"2.5\"}"),
                    new BytesArray("{\"v\":\"-3.9\"}"),
                    new BytesArray("{\"v\":\"1e3\"}"),
                    new BytesArray("{\"v\":\"7\"}")
                ),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.convertToNumeric(
                column(batch, 0),
                "v",
                NUMERIC_FIELD_TYPE,
                LongColumn.NumericKind.LONG,
                null
            );
            assertEquals(Column.Density.DENSE, col.density());

            LongValuesCursor values = col.values();
            assertEquals(4, values.size());
            assertEquals("2.5 truncates to 2", 2L, values.nextLong());
            assertEquals("-3.9 truncates to -3", -3L, values.nextLong());
            assertEquals("1e3 parses to 1000", 1000L, values.nextLong());
            assertEquals("plain integer string still parses", 7L, values.nextLong());
        }
    }

    public void testConvertReplacesNullAndEmptyStringWithNullValue() throws IOException {
        // long, explicit null, empty string, genuinely-absent doc. With a configured null_value, the
        // explicit null and the empty string become that value; the absent doc stays absent.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(
                    new BytesArray("{\"v\":10}"),
                    new BytesArray("{\"v\":null}"),
                    new BytesArray("{\"v\":\"\"}"),
                    new BytesArray("{\"other\":1}")
                ),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.convertToNumeric(column(batch, 0), "v", NUMERIC_FIELD_TYPE, LongColumn.NumericKind.LONG, 7L);
            assertEquals(Column.Density.SPARSE, col.density());

            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(10L, tuples.longValue());
            assertEquals(1, tuples.nextDoc());
            assertEquals("explicit null becomes null_value", 7L, tuples.longValue());
            assertEquals(2, tuples.nextDoc());
            assertEquals("empty string becomes null_value", 7L, tuples.longValue());
            assertEquals("absent doc stays absent", DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());
        }
    }

    public void testConvertWithoutNullValueLeavesNullAndEmptyStringAbsent() throws IOException {
        // Same shape, but with no null_value configured (null): explicit null and empty string are absent.
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"v\":10}"), new BytesArray("{\"v\":null}"), new BytesArray("{\"v\":\"\"}")),
                XContentType.JSON
            )
        ) {
            LongColumn col = EicfLuceneColumns.convertToNumeric(
                column(batch, 0),
                "v",
                NUMERIC_FIELD_TYPE,
                LongColumn.NumericKind.LONG,
                null
            );
            assertEquals(Column.Density.SPARSE, col.density());

            LongTupleCursor tuples = col.tuples();
            assertEquals(0, tuples.nextDoc());
            assertEquals(10L, tuples.longValue());
            assertEquals("null and empty string are absent when no null_value", DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());
        }
    }

    // -------------------------------------------------------------------------
    // Dispatch + unsupported kinds
    // -------------------------------------------------------------------------

    public void testOfDispatchesByKind() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"n\":1,\"s\":\"x\"}"), new BytesArray("{\"n\":2,\"s\":\"y\"}")),
                XContentType.JSON
            )
        ) {
            Column n = EicfLuceneColumns.of(column(batch, 0), "n", NUMERIC_FIELD_TYPE);
            Column s = EicfLuceneColumns.of(column(batch, 1), "s", BINARY_FIELD_TYPE);
            assertTrue(n instanceof LongColumn);
            assertTrue(s instanceof BinaryColumn);
        }
    }

    public void testUnsupportedKindThrows() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"b\":true}")), XContentType.JSON)) {
            EicfColumn bool = column(batch, 0);
            expectThrows(UnsupportedOperationException.class, () -> EicfLuceneColumns.of(bool, "b", NUMERIC_FIELD_TYPE));
            expectThrows(IllegalArgumentException.class, () -> EicfLuceneColumns.longColumn(bool, "b", NUMERIC_FIELD_TYPE));
            expectThrows(IllegalArgumentException.class, () -> EicfLuceneColumns.binaryColumn(bool, "b", BINARY_FIELD_TYPE));
        }
    }

    public void testFreshCursorsAreIndependent() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"v\":10}"), new BytesArray("{\"v\":20}")), XContentType.JSON)) {
            LongColumn col = EicfLuceneColumns.longColumn(column(batch, 0), "v", NUMERIC_FIELD_TYPE);
            LongTupleCursor a = col.tuples();
            assertEquals(0, a.nextDoc());
            // A second cursor starts fresh at the beginning.
            LongTupleCursor b = col.tuples();
            assertEquals(0, b.nextDoc());
            assertEquals(10L, b.longValue());
        }
    }

    // -------------------------------------------------------------------------
    // SeparateCountColumnBuilder — two-segment addValue
    // -------------------------------------------------------------------------

    /**
     * The two-segment {@code addValue(prefix, value)} (used by the flattened mapper to compose
     * {@code key\0value} entries without an intermediate buffer) must produce byte-identical values and
     * counts to assembling the entry first and calling the single-segment {@code addValue}. Covers a
     * multi-value document (which is sorted), a single-value document (raw encoding), an absent
     * document, and duplicate entries (kept, not deduplicated).
     */
    public void testSeparateCountTwoSegmentMatchesConcatenated() {
        final int docCount = 4;
        EicfLuceneColumns.SeparateCountColumnBuilder twoSeg = EicfLuceneColumns.separateCountColumnBuilder(
            docCount,
            "v",
            BINARY_FIELD_TYPE,
            "v.counts",
            NUMERIC_FIELD_TYPE
        );
        EicfLuceneColumns.SeparateCountColumnBuilder ref = EicfLuceneColumns.separateCountColumnBuilder(
            docCount,
            "v",
            BINARY_FIELD_TYPE,
            "v.counts",
            NUMERIC_FIELD_TYPE
        );

        // Per document: a list of (prefix, value) entries.
        String[][][] docs = {
            { { "k2\0", "beta" }, { "k1\0", "alpha" } }, // multi-value → sorted
            { { "k1\0", "z" } },                         // single value → raw bytes
            {},                                          // absent
            { { "k\0", "x" }, { "k\0", "x" } }           // duplicates kept (no dedup)
        };
        for (String[][] doc : docs) {
            twoSeg.startDoc();
            ref.startDoc();
            for (String[] entry : doc) {
                byte[] prefix = entry[0].getBytes(StandardCharsets.UTF_8);
                byte[] value = entry[1].getBytes(StandardCharsets.UTF_8);
                twoSeg.addValue(prefix, 0, prefix.length, value, 0, value.length);
                byte[] concatenated = new byte[prefix.length + value.length];
                System.arraycopy(prefix, 0, concatenated, 0, prefix.length);
                System.arraycopy(value, 0, concatenated, prefix.length, value.length);
                ref.addValue(concatenated, 0, concatenated.length);
            }
            twoSeg.endDoc();
            ref.endDoc();
        }

        BytesRef[] twoSegValues = readBinaryByDoc(twoSeg.buildValues(), docCount);
        BytesRef[] refValues = readBinaryByDoc(ref.buildValues(), docCount);
        assertArrayEquals("two-segment values must match concatenated single-segment values", refValues, twoSegValues);

        Long[] twoSegCounts = readCountsByDoc(twoSeg.buildCounts(), docCount);
        Long[] refCounts = readCountsByDoc(ref.buildCounts(), docCount);
        assertArrayEquals("two-segment counts must match", refCounts, twoSegCounts);

        // Anchor the expected shapes explicitly so the test is not purely self-referential.
        assertArrayEquals(new Long[] { 2L, 1L, null, 2L }, twoSegCounts);
        assertEquals("single-value doc is stored as raw bytes", new BytesRef("k1\0z"), twoSegValues[1]);
        assertNull("empty doc is absent", twoSegValues[2]);
    }

    /** Reads a (possibly sparse) {@link BinaryColumn} into a per-document array; {@code null} marks an absent doc. */
    private static BytesRef[] readBinaryByDoc(BinaryColumn col, int docCount) {
        BytesRef[] out = new BytesRef[docCount];
        ObjectTupleCursor<BytesRef> cursor = col.tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            out[doc] = BytesRef.deepCopyOf(cursor.value());
        }
        return out;
    }

    /** Reads a (possibly sparse) counts {@link LongColumn} into a per-document array; {@code null} marks an absent doc. */
    private static Long[] readCountsByDoc(LongColumn col, int docCount) {
        Long[] out = new Long[docCount];
        LongTupleCursor cursor = col.tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            out[doc] = cursor.longValue();
        }
        return out;
    }

    // -------------------------------------------------------------------------
    // BinaryColumnBuilder — paged backing across page boundaries
    // -------------------------------------------------------------------------

    /**
     * The paged binary column must read back values correctly even when they straddle the 16KB page
     * boundaries of the backing buffer — the common case is a zero-copy view into one page, the rare case a
     * gather across pages. Builds well over a page of data so both cases occur, and verifies both cursors.
     */
    public void testPagedBinaryColumnAcrossPageBoundaries() {
        final int docCount = 50;
        final int valueLen = 1000; // 50 * 1000 ≈ 49KB over 16KB pages → several values straddle a boundary
        EicfLuceneColumns.BinaryColumnBuilder builder = EicfLuceneColumns.binaryColumnBuilder(docCount, "v", BINARY_FIELD_TYPE);
        byte[][] expected = new byte[docCount][];
        for (int d = 0; d < docCount; d++) {
            byte[] value = new byte[valueLen];
            for (int i = 0; i < valueLen; i++) {
                value[i] = (byte) (d * 31 + i);
            }
            expected[d] = value;
            builder.addBytes(value, 0, value.length);
        }
        BinaryColumn col = builder.build();
        assertEquals(Column.Density.DENSE, col.density());

        // tuples() cursor
        ObjectTupleCursor<BytesRef> tuples = col.tuples();
        for (int d = 0; d < docCount; d++) {
            assertEquals(d, tuples.nextDoc());
            assertEquals("doc " + d, new BytesRef(expected[d]), tuples.value());
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());

        // dense values() cursor (fresh, independent walk)
        BytesRefValuesCursor values = col.values();
        assertEquals(docCount, values.size());
        for (int d = 0; d < docCount; d++) {
            assertEquals("value " + d, new BytesRef(expected[d]), values.nextValue());
        }
    }

    /** A sparse paged column with an absent document and an empty (zero-length) present value. */
    public void testPagedBinaryColumnSparseAndEmptyValues() {
        final int docCount = 4;
        EicfLuceneColumns.BinaryColumnBuilder builder = EicfLuceneColumns.binaryColumnBuilder(docCount, "v", BINARY_FIELD_TYPE);
        builder.addBytes("alpha".getBytes(StandardCharsets.UTF_8), 0, 5);
        builder.addAbsent();
        builder.addBytes(new byte[0], 0, 0); // empty but present
        builder.addBytes("gamma".getBytes(StandardCharsets.UTF_8), 0, 5);
        BinaryColumn col = builder.build();
        assertEquals(Column.Density.SPARSE, col.density());

        ObjectTupleCursor<BytesRef> tuples = col.tuples();
        assertEquals(0, tuples.nextDoc());
        assertEquals(new BytesRef("alpha"), tuples.value());
        assertEquals(2, tuples.nextDoc()); // doc 1 is absent and skipped
        assertEquals(new BytesRef(""), tuples.value());
        assertEquals(3, tuples.nextDoc());
        assertEquals(new BytesRef("gamma"), tuples.value());
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, tuples.nextDoc());
    }
}
