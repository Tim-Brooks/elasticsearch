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
            LongColumn col = EicfLuceneColumns.convertToNumeric(column(batch, 0), "v", NUMERIC_FIELD_TYPE, LongColumn.NumericKind.LONG);
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
            LongColumn col = EicfLuceneColumns.convertToNumeric(column(batch, 0), "v", NUMERIC_FIELD_TYPE, LongColumn.NumericKind.DOUBLE);
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
            LongColumn col = EicfLuceneColumns.convertToNumeric(column(batch, 0), "v", NUMERIC_FIELD_TYPE, LongColumn.NumericKind.LONG);
            assertEquals(Column.Density.DENSE, col.density());

            LongValuesCursor values = col.values();
            assertEquals(3, values.size());
            assertEquals(1L, values.nextLong());
            assertEquals(2L, values.nextLong());
            assertEquals(3L, values.nextLong());
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
}
