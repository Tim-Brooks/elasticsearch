/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceColumn;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Tests that {@link EicfColumn} access matches row-based access via {@link EicfRow},
 * and that both produce the expected values for every supported column kind.
 *
 * <p>In EICF, {@code row(d).getXxx(c)} delegates to {@code column(c).getXxx(d)}, so
 * these tests verify both the column vector layout and the delegation path.
 */
public class EicfColumnTests extends ESTestCase {

    /**
     * Builds a batch that covers every supported column kind across multiple documents:
     * <ul>
     *   <li>col 0 {@code "n"}:     LONG (ints upcast)</li>
     *   <li>col 1 {@code "f"}:     DOUBLE (floats upcast)</li>
     *   <li>col 2 {@code "b"}:     BOOL</li>
     *   <li>col 3 {@code "s"}:     STRING</li>
     *   <li>col 4 {@code "arr"}:   ARRAY</li>
     *   <li>col 5 {@code "nu"}:    NUMERIC_UNION (long row + double row)</li>
     *   <li>col 6 {@code "u"}:     UNION (string row + long row)</li>
     *   <li>col 7 {@code "opt"}:   LONG, absent in doc 2</li>
     * </ul>
     */
    private static EicfBatch buildAllKindsBatch() throws IOException {
        return EicfEncoder.encode(
            List.of(
                // doc 0
                new BytesArray("{\"n\":10,\"f\":1.5,\"b\":true,\"s\":\"hello\"," + "\"arr\":[1,2],\"nu\":100,\"u\":\"word\",\"opt\":99}"),
                // doc 1: nu is double (→ NUMERIC_UNION); u is long (→ UNION)
                new BytesArray("{\"n\":20,\"f\":2.5,\"b\":false,\"s\":\"world\"," + "\"arr\":[3,4,5],\"nu\":3.14,\"u\":42,\"opt\":77}"),
                // doc 2: opt is absent
                new BytesArray("{\"n\":30,\"f\":3.5,\"b\":true,\"s\":\"end\"," + "\"arr\":[6],\"nu\":200,\"u\":\"last\"}")
            ),
            XContentType.JSON
        );
    }

    // -------------------------------------------------------------------------
    // Cross-check: column(c).getXxx(d) == row(d).getXxx(c)
    // -------------------------------------------------------------------------

    /**
     * Verifies that for every column and document, the column-path and row-path
     * return identical type bytes, absent/null flags, and typed values.
     */
    public void testColumnViewMatchesRowView() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            int docCount = batch.docCount();
            int colCount = batch.columnCount();
            assertTrue("batch must have at least one row", docCount > 0);
            assertTrue("batch must have at least one column", colCount > 0);

            for (int c = 0; c < colCount; c++) {
                SourceColumn col = batch.column(c);
                assertEquals("columnIndex mismatch for col " + c, c, col.columnIndex());
                assertEquals("docCount mismatch for col " + c, docCount, col.docCount());

                for (int d = 0; d < docCount; d++) {
                    SourceRow row = batch.row(d);

                    assertEquals("type mismatch at col=" + c + " doc=" + d, row.getTypeByte(c), col.getTypeByte(d));
                    assertEquals("isAbsent mismatch at col=" + c + " doc=" + d, row.isAbsent(c), col.isAbsent(d));
                    assertEquals("isNull mismatch at col=" + c + " doc=" + d, row.isNull(c), col.isNull(d));

                    if (row.isAbsent(c) == false && row.isNull(c) == false) {
                        byte type = row.getTypeByte(c);
                        switch (type) {
                            case EirfType.LONG -> assertEquals(
                                "long mismatch at col=" + c + " doc=" + d,
                                row.getLongValue(c),
                                col.getLongValue(d)
                            );
                            case EirfType.DOUBLE -> assertEquals(
                                "double mismatch at col=" + c + " doc=" + d,
                                row.getDoubleValue(c),
                                col.getDoubleValue(d),
                                0.0
                            );
                            case EirfType.TRUE, EirfType.FALSE -> assertEquals(
                                "boolean mismatch at col=" + c + " doc=" + d,
                                row.getBooleanValue(c),
                                col.getBooleanValue(d)
                            );
                            case EirfType.STRING -> assertEquals(
                                "string mismatch at col=" + c + " doc=" + d,
                                row.getStringValue(c).string(),
                                col.getStringValue(d).string()
                            );
                            case EirfType.FIXED_ARRAY, EirfType.UNION_ARRAY -> {
                                // array values: just verify neither throws and returns non-null
                                assertNotNull("array at col=" + c + " doc=" + d + " via row", row.getArrayValue(c));
                                assertNotNull("array at col=" + c + " doc=" + d + " via col", col.getArrayValue(d));
                            }
                        }
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Per-kind correctness
    // -------------------------------------------------------------------------

    public void testLongColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(0); // "n" — all longs
            assertEquals(EirfType.LONG, col.getTypeByte(0));
            assertEquals(EirfType.LONG, col.getTypeByte(1));
            assertEquals(EirfType.LONG, col.getTypeByte(2));
            assertEquals(10L, col.getLongValue(0));
            assertEquals(20L, col.getLongValue(1));
            assertEquals(30L, col.getLongValue(2));
            assertFalse(col.isAbsent(0));
            assertFalse(col.isAbsent(1));
            assertFalse(col.isAbsent(2));
        }
    }

    public void testDoubleColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(1); // "f" — all doubles
            assertEquals(EirfType.DOUBLE, col.getTypeByte(0));
            assertEquals(EirfType.DOUBLE, col.getTypeByte(1));
            assertEquals(EirfType.DOUBLE, col.getTypeByte(2));
            assertEquals(1.5, col.getDoubleValue(0), 1e-10);
            assertEquals(2.5, col.getDoubleValue(1), 1e-10);
            assertEquals(3.5, col.getDoubleValue(2), 1e-10);
        }
    }

    public void testBoolColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(2); // "b" — all booleans
            assertEquals(EirfType.TRUE, col.getTypeByte(0));
            assertEquals(EirfType.FALSE, col.getTypeByte(1));
            assertEquals(EirfType.TRUE, col.getTypeByte(2));
            assertTrue(col.getBooleanValue(0));
            assertFalse(col.getBooleanValue(1));
            assertTrue(col.getBooleanValue(2));
        }
    }

    public void testStringColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(3); // "s" — all strings
            assertEquals(EirfType.STRING, col.getTypeByte(0));
            assertEquals(EirfType.STRING, col.getTypeByte(1));
            assertEquals(EirfType.STRING, col.getTypeByte(2));
            assertEquals("hello", col.getStringValue(0).string());
            assertEquals("world", col.getStringValue(1).string());
            assertEquals("end", col.getStringValue(2).string());
        }
    }

    public void testArrayColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(4); // "arr"
            assertEquals(EirfType.FIXED_ARRAY, col.getTypeByte(0));
            assertEquals(EirfType.FIXED_ARRAY, col.getTypeByte(1));
            assertEquals(EirfType.FIXED_ARRAY, col.getTypeByte(2));

            var r0 = col.getArrayValue(0);
            assertTrue(r0.next());
            assertEquals(1, r0.intValue());
            assertTrue(r0.next());
            assertEquals(2, r0.intValue());
            assertFalse(r0.next());

            var r1 = col.getArrayValue(1);
            assertTrue(r1.next());
            assertEquals(3, r1.intValue());
            assertTrue(r1.next());
            assertEquals(4, r1.intValue());
            assertTrue(r1.next());
            assertEquals(5, r1.intValue());
            assertFalse(r1.next());

            var r2 = col.getArrayValue(2);
            assertTrue(r2.next());
            assertEquals(6, r2.intValue());
            assertFalse(r2.next());
        }
    }

    public void testNumericUnionColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(5); // "nu" — NUMERIC_UNION

            // doc 0: long value 100
            assertEquals(EirfType.LONG, col.getTypeByte(0));
            assertEquals(100L, col.getLongValue(0));

            // doc 1: double value 3.14 (triggers NUMERIC_UNION)
            assertEquals(EirfType.DOUBLE, col.getTypeByte(1));
            assertEquals(3.14, col.getDoubleValue(1), 1e-10);

            // doc 2: long value 200
            assertEquals(EirfType.LONG, col.getTypeByte(2));
            assertEquals(200L, col.getLongValue(2));
        }
    }

    public void testUnionColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(6); // "u" — UNION (string + long mix)

            // doc 0: string "word"
            assertEquals(EirfType.STRING, col.getTypeByte(0));
            assertEquals("word", col.getStringValue(0).string());

            // doc 1: long 42 (triggers UNION)
            assertEquals(EirfType.LONG, col.getTypeByte(1));
            assertEquals(42L, col.getLongValue(1));

            // doc 2: string "last"
            assertEquals(EirfType.STRING, col.getTypeByte(2));
            assertEquals("last", col.getStringValue(2).string());
        }
    }

    public void testAbsentColumn() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(7); // "opt" — absent in doc 2

            // docs 0 and 1: present as long
            assertFalse(col.isAbsent(0));
            assertEquals(EirfType.LONG, col.getTypeByte(0));
            assertEquals(99L, col.getLongValue(0));

            assertFalse(col.isAbsent(1));
            assertEquals(EirfType.LONG, col.getTypeByte(1));
            assertEquals(77L, col.getLongValue(1));

            // doc 2: absent
            assertTrue(col.isAbsent(2));
            assertEquals(EirfType.ABSENT, col.getTypeByte(2));
        }
    }

    // -------------------------------------------------------------------------
    // Bounds checks
    // -------------------------------------------------------------------------

    public void testColumnBoundsCheck() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(batch.columnCount()));
        }
    }

    public void testRowBoundsCheck() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            expectThrows(IndexOutOfBoundsException.class, () -> batch.row(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.row(batch.docCount()));
        }
    }

    public void testColumnDocOutOfRangeReturnsAbsent() throws IOException {
        try (EicfBatch batch = buildAllKindsBatch()) {
            SourceColumn col = batch.column(0);
            // Out-of-range doc index → absent (no exception), type byte is ABSENT
            assertTrue(col.isAbsent(-1));
            assertTrue(col.isAbsent(batch.docCount()));
            assertEquals(EirfType.ABSENT, col.getTypeByte(-1));
            assertEquals(EirfType.ABSENT, col.getTypeByte(batch.docCount()));
        }
    }

    // -------------------------------------------------------------------------
    // Column on a slice
    // -------------------------------------------------------------------------

    /**
     * Verifies that column access on a sliced batch returns the same values as the
     * corresponding row range in the parent batch.
     */
    public void testColumnOnSlice() throws IOException {
        try (EicfBatch parent = buildAllKindsBatch()) {
            int from = 1;
            int to = 3;
            SourceBatch sliced = parent.slice(from, to);

            int colCount = sliced.columnCount();
            assertEquals(parent.columnCount(), colCount);

            for (int c = 0; c < colCount; c++) {
                SourceColumn col = sliced.column(c);
                assertEquals(c, col.columnIndex());
                assertEquals(to - from, col.docCount());

                for (int d = 0; d < to - from; d++) {
                    assertEquals(
                        "type mismatch at col=" + c + " doc=" + d + " (parent row " + (from + d) + ")",
                        parent.column(c).getTypeByte(from + d),
                        col.getTypeByte(d)
                    );
                    assertEquals("isAbsent mismatch at col=" + c + " doc=" + d, parent.column(c).isAbsent(from + d), col.isAbsent(d));
                }
            }

            // Spot-check specific values in the slice
            // col 0 "n": doc 0 of slice = parent doc 1 → 20
            assertEquals(20L, sliced.column(0).getLongValue(0));
            // col 3 "s": doc 0 of slice = parent doc 1 → "world"
            assertEquals("world", sliced.column(3).getStringValue(0).string());

            sliced.close();
        }
    }

    // -------------------------------------------------------------------------
    // getIntValue / getFloatValue narrowing
    // -------------------------------------------------------------------------

    public void testGetIntValueNarrowsLong() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"n\":42}"), new BytesArray("{\"n\":100}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceColumn col = batch.column(0);
            assertEquals(42, col.getIntValue(0));
            assertEquals(100, col.getIntValue(1));
        }
    }

    public void testGetIntValueThrowsForLargeValue() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"big\":" + ((long) Integer.MAX_VALUE + 1) + "}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceColumn col = batch.column(0);
            expectThrows(ArithmeticException.class, () -> col.getIntValue(0));
        }
    }

    public void testGetFloatValueNarrowsDouble() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"f\":1.5}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceColumn col = batch.column(0);
            assertEquals(1.5f, col.getFloatValue(0), 0f);
        }
    }
}
