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
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link EicfBatch#slice}, covering full-range, empty, sub-range, nested,
 * and multi-kind scenarios, as well as bounds checking.
 */
public class EicfBatchSliceTests extends ESTestCase {

    /** Builds a batch of {@code count} sequential documents each containing {@code id} and {@code name}. */
    private static EicfBatch encodeDocs(int count) throws IOException {
        List<BytesReference> sources = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            sources.add(new BytesArray("{\"id\":" + i + ",\"name\":\"doc-" + i + "\"}"));
        }
        return EicfEncoder.encode(sources, XContentType.JSON);
    }

    // -------------------------------------------------------------------------
    // Sub-range slice
    // -------------------------------------------------------------------------

    public void testSliceRoundtripsRowValues() throws IOException {
        try (EicfBatch parent = encodeDocs(8)) {
            int from = randomIntBetween(0, 4);
            int to = randomIntBetween(from + 1, 8);
            SourceBatch sliced = parent.slice(from, to);

            assertEquals(to - from, sliced.docCount());
            assertEquals(parent.schema().leafCount(), sliced.schema().leafCount());
            assertEquals(parent.schema().nonLeafCount(), sliced.schema().nonLeafCount());

            for (int i = 0; i < to - from; i++) {
                SourceRow parentRow = parent.row(from + i);
                SourceRow slicedRow = sliced.row(i);

                assertEquals("id mismatch at slice row " + i, parentRow.getLongValue(0), slicedRow.getLongValue(0));
                assertEquals("name mismatch at slice row " + i, parentRow.getStringValue(1).string(), slicedRow.getStringValue(1).string());
            }
            sliced.close();
        }
    }

    // -------------------------------------------------------------------------
    // Full-range slice (no-op view)
    // -------------------------------------------------------------------------

    public void testFullSliceEqualsParent() throws IOException {
        try (EicfBatch parent = encodeDocs(5)) {
            SourceBatch sliced = parent.slice(0, parent.docCount());
            assertEquals(parent.docCount(), sliced.docCount());

            for (int i = 0; i < parent.docCount(); i++) {
                assertEquals(parent.row(i).getLongValue(0), sliced.row(i).getLongValue(0));
                assertEquals(parent.row(i).getStringValue(1).string(), sliced.row(i).getStringValue(1).string());
            }
            sliced.close(); // no-op: full-range slice shares the parent's buffer
        }
    }

    // -------------------------------------------------------------------------
    // Empty slice
    // -------------------------------------------------------------------------

    public void testEmptySlice() throws IOException {
        try (EicfBatch parent = encodeDocs(4)) {
            SourceBatch empty = parent.slice(2, 2);
            assertEquals(0, empty.docCount());
            assertEquals(parent.columnCount(), empty.columnCount());
            empty.close();
        }
    }

    // -------------------------------------------------------------------------
    // Nested slice
    // -------------------------------------------------------------------------

    public void testNestedSlice() throws IOException {
        try (EicfBatch parent = encodeDocs(10)) {
            SourceBatch outer = parent.slice(2, 8); // rows 2..7 from parent
            SourceBatch inner = outer.slice(1, 4);  // outer rows 1..3 == parent rows 3..5

            assertEquals(3, inner.docCount());
            for (int i = 0; i < 3; i++) {
                long expectedId = parent.row(3 + i).getLongValue(0);
                long actualId = inner.row(i).getLongValue(0);
                assertEquals("id mismatch at inner row " + i, expectedId, actualId);

                String expectedName = parent.row(3 + i).getStringValue(1).string();
                String actualName = inner.row(i).getStringValue(1).string();
                assertEquals("name mismatch at inner row " + i, expectedName, actualName);
            }
            inner.close();
            outer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Absent values preserved across slice
    // -------------------------------------------------------------------------

    public void testSlicePreservesAbsent() throws IOException {
        // doc 0: has "a" only; doc 1: has "a" and "b"; doc 2: has "a" only
        List<BytesReference> sources = List.of(
            new BytesArray("{\"a\":1}"),
            new BytesArray("{\"a\":2,\"b\":99}"),
            new BytesArray("{\"a\":3}")
        );
        try (EicfBatch parent = EicfEncoder.encode(sources, XContentType.JSON)) {
            // Slice rows 1..2 (docs 1 and 2 from parent)
            SourceBatch sliced = parent.slice(1, 3);
            assertEquals(2, sliced.docCount());

            // sliced row 0 = parent doc 1: a=2, b=99
            assertFalse(sliced.row(0).isAbsent(0));
            assertEquals(2L, sliced.row(0).getLongValue(0));
            assertFalse(sliced.row(0).isAbsent(1));
            assertEquals(99L, sliced.row(0).getLongValue(1));

            // sliced row 1 = parent doc 2: a=3, b absent
            assertFalse(sliced.row(1).isAbsent(0));
            assertEquals(3L, sliced.row(1).getLongValue(0));
            assertTrue(sliced.row(1).isAbsent(1));
            assertEquals(EirfType.ABSENT, sliced.row(1).getTypeByte(1));

            sliced.close();
        }
    }

    // -------------------------------------------------------------------------
    // All column kinds preserved across slice
    // -------------------------------------------------------------------------

    public void testSliceAllColumnKinds() throws IOException {
        // Build a batch that induces LONG, DOUBLE, BOOL, STRING, ARRAY, NUMERIC_UNION, UNION
        List<BytesReference> sources = List.of(
            new BytesArray("{\"n\":10,\"f\":1.5,\"b\":true,\"s\":\"alpha\",\"arr\":[1],\"nu\":100,\"u\":\"x\"}"),
            new BytesArray("{\"n\":20,\"f\":2.5,\"b\":false,\"s\":\"beta\",\"arr\":[2,3],\"nu\":3.14,\"u\":42}"),
            new BytesArray("{\"n\":30,\"f\":3.5,\"b\":true,\"s\":\"gamma\",\"arr\":[4,5,6],\"nu\":200,\"u\":\"y\"}")
        );
        try (EicfBatch parent = EicfEncoder.encode(sources, XContentType.JSON)) {
            // Slice the middle and last rows
            SourceBatch sliced = parent.slice(1, 3);
            assertEquals(2, sliced.docCount());

            // Verify values match parent rows 1 and 2
            for (int i = 0; i < 2; i++) {
                SourceRow pr = parent.row(1 + i);
                SourceRow sr = sliced.row(i);

                // LONG col
                assertEquals("n row " + i, pr.getLongValue(0), sr.getLongValue(0));
                // DOUBLE col
                assertEquals("f row " + i, pr.getDoubleValue(1), sr.getDoubleValue(1), 0.0);
                // BOOL col
                assertEquals("b row " + i, pr.getBooleanValue(2), sr.getBooleanValue(2));
                // STRING col
                assertEquals("s row " + i, pr.getStringValue(3).string(), sr.getStringValue(3).string());
                // NUMERIC_UNION col: type byte preserved
                assertEquals("nu type row " + i, pr.getTypeByte(5), sr.getTypeByte(5));
                // UNION col: type byte preserved
                assertEquals("u type row " + i, pr.getTypeByte(6), sr.getTypeByte(6));
            }

            // Spot-check NUMERIC_UNION: row 0 of slice = parent row 1 → double 3.14
            assertEquals(EirfType.DOUBLE, sliced.row(0).getTypeByte(5));
            assertEquals(3.14, sliced.row(0).getDoubleValue(5), 1e-10);

            // Spot-check UNION: row 1 of slice = parent row 2 → string "y"
            assertEquals(EirfType.STRING, sliced.row(1).getTypeByte(6));
            assertEquals("y", sliced.row(1).getStringValue(6).string());

            sliced.close();
        }
    }

    // -------------------------------------------------------------------------
    // Invalid range throws
    // -------------------------------------------------------------------------

    public void testInvalidRangeThrows() throws IOException {
        try (EicfBatch parent = encodeDocs(3)) {
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(-1, 2));
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(0, 4));
            expectThrows(IndexOutOfBoundsException.class, () -> parent.slice(2, 1));
        }
    }

    // -------------------------------------------------------------------------
    // No-op close on slice
    // -------------------------------------------------------------------------

    /**
     * Verifies that closing a sub-range slice does not affect the parent batch — values
     * must remain readable after the slice is closed, because the slice owns no buffers.
     */
    public void testSliceCloseDoesNotAffectParent() throws IOException {
        try (EicfBatch parent = encodeDocs(4)) {
            SourceBatch sliced = parent.slice(1, 3);
            sliced.close(); // no-op

            // Parent must still be readable
            assertEquals(4, parent.docCount());
            assertEquals(0L, parent.row(0).getLongValue(0));
            assertEquals(3L, parent.row(3).getLongValue(0));
        }
    }
}
