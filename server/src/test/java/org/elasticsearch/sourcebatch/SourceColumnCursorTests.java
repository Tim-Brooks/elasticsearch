/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eicf.EicfBatch;
import org.elasticsearch.eicf.EicfEncoder;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowBuilder;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Tests {@link SourceColumnCursor}: the specialized EICF cursors (LONG / DOUBLE / STRING / UNION) and
 * the generic random-access cursor used by the row-major {@link org.elasticsearch.eirf.EirfColumn}. A
 * cursor must visit every document in order, report {@link EirfType#ABSENT} for absent documents, and
 * expose the value matching {@link SourceColumnCursor#type()}.
 */
public class SourceColumnCursorTests extends ESTestCase {

    private static EicfBatch encode(String... docs) throws IOException {
        List<BytesReference> sources = new java.util.ArrayList<>();
        for (String doc : docs) {
            sources.add(new BytesArray(doc));
        }
        return EicfEncoder.encode(sources, XContentType.JSON);
    }

    private static SourceColumn columnByName(SourceBatch batch, String name) {
        for (int i = 0; i < batch.columnCount(); i++) {
            if (batch.schema().getFullPath(i).equals(name)) {
                return batch.column(i);
            }
        }
        throw new AssertionError("no column named [" + name + "]");
    }

    public void testLongColumnCursor() throws IOException {
        // "v" present in docs 0 and 2, absent in doc 1.
        try (EicfBatch batch = encode("{\"v\":10}", "{\"o\":1}", "{\"v\":30}")) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();

            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(10L, cursor.longValue());

            assertTrue(cursor.advance());
            assertEquals(EirfType.ABSENT, cursor.type());

            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(30L, cursor.longValue());

            assertFalse(cursor.advance());
        }
    }

    public void testDoubleColumnCursor() throws IOException {
        try (EicfBatch batch = encode("{\"v\":1.5}", "{\"o\":1}", "{\"v\":-3.25}")) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();

            assertTrue(cursor.advance());
            assertEquals(EirfType.DOUBLE, cursor.type());
            assertEquals(1.5, cursor.doubleValue(), 0.0);

            assertTrue(cursor.advance());
            assertEquals(EirfType.ABSENT, cursor.type());

            assertTrue(cursor.advance());
            assertEquals(EirfType.DOUBLE, cursor.type());
            assertEquals(-3.25, cursor.doubleValue(), 0.0);

            assertFalse(cursor.advance());
        }
    }

    public void testStringColumnCursor() throws IOException {
        try (EicfBatch batch = encode("{\"v\":\"alpha\"}", "{\"o\":1}", "{\"v\":\"gamma\"}")) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();

            assertTrue(cursor.advance());
            assertEquals(EirfType.STRING, cursor.type());
            assertEquals("alpha", cursor.stringValue().string());

            assertTrue(cursor.advance());
            assertEquals(EirfType.ABSENT, cursor.type());

            assertTrue(cursor.advance());
            assertEquals(EirfType.STRING, cursor.type());
            assertEquals("gamma", cursor.stringValue().string());

            assertFalse(cursor.advance());
        }
    }

    public void testUnionColumnCursor() throws IOException {
        // A heterogeneous "v": long, string, double, then absent — promotes to a UNION column.
        try (EicfBatch batch = encode("{\"v\":10}", "{\"v\":\"hi\"}", "{\"v\":2.5}", "{\"o\":1}")) {
            SourceColumnCursor cursor = columnByName(batch, "v").cursor();

            assertTrue(cursor.advance());
            assertEquals(EirfType.LONG, cursor.type());
            assertEquals(10L, cursor.longValue());

            assertTrue(cursor.advance());
            assertEquals(EirfType.STRING, cursor.type());
            assertEquals("hi", cursor.stringValue().string());

            assertTrue(cursor.advance());
            assertEquals(EirfType.DOUBLE, cursor.type());
            assertEquals(2.5, cursor.doubleValue(), 0.0);

            assertTrue(cursor.advance());
            assertEquals(EirfType.ABSENT, cursor.type());

            assertFalse(cursor.advance());
        }
    }

    public void testGenericCursorOverRowMajorColumn() {
        // The row-major EirfColumn uses the default random-access cursor; verify it matches the getters,
        // including absence (the "score" float is absent in row 1).
        EirfRowBuilder builder = new EirfRowBuilder();
        builder.startDocument();
        builder.setLong("ts", 1_000_000L);
        builder.setFloat("score", 3.14f);
        builder.endDocument();
        builder.startDocument();
        builder.setLong("ts", 2_000_000L);
        // score absent
        builder.endDocument();

        try (EirfBatch batch = builder.build()) {
            SourceColumnCursor ts = columnByName(batch, "ts").cursor();
            assertTrue(ts.advance());
            assertEquals(EirfType.LONG, ts.type());
            assertEquals(1_000_000L, ts.longValue());
            assertTrue(ts.advance());
            assertEquals(EirfType.LONG, ts.type());
            assertEquals(2_000_000L, ts.longValue());
            assertFalse(ts.advance());

            SourceColumnCursor score = columnByName(batch, "score").cursor();
            assertTrue(score.advance());
            assertEquals(EirfType.FLOAT, score.type());
            assertTrue(score.advance());
            assertEquals(EirfType.ABSENT, score.type());
            assertFalse(score.advance());
        }
    }
}
