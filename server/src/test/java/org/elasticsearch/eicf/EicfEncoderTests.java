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
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Round-trip JSON tests for {@link EicfEncoder}.
 *
 * <p>Key behavioural differences from EIRF:
 * <ul>
 *   <li>JSON integers and longs both produce columns with type byte {@code LONG} (no INT narrowing).
 *   <li>JSON floats and doubles both produce columns with type byte {@code DOUBLE} (no FLOAT narrowing).
 *   <li>Absent fields are tracked in a per-column bitset rather than a per-row type-byte slot.
 *   <li>Explicit nulls, or any type conflict in a column, yield a {@code UNION} column.
 *   <li>An integer+float mix in one column yields a {@code UNION} column (per-document LONG/DOUBLE type bytes).
 * </ul>
 */
public class EicfEncoderTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Basic round-trips
    // -------------------------------------------------------------------------

    public void testRoundTripSimpleDocuments() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\",\"age\":25}")
        );
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            assertEquals(2, batch.docCount());
            assertEquals(2, batch.columnCount());
            EirfSchema schema = batch.schema();
            assertEquals("name", schema.getFullPath(0));
            assertEquals("age", schema.getFullPath(1));

            // name column: STRING kind
            SourceRow row0 = batch.row(0);
            assertEquals(EirfType.STRING, row0.getTypeByte(0));
            assertEquals("alice", row0.getStringValue(0).string());
            SourceRow row1 = batch.row(1);
            assertEquals("bob", row1.getStringValue(0).string());

            // age column: LONG kind (upcast from JSON integer)
            assertEquals(EirfType.LONG, row0.getTypeByte(1));
            assertEquals(30L, row0.getLongValue(1));
            assertEquals(25L, row1.getLongValue(1));
        }
    }

    public void testUpcastInt() throws IOException {
        // JSON integers → LONG, not INT
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"small\":42,\"big\":" + Long.MAX_VALUE + "}")),
                XContentType.JSON
            )
        ) {
            SourceRow row = batch.row(0);
            assertEquals(EirfType.LONG, row.getTypeByte(0));
            assertEquals(42L, row.getLongValue(0));
            assertEquals(EirfType.LONG, row.getTypeByte(1));
            assertEquals(Long.MAX_VALUE, row.getLongValue(1));
        }
    }

    public void testUpcastFloat() throws IOException {
        // JSON floats → DOUBLE, not FLOAT
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"pi\":3.14,\"exact\":1.0}")), XContentType.JSON)) {
            SourceRow row = batch.row(0);
            assertEquals(EirfType.DOUBLE, row.getTypeByte(0));
            assertEquals(3.14, row.getDoubleValue(0), 1e-10);
            assertEquals(EirfType.DOUBLE, row.getTypeByte(1));
            assertEquals(1.0, row.getDoubleValue(1), 0.0);
        }
    }

    public void testBoolean() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"active\":true}"),
            new BytesArray("{\"active\":false}"),
            new BytesArray("{\"active\":true}")
        );
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            assertEquals(1, batch.columnCount());
            SourceRow r0 = batch.row(0);
            SourceRow r1 = batch.row(1);
            SourceRow r2 = batch.row(2);
            assertEquals(EirfType.TRUE, r0.getTypeByte(0));
            assertTrue(r0.getBooleanValue(0));
            assertEquals(EirfType.FALSE, r1.getTypeByte(0));
            assertFalse(r1.getBooleanValue(0));
            assertEquals(EirfType.TRUE, r2.getTypeByte(0));
        }
    }

    // -------------------------------------------------------------------------
    // Missing fields (absent bitset)
    // -------------------------------------------------------------------------

    public void testMissingFields() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\"}"),
            new BytesArray("{\"age\":35,\"email\":\"c@d.com\"}")
        );
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            assertEquals(3, batch.docCount());
            assertEquals(3, batch.columnCount()); // name, age, email

            SourceRow row0 = batch.row(0);
            SourceRow row1 = batch.row(1);
            SourceRow row2 = batch.row(2);

            // row0: name present, age present, email absent
            assertFalse(row0.isAbsent(0));
            assertFalse(row0.isAbsent(1));
            assertTrue(row0.isAbsent(2));

            // row1: name present, age absent, email absent
            assertFalse(row1.isAbsent(0));
            assertTrue(row1.isAbsent(1));
            assertTrue(row1.isAbsent(2));

            // row2: name absent, age present, email present
            assertTrue(row2.isAbsent(0));
            assertFalse(row2.isAbsent(1));
            assertFalse(row2.isAbsent(2));
            assertEquals("c@d.com", row2.getStringValue(2).string());
        }
    }

    // -------------------------------------------------------------------------
    // Type promotion
    // -------------------------------------------------------------------------

    public void testExplicitNullProducesUnion() throws IOException {
        // Explicit null in any row forces the column to UNION
        List<BytesReference> sources = List.of(new BytesArray("{\"x\":1}"), new BytesArray("{\"x\":null}"), new BytesArray("{\"x\":3}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceRow row0 = batch.row(0);
            SourceRow row1 = batch.row(1);
            SourceRow row2 = batch.row(2);

            assertEquals(EirfType.LONG, row0.getTypeByte(0));
            assertEquals(1L, row0.getLongValue(0));

            assertTrue(row1.isNull(0));
            assertEquals(EirfType.NULL, row1.getTypeByte(0));

            assertEquals(EirfType.LONG, row2.getTypeByte(0));
            assertEquals(3L, row2.getLongValue(0));
        }
    }

    public void testNumericMixProducesUnion() throws IOException {
        // int in one row + float in another → UNION (per-document LONG / DOUBLE type bytes)
        List<BytesReference> sources = List.of(new BytesArray("{\"v\":10}"), new BytesArray("{\"v\":3.14}"), new BytesArray("{\"v\":20}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceRow row0 = batch.row(0);
            SourceRow row1 = batch.row(1);
            SourceRow row2 = batch.row(2);

            assertEquals(EirfType.LONG, row0.getTypeByte(0));
            assertEquals(10L, row0.getLongValue(0));

            assertEquals(EirfType.DOUBLE, row1.getTypeByte(0));
            assertEquals(3.14, row1.getDoubleValue(0), 1e-10);

            assertEquals(EirfType.LONG, row2.getTypeByte(0));
            assertEquals(20L, row2.getLongValue(0));
        }
    }

    public void testStringAndNumberMixProducesUnion() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"mixed\":\"hello\"}"), new BytesArray("{\"mixed\":42}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceRow row0 = batch.row(0);
            SourceRow row1 = batch.row(1);

            assertEquals(EirfType.STRING, row0.getTypeByte(0));
            assertEquals("hello", row0.getStringValue(0).string());

            assertEquals(EirfType.LONG, row1.getTypeByte(0));
            assertEquals(42L, row1.getLongValue(0));
        }
    }

    public void testBooleanAndNumberMixProducesUnion() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"x\":true}"), new BytesArray("{\"x\":1}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            SourceRow row0 = batch.row(0);
            SourceRow row1 = batch.row(1);

            assertEquals(EirfType.TRUE, row0.getTypeByte(0));
            assertTrue(row0.getBooleanValue(0));

            assertEquals(EirfType.LONG, row1.getTypeByte(0));
            assertEquals(1L, row1.getLongValue(0));
        }
    }

    // -------------------------------------------------------------------------
    // Arrays
    // -------------------------------------------------------------------------

    public void testHomogeneousArray() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"tags\":[\"a\",\"b\",\"c\"]}")), XContentType.JSON)) {
            SourceRow row = batch.row(0);
            assertEquals(EirfType.FIXED_ARRAY, row.getTypeByte(0));
            EirfArrayReader reader = row.getArrayValue(0);
            assertTrue(reader.next());
            assertEquals("a", reader.stringValue());
            assertTrue(reader.next());
            assertEquals("b", reader.stringValue());
            assertTrue(reader.next());
            assertEquals("c", reader.stringValue());
            assertFalse(reader.next());
        }
    }

    public void testMixedTypeArray() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"data\":[42,\"hello\",true]}")), XContentType.JSON)) {
            SourceRow row = batch.row(0);
            assertEquals(EirfType.UNION_ARRAY, row.getTypeByte(0));
            EirfArrayReader reader = row.getArrayValue(0);
            assertTrue(reader.next());
            assertEquals(EirfType.INT, reader.type()); // array elements still use INT narrowing (EIRF packing)
            assertEquals(42, reader.intValue());
            assertTrue(reader.next());
            assertEquals(EirfType.STRING, reader.type());
            assertEquals("hello", reader.stringValue());
            assertTrue(reader.next());
            assertEquals(EirfType.TRUE, reader.type());
            assertFalse(reader.next());
        }
    }

    public void testLongArrayAcrossMultipleDocs() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"ids\":[1,2,3]}"), new BytesArray("{\"ids\":[100,200]}"));
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            EirfArrayReader r0 = batch.row(0).getArrayValue(0);
            assertTrue(r0.next());
            assertEquals(1, r0.intValue());
            assertTrue(r0.next());
            assertEquals(2, r0.intValue());
            assertTrue(r0.next());
            assertEquals(3, r0.intValue());
            assertFalse(r0.next());

            EirfArrayReader r1 = batch.row(1).getArrayValue(0);
            assertTrue(r1.next());
            assertEquals(100, r1.intValue());
            assertTrue(r1.next());
            assertEquals(200, r1.intValue());
            assertFalse(r1.next());
        }
    }

    // -------------------------------------------------------------------------
    // Nested objects
    // -------------------------------------------------------------------------

    public void testNestedObject() throws IOException {
        try (
            EicfBatch batch = EicfEncoder.encode(
                List.of(new BytesArray("{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"ok\"}")),
                XContentType.JSON
            )
        ) {
            assertEquals(3, batch.columnCount());
            EirfSchema schema = batch.schema();
            assertEquals("user.name", schema.getFullPath(0));
            assertEquals("user.age", schema.getFullPath(1));
            assertEquals("status", schema.getFullPath(2));

            SourceRow row = batch.row(0);
            assertEquals("alice", row.getStringValue(0).string());
            assertEquals(30L, row.getLongValue(1));
            assertEquals("ok", row.getStringValue(2).string());
        }
    }

    // -------------------------------------------------------------------------
    // All-absent column
    // -------------------------------------------------------------------------

    public void testAllAbsentColumn() throws IOException {
        // Column "b" only appears in the second doc; "c" never appears in any doc (won't be created)
        List<BytesReference> sources = List.of(
            new BytesArray("{\"a\":1}"),
            new BytesArray("{\"a\":2,\"b\":99}"),
            new BytesArray("{\"a\":3}")
        );
        try (EicfBatch batch = EicfEncoder.encode(sources, XContentType.JSON)) {
            assertEquals(2, batch.columnCount()); // a, b

            // column a: present in all docs
            assertFalse(batch.row(0).isAbsent(0));
            assertFalse(batch.row(1).isAbsent(0));
            assertFalse(batch.row(2).isAbsent(0));

            // column b: absent in docs 0 and 2
            assertTrue(batch.row(0).isAbsent(1));
            assertFalse(batch.row(1).isAbsent(1));
            assertEquals(99L, batch.row(1).getLongValue(1));
            assertTrue(batch.row(2).isAbsent(1));
        }
    }

    // -------------------------------------------------------------------------
    // Edge cases
    // -------------------------------------------------------------------------

    public void testEmptyBatch() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(), XContentType.JSON)) {
            assertEquals(0, batch.docCount());
            assertEquals(0, batch.columnCount());
        }
    }

    public void testSingleDocument() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"k\":\"v\"}")), XContentType.JSON)) {
            assertEquals(1, batch.docCount());
            assertEquals(1, batch.columnCount());
            assertEquals("v", batch.row(0).getStringValue(0).string());
        }
    }

    public void testDuplicateFieldThrows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> EicfEncoder.encode(List.of(new BytesArray("{\"a\":1,\"a\":2}")), XContentType.JSON)
        );
    }

    public void testEmptyObjectIsAbsent() throws IOException {
        // An empty object contributes no values, so it is treated as absent and emits no leaf column
        // (see the TODO in EicfEncoder#flattenObject). A sibling scalar is still encoded normally.
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"a\":1,\"obj\":{}}")), XContentType.JSON)) {
            assertEquals(1, batch.schema().leafCount());
            assertEquals("a", batch.schema().getFullPath(0));
        }
        // A nested empty object is likewise dropped, leaving only the real leaves.
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"outer\":{\"b\":2,\"empty\":{}}}")), XContentType.JSON)) {
            assertEquals(1, batch.schema().leafCount());
            assertEquals("outer.b", batch.schema().getFullPath(0));
        }
        // A document whose only field is an empty object encodes with no leaves at all.
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"obj\":{}}")), XContentType.JSON)) {
            assertEquals(0, batch.schema().leafCount());
        }
    }

    public void testBoundsCheck() throws IOException {
        try (EicfBatch batch = EicfEncoder.encode(List.of(new BytesArray("{\"x\":1}")), XContentType.JSON)) {
            expectThrows(IndexOutOfBoundsException.class, () -> batch.row(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.row(1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(-1));
            expectThrows(IndexOutOfBoundsException.class, () -> batch.column(1));
        }
    }

    // -------------------------------------------------------------------------
    // Multi-partition encoding (one partition per destination shard)
    // -------------------------------------------------------------------------

    public void testPartitionsRouteIndependently() throws IOException {
        try (EicfEncoder encoder = new EicfEncoder()) {
            // docs 0 and 2 route to partition 0, doc 1 routes to partition 1.
            encoder.parseToScratch(new BytesArray("{\"name\":\"a\",\"n\":1}"), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertEquals(0, encoder.commitScratchTo(0));
            encoder.parseToScratch(new BytesArray("{\"name\":\"b\",\"n\":2}"), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertEquals(0, encoder.commitScratchTo(1));
            encoder.parseToScratch(new BytesArray("{\"name\":\"c\",\"n\":3}"), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            assertEquals(1, encoder.commitScratchTo(0));

            try (EicfBatch p0 = encoder.buildPartition(0)) {
                assertEquals(2, p0.docCount());
                assertEquals("a", p0.row(0).getStringValue(0).string());
                assertEquals(1L, p0.row(0).getLongValue(1));
                assertEquals("c", p0.row(1).getStringValue(0).string());
                assertEquals(3L, p0.row(1).getLongValue(1));
            }
            try (EicfBatch p1 = encoder.buildPartition(1)) {
                assertEquals(1, p1.docCount());
                assertEquals("b", p1.row(0).getStringValue(0).string());
                assertEquals(2L, p1.row(0).getLongValue(1));
            }
        }
    }

    public void testPartitionSchemaGrowthBackfillsAbsent() throws IOException {
        try (EicfEncoder encoder = new EicfEncoder()) {
            // doc 0 (partition 0) only has "a"; doc 1 (partition 1) introduces column "b".
            encoder.parseToScratch(new BytesArray("{\"a\":1}"), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            encoder.commitScratchTo(0);
            encoder.parseToScratch(new BytesArray("{\"a\":2,\"b\":\"x\"}"), XContentType.JSON, EirfEncoder.LeafSink.NO_OP);
            encoder.commitScratchTo(1);

            try (EicfBatch p0 = encoder.buildPartition(0)) {
                assertEquals(1, p0.docCount());
                assertEquals(2, p0.columnCount()); // "b" exists in the shared schema, back-filled absent here
                assertEquals(1L, p0.row(0).getLongValue(0));
                assertTrue("column b is absent for the partition-0 document", p0.row(0).isAbsent(1));
            }
            try (EicfBatch p1 = encoder.buildPartition(1)) {
                assertEquals(1, p1.docCount());
                assertEquals(2L, p1.row(0).getLongValue(0));
                assertEquals("x", p1.row(0).getStringValue(1).string());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Routing parity: the LeafSink must fire identically to EirfEncoder
    // -------------------------------------------------------------------------

    public void testLeafSinkParityWithEirfEncoder() throws IOException {
        List<BytesReference> docs = List.of(
            new BytesArray(
                "{\"s\":\"hello\",\"i\":42,\"big\":"
                    + Long.MAX_VALUE
                    + ",\"f\":3.5,"
                    + "\"d\":3.14159265358979,\"b\":true,\"nil\":null,\"arr\":[1,2,3],\"obj\":{\"k\":7}}"
            )
        );
        for (boolean rawText : new boolean[] { false, true }) {
            RecordingSink eirfEvents = new RecordingSink(rawText);
            RecordingSink eicfEvents = new RecordingSink(rawText);
            try (EirfEncoder eirf = new EirfEncoder(); EicfEncoder eicf = new EicfEncoder()) {
                for (BytesReference doc : docs) {
                    eirf.parseToScratch(doc, XContentType.JSON, eirfEvents);
                    eirf.commitScratchTo(0);
                    eicf.parseToScratch(doc, XContentType.JSON, eicfEvents);
                    eicf.commitScratchTo(0);
                }
            }
            assertEquals("LeafSink callbacks must match (passRawText=" + rawText + ")", eirfEvents.events, eicfEvents.events);
            assertFalse("sanity: some leaf callbacks were recorded", eirfEvents.events.isEmpty());
        }
    }

    /** Records every {@link EirfEncoder.LeafSink} callback as a stable string, for cross-encoder comparison. */
    private static final class RecordingSink implements EirfEncoder.LeafSink {
        private final boolean rawText;
        final List<String> events = new ArrayList<>();

        RecordingSink(boolean rawText) {
            this.rawText = rawText;
        }

        @Override
        public boolean passRawText() {
            return rawText;
        }

        @Override
        public void onTextPrimitive(int columnIndex, String dottedPath, byte type, XContentString.UTF8Bytes textBytes) {
            String text = new String(textBytes.bytes(), textBytes.offset(), textBytes.length(), StandardCharsets.UTF_8);
            events.add("text c=" + columnIndex + " p=" + dottedPath + " t=" + EirfType.name(type) + " v=" + text);
        }

        @Override
        public void onLongPrimitive(int columnIndex, String dottedPath, byte type, long value) {
            events.add("long c=" + columnIndex + " p=" + dottedPath + " t=" + EirfType.name(type) + " v=" + value);
        }

        @Override
        public void onDoublePrimitive(int columnIndex, String dottedPath, byte type, double value) {
            events.add("double c=" + columnIndex + " p=" + dottedPath + " t=" + EirfType.name(type) + " v=" + value);
        }

        @Override
        public void onBooleanPrimitive(int columnIndex, String dottedPath, boolean value) {
            events.add("bool c=" + columnIndex + " p=" + dottedPath + " v=" + value);
        }

        @Override
        public void onArrayLeaf(int columnIndex, String dottedPath) {
            events.add("array c=" + columnIndex + " p=" + dottedPath);
        }
    }
}
