/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DocumentBatchRowBuilderTests extends ESTestCase {

    public void testSimpleScalarDocuments() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice", false);
            builder.setLong("age", 30, false);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob", false);
            builder.setLong("age", 25, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(2, batch.columnCount());

            DocBatchSchema schema = batch.schema();
            assertEquals("name", schema.getColumnName(0));
            assertEquals("age", schema.getColumnName(1));

            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals(RowType.STRING, row0.getBaseType(0));
            assertEquals("alice", row0.getStringValue(0));
            assertEquals(RowType.LONG, row0.getBaseType(1));
            assertEquals(30L, row0.getLongValue(1));

            DocBatchRowReader row1 = batch.getRowReader(1);
            assertEquals("bob", row1.getStringValue(0));
            assertEquals(25L, row1.getLongValue(1));

            batch.close();
        }
    }

    public void testNestedObjectFieldsWithObjectFlag() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setString("user.name", "alice", true);
            builder.setLong("user.age", 30, true);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            assertEquals(1, batch.docCount());

            DocBatchSchema schema = batch.schema();
            assertEquals("user.name", schema.getColumnName(0));
            assertEquals("user.age", schema.getColumnName(1));

            DocBatchRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.isFromObject(0));
            assertTrue(row0.isFromObject(1));
            assertEquals(RowType.STRING, row0.getBaseType(0));
            assertEquals("alice", row0.getStringValue(0));
            assertEquals(30L, row0.getLongValue(1));

            batch.close();
        }
    }

    public void testSchemaEvolution() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            // Doc 0: name and age
            builder.startDocument();
            builder.setString("name", "alice", false);
            builder.setLong("age", 30, false);
            builder.endDocument();

            // Doc 1: name and email (new column)
            builder.startDocument();
            builder.setString("name", "bob", false);
            builder.setString("email", "bob@test.com", false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(3, batch.columnCount());

            // Doc 0: email is NULL (column didn't exist yet)
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals("alice", row0.getStringValue(0));
            assertEquals(30L, row0.getLongValue(1));
            assertTrue(row0.isNull(2));

            // Doc 1: age is NULL, email is present
            DocBatchRowReader row1 = batch.getRowReader(1);
            assertEquals("bob", row1.getStringValue(0));
            assertTrue(row1.isNull(1));
            assertEquals("bob@test.com", row1.getStringValue(2));

            batch.close();
        }
    }

    public void testMissingFields() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice", false);
            builder.setLong("age", 30, false);
            builder.endDocument();

            // Second doc only has age
            builder.startDocument();
            builder.setLong("age", 25, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();

            DocBatchRowReader row1 = batch.getRowReader(1);
            assertTrue(row1.isNull(0)); // name absent
            assertFalse(row1.isNull(1)); // age present
            assertEquals(25L, row1.getLongValue(1));

            batch.close();
        }
    }

    public void testBooleanValues() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setBoolean("active", true, false);
            builder.setBoolean("deleted", false, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals(RowType.TRUE, row0.getBaseType(0));
            assertEquals(RowType.FALSE, row0.getBaseType(1));
            assertTrue(row0.getBooleanValue(0));
            assertFalse(row0.getBooleanValue(1));

            batch.close();
        }
    }

    public void testDoubleValues() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setDouble("score", 3.14, false);
            builder.setDouble("weight", -1.5, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals(RowType.DOUBLE, row0.getBaseType(0));
            assertEquals(3.14, row0.getDoubleValue(0), 0.001);
            assertEquals(-1.5, row0.getDoubleValue(1), 0.0);

            batch.close();
        }
    }

    public void testNullValues() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setNull("field", false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.isNull(0));
            assertEquals(RowType.NULL, row0.getBaseType(0));

            batch.close();
        }
    }

    public void testXContentArrayColumn() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            byte[] jsonArray = "[1,2,3,4,5,6,7,8,9,10]".getBytes(StandardCharsets.UTF_8);
            builder.startDocument();
            builder.setXContentArray("nums", new BytesArray(jsonArray), false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals(RowType.XCONTENT_ARRAY, row0.getBaseType(0));
            byte[] arrayBytes = row0.getArrayValue(0);
            String arrayJson = new String(arrayBytes, StandardCharsets.UTF_8);
            assertEquals("[1,2,3,4,5,6,7,8,9,10]", arrayJson);

            batch.close();
        }
    }

    public void testRoundTripReadableByRowReader() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "test", false);
            builder.setLong("count", 42, false);
            builder.setDouble("value", 2.718, false);
            builder.setBoolean("flag", true, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();

            DocBatchRowReader reader = batch.getRowReader(0);
            assertEquals(4, reader.columnCount());
            assertEquals("test", reader.getStringValue(0));
            assertEquals(42L, reader.getLongValue(1));
            assertEquals(2.718, reader.getDoubleValue(2), 0.001);
            assertTrue(reader.getBooleanValue(3));

            // Test iteration
            DocBatchRowIterator it = reader.iterator();
            List<Integer> cols = new ArrayList<>();
            while (it.next()) {
                if (it.isNull() == false) cols.add(it.column());
            }
            assertEquals(List.of(0, 1, 2, 3), cols);

            batch.close();
        }
    }

    public void testMixedTypesAcrossDocuments() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            // Doc 0: val is LONG
            builder.startDocument();
            builder.setLong("val", 42, false);
            builder.endDocument();

            // Doc 1: val is STRING
            builder.startDocument();
            builder.setString("val", "hello", false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();

            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals(RowType.LONG, row0.getBaseType(0));
            assertEquals(42L, row0.getLongValue(0));

            DocBatchRowReader row1 = batch.getRowReader(1);
            assertEquals(RowType.STRING, row1.getBaseType(0));
            assertEquals("hello", row1.getStringValue(0));

            batch.close();
        }
    }

    public void testEquivalenceWithEncoder() throws IOException {
        // Build via encoder (from JSON)
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"age\":30}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\",\"age\":25}", XContentType.JSON));
        RowDocumentBatch encoderBatch = DocumentBatchRowEncoder.encode(requests);

        // Build via builder (programmatic)
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice", false);
            builder.setLong("age", 30, false);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob", false);
            builder.setLong("age", 25, false);
            builder.endDocument();

            RowDocumentBatch builderBatch = builder.build();

            // Verify identical structure
            assertEquals(encoderBatch.docCount(), builderBatch.docCount());
            assertEquals(encoderBatch.columnCount(), builderBatch.columnCount());

            for (int doc = 0; doc < encoderBatch.docCount(); doc++) {
                DocBatchRowReader encoderRow = encoderBatch.getRowReader(doc);
                DocBatchRowReader builderRow = builderBatch.getRowReader(doc);
                assertEquals(encoderRow.columnCount(), builderRow.columnCount());
                for (int col = 0; col < encoderRow.columnCount(); col++) {
                    assertEquals("Type mismatch at doc " + doc + " col " + col, encoderRow.getBaseType(col), builderRow.getBaseType(col));
                    assertEquals(
                        "Object flag mismatch at doc " + doc + " col " + col,
                        encoderRow.isFromObject(col),
                        builderRow.isFromObject(col)
                    );
                    if (encoderRow.isNull(col) == false) {
                        byte baseType = encoderRow.getBaseType(col);
                        switch (baseType) {
                            case RowType.STRING -> assertEquals(encoderRow.getStringValue(col), builderRow.getStringValue(col));
                            case RowType.LONG -> assertEquals(encoderRow.getLongValue(col), builderRow.getLongValue(col));
                            case RowType.DOUBLE -> assertEquals(encoderRow.getDoubleValue(col), builderRow.getDoubleValue(col), 0.0);
                            case RowType.TRUE, RowType.FALSE -> assertEquals(
                                encoderRow.getBooleanValue(col),
                                builderRow.getBooleanValue(col)
                            );
                        }
                    }
                }
            }

            builderBatch.close();
        }
        encoderBatch.close();
    }

    public void testMultipleDocumentsWithManyColumns() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            // First document with 20 columns
            builder.startDocument();
            for (int i = 0; i < 20; i++) {
                builder.setLong("col" + i, i, false);
            }
            builder.endDocument();

            // Second document with same columns
            builder.startDocument();
            for (int i = 0; i < 20; i++) {
                builder.setLong("col" + i, i * 10, false);
            }
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(20, batch.columnCount());

            DocBatchRowReader row0 = batch.getRowReader(0);
            for (int i = 0; i < 20; i++) {
                assertEquals(i, row0.getLongValue(i));
            }

            DocBatchRowReader row1 = batch.getRowReader(1);
            for (int i = 0; i < 20; i++) {
                assertEquals(i * 10, row1.getLongValue(i));
            }

            batch.close();
        }
    }

    public void testSetStringFromRawBytes() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            byte[] utf8 = "hello world".getBytes(StandardCharsets.UTF_8);
            builder.startDocument();
            builder.setString("msg", utf8, 0, utf8.length, false);
            builder.endDocument();

            RowDocumentBatch batch = builder.build();
            DocBatchRowReader row0 = batch.getRowReader(0);
            assertEquals("hello world", row0.getStringValue(0));

            batch.close();
        }
    }

    public void testStartDocumentWithoutEnd() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::startDocument);
        }
    }

    public void testEndDocumentWithoutStart() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            expectThrows(IllegalStateException.class, builder::endDocument);
        }
    }

    public void testBuildWhileInDocument() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::build);
        }
    }

    public void testSetValueOutsideDocument() {
        try (DocumentBatchRowBuilder builder = new DocumentBatchRowBuilder()) {
            expectThrows(IllegalStateException.class, () -> builder.setString("name", "test", false));
        }
    }
}
