/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class EirfRowBuilderTests extends ESTestCase {

    public void testSimpleScalarDocuments() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setInt("age", 25);
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(2, batch.docCount());
            assertEquals(2, batch.columnCount());

            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals("alice", row0.getStringValue(0));
            assertEquals(30, row0.getIntValue(1));

            EirfRowReader row1 = batch.getRowReader(1);
            assertEquals("bob", row1.getStringValue(0));
            assertEquals(25, row1.getIntValue(1));

            batch.close();
        }
    }

    public void testNestedObjectFields() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("user.name", "alice");
            builder.setInt("user.age", 30);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfSchema schema = batch.schema();
            assertEquals("user.name", schema.getFullPath(0));
            assertEquals("user.age", schema.getFullPath(1));

            batch.close();
        }
    }

    public void testIntAndLong() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setInt("small", 42);
            builder.setLong("big", Long.MAX_VALUE);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals(EirfType.INT, row0.getTypeByte(0));
            assertEquals(42, row0.getIntValue(0));
            assertEquals(EirfType.LONG, row0.getTypeByte(1));
            assertEquals(Long.MAX_VALUE, row0.getLongValue(1));

            batch.close();
        }
    }

    public void testFloatAndDouble() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setFloat("f", 1.5f);
            builder.setDouble("d", 1.23456789012345);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertEquals(EirfType.FLOAT, row0.getTypeByte(0));
            assertEquals(1.5f, row0.getFloatValue(0), 0.0f);
            assertEquals(EirfType.DOUBLE, row0.getTypeByte(1));
            assertEquals(1.23456789012345, row0.getDoubleValue(1), 0.0);

            batch.close();
        }
    }

    public void testBooleanValues() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setBoolean("active", true);
            builder.setBoolean("deleted", false);
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.getBooleanValue(0));
            assertFalse(row0.getBooleanValue(1));

            batch.close();
        }
    }

    public void testNullValues() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setNull("field");
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertTrue(batch.getRowReader(0).isNull(0));

            batch.close();
        }
    }

    public void testXContentColumn() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            byte[] jsonArray = "[1,2,3]".getBytes(StandardCharsets.UTF_8);
            builder.startDocument();
            builder.setXContent("nums", new BytesArray(jsonArray));
            builder.endDocument();

            EirfBatch batch = builder.build();
            EirfRowReader row0 = batch.getRowReader(0);
            byte type = row0.getTypeByte(0);
            assertTrue(type == EirfType.SMALL_XCONTENT || type == EirfType.XCONTENT);
            String json = new String(row0.getXContentValue(0), StandardCharsets.UTF_8);
            assertEquals("[1,2,3]", json);

            batch.close();
        }
    }

    public void testSchemaEvolution() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setString("email", "bob@test.com");
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(3, batch.columnCount());

            EirfRowReader row0 = batch.getRowReader(0);
            assertTrue(row0.isNull(2));

            EirfRowReader row1 = batch.getRowReader(1);
            assertTrue(row1.isNull(1));
            assertEquals("bob@test.com", row1.getStringValue(2));

            batch.close();
        }
    }

    public void testEquivalenceWithEncoder() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\",\"age\":25}")
        );
        EirfBatch encoderBatch = EirfEncoder.encode(sources, XContentType.JSON);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            builder.setString("name", "alice");
            builder.setInt("age", 30);
            builder.endDocument();

            builder.startDocument();
            builder.setString("name", "bob");
            builder.setInt("age", 25);
            builder.endDocument();

            EirfBatch builderBatch = builder.build();

            assertEquals(encoderBatch.docCount(), builderBatch.docCount());
            assertEquals(encoderBatch.columnCount(), builderBatch.columnCount());

            for (int doc = 0; doc < encoderBatch.docCount(); doc++) {
                EirfRowReader er = encoderBatch.getRowReader(doc);
                EirfRowReader br = builderBatch.getRowReader(doc);
                for (int col = 0; col < er.columnCount(); col++) {
                    assertEquals(er.getTypeByte(col), br.getTypeByte(col));
                }
            }

            builderBatch.close();
        }
        encoderBatch.close();
    }

    public void testStartDocumentWithoutEnd() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::startDocument);
        }
    }

    public void testEndDocumentWithoutStart() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            expectThrows(IllegalStateException.class, builder::endDocument);
        }
    }

    public void testBuildWhileInDocument() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            expectThrows(IllegalStateException.class, builder::build);
        }
    }

    public void testSetValueOutsideDocument() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            expectThrows(IllegalStateException.class, () -> builder.setString("name", "test"));
        }
    }

    public void testManyColumns() {
        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            builder.startDocument();
            for (int i = 0; i < 20; i++) {
                builder.setInt("col" + i, i);
            }
            builder.endDocument();

            EirfBatch batch = builder.build();
            assertEquals(20, batch.columnCount());
            EirfRowReader row0 = batch.getRowReader(0);
            for (int i = 0; i < 20; i++) {
                assertEquals(i, row0.getIntValue(i));
            }

            batch.close();
        }
    }
}
