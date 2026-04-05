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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EirfConversionTests extends ESTestCase {

    // ---- EirfRowToXContent tests ----

    public void testRowToXContentFlatDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"age\":30}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("alice", result.get("name"));
        assertEquals(30, result.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentNestedDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\"}")),
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("active", result.get("status"));
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        assertEquals(30, user.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentDeepNesting() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"a\":{\"b\":{\"c\":42}}}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> a = (Map<String, Object>) result.get("a");
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertEquals(42, b.get("c"));

        batch.close();
    }

    public void testRowToXContentWithArray() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"tags\":[\"a\",\"b\"]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("alice", result.get("name"));
        assertEquals(List.of("a", "b"), result.get("tags"));

        batch.close();
    }

    public void testRowToXContentSkipsNullFields() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"name\":\"alice\",\"age\":30}"), new BytesArray("{\"age\":25}")),
            XContentType.JSON
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(1), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertFalse(result.containsKey("name"));
        assertEquals(25, result.get("age"));

        batch.close();
    }

    public void testRowToXContentWithBooleans() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"active\":true,\"deleted\":false}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals(true, result.get("active"));
        assertEquals(false, result.get("deleted"));

        batch.close();
    }

    // ---- EirfRowToMap tests ----

    public void testRowToMapFlatDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"name\":\"alice\",\"age\":30}")), XContentType.JSON);

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        assertEquals("alice", map.get("name"));
        // 30 is stored as INT
        assertEquals(30, map.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToMapNestedDocument() throws IOException {
        EirfBatch batch = EirfEncoder.encode(
            List.of(new BytesArray("{\"user\":{\"name\":\"alice\"},\"status\":\"active\"}")),
            XContentType.JSON
        );

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        assertEquals("active", map.get("status"));
        Map<String, Object> user = (Map<String, Object>) map.get("user");
        assertEquals("alice", user.get("name"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToMapDeepNesting() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"a\":{\"b\":{\"c\":42}}}")), XContentType.JSON);

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        Map<String, Object> a = (Map<String, Object>) map.get("a");
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertEquals(42, b.get("c"));

        batch.close();
    }

    public void testRowToMapWithArray() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"tags\":[\"a\",\"b\",\"c\"]}")), XContentType.JSON);

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        assertEquals(List.of("a", "b", "c"), map.get("tags"));

        batch.close();
    }

    public void testRowToMapBooleans() throws IOException {
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"flag\":true}")), XContentType.JSON);

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        assertEquals(Boolean.TRUE, map.get("flag"));

        batch.close();
    }

    // ---- EirfMapToRow tests ----

    public void testMapToRowFlatDocument() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", "alice");
        doc.put("age", 30);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            EirfRowReader row = batch.getRowReader(0);
            assertEquals("alice", row.getStringValue(0));
            assertEquals(EirfType.INT, row.getTypeByte(1));
            assertEquals(30, row.getIntValue(1));

            batch.close();
        }
    }

    public void testMapToRowNestedDocument() throws IOException {
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "alice");
        user.put("age", 30);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("user", user);
        doc.put("status", "active");

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            EirfSchema schema = batch.schema();
            assertEquals("user.name", schema.getFullPath(0));
            assertEquals("user.age", schema.getFullPath(1));
            assertEquals("status", schema.getFullPath(2));

            batch.close();
        }
    }

    public void testMapToRowWithList() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("tags", List.of("a", "b"));

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            byte type = batch.getRowReader(0).getTypeByte(0);
            // Lists with uniform leaf types produce FIXED_ARRAY
            assertTrue(type == EirfType.SMALL_FIXED_ARRAY || type == EirfType.FIXED_ARRAY);

            batch.close();
        }
    }

    public void testMapToRowNullValue() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("field", null);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            assertTrue(batch.getRowReader(0).isNull(0));

            batch.close();
        }
    }

    // ---- Round-trip tests ----

    @SuppressWarnings("unchecked")
    public void testMapRoundTrip() throws IOException {
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "alice");
        user.put("age", 30);
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("user", user);
        original.put("status", "active");
        original.put("score", 3.14);
        original.put("active", true);

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(original, builder);
            EirfBatch batch = builder.build();

            Map<String, Object> roundTripped = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());

            assertEquals("active", roundTripped.get("status"));
            // 3.14 might be narrowed to float
            Number score = (Number) roundTripped.get("score");
            assertEquals(3.14, score.doubleValue(), 0.01);
            assertEquals(true, roundTripped.get("active"));
            Map<String, Object> rtUser = (Map<String, Object>) roundTripped.get("user");
            assertEquals("alice", rtUser.get("name"));
            assertEquals(30, rtUser.get("age"));

            batch.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void testJsonRoundTrip() throws IOException {
        String json = "{\"user\":{\"name\":\"alice\",\"age\":30},\"status\":\"active\",\"score\":3.14}";
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        assertEquals("active", result.get("status"));
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        assertEquals(30, user.get("age"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testJsonToMapRoundTrip() throws IOException {
        String json = "{\"a\":{\"b\":{\"c\":42}},\"x\":\"y\"}";
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);

        Map<String, Object> map = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
        assertEquals("y", map.get("x"));
        Map<String, Object> a = (Map<String, Object>) map.get("a");
        Map<String, Object> b = (Map<String, Object>) a.get("b");
        assertEquals(42, b.get("c"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentMultipleNestedSiblings() throws IOException {
        String json = "{\"user\":{\"name\":\"alice\"},\"meta\":{\"version\":1}}";
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        Map<String, Object> user = (Map<String, Object>) result.get("user");
        assertEquals("alice", user.get("name"));
        Map<String, Object> meta = (Map<String, Object>) result.get("meta");
        assertEquals(1, meta.get("version"));

        batch.close();
    }

    public void testIntNarrowingRoundTripThroughMap() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("small", 42L); // Long that fits in int
        doc.put("big", Long.MAX_VALUE); // Long that doesn't

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            Map<String, Object> result = EirfRowToMap.toMap(batch.getRowReader(0), batch.schema());
            // 42L was narrowed to int
            assertEquals(42, result.get("small"));
            assertEquals(Long.MAX_VALUE, result.get("big"));

            batch.close();
        }
    }

    public void testFloatNarrowingRoundTripThroughMap() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("exact", 1.5); // Double that fits in float
        doc.put("precise", 1.23456789012345); // Double that doesn't

        try (EirfRowBuilder builder = new EirfRowBuilder()) {
            EirfMapToRow.writeToBuilder(doc, builder);
            EirfBatch batch = builder.build();

            EirfRowReader row = batch.getRowReader(0);
            assertEquals(EirfType.FLOAT, row.getTypeByte(0));
            assertEquals(EirfType.DOUBLE, row.getTypeByte(1));

            batch.close();
        }
    }
}
