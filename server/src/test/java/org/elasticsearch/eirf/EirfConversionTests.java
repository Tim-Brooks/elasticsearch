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
import java.util.List;
import java.util.Map;

public class EirfConversionTests extends ESTestCase {

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

    public void testRowToXContentFloatEmitsWithDoublePrecision() throws IOException {
        // 0.10000000149011612 is the exact double representation of 0.1f. The encoder stores it as FLOAT
        // (because (double)(float)val == val), and we want the rehydrated JSON to keep enough precision for
        // downstream double parsers rather than collapsing to "0.1".
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"x\":0.10000000149011612}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertTrue("expected double-precision textual form of 0.1f, got: " + json, json.contains("0.10000000149011612"));

        batch.close();
    }

    @SuppressWarnings("unchecked")
    public void testRowToXContentFloatArrayElementsDeserializeAsDouble() throws IOException {
        // 0.5 and 0.25 are FLOAT-exact, so they pack as a FIXED_ARRAY of FLOAT. After the cast-to-double
        // fix, array elements round-trip through the double branch of XContentParser.
        EirfBatch batch = EirfEncoder.encode(List.of(new BytesArray("{\"xs\":[0.5,0.25]}")), XContentType.JSON);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        EirfRowToXContent.writeRow(batch.getRowReader(0), batch.schema(), builder);
        builder.close();

        Map<String, Object> result = XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        List<Object> xs = (List<Object>) result.get("xs");
        assertEquals(2, xs.size());
        assertTrue("expected double elements, got: " + xs.get(0).getClass(), xs.get(0) instanceof Double);
        assertTrue("expected double elements, got: " + xs.get(1).getClass(), xs.get(1) instanceof Double);
        assertEquals(0.5, (Double) xs.get(0), 0.0);
        assertEquals(0.25, (Double) xs.get(1), 0.0);

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
}
