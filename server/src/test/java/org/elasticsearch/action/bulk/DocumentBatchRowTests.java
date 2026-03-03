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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DocumentBatchRowTests extends ESTestCase {

    public void testRoundTripSimpleDocuments() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"age\":30}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\",\"age\":25}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("3").source("{\"name\":\"charlie\",\"age\":35}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(3, batch.docCount());
        assertEquals(2, batch.columnCount()); // name and age

        DocBatchSchema schema = batch.schema();
        assertEquals("name", schema.getColumnName(0));
        assertEquals("age", schema.getColumnName(1));

        // Doc 0
        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.STRING, row0.getBaseType(0));
        assertEquals("alice", row0.getStringValue(0));
        assertEquals(RowType.LONG, row0.getBaseType(1));
        assertEquals(30L, row0.getLongValue(1));

        // Doc 1
        DocBatchRowReader row1 = batch.getRowReader(1);
        assertEquals("bob", row1.getStringValue(0));
        assertEquals(25L, row1.getLongValue(1));

        // Doc 2
        DocBatchRowReader row2 = batch.getRowReader(2);
        assertEquals("charlie", row2.getStringValue(0));
        assertEquals(35L, row2.getLongValue(1));

        batch.close();
    }

    public void testRoundTripNestedObjects() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"user\":{\"name\":\"alice\",\"age\":30}}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"user\":{\"name\":\"bob\",\"age\":25}}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(2, batch.docCount());
        assertEquals(2, batch.columnCount());

        DocBatchSchema schema = batch.schema();
        assertEquals("user.name", schema.getColumnName(0));
        assertEquals("user.age", schema.getColumnName(1));

        // Verify object flag is set
        DocBatchRowReader row0 = batch.getRowReader(0);
        assertTrue(row0.isFromObject(0));
        assertTrue(row0.isFromObject(1));
        assertEquals(RowType.STRING, row0.getBaseType(0));
        assertEquals("alice", row0.getStringValue(0));
        assertEquals(30L, row0.getLongValue(1));

        DocBatchRowReader row1 = batch.getRowReader(1);
        assertTrue(row1.isFromObject(0));
        assertEquals("bob", row1.getStringValue(0));
        assertEquals(25L, row1.getLongValue(1));

        batch.close();
    }

    public void testRoundTripWithArrays() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"tags\":[\"a\",\"b\"]}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\",\"tags\":[\"c\"]}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(2, batch.docCount());
        assertEquals(2, batch.columnCount());

        DocBatchSchema schema = batch.schema();
        assertEquals("name", schema.getColumnName(0));
        assertEquals("tags", schema.getColumnName(1));

        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.STRING, row0.getBaseType(0));
        assertEquals(RowType.ARRAY, row0.getBaseType(1));
        assertEquals("alice", row0.getStringValue(0));
        // Array value is raw JSON bytes
        byte[] arrayBytes = row0.getArrayValue(1);
        String arrayJson = new String(arrayBytes, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(arrayJson.contains("\"a\""));
        assertTrue(arrayJson.contains("\"b\""));

        batch.close();
    }

    public void testRoundTripWithMissingFields() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"name\":\"alice\",\"age\":30}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"name\":\"bob\"}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("3").source("{\"age\":35,\"email\":\"c@d.com\"}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(3, batch.docCount());
        assertEquals(3, batch.columnCount()); // name, age, email

        // Doc 0: has name+age, missing email
        DocBatchRowReader row0 = batch.getRowReader(0);
        assertFalse(row0.isNull(0)); // name present
        assertFalse(row0.isNull(1)); // age present
        assertTrue(row0.isNull(2));  // email absent

        // Doc 1: has name, missing age+email
        DocBatchRowReader row1 = batch.getRowReader(1);
        assertFalse(row1.isNull(0)); // name present
        assertTrue(row1.isNull(1));  // age absent
        assertTrue(row1.isNull(2));  // email absent

        // Doc 2: missing name, has age+email
        DocBatchRowReader row2 = batch.getRowReader(2);
        assertTrue(row2.isNull(0));  // name absent
        assertFalse(row2.isNull(1)); // age present
        assertFalse(row2.isNull(2)); // email present
        assertEquals("c@d.com", row2.getStringValue(2));

        batch.close();
    }

    public void testBooleanValues() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"active\":true,\"deleted\":false}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"active\":false,\"deleted\":true}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.TRUE, row0.getBaseType(0));
        assertEquals(RowType.FALSE, row0.getBaseType(1));
        assertTrue(row0.getBooleanValue(0));
        assertFalse(row0.getBooleanValue(1));

        DocBatchRowReader row1 = batch.getRowReader(1);
        assertEquals(RowType.FALSE, row1.getBaseType(0));
        assertEquals(RowType.TRUE, row1.getBaseType(1));
        assertFalse(row1.getBooleanValue(0));
        assertTrue(row1.getBooleanValue(1));

        batch.close();
    }

    public void testDoubleValues() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"score\":3.14,\"weight\":2.718}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"score\":0.0,\"weight\":-1.5}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.DOUBLE, row0.getBaseType(0));
        assertEquals(3.14, row0.getDoubleValue(0), 0.001);
        assertEquals(2.718, row0.getDoubleValue(1), 0.001);

        DocBatchRowReader row1 = batch.getRowReader(1);
        assertEquals(0.0, row1.getDoubleValue(0), 0.0);
        assertEquals(-1.5, row1.getDoubleValue(1), 0.0);

        batch.close();
    }

    public void testLongValues() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"big\":" + Long.MAX_VALUE + "}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"big\":" + Long.MIN_VALUE + "}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.LONG, row0.getBaseType(0));
        assertEquals(Long.MAX_VALUE, row0.getLongValue(0));

        DocBatchRowReader row1 = batch.getRowReader(1);
        assertEquals(Long.MIN_VALUE, row1.getLongValue(0));

        batch.close();
    }

    public void testSchemaIteration() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"a\":1,\"b\":\"x\",\"c\":true}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        DocBatchSchema schema = batch.schema();
        assertEquals(3, schema.columnCount());
        assertEquals("a", schema.getColumnName(0));
        assertEquals("b", schema.getColumnName(1));
        assertEquals("c", schema.getColumnName(2));

        // Lookup by name
        assertEquals(0, schema.getColumnIndex("a"));
        assertEquals(1, schema.getColumnIndex("b"));
        assertEquals(2, schema.getColumnIndex("c"));
        assertEquals(-1, schema.getColumnIndex("nonexistent"));

        batch.close();
    }

    public void testRowIteration() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"a\":1,\"b\":\"x\"}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"b\":\"y\"}", XContentType.JSON)); // missing 'a'

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        // Doc 0: both fields present
        DocBatchRowReader row0 = batch.getRowReader(0);
        List<Integer> row0Cols = new ArrayList<>();
        row0.forEachField((col, typeByte) -> row0Cols.add(col));
        assertEquals(List.of(0, 1), row0Cols);

        // Doc 1: only 'b' present, 'a' is NULL
        DocBatchRowReader row1 = batch.getRowReader(1);
        List<Integer> row1Cols = new ArrayList<>();
        row1.forEachField((col, typeByte) -> row1Cols.add(col));
        assertEquals(List.of(1), row1Cols);

        batch.close();
    }

    public void testEmptyDocument() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(1, batch.docCount());
        assertEquals(0, batch.columnCount());

        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(0, row0.columnCount());

        // forEachField should not call the consumer
        List<Integer> cols = new ArrayList<>();
        row0.forEachField((col, typeByte) -> cols.add(col));
        assertTrue(cols.isEmpty());

        batch.close();
    }

    public void testMixedTypeSameField() throws IOException {
        List<IndexRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("1").source("{\"val\":42}", XContentType.JSON));
        requests.add(new IndexRequest("test").id("2").source("{\"val\":\"hello\"}", XContentType.JSON));

        RowDocumentBatch batch = DocumentBatchRowEncoder.encode(requests);

        assertEquals(2, batch.docCount());
        assertEquals(1, batch.columnCount());

        // Doc 0: val is LONG
        DocBatchRowReader row0 = batch.getRowReader(0);
        assertEquals(RowType.LONG, row0.getBaseType(0));
        assertEquals(42L, row0.getLongValue(0));

        // Doc 1: val is STRING (no widening — each row has its own type byte)
        DocBatchRowReader row1 = batch.getRowReader(1);
        assertEquals(RowType.STRING, row1.getBaseType(0));
        assertEquals("hello", row1.getStringValue(0));

        batch.close();
    }
}
