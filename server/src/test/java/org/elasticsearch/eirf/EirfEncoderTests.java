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

public class EirfEncoderTests extends ESTestCase {

    public void testRoundTripSimpleDocuments() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\",\"age\":25}")
        );

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(2, batch.docCount());
        assertEquals(2, batch.columnCount());

        EirfSchema schema = batch.schema();
        assertEquals("name", schema.getFullPath(0));
        assertEquals("age", schema.getFullPath(1));

        EirfRowReader row0 = batch.getRowReader(0);
        assertEquals("alice", row0.getStringValue(0));
        // 30 fits in int
        assertEquals(EirfType.INT, row0.getTypeByte(1));
        assertEquals(30, row0.getIntValue(1));

        EirfRowReader row1 = batch.getRowReader(1);
        assertEquals("bob", row1.getStringValue(0));
        assertEquals(25, row1.getIntValue(1));

        batch.close();
    }

    public void testIntNarrowing() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"small\":42,\"big\":" + Long.MAX_VALUE + "}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        // 42 fits in int
        assertEquals(EirfType.INT, row0.getTypeByte(0));
        assertEquals(42, row0.getIntValue(0));
        // Long.MAX_VALUE doesn't fit in int
        assertEquals(EirfType.LONG, row0.getTypeByte(1));
        assertEquals(Long.MAX_VALUE, row0.getLongValue(1));

        batch.close();
    }

    public void testFloatNarrowing() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"exact\":1.5,\"precise\":1.23456789012345}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        // 1.5 round-trips through float
        assertEquals(EirfType.FLOAT, row0.getTypeByte(0));
        assertEquals(1.5f, row0.getFloatValue(0), 0.0f);
        // 1.23456789012345 loses precision as float
        assertEquals(EirfType.DOUBLE, row0.getTypeByte(1));
        assertEquals(1.23456789012345, row0.getDoubleValue(1), 0.0);

        batch.close();
    }

    public void testSmallVariantsUsedForSmallDocuments() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"name\":\"alice\"}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        // Should use SMALL_STRING since var section is tiny
        assertEquals(EirfType.SMALL_STRING, row0.getTypeByte(0));
        assertEquals("alice", row0.getStringValue(0));

        batch.close();
    }

    public void testRoundTripNestedObjects() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"user\":{\"name\":\"alice\",\"age\":30}}"),
            new BytesArray("{\"user\":{\"name\":\"bob\",\"age\":25}}")
        );

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(2, batch.docCount());
        EirfSchema schema = batch.schema();
        assertEquals(2, schema.nonLeafCount());
        assertEquals("user.name", schema.getFullPath(0));
        assertEquals("user.age", schema.getFullPath(1));

        assertEquals("alice", batch.getRowReader(0).getStringValue(0));

        batch.close();
    }

    public void testDeepNesting() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"a\":{\"b\":{\"c\":42}}}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfSchema schema = batch.schema();
        assertEquals(3, schema.nonLeafCount());
        assertEquals("a.b.c", schema.getFullPath(0));
        assertEquals(42, batch.getRowReader(0).getIntValue(0));

        batch.close();
    }

    public void testFixedArrayAllSameType() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"tags\":[\"a\",\"b\",\"c\"]}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        byte type = row0.getTypeByte(0);
        // Should be a fixed array (small variant) since all strings
        assertTrue(type == EirfType.SMALL_FIXED_ARRAY || type == EirfType.FIXED_ARRAY);

        byte[] arrayData = row0.getArrayValue(0);
        EirfArray reader = new EirfArray(arrayData, true);
        assertEquals(3, reader.count());
        assertTrue(reader.next());
        assertEquals("a", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("b", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("c", reader.stringValue());

        batch.close();
    }

    public void testUnionArrayMixedTypes() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"data\":[42,\"hello\",true]}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        byte type = row0.getTypeByte(0);
        assertTrue(type == EirfType.SMALL_UNION_ARRAY || type == EirfType.UNION_ARRAY);

        byte[] arrayData = row0.getArrayValue(0);
        EirfArray reader = new EirfArray(arrayData, false);
        assertEquals(3, reader.count());

        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());

        assertTrue(reader.next());
        assertEquals(EirfType.STRING, reader.type());
        assertEquals("hello", reader.stringValue());

        assertTrue(reader.next());
        assertEquals(EirfType.TRUE, reader.type());

        batch.close();
    }

    public void testLargeArrayFallsBackToXContent() throws IOException {
        StringBuilder json = new StringBuilder("{\"nums\":[");
        for (int i = 0; i < EirfType.MAX_SMALL_ARRAY_SIZE + 1; i++) {
            if (i > 0) json.append(",");
            json.append(i);
        }
        json.append("]}");

        List<BytesReference> sources = List.of(new BytesArray(json.toString()));
        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        byte type = row0.getTypeByte(0);
        assertTrue(type == EirfType.SMALL_XCONTENT || type == EirfType.XCONTENT);

        String arrayJson = new String(row0.getXContentValue(0), StandardCharsets.UTF_8);
        assertTrue(arrayJson.startsWith("["));
        assertTrue(arrayJson.endsWith("]"));

        batch.close();
    }

    public void testArrayWithNestedObjectFallsBackToXContent() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"items\":[{\"name\":\"a\"},{\"name\":\"b\"}]}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        byte type = batch.getRowReader(0).getTypeByte(0);
        assertTrue(type == EirfType.SMALL_XCONTENT || type == EirfType.XCONTENT);

        batch.close();
    }

    public void testEmptySmallArray() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"tags\":[]}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        // Empty array: union array with 0 elements (all same type vacuously fails since count=0)
        byte type = row0.getTypeByte(0);
        assertTrue(EirfType.isSmallVariable(type) || EirfType.isLargeVariable(type));

        batch.close();
    }

    public void testMissingFields() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"name\":\"bob\"}"),
            new BytesArray("{\"age\":35,\"email\":\"c@d.com\"}")
        );

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(3, batch.docCount());
        assertEquals(3, batch.columnCount());

        EirfRowReader row0 = batch.getRowReader(0);
        assertFalse(row0.isNull(0));
        assertFalse(row0.isNull(1));
        assertTrue(row0.isNull(2));

        EirfRowReader row1 = batch.getRowReader(1);
        assertFalse(row1.isNull(0));
        assertTrue(row1.isNull(1));
        assertTrue(row1.isNull(2));

        EirfRowReader row2 = batch.getRowReader(2);
        assertTrue(row2.isNull(0));
        assertFalse(row2.isNull(1));
        assertFalse(row2.isNull(2));
        assertEquals("c@d.com", row2.getStringValue(2));

        batch.close();
    }

    public void testBooleanValues() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"active\":true,\"deleted\":false}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        assertTrue(row0.getBooleanValue(0));
        assertFalse(row0.getBooleanValue(1));

        batch.close();
    }

    public void testEmptyDocument() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(1, batch.docCount());
        assertEquals(0, batch.columnCount());

        batch.close();
    }

    public void testSchemaEvolutionAcrossDocuments() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"name\":\"alice\",\"age\":30}"),
            new BytesArray("{\"email\":\"b@c.com\"}"),
            new BytesArray("{\"name\":\"charlie\"}")
        );

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(3, batch.docCount());
        assertEquals(3, batch.columnCount());

        EirfRowReader row0 = batch.getRowReader(0);
        assertEquals(2, row0.columnCount());
        assertTrue(row0.isNull(2));

        batch.close();
    }

    public void testMixedTypeSameField() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"val\":42}"), new BytesArray("{\"val\":\"hello\"}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        assertEquals(EirfType.INT, batch.getRowReader(0).getTypeByte(0));
        assertEquals(42, batch.getRowReader(0).getIntValue(0));
        byte type1 = batch.getRowReader(1).getTypeByte(0);
        assertTrue(type1 == EirfType.SMALL_STRING || type1 == EirfType.STRING);
        assertEquals("hello", batch.getRowReader(1).getStringValue(0));

        batch.close();
    }

    public void testIncrementalEncoding() throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            encoder.addDocument(new BytesArray("{\"name\":\"alice\",\"age\":30}"), XContentType.JSON);
            encoder.addDocument(new BytesArray("{\"name\":\"bob\",\"age\":25}"), XContentType.JSON);

            EirfBatch batch = encoder.build();
            assertEquals(2, batch.docCount());
            assertEquals("alice", batch.getRowReader(0).getStringValue(0));
            assertEquals("bob", batch.getRowReader(1).getStringValue(0));
            batch.close();
        }
    }

    public void testNegativeIntNarrowing() throws IOException {
        List<BytesReference> sources = List.of(new BytesArray("{\"val\":-100}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        assertEquals(EirfType.INT, row0.getTypeByte(0));
        assertEquals(-100, row0.getIntValue(0));

        batch.close();
    }

    public void testIntBoundary() throws IOException {
        // Integer.MAX_VALUE fits in INT, Integer.MAX_VALUE + 1 needs LONG
        List<BytesReference> sources = List.of(
            new BytesArray("{\"a\":" + Integer.MAX_VALUE + ",\"b\":" + ((long) Integer.MAX_VALUE + 1) + "}")
        );

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        assertEquals(EirfType.INT, row0.getTypeByte(0));
        assertEquals(Integer.MAX_VALUE, row0.getIntValue(0));
        assertEquals(EirfType.LONG, row0.getTypeByte(1));
        assertEquals((long) Integer.MAX_VALUE + 1, row0.getLongValue(1));

        batch.close();
    }

    public void testFixedArrayWithIntNarrowing() throws IOException {
        // All ints that fit in i32 -> should produce FIXED_ARRAY with INT elements
        List<BytesReference> sources = List.of(new BytesArray("{\"nums\":[1,2,3]}"));

        EirfBatch batch = EirfEncoder.encode(sources, XContentType.JSON);

        EirfRowReader row0 = batch.getRowReader(0);
        byte type = row0.getTypeByte(0);
        assertTrue(type == EirfType.SMALL_FIXED_ARRAY || type == EirfType.FIXED_ARRAY);

        byte[] arrayData = row0.getArrayValue(0);
        EirfArray reader = new EirfArray(arrayData, true);
        assertEquals(3, reader.count());
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(1, reader.intValue());
        assertTrue(reader.next());
        assertEquals(2, reader.intValue());
        assertTrue(reader.next());
        assertEquals(3, reader.intValue());

        batch.close();
    }
}
