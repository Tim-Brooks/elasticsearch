/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentString;

public class EirfArrayTests extends ESTestCase {

    public void testEmptyUnionArray() {
        byte[] packed = EirfEncoder.packUnionArray(new byte[0], new Object[0], 0);
        EirfArray reader = new EirfArray(packed, false);
        assertFalse(reader.next());
    }

    public void testEmptyFixedArray() {
        // Fixed array with 0 elements: empty byte array
        byte[] packed = new byte[0];
        EirfArray reader = new EirfArray(packed, true);
        assertFalse(reader.next());
    }

    public void testUnionArraySingleInt() {
        byte[] elemTypes = { EirfType.INT };
        Object[] elemData = { 42L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemData, 1);

        EirfArray reader = new EirfArray(packed, false);
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayInts() {
        Object[] elemData = { 1L, 2L, 3L };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.INT, elemData, 3);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(1, reader.intValue());
        assertTrue(reader.next());
        assertEquals(2, reader.intValue());
        assertTrue(reader.next());
        assertEquals(3, reader.intValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayStrings() {
        byte[] utf8a = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] utf8b = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Object[] elemData = { new XContentString.UTF8Bytes(utf8a, 0, utf8a.length), new XContentString.UTF8Bytes(utf8b, 0, utf8b.length) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.STRING, elemData, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals("hello", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("world", reader.stringValue());
        assertFalse(reader.next());
    }

    public void testUnionArrayMixedTypes() {
        byte[] utf8 = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] elemTypes = { EirfType.INT, EirfType.STRING, EirfType.TRUE, EirfType.NULL, EirfType.FLOAT };
        Object[] elemData = new Object[5];
        elemData[0] = 42L;
        elemData[1] = new XContentString.UTF8Bytes(utf8, 0, utf8.length);
        // TRUE and NULL have no data
        elemData[4] = (long) Float.floatToRawIntBits(3.14f);

        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemData, 5);
        EirfArray reader = new EirfArray(packed, false);

        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());

        assertTrue(reader.next());
        assertEquals(EirfType.STRING, reader.type());
        assertEquals("world", reader.stringValue());

        assertTrue(reader.next());
        assertEquals(EirfType.TRUE, reader.type());
        assertTrue(reader.booleanValue());
        reader.advance();

        assertTrue(reader.next());
        assertEquals(EirfType.NULL, reader.type());
        assertTrue(reader.isNull());
        reader.advance();

        assertTrue(reader.next());
        assertEquals(EirfType.FLOAT, reader.type());
        assertEquals(3.14f, reader.floatValue(), 0.001f);

        assertFalse(reader.next());
    }

    public void testBooleanValues() {
        byte[] elemTypes = { EirfType.TRUE, EirfType.FALSE };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, new Object[2], 2);

        EirfArray reader = new EirfArray(packed, false);
        assertTrue(reader.next());
        assertTrue(reader.booleanValue());
        reader.advance();
        assertTrue(reader.next());
        assertFalse(reader.booleanValue());
        reader.advance();
        assertFalse(reader.next());
    }

    public void testWithOffset() {
        byte[] elemTypes = { EirfType.INT };
        Object[] elemData = { 99L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemData, 1);

        byte[] withPrefix = new byte[5 + packed.length];
        System.arraycopy(packed, 0, withPrefix, 5, packed.length);

        EirfArray reader = new EirfArray(withPrefix, 5, packed.length, false);
        assertTrue(reader.next());
        assertEquals(99, reader.intValue());
    }

    public void testFixedArrayLongs() {
        Object[] elemData = { Long.MAX_VALUE, Long.MIN_VALUE };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.LONG, elemData, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals(Long.MAX_VALUE, reader.longValue());
        assertTrue(reader.next());
        assertEquals(Long.MIN_VALUE, reader.longValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayDoubles() {
        Object[] elemData = { Double.doubleToRawLongBits(3.14), Double.doubleToRawLongBits(-2.718) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.DOUBLE, elemData, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals(3.14, reader.doubleValue(), 0.001);
        assertTrue(reader.next());
        assertEquals(-2.718, reader.doubleValue(), 0.001);
        assertFalse(reader.next());
    }

    public void testUnionArrayWithCompoundKeyValue() {
        // Create a KEY_VALUE element: key_length(i32 LE)=1, key="a", type=INT, value=42 (LE)
        byte[] kvPayload = new byte[] {
            1,
            0,
            0,
            0,      // key_length = 1 (i32 LE)
            'a',              // key bytes
            EirfType.INT,     // type
            42,
            0,
            0,
            0       // INT value = 42 (LE)
        };

        byte[] elemTypes = { EirfType.KEY_VALUE };
        Object[] elemData = { kvPayload };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemData, 1);

        EirfArray reader = new EirfArray(packed, false);
        assertTrue(reader.next());
        assertEquals(EirfType.KEY_VALUE, reader.type());
        assertEquals(kvPayload.length, reader.compoundLength());
        reader.skipCompound();
        assertFalse(reader.next());
    }
}
