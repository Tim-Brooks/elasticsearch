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
        byte[] packed = EirfEncoder.packUnionArray(new byte[0], new long[0], new XContentString.UTF8Bytes[0], 0);
        EirfArray reader = new EirfArray(packed, false);
        assertEquals(0, reader.count());
        assertFalse(reader.next());
    }

    public void testEmptyFixedArray() {
        // Fixed array with 0 elements: just count byte
        byte[] packed = new byte[] { 0 };
        EirfArray reader = new EirfArray(packed, true);
        assertEquals(0, reader.count());
        assertFalse(reader.next());
    }

    public void testUnionArraySingleInt() {
        byte[] elemTypes = { EirfType.INT };
        long[] elemFixed = { 42L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemFixed, new XContentString.UTF8Bytes[1], 1);

        EirfArray reader = new EirfArray(packed, false);
        assertEquals(1, reader.count());
        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayInts() {
        byte[] packed = EirfEncoder.packFixedArray(EirfType.INT, new long[] { 1, 2, 3 }, null, 3);

        EirfArray reader = new EirfArray(packed, true);
        assertEquals(3, reader.count());
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
        XContentString.UTF8Bytes[] strings = {
            new XContentString.UTF8Bytes(utf8a, 0, utf8a.length),
            new XContentString.UTF8Bytes(utf8b, 0, utf8b.length) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.STRING, new long[2], strings, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertEquals(2, reader.count());
        assertTrue(reader.next());
        assertEquals("hello", reader.stringValue());
        assertTrue(reader.next());
        assertEquals("world", reader.stringValue());
        assertFalse(reader.next());
    }

    public void testUnionArrayMixedTypes() {
        byte[] elemTypes = { EirfType.INT, EirfType.STRING, EirfType.TRUE, EirfType.NULL, EirfType.FLOAT };
        long[] elemFixed = new long[5];
        elemFixed[0] = 42;
        elemFixed[4] = Float.floatToRawIntBits(3.14f);
        XContentString.UTF8Bytes[] strings = new XContentString.UTF8Bytes[5];
        byte[] utf8 = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        strings[1] = new XContentString.UTF8Bytes(utf8, 0, utf8.length);

        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemFixed, strings, 5);
        EirfArray reader = new EirfArray(packed, false);
        assertEquals(5, reader.count());

        assertTrue(reader.next());
        assertEquals(EirfType.INT, reader.type());
        assertEquals(42, reader.intValue());

        assertTrue(reader.next());
        assertEquals(EirfType.STRING, reader.type());
        assertEquals("world", reader.stringValue());

        assertTrue(reader.next());
        assertEquals(EirfType.TRUE, reader.type());
        assertTrue(reader.booleanValue());

        assertTrue(reader.next());
        assertEquals(EirfType.NULL, reader.type());
        assertTrue(reader.isNull());

        assertTrue(reader.next());
        assertEquals(EirfType.FLOAT, reader.type());
        assertEquals(3.14f, reader.floatValue(), 0.001f);

        assertFalse(reader.next());
    }

    public void testBooleanValues() {
        byte[] elemTypes = { EirfType.TRUE, EirfType.FALSE };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, new long[2], new XContentString.UTF8Bytes[2], 2);

        EirfArray reader = new EirfArray(packed, false);
        assertTrue(reader.next());
        assertTrue(reader.booleanValue());
        assertTrue(reader.next());
        assertFalse(reader.booleanValue());
        assertFalse(reader.next());
    }

    public void testWithOffset() {
        byte[] elemTypes = { EirfType.INT };
        long[] elemFixed = { 99L };
        byte[] packed = EirfEncoder.packUnionArray(elemTypes, elemFixed, new XContentString.UTF8Bytes[1], 1);

        byte[] withPrefix = new byte[5 + packed.length];
        System.arraycopy(packed, 0, withPrefix, 5, packed.length);

        EirfArray reader = new EirfArray(withPrefix, 5, false);
        assertEquals(1, reader.count());
        assertTrue(reader.next());
        assertEquals(99, reader.intValue());
    }

    public void testFixedArrayLongs() {
        long[] values = { Long.MAX_VALUE, Long.MIN_VALUE };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.LONG, values, null, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals(Long.MAX_VALUE, reader.longValue());
        assertTrue(reader.next());
        assertEquals(Long.MIN_VALUE, reader.longValue());
        assertFalse(reader.next());
    }

    public void testFixedArrayDoubles() {
        long[] values = { Double.doubleToRawLongBits(3.14), Double.doubleToRawLongBits(-2.718) };
        byte[] packed = EirfEncoder.packFixedArray(EirfType.DOUBLE, values, null, 2);

        EirfArray reader = new EirfArray(packed, true);
        assertTrue(reader.next());
        assertEquals(3.14, reader.doubleValue(), 0.001);
        assertTrue(reader.next());
        assertEquals(-2.718, reader.doubleValue(), 0.001);
        assertFalse(reader.next());
    }
}
