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

public class EirfTypeTests extends ESTestCase {

    public void testFixedSizeZeroByte() {
        assertEquals(0, EirfType.fixedSize(EirfType.NULL));
        assertEquals(0, EirfType.fixedSize(EirfType.TRUE));
        assertEquals(0, EirfType.fixedSize(EirfType.FALSE));
    }

    public void testFixedSizeFourByte() {
        assertEquals(4, EirfType.fixedSize(EirfType.INT));
        assertEquals(4, EirfType.fixedSize(EirfType.FLOAT));
        assertEquals(4, EirfType.fixedSize(EirfType.SMALL_STRING));
        assertEquals(4, EirfType.fixedSize(EirfType.SMALL_BINARY));
        assertEquals(4, EirfType.fixedSize(EirfType.SMALL_UNION_ARRAY));
        assertEquals(4, EirfType.fixedSize(EirfType.SMALL_FIXED_ARRAY));
        assertEquals(4, EirfType.fixedSize(EirfType.SMALL_KEY_VALUE));
    }

    public void testFixedSizeEightByte() {
        assertEquals(8, EirfType.fixedSize(EirfType.LONG));
        assertEquals(8, EirfType.fixedSize(EirfType.DOUBLE));
        assertEquals(8, EirfType.fixedSize(EirfType.STRING));
        assertEquals(8, EirfType.fixedSize(EirfType.BINARY));
        assertEquals(8, EirfType.fixedSize(EirfType.UNION_ARRAY));
        assertEquals(8, EirfType.fixedSize(EirfType.FIXED_ARRAY));
        assertEquals(8, EirfType.fixedSize(EirfType.KEY_VALUE));
    }

    public void testIsSmallVariable() {
        assertTrue(EirfType.isSmallVariable(EirfType.SMALL_STRING));
        assertTrue(EirfType.isSmallVariable(EirfType.SMALL_BINARY));
        assertTrue(EirfType.isSmallVariable(EirfType.SMALL_UNION_ARRAY));
        assertTrue(EirfType.isSmallVariable(EirfType.SMALL_FIXED_ARRAY));
        assertTrue(EirfType.isSmallVariable(EirfType.SMALL_KEY_VALUE));
        assertFalse(EirfType.isSmallVariable(EirfType.STRING));
        assertFalse(EirfType.isSmallVariable(EirfType.INT));
        assertFalse(EirfType.isSmallVariable(EirfType.NULL));
    }

    public void testIsLargeVariable() {
        assertTrue(EirfType.isLargeVariable(EirfType.STRING));
        assertTrue(EirfType.isLargeVariable(EirfType.BINARY));
        assertTrue(EirfType.isLargeVariable(EirfType.UNION_ARRAY));
        assertTrue(EirfType.isLargeVariable(EirfType.FIXED_ARRAY));
        assertTrue(EirfType.isLargeVariable(EirfType.KEY_VALUE));
        assertFalse(EirfType.isLargeVariable(EirfType.SMALL_STRING));
        assertFalse(EirfType.isLargeVariable(EirfType.LONG));
    }

    public void testSmallToLargeAndBack() {
        assertEquals(EirfType.STRING, EirfType.smallToLarge(EirfType.SMALL_STRING));
        assertEquals(EirfType.BINARY, EirfType.smallToLarge(EirfType.SMALL_BINARY));
        assertEquals(EirfType.UNION_ARRAY, EirfType.smallToLarge(EirfType.SMALL_UNION_ARRAY));
        assertEquals(EirfType.FIXED_ARRAY, EirfType.smallToLarge(EirfType.SMALL_FIXED_ARRAY));
        assertEquals(EirfType.KEY_VALUE, EirfType.smallToLarge(EirfType.SMALL_KEY_VALUE));

        assertEquals(EirfType.SMALL_STRING, EirfType.largeToSmall(EirfType.STRING));
        assertEquals(EirfType.SMALL_BINARY, EirfType.largeToSmall(EirfType.BINARY));
        assertEquals(EirfType.SMALL_UNION_ARRAY, EirfType.largeToSmall(EirfType.UNION_ARRAY));
        assertEquals(EirfType.SMALL_FIXED_ARRAY, EirfType.largeToSmall(EirfType.FIXED_ARRAY));
        assertEquals(EirfType.SMALL_KEY_VALUE, EirfType.largeToSmall(EirfType.KEY_VALUE));
    }

    public void testNameForAllTypes() {
        assertEquals("NULL", EirfType.name(EirfType.NULL));
        assertEquals("TRUE", EirfType.name(EirfType.TRUE));
        assertEquals("FALSE", EirfType.name(EirfType.FALSE));
        assertEquals("INT", EirfType.name(EirfType.INT));
        assertEquals("FLOAT", EirfType.name(EirfType.FLOAT));
        assertEquals("SMALL_STRING", EirfType.name(EirfType.SMALL_STRING));
        assertEquals("LONG", EirfType.name(EirfType.LONG));
        assertEquals("DOUBLE", EirfType.name(EirfType.DOUBLE));
        assertEquals("STRING", EirfType.name(EirfType.STRING));
        assertEquals("UNION_ARRAY", EirfType.name(EirfType.UNION_ARRAY));
        assertEquals("FIXED_ARRAY", EirfType.name(EirfType.FIXED_ARRAY));
        assertEquals("KEY_VALUE", EirfType.name(EirfType.KEY_VALUE));
    }

    public void testNameForUnknownType() {
        String name = EirfType.name((byte) 0xFF);
        assertTrue(name.startsWith("UNKNOWN"));
    }
}
