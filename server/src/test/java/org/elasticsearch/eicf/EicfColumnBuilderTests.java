/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link EicfColumnBuilder} focused on kind selection, lazy materialisation of the
 * auxiliary fields (absent bitset / type vector / offset vector), and promotion to a union column.
 *
 * <p>The builder's output is inspected both structurally (which {@link EicfColumnData} fields are
 * {@code null}) and behaviourally (by reading values back through an {@link EicfColumn}).
 */
public class EicfColumnBuilderTests extends ESTestCase {

    private static EicfColumn read(EicfColumnData col) {
        return new EicfColumn(0, col.kind(), col.docCount(), col.absentBitset(), col.typeVector(), col.offsets(), col.data());
    }

    // -------------------------------------------------------------------------
    // Homogeneous columns materialise only the fields they need
    // -------------------------------------------------------------------------

    public void testLongColumnHasNoAuxiliaryFields() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addLong(1);
        b.addLong(2);
        b.addLong(3);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.LONG, col.kind());
        assertNull("no absent docs → no absent bitset", col.absentBitset());
        assertNull("homogeneous long → no type vector", col.typeVector());
        assertNull("fixed-width → no offset vector", col.offsets());
        assertEquals(3 * 8, col.data().length());

        EicfColumn r = read(col);
        assertEquals(EirfType.LONG, r.getTypeByte(0));
        assertEquals(1L, r.getLongValue(0));
        assertEquals(3L, r.getLongValue(2));
    }

    public void testDoubleColumn() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addDouble(1.5);
        b.addDouble(2.5);
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.DOUBLE, col.kind());
        assertNull(col.typeVector());
        assertNull(col.offsets());

        EicfColumn r = read(col);
        assertEquals(EirfType.DOUBLE, r.getTypeByte(0));
        assertEquals(2.5, r.getDoubleValue(1), 0.0);
    }

    public void testBoolColumnStoresValueBitsetAsData() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addBoolean(true);
        b.addBoolean(false);
        b.addBoolean(true);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.BOOL, col.kind());
        assertNull("bool value lives in the data bitset, not a type vector", col.typeVector());
        assertNull(col.offsets());
        assertEquals("value bitset is one 64-bit word", EicfColumnBuilder.bitsetBytes(3), col.data().length());

        EicfColumn r = read(col);
        assertEquals(EirfType.TRUE, r.getTypeByte(0));
        assertTrue(r.getBooleanValue(0));
        assertEquals(EirfType.FALSE, r.getTypeByte(1));
        assertFalse(r.getBooleanValue(1));
        assertTrue(r.getBooleanValue(2));
    }

    public void testStringColumnHasOffsetsButNoTypeVector() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addString(utf8("alpha"));
        b.addString(utf8("be"));
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.STRING, col.kind());
        assertNull(col.typeVector());
        assertNotNull("variable-length → offset vector", col.offsets());
        assertEquals((2 + 1) * 4, col.offsets().length());
        assertEquals("alpha".length() + "be".length(), col.data().length());

        EicfColumn r = read(col);
        assertEquals("alpha", r.getStringValue(0).string());
        assertEquals("be", r.getStringValue(1).string());
    }

    // -------------------------------------------------------------------------
    // Absent tracking
    // -------------------------------------------------------------------------

    public void testAbsentInMiddleCreatesBitset() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addLong(10);
        b.addAbsent();
        b.addLong(30);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.LONG, col.kind());
        assertNotNull("an absent doc → absent bitset", col.absentBitset());

        EicfColumn r = read(col);
        assertFalse(r.isAbsent(0));
        assertEquals(10L, r.getLongValue(0));
        assertTrue(r.isAbsent(1));
        assertEquals(EirfType.ABSENT, r.getTypeByte(1));
        assertFalse(r.isAbsent(2));
        assertEquals(30L, r.getLongValue(2));
    }

    public void testLeadingAbsentsBackfilledWhenFirstValueArrives() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addAbsent();
        b.addAbsent();
        b.addLong(99);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.LONG, col.kind());
        EicfColumn r = read(col);
        assertTrue(r.isAbsent(0));
        assertTrue(r.isAbsent(1));
        assertFalse(r.isAbsent(2));
        assertEquals(99L, r.getLongValue(2));
    }

    public void testAllAbsentDefaultsToLong() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addAbsent();
        b.addAbsent();
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.LONG, col.kind());
        assertNotNull(col.absentBitset());
        assertEquals(2 * 8, col.data().length());

        EicfColumn r = read(col);
        assertTrue(r.isAbsent(0));
        assertTrue(r.isAbsent(1));
        assertEquals(EirfType.ABSENT, r.getTypeByte(0));
    }

    // -------------------------------------------------------------------------
    // Promotion to union
    // -------------------------------------------------------------------------

    public void testLongThenDoublePromotesToUnion() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addLong(10);
        b.addDouble(2.5);
        b.addLong(30);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.UNION, col.kind());
        assertNotNull("union → per-doc type vector", col.typeVector());
        assertNotNull("union → offset vector", col.offsets());

        EicfColumn r = read(col);
        assertEquals(EirfType.LONG, r.getTypeByte(0));
        assertEquals(10L, r.getLongValue(0));
        assertEquals(EirfType.DOUBLE, r.getTypeByte(1));
        assertEquals(2.5, r.getDoubleValue(1), 0.0);
        assertEquals(EirfType.LONG, r.getTypeByte(2));
        assertEquals(30L, r.getLongValue(2));
    }

    public void testStringThenLongPromotesToUnion() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addString(utf8("hello"));
        b.addLong(42);
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.UNION, col.kind());
        EicfColumn r = read(col);
        assertEquals(EirfType.STRING, r.getTypeByte(0));
        assertEquals("hello", r.getStringValue(0).string());
        assertEquals(EirfType.LONG, r.getTypeByte(1));
        assertEquals(42L, r.getLongValue(1));
    }

    public void testBooleanThenStringPromotesToUnion() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addBoolean(true);
        b.addString(utf8("x"));
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.UNION, col.kind());
        EicfColumn r = read(col);
        assertEquals(EirfType.TRUE, r.getTypeByte(0));
        assertTrue(r.getBooleanValue(0));
        assertEquals(EirfType.STRING, r.getTypeByte(1));
        assertEquals("x", r.getStringValue(1).string());
    }

    public void testExplicitNullPromotesToUnion() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addLong(1);
        b.addNull();
        b.addLong(3);
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.UNION, col.kind());
        EicfColumn r = read(col);
        assertEquals(1L, r.getLongValue(0));
        assertTrue(r.isNull(1));
        assertEquals(EirfType.NULL, r.getTypeByte(1));
        assertFalse(r.isAbsent(1)); // an explicit null is present, not absent
        assertEquals(3L, r.getLongValue(2));
    }

    public void testLeadingNullStartsUnion() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addNull();
        b.addLong(7);
        EicfColumnData col = b.finish(2);

        assertEquals(EicfColumnKind.UNION, col.kind());
        EicfColumn r = read(col);
        assertTrue(r.isNull(0));
        assertEquals(7L, r.getLongValue(1));
    }

    public void testPromotionPreservesAbsentRows() {
        EicfColumnBuilder b = new EicfColumnBuilder();
        b.addLong(1);
        b.addAbsent();
        b.addDouble(2.5); // promotes; the absent row must be replayed into the union
        EicfColumnData col = b.finish(3);

        assertEquals(EicfColumnKind.UNION, col.kind());
        EicfColumn r = read(col);
        assertEquals(1L, r.getLongValue(0));
        assertTrue(r.isAbsent(1));
        assertEquals(EirfType.ABSENT, r.getTypeByte(1));
        assertEquals(2.5, r.getDoubleValue(2), 0.0);
    }

    private static org.elasticsearch.xcontent.XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new org.elasticsearch.xcontent.XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }
}
