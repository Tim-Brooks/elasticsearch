/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.eirf.SourceBatches;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Round-trip tests for the EICF serialized backing path: an in-memory batch built by
 * {@link EicfEncoder} is serialized via {@link EicfBatch#data()}, reconstructed through
 * {@link SourceBatches#fromBytes}, and the reconstructed batch is asserted to read identically to
 * the in-memory one for every column and document.
 */
public class EicfSerializationTests extends ESTestCase {

    /**
     * Documents exercising every column kind plus absent and explicit-null rows:
     * {@code n}=LONG, {@code f}=DOUBLE, {@code b}=BOOL, {@code s}=STRING, {@code arr}=ARRAY,
     * {@code u}=UNION (string+long), {@code mix}=UNION (long+double), {@code opt}=LONG with an
     * absent row, {@code nul}=UNION with an explicit null.
     */
    private static List<BytesReference> allKindsDocs() {
        return List.of(
            new BytesArray("{\"n\":10,\"f\":1.5,\"b\":true,\"s\":\"hello\",\"arr\":[1,2],\"u\":\"word\",\"mix\":1,\"opt\":5,\"nul\":1}"),
            new BytesArray("{\"n\":20,\"f\":2.5,\"b\":false,\"s\":\"world\",\"arr\":[3],\"u\":42,\"mix\":2.5,\"nul\":null}"),
            new BytesArray("{\"n\":30,\"f\":3.5,\"b\":true,\"s\":\"end\",\"arr\":[4,5,6],\"u\":\"last\",\"mix\":7,\"opt\":9,\"nul\":3}")
        );
    }

    public void testRoundTripAllKinds() throws IOException {
        try (EicfBatch inMemory = EicfEncoder.encode(allKindsDocs(), XContentType.JSON)) {
            BytesReference bytes = inMemory.data();
            try (SourceBatch reconstructed = SourceBatches.fromBytes(bytes, () -> {})) {
                assertTrue("magic must route to EICF", reconstructed instanceof EicfBatch);
                assertBatchesEqual(inMemory, reconstructed);
            }
        }
    }

    public void testReSerializeIsStable() throws IOException {
        // Serializing, reconstructing, and re-serializing must yield byte-identical output.
        try (EicfBatch inMemory = EicfEncoder.encode(allKindsDocs(), XContentType.JSON)) {
            BytesReference first = inMemory.data();
            try (SourceBatch reconstructed = SourceBatches.fromBytes(first, () -> {})) {
                BytesReference second = reconstructed.data();
                assertEquals(first, second);
            }
        }
    }

    public void testRoundTripEmptyBatch() throws IOException {
        try (EicfBatch inMemory = EicfEncoder.encode(List.of(), XContentType.JSON)) {
            try (SourceBatch reconstructed = SourceBatches.fromBytes(inMemory.data(), () -> {})) {
                assertEquals(0, reconstructed.docCount());
                assertEquals(0, reconstructed.columnCount());
            }
        }
    }

    public void testRoundTripAfterSlice() throws IOException {
        try (EicfBatch parent = EicfEncoder.encode(allKindsDocs(), XContentType.JSON)) {
            SourceBatch sliced = parent.slice(1, 3); // an in-memory batch produced by slicing
            BytesReference bytes = sliced.data();
            try (SourceBatch reconstructed = SourceBatches.fromBytes(bytes, () -> {})) {
                assertBatchesEqual(sliced, reconstructed);
            }
            sliced.close();
        }
    }

    // -------------------------------------------------------------------------
    // Comparison helpers
    // -------------------------------------------------------------------------

    private static void assertBatchesEqual(SourceBatch expected, SourceBatch actual) {
        assertEquals("docCount", expected.docCount(), actual.docCount());
        assertEquals("columnCount", expected.columnCount(), actual.columnCount());
        assertEquals("nonLeafCount", expected.schema().nonLeafCount(), actual.schema().nonLeafCount());
        for (int c = 0; c < expected.columnCount(); c++) {
            assertEquals("leaf path " + c, expected.schema().getFullPath(c), actual.schema().getFullPath(c));
        }

        for (int d = 0; d < expected.docCount(); d++) {
            SourceRow e = expected.row(d);
            SourceRow a = actual.row(d);
            for (int c = 0; c < expected.columnCount(); c++) {
                String at = " at col=" + c + " doc=" + d;
                assertEquals("isAbsent" + at, e.isAbsent(c), a.isAbsent(c));
                assertEquals("isNull" + at, e.isNull(c), a.isNull(c));
                byte type = e.getTypeByte(c);
                assertEquals("type" + at, type, a.getTypeByte(c));
                if (e.isAbsent(c)) {
                    continue;
                }
                switch (type) {
                    case EirfType.LONG -> assertEquals("long" + at, e.getLongValue(c), a.getLongValue(c));
                    case EirfType.DOUBLE -> assertEquals("double" + at, e.getDoubleValue(c), a.getDoubleValue(c), 0.0);
                    case EirfType.STRING -> assertEquals("string" + at, e.getStringValue(c).string(), a.getStringValue(c).string());
                    case EirfType.TRUE, EirfType.FALSE -> assertEquals("bool" + at, e.getBooleanValue(c), a.getBooleanValue(c));
                    case EirfType.FIXED_ARRAY, EirfType.UNION_ARRAY -> assertArraysEqual(e.getArrayValue(c), a.getArrayValue(c), at);
                    case EirfType.NULL -> {
                    } // no payload
                    default -> throw new AssertionError("unexpected type " + EirfType.name(type) + at);
                }
            }
        }
    }

    private static void assertArraysEqual(EirfArrayReader expected, EirfArrayReader actual, String at) {
        while (expected.next()) {
            assertTrue("actual array ended early" + at, actual.next());
            byte type = expected.type();
            assertEquals("array element type" + at, type, actual.type());
            switch (type) {
                case EirfType.INT -> assertEquals("array int" + at, expected.intValue(), actual.intValue());
                case EirfType.STRING -> assertEquals("array string" + at, expected.stringValue(), actual.stringValue());
                default -> {
                } // other element types are not produced by this test's data
            }
        }
        assertFalse("actual array has extra elements" + at, actual.next());
    }
}
