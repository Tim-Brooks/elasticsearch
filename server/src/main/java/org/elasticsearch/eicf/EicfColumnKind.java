/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

/**
 * Column kind constants for the Elastic Internal Column Format (EICF).
 *
 * <p>Each leaf column in an EICF batch is stored as a typed vector plus an absent bitset.
 * The kind byte determines the physical layout of the column data blob and is persisted
 * in the column index section of the batch header.
 *
 * <p>Binary layout per kind:
 * <pre>
 * LONG/DOUBLE:    absent_bitset | values[docCount * 8]
 * BOOL:           absent_bitset | value_bitset
 * STRING/BINARY:  absent_bitset | offsets[(docCount+1) * 4] | bytes
 * ARRAY:          absent_bitset | type_vec[docCount] | offsets[(docCount+1) * 4] | packed_bytes
 * NUMERIC_UNION:  absent_bitset | is_decimal_bitset | values[docCount * 8]
 * UNION:          absent_bitset | type_vec[docCount] | offsets[(docCount+1) * 4] | dense_values
 * </pre>
 * where {@code absent_bitset = ((docCount + 63) / 64) * 8} bytes (LE longs, bit set = absent).
 */
public final class EicfColumnKind {

    /** Sentinel used only during building — never written to disk. */
    static final byte NONE = 0x00;

    /**
     * All values are {@code long}s. JSON ints and longs are both upcast to 64-bit.
     * Layout: {@code absent_bitset | values[docCount * 8]} (LE longs).
     */
    public static final byte LONG = 0x01;

    /**
     * All values are {@code double}s. JSON floats and doubles are both upcast to 64-bit.
     * Layout: {@code absent_bitset | values[docCount * 8]} (LE raw bits).
     */
    public static final byte DOUBLE = 0x02;

    /**
     * All values are booleans.
     * Layout: {@code absent_bitset | value_bitset} (bit set = {@code true}).
     */
    public static final byte BOOL = 0x03;

    /**
     * All values are UTF-8 strings.
     * Layout: {@code absent_bitset | offsets[(docCount+1) * 4] | utf8_bytes} (LE i32 offsets).
     */
    public static final byte STRING = 0x04;

    /**
     * All values are raw binary bytes.
     * Layout: {@code absent_bitset | offsets[(docCount+1) * 4] | bytes} (LE i32 offsets).
     */
    public static final byte BINARY = 0x05;

    /**
     * All values are arrays (each row may be a FIXED_ARRAY or UNION_ARRAY).
     * Layout: {@code absent_bitset | type_vec[docCount] | offsets[(docCount+1) * 4] | packed_bytes}.
     * The type_vec carries the EirfType array kind ({@code FIXED_ARRAY} or {@code UNION_ARRAY})
     * per row. Packed bytes are read via {@link org.elasticsearch.eirf.EirfArrayReader}.
     */
    public static final byte ARRAY = 0x06;

    /**
     * A mix of long and double values. No variable-length offsets are needed — each slot is a
     * fixed 8 bytes — with an is-decimal bitset distinguishing longs from doubles.
     * Layout: {@code absent_bitset | is_decimal_bitset | values[docCount * 8]}.
     */
    public static final byte NUMERIC_UNION = 0x07;

    /**
     * A heterogeneous column. A per-row type vector determines the EirfType for each row, and a
     * dense value buffer holds the payload. Handles any type combination including explicit null.
     * Layout: {@code absent_bitset | type_vec[docCount] | offsets[(docCount+1) * 4] | dense_values}.
     * Zero-byte types (ABSENT, NULL, TRUE, FALSE) contribute 0 bytes to {@code dense_values}.
     * Fixed-size numerics (LONG, DOUBLE) contribute 8 bytes. Variable types contribute raw bytes
     * (length determined by the offset delta).
     */
    public static final byte UNION = 0x08;

    private EicfColumnKind() {}

    /** Returns a debug name for the given kind byte. */
    public static String name(byte kind) {
        return switch (kind) {
            case NONE -> "NONE";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case BOOL -> "BOOL";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case ARRAY -> "ARRAY";
            case NUMERIC_UNION -> "NUMERIC_UNION";
            case UNION -> "UNION";
            default -> "UNKNOWN(0x" + Integer.toHexString(kind & 0xFF) + ")";
        };
    }
}
