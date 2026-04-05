/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

/**
 * Type byte constants for the Elastic Internal Row Format (EIRF).
 *
 * <p>Fixed size derivation for SIMD offset scanning:
 * <pre>
 * type &lt;= 0x02  →  0 bytes  (NULL, TRUE, FALSE)
 * type &lt;= 0x09  →  4 bytes  (INT, FLOAT, SMALL_*)
 * type &gt;= 0x0A  →  8 bytes  (LONG, DOUBLE, STRING, BINARY, *_ARRAY, KEY_VALUE)
 * </pre>
 */
public final class EirfType {

    // 0-byte fixed types
    public static final byte NULL = 0x00;
    public static final byte TRUE = 0x01;
    public static final byte FALSE = 0x02;

    // 4-byte fixed types
    public static final byte INT = 0x03;
    public static final byte FLOAT = 0x04;
    public static final byte SMALL_STRING = 0x05;
    public static final byte SMALL_BINARY = 0x06;
    public static final byte SMALL_UNION_ARRAY = 0x07;
    public static final byte SMALL_FIXED_ARRAY = 0x08;
    public static final byte SMALL_KEY_VALUE = 0x09;

    // 8-byte fixed types
    public static final byte LONG = 0x0A;
    public static final byte DOUBLE = 0x0B;
    public static final byte STRING = 0x0C;
    public static final byte BINARY = 0x0D;
    public static final byte UNION_ARRAY = 0x0E;
    public static final byte FIXED_ARRAY = 0x0F;
    public static final byte KEY_VALUE = 0x10;

    /** Threshold for using small variable-length variants (var section must be under this). */
    public static final int SMALL_VAR_THRESHOLD = 65536;

    private EirfType() {}

    /**
     * Fixed-section size in bytes for the given type byte.
     */
    public static int fixedSize(byte typeByte) {
        if (typeByte <= FALSE) return 0;
        if (typeByte <= SMALL_KEY_VALUE) return 4;
        return 8;
    }

    /**
     * Returns true if this type has a variable-length payload (small variant, 4-byte fixed entry).
     */
    public static boolean isSmallVariable(byte typeByte) {
        return typeByte >= SMALL_STRING && typeByte <= SMALL_KEY_VALUE;
    }

    /**
     * Returns true if this type has a variable-length payload (large variant, 8-byte fixed entry).
     */
    public static boolean isLargeVariable(byte typeByte) {
        return typeByte >= STRING && typeByte <= KEY_VALUE;
    }

    /**
     * Returns true if this type has a variable-length payload (either small or large variant).
     */
    public static boolean isVariable(byte typeByte) {
        return isSmallVariable(typeByte) || isLargeVariable(typeByte);
    }

    /**
     * Converts a large variable type to its small variant.
     * STRING→SMALL_STRING, BINARY→SMALL_BINARY, etc.
     */
    public static byte largeToSmall(byte typeByte) {
        return (byte) (typeByte - 7);
    }

    /**
     * Converts a small variable type to its large variant.
     * SMALL_STRING→STRING, SMALL_BINARY→BINARY, etc.
     */
    public static byte smallToLarge(byte typeByte) {
        return (byte) (typeByte + 7);
    }

    /**
     * Returns the data size of this type in element position (inside arrays and KEY_VALUE values).
     * Variable-length types (STRING, KEY_VALUE, arrays) use a 4-byte length prefix in element position.
     * Returns -1 for variable-length types (caller must read the 4-byte length).
     */
    public static int elemDataSize(byte typeByte) {
        return switch (typeByte) {
            case NULL, TRUE, FALSE -> 0;
            case INT, FLOAT -> 4;
            case LONG, DOUBLE -> 8;
            default -> -1; // STRING, KEY_VALUE, UNION_ARRAY, FIXED_ARRAY: length-prefixed
        };
    }

    /**
     * Returns true if this type is a compound type (KEY_VALUE or array).
     */
    public static boolean isCompound(byte typeByte) {
        return typeByte == KEY_VALUE
            || typeByte == UNION_ARRAY
            || typeByte == FIXED_ARRAY
            || typeByte == SMALL_KEY_VALUE
            || typeByte == SMALL_UNION_ARRAY
            || typeByte == SMALL_FIXED_ARRAY;
    }

    public static String name(byte typeByte) {
        return switch (typeByte) {
            case NULL -> "NULL";
            case TRUE -> "TRUE";
            case FALSE -> "FALSE";
            case INT -> "INT";
            case FLOAT -> "FLOAT";
            case SMALL_STRING -> "SMALL_STRING";
            case SMALL_BINARY -> "SMALL_BINARY";
            case SMALL_UNION_ARRAY -> "SMALL_UNION_ARRAY";
            case SMALL_FIXED_ARRAY -> "SMALL_FIXED_ARRAY";
            case SMALL_KEY_VALUE -> "SMALL_KEY_VALUE";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case UNION_ARRAY -> "UNION_ARRAY";
            case FIXED_ARRAY -> "FIXED_ARRAY";
            case KEY_VALUE -> "KEY_VALUE";
            default -> "UNKNOWN(0x" + Integer.toHexString(typeByte & 0xFF) + ")";
        };
    }
}
