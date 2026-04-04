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
 * type &gt;= 0x0A  →  8 bytes  (LONG, DOUBLE, STRING, BINARY, *_ARRAY, XCONTENT)
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
    public static final byte SMALL_XCONTENT = 0x09;

    // 8-byte fixed types
    public static final byte LONG = 0x0A;
    public static final byte DOUBLE = 0x0B;
    public static final byte STRING = 0x0C;
    public static final byte BINARY = 0x0D;
    public static final byte UNION_ARRAY = 0x0E;
    public static final byte FIXED_ARRAY = 0x0F;
    public static final byte XCONTENT = 0x10;

    /** Maximum number of leaf elements in a compact typed array. */
    public static final int MAX_SMALL_ARRAY_SIZE = 32;

    /** Threshold for using small variable-length variants (var section must be under this). */
    public static final int SMALL_VAR_THRESHOLD = 65536;

    private EirfType() {}

    /**
     * Fixed-section size in bytes for the given type byte.
     */
    public static int fixedSize(byte typeByte) {
        if (typeByte <= FALSE) return 0;
        if (typeByte <= SMALL_XCONTENT) return 4;
        return 8;
    }

    /**
     * Returns true if this type has a variable-length payload (small variant, 4-byte fixed entry).
     */
    public static boolean isSmallVariable(byte typeByte) {
        return typeByte >= SMALL_STRING && typeByte <= SMALL_XCONTENT;
    }

    /**
     * Returns true if this type has a variable-length payload (large variant, 8-byte fixed entry).
     */
    public static boolean isLargeVariable(byte typeByte) {
        return typeByte >= STRING && typeByte <= XCONTENT;
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
            case SMALL_XCONTENT -> "SMALL_XCONTENT";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case UNION_ARRAY -> "UNION_ARRAY";
            case FIXED_ARRAY -> "FIXED_ARRAY";
            case XCONTENT -> "XCONTENT";
            default -> "UNKNOWN(0x" + Integer.toHexString(typeByte & 0xFF) + ")";
        };
    }
}
