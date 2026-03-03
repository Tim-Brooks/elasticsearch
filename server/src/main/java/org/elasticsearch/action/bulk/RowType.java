/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

/**
 * Type byte constants for the row-oriented document batch format.
 * Not an enum because the {@link #OBJECT_FLAG} composes with any base type via bitwise OR.
 */
public final class RowType {

    public static final byte NULL = 0x00;
    public static final byte TRUE = 0x01;
    public static final byte FALSE = 0x02;
    public static final byte LONG = 0x03;
    public static final byte DOUBLE = 0x04;
    public static final byte STRING = 0x05;
    public static final byte BINARY = 0x06;
    public static final byte ARRAY = 0x07;

    /** Set when the field came from a nested JSON object (dot-path flattened). */
    public static final byte OBJECT_FLAG = (byte) 0x80;

    /**
     * Fixed-section size in bytes for each base type.
     * NULL/TRUE/FALSE = 0 (value is implicit in the type byte).
     * LONG/DOUBLE = 8 (raw value).
     * STRING/BINARY/ARRAY = 8 (offset:u32 + length:u32 pair referencing var section).
     */
    private static final int[] FIXED_SIZE = { 0, 0, 0, 8, 8, 8, 8, 8 };

    private RowType() {}

    public static byte baseType(byte typeByte) {
        return (byte) (typeByte & 0x7F);
    }

    public static boolean isFromObject(byte typeByte) {
        return (typeByte & OBJECT_FLAG) != 0;
    }

    public static int fixedSize(byte typeByte) {
        return FIXED_SIZE[baseType(typeByte)];
    }

    public static boolean isVariable(byte typeByte) {
        byte base = baseType(typeByte);
        return base == STRING || base == BINARY || base == ARRAY;
    }

    public static String name(byte typeByte) {
        String baseName = switch (baseType(typeByte)) {
            case NULL -> "NULL";
            case TRUE -> "TRUE";
            case FALSE -> "FALSE";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case ARRAY -> "ARRAY";
            default -> "UNKNOWN(0x" + Integer.toHexString(baseType(typeByte) & 0xFF) + ")";
        };
        return isFromObject(typeByte) ? baseName + "|OBJECT" : baseName;
    }
}
