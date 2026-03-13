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
    public static final byte XCONTENT_ARRAY = 0x08;

    /** Maximum number of leaf elements in a small (typed) array. */
    public static final int MAX_SMALL_ARRAY_SIZE = 32;

    /**
     * Fixed-section size in bytes for each type.
     * NULL/TRUE/FALSE = 0 (value is implicit in the type byte).
     * LONG/DOUBLE = 8 (raw value).
     * STRING/BINARY/ARRAY/XCONTENT_ARRAY = 8 (offset:u32 + length:u32 pair referencing var section).
     */
    private static final int[] FIXED_SIZE = { 0, 0, 0, 8, 8, 8, 8, 8, 8 };

    private RowType() {}

    public static int fixedSize(byte typeByte) {
        return FIXED_SIZE[typeByte];
    }

    public static boolean isVariable(byte typeByte) {
        return typeByte == STRING || typeByte == BINARY || typeByte == ARRAY || typeByte == XCONTENT_ARRAY;
    }

    public static String name(byte typeByte) {
        return switch (typeByte) {
            case NULL -> "NULL";
            case TRUE -> "TRUE";
            case FALSE -> "FALSE";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case ARRAY -> "ARRAY";
            case XCONTENT_ARRAY -> "XCONTENT_ARRAY";
            default -> "UNKNOWN(0x" + Integer.toHexString(typeByte & 0xFF) + ")";
        };
    }
}
