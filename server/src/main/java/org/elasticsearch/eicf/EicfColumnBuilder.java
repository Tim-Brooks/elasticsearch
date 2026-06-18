/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.eirf.EirfType;

import java.util.Arrays;

/**
 * Accumulates per-document values for a single leaf column and serialises them into a typed
 * column blob when {@link #finish(int)} is called.
 *
 * <p>Type promotion is resolved lazily at {@code finish()} time by scanning the per-doc type
 * bytes. Promotion rules:
 * <ul>
 *   <li>All long → {@link EicfColumnKind#LONG}</li>
 *   <li>All double → {@link EicfColumnKind#DOUBLE}</li>
 *   <li>Long + double mix → {@link EicfColumnKind#NUMERIC_UNION}</li>
 *   <li>All boolean → {@link EicfColumnKind#BOOL}</li>
 *   <li>All string → {@link EicfColumnKind#STRING}</li>
 *   <li>All binary → {@link EicfColumnKind#BINARY}</li>
 *   <li>All array → {@link EicfColumnKind#ARRAY}</li>
 *   <li>Any explicit null, or any other type mix → {@link EicfColumnKind#UNION}</li>
 * </ul>
 * Absent (missing) values do not affect kind selection. A column whose every doc is absent
 * defaults to {@link EicfColumnKind#LONG}.
 *
 * <p>Usage: call one of the {@code add*} methods exactly once per document in document order.
 * Call {@link #addAbsent()} for documents where this column is not present.
 */
final class EicfColumnBuilder {

    private static final int INITIAL_CAPACITY = 16;

    /** EirfType byte per document (ABSENT = 0 by default). */
    private byte[] typeBytes;
    /** Raw 8-byte values for LONG (raw long) and DOUBLE (raw double bits). */
    private long[] numerics;
    /**
     * Variable-length payloads: UTF-8 bytes for STRING, raw bytes for BINARY,
     * packed array bytes for FIXED_ARRAY/UNION_ARRAY. {@code null} for fixed-size types.
     */
    private byte[][] varBytes;
    /** Number of {@code add*} calls made so far (== current doc index). */
    private int count;

    EicfColumnBuilder() {
        typeBytes = new byte[INITIAL_CAPACITY]; // zero = ABSENT
        numerics = new long[INITIAL_CAPACITY];
        varBytes = new byte[INITIAL_CAPACITY][];
    }

    // -------------------------------------------------------------------------
    // Per-doc setters
    // -------------------------------------------------------------------------

    void addAbsent() {
        ensureCapacity();
        // typeBytes[count] stays 0 (ABSENT)
        count++;
    }

    void addLong(long val) {
        ensureCapacity();
        typeBytes[count] = EirfType.LONG;
        numerics[count] = val;
        count++;
    }

    void addDouble(double val) {
        ensureCapacity();
        typeBytes[count] = EirfType.DOUBLE;
        numerics[count] = Double.doubleToRawLongBits(val);
        count++;
    }

    void addBoolean(boolean val) {
        ensureCapacity();
        typeBytes[count] = val ? EirfType.TRUE : EirfType.FALSE;
        count++;
    }

    void addNull() {
        ensureCapacity();
        typeBytes[count] = EirfType.NULL;
        count++;
    }

    /**
     * Adds a UTF-8 string value. The provided byte array is stored directly; callers must ensure
     * it is not mutated after this call.
     */
    void addString(byte[] utf8) {
        ensureCapacity();
        typeBytes[count] = EirfType.STRING;
        varBytes[count] = utf8;
        count++;
    }

    /**
     * Adds a raw binary value. The provided byte array is stored directly; callers must ensure
     * it is not mutated after this call.
     */
    void addBinary(byte[] bytes) {
        ensureCapacity();
        typeBytes[count] = EirfType.BINARY;
        varBytes[count] = bytes;
        count++;
    }

    /**
     * Adds an array value. {@code arrayType} must be {@code EirfType.FIXED_ARRAY} or
     * {@code EirfType.UNION_ARRAY}. The {@code packed} byte array is stored directly; callers
     * must ensure it is not mutated after this call.
     */
    void addArray(byte arrayType, byte[] packed) {
        assert arrayType == EirfType.FIXED_ARRAY || arrayType == EirfType.UNION_ARRAY : "arrayType must be FIXED_ARRAY or UNION_ARRAY";
        ensureCapacity();
        typeBytes[count] = arrayType;
        varBytes[count] = packed;
        count++;
    }

    // -------------------------------------------------------------------------
    // Serialisation
    // -------------------------------------------------------------------------

    /**
     * Determines the column kind from the accumulated type bytes and serialises the column blob.
     *
     * @param docCount total number of documents in the batch (must equal {@link #count})
     * @return the kind byte (see {@link EicfColumnKind}) in index 0 of the result, immediately
     *         followed by the column blob bytes starting at index 1
     */
    byte[] finish(int docCount) {
        assert count == docCount : "builder count " + count + " != docCount " + docCount;
        byte kind = determineKind(docCount);
        byte[] blob = buildBlob(kind, docCount);
        // Prepend kind byte so callers can read it from index 0
        byte[] result = new byte[1 + blob.length];
        result[0] = kind;
        System.arraycopy(blob, 0, result, 1, blob.length);
        return result;
    }

    private byte determineKind(int docCount) {
        byte kind = EicfColumnKind.NONE;
        for (int d = 0; d < docCount; d++) {
            byte t = typeBytes[d];
            if (t == EirfType.ABSENT) {
                continue;
            }
            if (t == EirfType.NULL) {
                return EicfColumnKind.UNION;
            }
            byte valueKind = kindForType(t);
            if (kind == EicfColumnKind.NONE) {
                kind = valueKind;
            } else if (kind != valueKind) {
                // Numeric promotion: long + double → numeric union
                if (isNumeric(kind) && isNumeric(valueKind)) {
                    kind = EicfColumnKind.NUMERIC_UNION;
                } else if (kind == EicfColumnKind.NUMERIC_UNION && isNumeric(valueKind)) {
                    // already numeric union; stay
                } else {
                    return EicfColumnKind.UNION;
                }
            }
        }
        return kind == EicfColumnKind.NONE ? EicfColumnKind.LONG : kind;
    }

    private static byte kindForType(byte t) {
        return switch (t) {
            case EirfType.LONG -> EicfColumnKind.LONG;
            case EirfType.DOUBLE -> EicfColumnKind.DOUBLE;
            case EirfType.TRUE, EirfType.FALSE -> EicfColumnKind.BOOL;
            case EirfType.STRING -> EicfColumnKind.STRING;
            case EirfType.BINARY -> EicfColumnKind.BINARY;
            case EirfType.FIXED_ARRAY, EirfType.UNION_ARRAY -> EicfColumnKind.ARRAY;
            default -> EicfColumnKind.UNION;
        };
    }

    private static boolean isNumeric(byte kind) {
        return kind == EicfColumnKind.LONG || kind == EicfColumnKind.DOUBLE || kind == EicfColumnKind.NUMERIC_UNION;
    }

    // -------------------------------------------------------------------------
    // Blob builders per kind
    // -------------------------------------------------------------------------

    private byte[] buildBlob(byte kind, int docCount) {
        return switch (kind) {
            case EicfColumnKind.LONG, EicfColumnKind.DOUBLE -> buildLongOrDoubleBlob(docCount);
            case EicfColumnKind.BOOL -> buildBoolBlob(docCount);
            case EicfColumnKind.STRING, EicfColumnKind.BINARY -> buildStringOrBinaryBlob(docCount);
            case EicfColumnKind.ARRAY -> buildArrayBlob(docCount);
            case EicfColumnKind.NUMERIC_UNION -> buildNumericUnionBlob(docCount);
            case EicfColumnKind.UNION -> buildUnionBlob(docCount);
            default -> throw new IllegalStateException("Unknown kind: " + EicfColumnKind.name(kind));
        };
    }

    /** LONG / DOUBLE: {@code absent_bitset | values[docCount * 8]}. */
    private byte[] buildLongOrDoubleBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        byte[] out = new byte[bsBytes + docCount * 8];
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] == EirfType.ABSENT) {
                setBit(out, 0, d);
            } else {
                ByteUtils.writeLongLE(numerics[d], out, bsBytes + d * 8);
            }
        }
        return out;
    }

    /** BOOL: {@code absent_bitset | value_bitset}. */
    private byte[] buildBoolBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        byte[] out = new byte[2 * bsBytes];
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] == EirfType.ABSENT) {
                setBit(out, 0, d);
            } else if (typeBytes[d] == EirfType.TRUE) {
                setBit(out, bsBytes, d);
            }
        }
        return out;
    }

    /** STRING / BINARY: {@code absent_bitset | offsets[(docCount+1)*4] | bytes}. */
    private byte[] buildStringOrBinaryBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        int dataLen = 0;
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] != EirfType.ABSENT && varBytes[d] != null) {
                dataLen += varBytes[d].length;
            }
        }
        int offsetsSize = (docCount + 1) * 4;
        byte[] out = new byte[bsBytes + offsetsSize + dataLen];
        int cumOffset = 0;
        int writePos = bsBytes + offsetsSize;
        ByteUtils.writeIntLE(0, out, bsBytes);
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] == EirfType.ABSENT) {
                setBit(out, 0, d);
                // offset unchanged
            } else {
                byte[] vb = varBytes[d];
                int len = vb != null ? vb.length : 0;
                if (len > 0) {
                    System.arraycopy(vb, 0, out, writePos, len);
                    writePos += len;
                    cumOffset += len;
                }
            }
            ByteUtils.writeIntLE(cumOffset, out, bsBytes + (d + 1) * 4);
        }
        return out;
    }

    /**
     * ARRAY: {@code absent_bitset | type_vec[docCount] | offsets[(docCount+1)*4] | packed_bytes}.
     */
    private byte[] buildArrayBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        int dataLen = 0;
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] != EirfType.ABSENT && varBytes[d] != null) {
                dataLen += varBytes[d].length;
            }
        }
        int typeVecSize = docCount;
        int offsetsSize = (docCount + 1) * 4;
        byte[] out = new byte[bsBytes + typeVecSize + offsetsSize + dataLen];
        int typeVecOffset = bsBytes;
        int offsetsStart = typeVecOffset + typeVecSize;
        int cumOffset = 0;
        int writePos = offsetsStart + offsetsSize;
        ByteUtils.writeIntLE(0, out, offsetsStart);
        for (int d = 0; d < docCount; d++) {
            if (typeBytes[d] == EirfType.ABSENT) {
                setBit(out, 0, d);
                // typeVec[d] stays 0; offset unchanged
            } else {
                out[typeVecOffset + d] = typeBytes[d]; // FIXED_ARRAY or UNION_ARRAY
                byte[] vb = varBytes[d];
                int len = vb != null ? vb.length : 0;
                if (len > 0) {
                    System.arraycopy(vb, 0, out, writePos, len);
                    writePos += len;
                    cumOffset += len;
                }
            }
            ByteUtils.writeIntLE(cumOffset, out, offsetsStart + (d + 1) * 4);
        }
        return out;
    }

    /**
     * NUMERIC_UNION: {@code absent_bitset | is_decimal_bitset | values[docCount * 8]}.
     * The is-decimal bit is set when the value for that row is a double.
     */
    private byte[] buildNumericUnionBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        byte[] out = new byte[2 * bsBytes + docCount * 8];
        for (int d = 0; d < docCount; d++) {
            byte t = typeBytes[d];
            if (t == EirfType.ABSENT) {
                setBit(out, 0, d);
            } else {
                if (t == EirfType.DOUBLE) {
                    setBit(out, bsBytes, d);
                }
                ByteUtils.writeLongLE(numerics[d], out, 2 * bsBytes + d * 8);
            }
        }
        return out;
    }

    /**
     * UNION: {@code absent_bitset | type_vec[docCount] | offsets[(docCount+1)*4] | dense_values}.
     * Dense values: 0 bytes for ABSENT/NULL/TRUE/FALSE, 8 bytes for LONG/DOUBLE,
     * raw bytes for STRING/BINARY/arrays.
     */
    private byte[] buildUnionBlob(int docCount) {
        int bsBytes = bitsetBytes(docCount);
        // Compute total dense data size
        int dataLen = 0;
        for (int d = 0; d < docCount; d++) {
            dataLen += unionValueSize(d);
        }
        int typeVecSize = docCount;
        int offsetsSize = (docCount + 1) * 4;
        byte[] out = new byte[bsBytes + typeVecSize + offsetsSize + dataLen];
        int typeVecOffset = bsBytes;
        int offsetsStart = typeVecOffset + typeVecSize;
        int denseStart = offsetsStart + offsetsSize;
        int cumOffset = 0;
        int writePos = denseStart;
        ByteUtils.writeIntLE(0, out, offsetsStart);
        for (int d = 0; d < docCount; d++) {
            byte t = typeBytes[d];
            // Write absent bitset even for union (consistent across all kinds)
            if (t == EirfType.ABSENT) {
                setBit(out, 0, d);
            }
            out[typeVecOffset + d] = t;
            int size = unionValueSize(d);
            if (size == 8) {
                ByteUtils.writeLongLE(numerics[d], out, writePos);
                writePos += 8;
                cumOffset += 8;
            } else if (size > 0) {
                byte[] vb = varBytes[d];
                System.arraycopy(vb, 0, out, writePos, size);
                writePos += size;
                cumOffset += size;
            }
            ByteUtils.writeIntLE(cumOffset, out, offsetsStart + (d + 1) * 4);
        }
        return out;
    }

    private int unionValueSize(int d) {
        return switch (typeBytes[d]) {
            case EirfType.ABSENT, EirfType.NULL, EirfType.TRUE, EirfType.FALSE -> 0;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            default -> varBytes[d] != null ? varBytes[d].length : 0;
        };
    }

    /** Size of a bitset in bytes for {@code docCount} bits. */
    static int bitsetBytes(int docCount) {
        return ((docCount + 63) / 64) * 8;
    }

    /** Sets bit {@code d} in a bitset stored at {@code bitsetOffset} within {@code buf}. */
    static void setBit(byte[] buf, int bitsetOffset, int d) {
        int wordIdx = d / 64;
        int bitIdx = d & 63;
        // LE long: bit bitIdx is in byte wordIdx*8 + bitIdx/8, at position bitIdx%8
        int bytePos = bitsetOffset + wordIdx * 8 + bitIdx / 8;
        buf[bytePos] |= (byte) (1 << (bitIdx & 7));
    }

    // -------------------------------------------------------------------------
    // Capacity management
    // -------------------------------------------------------------------------

    private void ensureCapacity() {
        if (count >= typeBytes.length) {
            int newCap = typeBytes.length * 2;
            typeBytes = Arrays.copyOf(typeBytes, newCap);
            numerics = Arrays.copyOf(numerics, newCap);
            varBytes = Arrays.copyOf(varBytes, newCap);
        }
    }
}
