/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;

/**
 * A forward-only reader over a array in EIRF format.
 *
 * <p>Two formats (both byte-length-terminated, no element count):
 * <ul>
 *   <li><b>Union:</b> per element: type(1) + data</li>
 *   <li><b>Fixed:</b> element_type(1) + per element: data only</li>
 * </ul>
 *
 * <p>Element data sizes: INT/FLOAT=4 bytes LE, LONG/DOUBLE=8 bytes LE,
 * STRING=i32 length LE + UTF-8 bytes, NULL/TRUE/FALSE=0 bytes,
 * KEY_VALUE/UNION_ARRAY/FIXED_ARRAY=i32 length LE + payload bytes.
 */
public final class EirfArray {

    private final byte[] data;
    private final int endOffset;
    private final boolean fixed;
    private final byte fixedType; // only meaningful when fixed=true

    private int pos;
    private byte elemType;

    /**
     * Creates an array reader.
     * @param data the packed array bytes
     * @param offset start offset in data
     * @param length total byte length of the array payload
     * @param fixed true for FIXED_ARRAY format, false for UNION_ARRAY
     */
    public EirfArray(byte[] data, int offset, int length, boolean fixed) {
        this.data = data;
        this.endOffset = offset + length;
        this.fixed = fixed;
        if (fixed && length > 0) {
            this.fixedType = data[offset];
            this.pos = offset + 1; // past shared type byte
        } else if (fixed) {
            this.fixedType = EirfType.NULL;
            this.pos = offset;
        } else {
            this.fixedType = 0;
            this.pos = offset;
        }
    }

    /** Creates an array reader over the full byte array. */
    public EirfArray(byte[] data, boolean fixed) {
        this(data, 0, data.length, fixed);
    }

    /**
     * Advances to the next element. Returns false when all bytes have been consumed.
     */
    public boolean next() {
        if (pos >= endOffset) {
            return false;
        }
        if (fixed) {
            elemType = fixedType;
        } else {
            elemType = data[pos];
            pos++;
        }
        return true;
    }

    /**
     * Advances past the current element's data. Must be called after next() and before
     * calling next() again, unless a value accessor (which implicitly sizes the element) is used.
     * For compound types, this skips the entire nested structure.
     */
    public void advance() {
        pos += currentDataSize();
    }

    public byte type() {
        return elemType;
    }

    public boolean isNull() {
        return elemType == EirfType.NULL;
    }

    public boolean booleanValue() {
        if (elemType == EirfType.TRUE) return true;
        if (elemType == EirfType.FALSE) return false;
        throw new IllegalStateException("Element is not a boolean, type=" + EirfType.name(elemType));
    }

    public int intValue() {
        int val = ByteUtils.readIntLE(data, pos);
        pos += 4;
        return val;
    }

    public float floatValue() {
        float val = Float.intBitsToFloat(ByteUtils.readIntLE(data, pos));
        pos += 4;
        return val;
    }

    public long longValue() {
        long val = ByteUtils.readLongLE(data, pos);
        pos += 8;
        return val;
    }

    public double doubleValue() {
        double val = Double.longBitsToDouble(ByteUtils.readLongLE(data, pos));
        pos += 8;
        return val;
    }

    public String stringValue() {
        int len = ByteUtils.readIntLE(data, pos);
        String val = new String(data, pos + 4, len, StandardCharsets.UTF_8);
        pos += 4 + len;
        return val;
    }

    /**
     * Creates a child {@link EirfArray} reader over the current compound array element's payload
     * and advances past it. The current element must be a UNION_ARRAY or FIXED_ARRAY.
     */
    public EirfArray nestedArray() {
        int len = ByteUtils.readIntLE(data, pos);
        int off = pos + 4;
        boolean isFixed = elemType == EirfType.FIXED_ARRAY;
        pos = off + len;
        return new EirfArray(data, off, len, isFixed);
    }

    /**
     * Creates a child {@link EirfKeyValue} reader over the current compound element's payload
     * and advances past it. The current element must be of type KEY_VALUE.
     */
    public EirfKeyValue nestedKeyValue() {
        int len = ByteUtils.readIntLE(data, pos);
        int off = pos + 4;
        pos = off + len;
        return new EirfKeyValue(data, off, len);
    }

    private int currentDataSize() {
        int size = EirfType.elemDataSize(elemType);
        return size == -1 ? 4 + ByteUtils.readIntLE(data, pos) : size;
    }
}
