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
 * A forward-only reader over an array in EIRF format.
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
 *
 * <p>Usage: call {@link #next()} to advance to each element, then use the appropriate
 * value accessor. {@code next()} handles all positioning — value accessors are pure reads
 * and do not advance the cursor. There is no need to call {@code advance()} or consume
 * the value before calling {@code next()} again.
 */
public final class EirfArray {

    private final byte[] data;
    private final int endOffset;
    private final boolean fixed;
    private final byte fixedType; // only meaningful when fixed=true

    private int pos;
    private byte elemType;
    private int dataStart; // start of current element's data (past type byte)
    private int dataEnd;   // end of current element's data (next element starts here)

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
        this.dataEnd = this.pos;
    }

    /** Creates an array reader over the full byte array. */
    public EirfArray(byte[] data, boolean fixed) {
        this(data, 0, data.length, fixed);
    }

    /**
     * Advances to the next element. Returns false when all bytes have been consumed.
     * Handles all positioning — any unconsumed data from the previous element is skipped automatically.
     */
    public boolean next() {
        pos = dataEnd;
        if (pos >= endOffset) {
            return false;
        }
        if (fixed) {
            elemType = fixedType;
        } else {
            elemType = data[pos];
            pos++;
        }
        dataStart = pos;
        int size = EirfType.elemDataSize(elemType);
        dataEnd = size >= 0 ? pos + size : pos + 4 + ByteUtils.readIntLE(data, pos);
        return true;
    }

    public byte type() {
        return elemType;
    }

    public boolean isNull() {
        return elemType == EirfType.NULL;
    }

    public boolean booleanValue() {
        if (elemType == EirfType.TRUE) {
            return true;
        }
        if (elemType == EirfType.FALSE) {
            return false;
        }
        throw new IllegalStateException("Element is not a boolean, type=" + EirfType.name(elemType));
    }

    public int intValue() {
        return ByteUtils.readIntLE(data, dataStart);
    }

    public float floatValue() {
        return Float.intBitsToFloat(ByteUtils.readIntLE(data, dataStart));
    }

    public long longValue() {
        return ByteUtils.readLongLE(data, dataStart);
    }

    public double doubleValue() {
        return Double.longBitsToDouble(ByteUtils.readLongLE(data, dataStart));
    }

    public String stringValue() {
        int len = ByteUtils.readIntLE(data, dataStart);
        return new String(data, dataStart + 4, len, StandardCharsets.UTF_8);
    }

    /**
     * Creates a child {@link EirfArray} reader over the current compound array element's payload.
     * The current element must be a UNION_ARRAY or FIXED_ARRAY.
     */
    public EirfArray nestedArray() {
        int len = ByteUtils.readIntLE(data, dataStart);
        int off = dataStart + 4;
        boolean isFixed = elemType == EirfType.FIXED_ARRAY;
        return new EirfArray(data, off, len, isFixed);
    }

    /**
     * Creates a child {@link EirfKeyValue} reader over the current compound element's payload.
     * The current element must be of type KEY_VALUE.
     */
    public EirfKeyValue nestedKeyValue() {
        int len = ByteUtils.readIntLE(data, dataStart);
        int off = dataStart + 4;
        return new EirfKeyValue(data, off, len);
    }
}
