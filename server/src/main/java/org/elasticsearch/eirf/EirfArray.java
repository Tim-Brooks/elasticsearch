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
 * A forward-only reader over a compact typed array in EIRF format.
 *
 * <p>Two formats:
 * <ul>
 *   <li><b>Union:</b> count(1) | per element: type(1) + data</li>
 *   <li><b>Fixed:</b> count(1) | element_type(1) | per element: data only</li>
 * </ul>
 *
 * <p>Element data sizes: INT/FLOAT=4 bytes, LONG/DOUBLE=8 bytes,
 * STRING=4 bytes length + UTF-8 bytes, NULL/TRUE/FALSE=0 bytes.
 */
public final class EirfArray {

    private final byte[] data;
    private final int count;
    private final boolean fixed;
    private final byte fixedType; // only meaningful when fixed=true

    private int idx = -1;
    private int pos;
    private byte elemType;

    /**
     * Creates an array reader.
     * @param data the packed array bytes
     * @param offset start offset in data
     * @param fixed true for FIXED_ARRAY format, false for UNION_ARRAY
     */
    public EirfArray(byte[] data, int offset, boolean fixed) {
        this.data = data;
        this.fixed = fixed;
        this.count = data[offset] & 0xFF;
        if (fixed && count > 0) {
            this.fixedType = data[offset + 1];
            this.pos = offset + 2;
        } else if (fixed) {
            this.fixedType = EirfType.NULL;
            this.pos = offset + 1; // no type byte when count=0
        } else {
            this.fixedType = 0;
            this.pos = offset + 1;
        }
    }

    /** Creates a union array reader at offset 0. */
    public EirfArray(byte[] data) {
        this(data, 0, false);
    }

    /** Creates an array reader at offset 0. */
    public EirfArray(byte[] data, boolean fixed) {
        this(data, 0, fixed);
    }

    public int count() {
        return count;
    }

    /**
     * Advances to the next element. Returns false when all elements have been visited.
     */
    public boolean next() {
        if (idx >= 0) {
            pos += dataSize();
        }
        idx++;
        if (idx >= count) {
            return false;
        }
        if (fixed) {
            elemType = fixedType;
        } else {
            elemType = data[pos];
            pos++; // past type byte
        }
        return true;
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
        throw new IllegalStateException("Element " + idx + " is not a boolean, type=" + EirfType.name(elemType));
    }

    public int intValue() {
        return ByteUtils.readIntBE(data, pos);
    }

    public float floatValue() {
        return Float.intBitsToFloat(ByteUtils.readIntBE(data, pos));
    }

    public long longValue() {
        return ByteUtils.readLongBE(data, pos);
    }

    public double doubleValue() {
        return Double.longBitsToDouble(ByteUtils.readLongBE(data, pos));
    }

    public String stringValue() {
        int len = ByteUtils.readIntBE(data, pos);
        return new String(data, pos + 4, len, StandardCharsets.UTF_8);
    }

    public int stringOffset() {
        return pos + 4;
    }

    public int stringLength() {
        return ByteUtils.readIntBE(data, pos);
    }

    public byte[] stringBytes() {
        return data;
    }

    private int dataSize() {
        return switch (elemType) {
            case EirfType.INT, EirfType.FLOAT -> 4;
            case EirfType.LONG, EirfType.DOUBLE -> 8;
            case EirfType.STRING -> 4 + ByteUtils.readIntBE(data, pos);
            default -> 0; // NULL, TRUE, FALSE
        };
    }
}
