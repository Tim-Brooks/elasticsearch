/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.util.ByteUtils;

import java.nio.charset.StandardCharsets;

/**
 * A forward-only reader over a compact typed array (≤8 leaf elements).
 *
 * <p>Binary layout:
 * <pre>
 * count(1) | for each element: type(1) + data
 * </pre>
 * Where data is: nothing for NULL/TRUE/FALSE, 8 bytes BE for LONG/DOUBLE,
 * 4 bytes BE length + UTF-8 bytes for STRING.
 */
public final class SmallArrayReader {

    private final byte[] data;
    private final int count;

    // Cursor state
    private int idx = -1;
    private int pos;
    private byte elemType;

    public SmallArrayReader(byte[] data, int offset) {
        this.data = data;
        this.count = data[offset] & 0xFF;
        this.pos = offset + 1;
    }

    public SmallArrayReader(byte[] data) {
        this(data, 0);
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
        elemType = data[pos];
        pos++; // past type byte
        return true;
    }

    public byte type() {
        return elemType;
    }

    public boolean isNull() {
        return elemType == RowType.NULL;
    }

    public boolean booleanValue() {
        if (elemType == RowType.TRUE) return true;
        if (elemType == RowType.FALSE) return false;
        throw new IllegalStateException("Element " + idx + " is not a boolean, type=" + RowType.name(elemType));
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
            case RowType.LONG, RowType.DOUBLE -> 8;
            case RowType.STRING -> 4 + ByteUtils.readIntBE(data, pos);
            default -> 0; // NULL, TRUE, FALSE
        };
    }
}
