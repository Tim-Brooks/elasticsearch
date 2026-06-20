/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * A heterogeneous EICF column: a per-document type vector gives each row's {@link EirfType}, and a
 * dense value buffer delimited by a {@code (docCount + 1)}-entry offset vector holds the payload.
 * Zero-byte types (NULL/TRUE/FALSE) occupy no payload bytes, fixed-size numerics (LONG/DOUBLE) occupy
 * 8 bytes, and variable types occupy the bytes delimited by their offset delta. This is the only kind
 * that branches on type at read time; callers should consult {@link #getTypeByte} before choosing a
 * value getter.
 */
public final class EicfUnionColumn extends EicfColumn {

    private final byte[] typeVec;
    private final int typeVecBase;
    private final int[] offsets;
    private final byte[] data;
    private final int base;

    EicfUnionColumn(
        int columnIndex,
        int docCount,
        FixedBitSet absent,
        byte[] typeVec,
        int typeVecBase,
        int[] offsets,
        byte[] data,
        int base
    ) {
        super(columnIndex, docCount, absent);
        this.typeVec = typeVec;
        this.typeVecBase = typeVecBase;
        this.offsets = offsets;
        this.data = data;
        this.base = base;
    }

    @Override
    byte kind() {
        return EicfColumnKind.UNION;
    }

    @Override
    byte typeByteForPresent(int d) {
        return typeVec[typeVecBase + d];
    }

    @Override
    public boolean getBooleanValue(int d) {
        byte t = typeVec[typeVecBase + d];
        if (t == EirfType.TRUE) {
            return true;
        }
        if (t == EirfType.FALSE) {
            return false;
        }
        throw new IllegalStateException("Column " + columnIndex + " doc " + d + " is not boolean, type=" + EirfType.name(t));
    }

    @Override
    public long getLongValue(int d) {
        return ByteUtils.readLongLE(data, base + offsets[d]);
    }

    @Override
    public double getDoubleValue(int d) {
        return Double.longBitsToDouble(ByteUtils.readLongLE(data, base + offsets[d]));
    }

    @Override
    public Text getStringValue(int d) {
        int off0 = offsets[d];
        return new Text(new XContentString.UTF8Bytes(data, base + off0, offsets[d + 1] - off0));
    }

    @Override
    public BytesRef getBinaryValue(int d) {
        int off0 = offsets[d];
        return new BytesRef(data, base + off0, offsets[d + 1] - off0);
    }

    @Override
    public EirfArrayReader getArrayValue(int d) {
        boolean fixed = typeVec[typeVecBase + d] == EirfType.FIXED_ARRAY;
        int off0 = offsets[d];
        return new EirfArrayReader(data, base + off0, offsets[d + 1] - off0, fixed);
    }

    @Override
    public EirfKeyValueReader getKeyValue(int d) {
        int off0 = offsets[d];
        return new EirfKeyValueReader(data, base + off0, offsets[d + 1] - off0);
    }
}
