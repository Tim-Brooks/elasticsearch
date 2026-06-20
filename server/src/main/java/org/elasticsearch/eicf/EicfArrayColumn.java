/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eicf;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfType;

/**
 * An EICF column whose values are all arrays. A per-document type vector records each row's array
 * kind ({@link EirfType#FIXED_ARRAY} or {@link EirfType#UNION_ARRAY}); the packed array bytes live in
 * a contiguous {@code data} payload delimited by a {@code (docCount + 1)}-entry offset vector and are
 * read via {@link EirfArrayReader}.
 */
public final class EicfArrayColumn extends EicfColumn {

    private final byte[] typeVec;
    private final int typeVecBase;
    private final int[] offsets;
    private final byte[] data;
    private final int base;

    EicfArrayColumn(
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
        return EicfColumnKind.ARRAY;
    }

    @Override
    byte typeByteForPresent(int d) {
        return typeVec[typeVecBase + d];
    }

    @Override
    public EirfArrayReader getArrayValue(int d) {
        boolean fixed = typeVec[typeVecBase + d] == EirfType.FIXED_ARRAY;
        int off0 = offsets[d];
        return new EirfArrayReader(data, base + off0, offsets[d + 1] - off0, fixed);
    }
}
