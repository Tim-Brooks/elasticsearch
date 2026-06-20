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
import org.elasticsearch.eirf.EirfType;

/**
 * An EICF column whose values are all booleans, stored as a value bitset (bit set = {@code true})
 * held as little-endian 64-bit words. A present document's type byte is {@link EirfType#TRUE} or
 * {@link EirfType#FALSE} depending on its value bit.
 */
public final class EicfBoolColumn extends EicfColumn {

    private final long[] valueBits;

    EicfBoolColumn(int columnIndex, int docCount, FixedBitSet absent, long[] valueBits) {
        super(columnIndex, docCount, absent);
        this.valueBits = valueBits;
    }

    @Override
    byte kind() {
        return EicfColumnKind.BOOL;
    }

    @Override
    byte typeByteForPresent(int d) {
        return bitSet(d) ? EirfType.TRUE : EirfType.FALSE;
    }

    @Override
    public boolean getBooleanValue(int d) {
        return bitSet(d);
    }

    private boolean bitSet(int d) {
        return ((valueBits[d >>> 6] >>> (d & 63)) & 1L) != 0;
    }
}
