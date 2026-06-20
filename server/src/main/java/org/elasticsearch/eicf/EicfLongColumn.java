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
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.eirf.EirfType;

/**
 * An EICF column whose values are all {@code long}s, stored as contiguous little-endian 8-byte slots
 * ({@code data[base + d * 8]}). JSON ints and longs are both upcast to 64-bit here.
 *
 * <p>The raw slot buffer ({@link #valueBytes()} / {@link #valueBase()}) is exposed package-private so
 * {@link EicfLuceneColumns} can build a bulk cursor that copies LE longs straight into Lucene doc
 * values without re-deriving the layout.
 */
public final class EicfLongColumn extends EicfColumn {

    private final byte[] data;
    private final int base;

    EicfLongColumn(int columnIndex, int docCount, FixedBitSet absent, byte[] data, int base) {
        super(columnIndex, docCount, absent);
        this.data = data;
        this.base = base;
    }

    @Override
    byte kind() {
        return EicfColumnKind.LONG;
    }

    @Override
    byte typeByteForPresent(int d) {
        return EirfType.LONG;
    }

    @Override
    public long getLongValue(int d) {
        return ByteUtils.readLongLE(data, base + d * 8);
    }

    /** The raw 8-byte-slot buffer; slot {@code d} starts at {@code valueBase() + d * 8}. */
    byte[] valueBytes() {
        return data;
    }

    /** The byte offset of slot {@code 0} within {@link #valueBytes()}. */
    int valueBase() {
        return base;
    }
}
