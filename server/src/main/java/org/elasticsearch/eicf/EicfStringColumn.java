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
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.AbstractSourceColumnCursor;
import org.elasticsearch.sourcebatch.SourceColumnCursor;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

/**
 * An EICF column whose values are all UTF-8 strings, stored as a contiguous {@code data} payload with
 * a {@code (docCount + 1)}-entry offset vector delimiting each document's bytes
 * ({@code [offsets[d], offsets[d + 1])} within {@code data} starting at {@code base}).
 *
 * <p>The payload and offset vector are exposed package-private so {@link EicfLuceneColumns} can build
 * a bulk bytes cursor over the offset-delimited values.
 */
public final class EicfStringColumn extends EicfColumn {

    private final byte[] data;
    private final int base;
    private final int[] offsets;

    EicfStringColumn(int columnIndex, int docCount, FixedBitSet absent, byte[] data, int base, int[] offsets) {
        super(columnIndex, docCount, absent);
        this.data = data;
        this.base = base;
        this.offsets = offsets;
    }

    @Override
    byte kind() {
        return EicfColumnKind.STRING;
    }

    @Override
    byte typeByteForPresent(int d) {
        return EirfType.STRING;
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
    public SourceColumnCursor cursor() {
        return new AbstractSourceColumnCursor() {
            private int doc = -1;

            @Override
            public boolean advance() {
                return ++doc < docCount;
            }

            @Override
            public byte type() {
                return absent != null && absent.get(doc) ? EirfType.ABSENT : EirfType.STRING;
            }

            @Override
            public Text stringValue() {
                int off0 = offsets[doc];
                return new Text(new XContentString.UTF8Bytes(data, base + off0, offsets[doc + 1] - off0));
            }
        };
    }

    /** The contiguous value payload; document {@code d}'s bytes start at {@code dataBase() + offsets()[d]}. */
    byte[] dataBytes() {
        return data;
    }

    /** The byte offset of payload position {@code 0} within {@link #dataBytes()}. */
    int dataBase() {
        return base;
    }

    /** The {@code (docCount + 1)} offsets delimiting each document's value within the payload. */
    int[] offsets() {
        return offsets;
    }
}
