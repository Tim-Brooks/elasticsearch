/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.DenseBinaryColumn;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;

import java.nio.ByteOrder;

/**
 * A {@link DenseBinaryColumn} backed by a {@link BytesReference} containing packed little-endian
 * longs. Each page of the {@link BytesReference} is returned directly as a {@link BytesRef} chunk
 * via {@link #nextBytes()}, making iteration allocation-free.
 * <p>
 * Every page length must be a multiple of {@link Long#BYTES} so that it contains only complete
 * long values. This is validated at construction time.
 */
public final class BytesRefDenseBinaryColumn extends DenseBinaryColumn {

    private final BytesRef[] pages;
    private int pageIndex;

    public BytesRefDenseBinaryColumn(String name, IndexableFieldType fieldType, BytesReference data) {
        super(name, fieldType, ByteOrder.LITTLE_ENDIAN);
        this.pages = toPageArray(data);
    }

    @Override
    public BytesRef nextBytes() {
        if (pageIndex >= pages.length) {
            return null;
        }
        return pages[pageIndex++];
    }

    @Override
    public void reset() {
        pageIndex = 0;
    }

    static BytesRef[] toPageArray(BytesReference data) {
        BytesRef[] pages = BytesRefLongColumn.toPageArray(data);
        for (int i = 0; i < pages.length; i++) {
            if ((pages[i].length & 7) != 0) {
                throw new IllegalArgumentException("page [" + i + "] length [" + pages[i].length + "] is not a multiple of Long.BYTES");
            }
        }
        return pages;
    }
}
