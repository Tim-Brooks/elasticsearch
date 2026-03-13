/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.RoutingHashBuilder;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Extracts routing hash from sort-field columns during batch encoding.
 * Used for LogsDB / ForRoutingPath indices.
 */
class SortFieldRoutingExtractor implements RoutingMetadataExtractor {

    private final RoutingHashBuilder hashBuilder;
    private final Predicate<String> isRoutingPath;

    // Track which columns are routing columns
    private int[] routingColIndices = new int[4];
    private String[] routingColNames = new String[4];
    private int routingColCount = 0;

    // Track which column indices have been resolved
    private boolean[] resolved = new boolean[16];

    SortFieldRoutingExtractor(Predicate<String> isRoutingPath) {
        this.isRoutingPath = isRoutingPath;
        this.hashBuilder = new RoutingHashBuilder(isRoutingPath);
    }

    @Override
    public void resolveColumn(int colIdx, String fieldPath) {
        ensureResolvedCapacity(colIdx + 1);
        resolved[colIdx] = true;
        if (isRoutingPath.test(fieldPath)) {
            if (routingColCount == routingColIndices.length) {
                routingColIndices = Arrays.copyOf(routingColIndices, routingColCount * 2);
                routingColNames = Arrays.copyOf(routingColNames, routingColCount * 2);
            }
            routingColIndices[routingColCount] = colIdx;
            routingColNames[routingColCount] = fieldPath;
            routingColCount++;
        }
    }

    @Override
    public boolean isColumnResolved(int colIdx) {
        return colIdx < resolved.length && resolved[colIdx];
    }

    @Override
    public void extractFromScratch(DocumentBatchRowEncoder.ScratchBuffers scratch, IndexRequest request) {
        hashBuilder.clear();
        for (int i = 0; i < routingColCount; i++) {
            int colIdx = routingColIndices[i];
            String fieldName = routingColNames[i];
            byte typeByte = scratch.typeBytes[colIdx];
            byte baseType = typeByte;

            if (baseType == RowType.NULL) {
                continue;
            }

            BytesRef value;
            if (baseType == RowType.STRING) {
                XContentString.UTF8Bytes utf8 = (XContentString.UTF8Bytes) scratch.varData[colIdx];
                value = new BytesRef(utf8.bytes(), utf8.offset(), utf8.length());
            } else if (baseType == RowType.LONG) {
                long longVal = ByteUtils.readLongBE(scratch.fixedData, colIdx * 8);
                value = new BytesRef(Long.toString(longVal));
            } else if (baseType == RowType.DOUBLE) {
                long bits = ByteUtils.readLongBE(scratch.fixedData, colIdx * 8);
                value = new BytesRef(Double.toString(Double.longBitsToDouble(bits)));
            } else if (baseType == RowType.TRUE) {
                value = new BytesRef("true");
            } else if (baseType == RowType.FALSE) {
                value = new BytesRef("false");
            } else {
                continue;
            }

            hashBuilder.addMatching(fieldName, value);
        }
        int hash = hashBuilder.buildHash(() -> {
            throw new IllegalArgumentException("Error extracting routing: source didn't contain any routing fields");
        });
        request.setRoutingHash(hash);
    }

    @Override
    public void resetDocument() {
        hashBuilder.clear();
    }

    private void ensureResolvedCapacity(int needed) {
        if (needed > resolved.length) {
            resolved = Arrays.copyOf(resolved, Math.max(needed, resolved.length * 2));
        }
    }
}
