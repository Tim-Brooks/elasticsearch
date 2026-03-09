/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.xcontent.XContentString;

import java.util.Arrays;
import java.util.Set;

/**
 * Extracts TSID from dimension columns during batch encoding.
 * Used for TSDB / ForIndexDimensions indices.
 */
class TsidRoutingExtractor implements RoutingMetadataExtractor {

    private final Set<String> dimensionFields;

    // Track which columns are dimension columns
    private int[] dimColIndices = new int[4];
    private String[] dimColNames = new String[4];
    private int dimColCount = 0;

    // Track which column indices have been resolved
    private boolean[] resolved = new boolean[16];

    TsidRoutingExtractor(Set<String> dimensionFields) {
        this.dimensionFields = dimensionFields;
    }

    @Override
    public void resolveColumn(int colIdx, String fieldPath) {
        ensureResolvedCapacity(colIdx + 1);
        resolved[colIdx] = true;
        if (dimensionFields.contains(fieldPath)) {
            if (dimColCount == dimColIndices.length) {
                dimColIndices = Arrays.copyOf(dimColIndices, dimColCount * 2);
                dimColNames = Arrays.copyOf(dimColNames, dimColCount * 2);
            }
            dimColIndices[dimColCount] = colIdx;
            dimColNames[dimColCount] = fieldPath;
            dimColCount++;
        }
    }

    @Override
    public boolean isColumnResolved(int colIdx) {
        return colIdx < resolved.length && resolved[colIdx];
    }

    @Override
    public void extractFromScratch(DocumentBatchRowEncoder.ScratchBuffers scratch, IndexRequest request) {
        TsidBuilder tsidBuilder = new TsidBuilder(dimColCount);
        for (int i = 0; i < dimColCount; i++) {
            int colIdx = dimColIndices[i];
            String fieldName = dimColNames[i];
            byte typeByte = scratch.typeBytes[colIdx];
            byte baseType = RowType.baseType(typeByte);

            switch (baseType) {
                case RowType.STRING -> {
                    XContentString.UTF8Bytes utf8 = (XContentString.UTF8Bytes) scratch.varData[colIdx];
                    tsidBuilder.addStringDimension(fieldName, utf8);
                }
                case RowType.LONG -> {
                    long longVal = ByteUtils.readLongBE(scratch.fixedData, colIdx * 8);
                    tsidBuilder.addLongDimension(fieldName, longVal);
                }
                case RowType.DOUBLE -> {
                    long bits = ByteUtils.readLongBE(scratch.fixedData, colIdx * 8);
                    tsidBuilder.addDoubleDimension(fieldName, Double.longBitsToDouble(bits));
                }
                case RowType.TRUE -> tsidBuilder.addBooleanDimension(fieldName, true);
                case RowType.FALSE -> tsidBuilder.addBooleanDimension(fieldName, false);
                case RowType.NULL -> {
                    // skip null dimensions
                }
                default -> {
                    // skip unsupported types
                }
            }
        }
        if (tsidBuilder.size() > 0) {
            request.tsid(tsidBuilder.buildTsid());
        }
    }

    @Override
    public void resetDocument() {
        // No per-document state to reset — TsidBuilder is created fresh per document
    }

    private void ensureResolvedCapacity(int needed) {
        if (needed > resolved.length) {
            resolved = Arrays.copyOf(resolved, Math.max(needed, resolved.length * 2));
        }
    }
}
