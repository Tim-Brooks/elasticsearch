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
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.eirf.EirfType;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.xcontent.Text;

/**
 * A single-row view over an {@link EicfBatch}, backed by the batch's column vectors.
 *
 * <p>Every getter delegates to {@code batch.column(col).getXxx(docIndex)}, so access patterns
 * that iterate columns for a single row are efficient (direct column-vector access) while
 * patterns that iterate rows for a single column should use {@link EicfColumn} directly.
 *
 * <p>All getters are pure reads that do not advance any cursor state.
 */
public final class EicfRow implements SourceRow {

    private final EicfBatch batch;
    private final int docIndex;

    EicfRow(EicfBatch batch, int docIndex) {
        this.batch = batch;
        this.docIndex = docIndex;
    }

    @Override
    public EirfSchema schema() {
        return batch.schema();
    }

    @Override
    public int columnCount() {
        return batch.columnCount();
    }

    /**
     * Returns 0 as a placeholder. The columnar format does not store per-document source sizes;
     * callers that need a size proxy should track original source lengths separately.
     */
    @Override
    public int sizeInBytes() {
        return 0;
    }

    @Override
    public byte getTypeByte(int col) {
        if (col < 0 || col >= columnCount()) {
            return EirfType.ABSENT;
        }
        return batch.column(col).getTypeByte(docIndex);
    }

    @Override
    public boolean isAbsent(int col) {
        if (col < 0 || col >= columnCount()) {
            return true;
        }
        return batch.column(col).isAbsent(docIndex);
    }

    @Override
    public boolean isNull(int col) {
        if (col < 0 || col >= columnCount()) {
            return false;
        }
        return batch.column(col).isNull(docIndex);
    }

    @Override
    public boolean getBooleanValue(int col) {
        return batch.column(col).getBooleanValue(docIndex);
    }

    @Override
    public int getIntValue(int col) {
        return batch.column(col).getIntValue(docIndex);
    }

    @Override
    public float getFloatValue(int col) {
        return batch.column(col).getFloatValue(docIndex);
    }

    @Override
    public long getLongValue(int col) {
        return batch.column(col).getLongValue(docIndex);
    }

    @Override
    public double getDoubleValue(int col) {
        return batch.column(col).getDoubleValue(docIndex);
    }

    @Override
    public Text getStringValue(int col) {
        return batch.column(col).getStringValue(docIndex);
    }

    @Override
    public BytesRef getBinaryValue(int col) {
        return batch.column(col).getBinaryValue(docIndex);
    }

    @Override
    public EirfKeyValueReader getKeyValue(int col) {
        return batch.column(col).getKeyValue(docIndex);
    }

    @Override
    public EirfArrayReader getArrayValue(int col) {
        return batch.column(col).getArrayValue(docIndex);
    }
}
