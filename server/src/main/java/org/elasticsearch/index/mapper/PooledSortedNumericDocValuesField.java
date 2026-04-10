/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;

/**
 * A poolable sorted-numeric doc values field with doc values skipper support that allows
 * mutable field names and avoids boxing allocations.
 * <p>
 * Lucene's {@code Field.name} is {@code protected final}, so it cannot be mutated directly.
 * This subclass overrides {@link #name()} to return a mutable name, allowing field instances
 * to be reused across documents and batches. Lucene's {@code IndexingChain} exclusively uses
 * {@code field.name()} to access the field name, making this safe.
 * <p>
 * The field holds a reusable {@link MutableLong} returned from {@link #numericValue()}
 * to avoid allocating a boxed {@link Long} on every call during doc values indexing.
 * {@link #setLongValue(long)} is overridden to update the same mutable instance.
 */
final class PooledSortedNumericDocValuesField extends Field {

    private static final FieldType FIELD_TYPE;

    static {
        FIELD_TYPE = new FieldType();
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        FIELD_TYPE.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
        FIELD_TYPE.freeze();
    }

    private String pooledName;
    private final MutableLong mutableValue = new MutableLong();

    PooledSortedNumericDocValuesField() {
        super("_pooled", FIELD_TYPE);
        // fieldsData must be a Long so that setLongValue() passes Lucene's instanceof check.
        this.fieldsData = 0L;
        this.pooledName = "_pooled";
    }

    @Override
    public String name() {
        return pooledName;
    }

    @Override
    public Number numericValue() {
        return mutableValue;
    }

    @Override
    public void setLongValue(long value) {
        mutableValue.value = value;
    }

    /**
     * Resets this field for reuse with a new name and value.
     */
    void reset(String name, long value) {
        this.pooledName = name;
        this.mutableValue.value = value;
    }
}
