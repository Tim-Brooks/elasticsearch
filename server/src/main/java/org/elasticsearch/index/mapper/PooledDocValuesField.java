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
 * A poolable doc values field that allows mutable field names and avoids boxing allocations.
 * <p>
 * Lucene's {@code Field.name} is {@code protected final}, so it cannot be mutated directly.
 * This class overrides {@link #name()} to return a mutable name, allowing field instances
 * to be reused across documents and batches. Lucene's {@code IndexingChain} exclusively uses
 * {@code field.name()} to access the field name, making this safe.
 * <p>
 * The field holds a reusable {@link MutableLong} returned from {@link #numericValue()}
 * to avoid allocating a boxed {@link Long} on every call during doc values indexing.
 * {@link #setLongValue(long)} is overridden to update the same mutable instance.
 * <p>
 * A single concrete class is used for both {@link DocValuesType#NUMERIC} and
 * {@link DocValuesType#SORTED_NUMERIC} fields so that the JVM sees one {@link #name()}
 * override rather than multiple, reducing dispatch targets and avoiding itable lookups.
 */
final class PooledDocValuesField extends Field {

    private static final FieldType SORTED_NUMERIC_FIELD_TYPE;
    private static final FieldType NUMERIC_FIELD_TYPE;

    static {
        SORTED_NUMERIC_FIELD_TYPE = new FieldType();
        SORTED_NUMERIC_FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        SORTED_NUMERIC_FIELD_TYPE.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
        SORTED_NUMERIC_FIELD_TYPE.freeze();

        NUMERIC_FIELD_TYPE = new FieldType();
        NUMERIC_FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        NUMERIC_FIELD_TYPE.setDocValuesSkipIndexType(DocValuesSkipIndexType.RANGE);
        NUMERIC_FIELD_TYPE.freeze();
    }

    private String pooledName;
    private final MutableLong mutableValue = new MutableLong();

    private PooledDocValuesField(FieldType fieldType) {
        super("_pooled", fieldType);
        // fieldsData must be a Long so that setLongValue() passes Lucene's instanceof check.
        this.fieldsData = 0L;
        this.pooledName = "_pooled";
    }

    static PooledDocValuesField sortedNumeric() {
        return new PooledDocValuesField(SORTED_NUMERIC_FIELD_TYPE);
    }

    static PooledDocValuesField numeric() {
        return new PooledDocValuesField(NUMERIC_FIELD_TYPE);
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
