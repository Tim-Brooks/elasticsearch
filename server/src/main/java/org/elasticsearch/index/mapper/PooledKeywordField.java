/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.util.BytesRef;

/**
 * A poolable keyword field that supports mutable field names and values.
 * <p>
 * Lucene's {@code Field.name} is {@code protected final}, so it cannot be mutated directly.
 * This subclass overrides {@link #name()} to return a mutable name, allowing field instances
 * to be reused across documents and batches. The {@link FieldType} is also mutable so the
 * same pooled instance can be used for keyword fields with different configurations.
 * <p>
 * Lucene's {@code IndexingChain} exclusively uses {@code field.name()} to access the field name
 * and copies the {@link BytesRef} value during indexing, making this safe for reuse.
 */
final class PooledKeywordField extends KeywordFieldMapper.KeywordField {

    private String pooledName;
    private FieldType pooledFieldType;

    PooledKeywordField() {
        super("_pooled", new BytesRef(), KeywordFieldMapper.Defaults.FIELD_TYPE);
        this.pooledName = "_pooled";
        this.pooledFieldType = KeywordFieldMapper.Defaults.FIELD_TYPE;
    }

    @Override
    public String name() {
        return pooledName;
    }

    @Override
    public FieldType fieldType() {
        return pooledFieldType;
    }

    @Override
    public InvertableType invertableType() {
        return InvertableType.BINARY;
    }

    /**
     * Resets this field for reuse with a new name, value, and field type.
     */
    void reset(String name, BytesRef value, FieldType fieldType) {
        this.pooledName = name;
        this.fieldsData = value;
        this.pooledFieldType = fieldType;
    }
}
