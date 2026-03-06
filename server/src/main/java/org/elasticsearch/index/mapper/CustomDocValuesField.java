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
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.index.DocValuesType;

// used for binary, geo and range fields
public abstract class CustomDocValuesField extends Field {

    public static final FieldType TYPE;
    static {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.BINARY);
        ft.setOmitNorms(true);
        TYPE = Mapper.freezeAndDeduplicateFieldType(ft);
    }

    protected CustomDocValuesField(String name) {
        super(name, TYPE);
    }

    @Override
    public InvertableType invertableType() {
        return InvertableType.BINARY;
    }
}
