/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.VectorSimilarity;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {
    private final boolean indexed;

    public DenseVectorFieldTypeTests() {
        this.indexed = randomBoolean();
    }

    private DenseVectorFieldType createFieldType() {
        return new DenseVectorFieldType("f", Version.CURRENT, 5, indexed, VectorSimilarity.cosine, Collections.emptyMap());
    }

    public void testHasDocValues() {
        DenseVectorFieldType ft = createFieldType();
        assertNotEquals(indexed, ft.hasDocValues());
    }

    public void testIsSearchable() {
        DenseVectorFieldType ft = createFieldType();
        assertEquals(indexed, ft.isSearchable());
    }

    public void testIsAggregatable() {
        DenseVectorFieldType ft = createFieldType();
        assertFalse(ft.isAggregatable());
    }

    public void testFielddataBuilder() {
        DenseVectorFieldType ft = createFieldType();
        assertNotNull(ft.fielddataBuilder("index", () -> {
            throw new UnsupportedOperationException();
        }));
    }

    public void testDocValueFormat() {
        DenseVectorFieldType ft = createFieldType();
        expectThrows(IllegalArgumentException.class, () -> ft.docValueFormat(null, null));
    }

    public void testFetchSourceValue() throws IOException {
        DenseVectorFieldType ft = createFieldType();
        List<Double> vector = List.of(0.0, 1.0, 2.0, 3.0, 4.0);
        assertEquals(vector, fetchSourceValue(ft, vector));
    }

    public void testCreateKnnQuery() {
        DenseVectorFieldType unindexedField = new DenseVectorFieldType("f", Version.CURRENT,
            3, false, VectorSimilarity.cosine, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unindexedField.createKnnQuery(
            new float[]{0.3f, 0.1f, 1.0f}, 10));
        assertThat(e.getMessage(), containsString("[knn] queries are not supported if [index] is disabled"));

        DenseVectorFieldType dotProductField = new DenseVectorFieldType("f", Version.CURRENT,
            3, true, VectorSimilarity.dot_product, Collections.emptyMap());
        e = expectThrows(IllegalArgumentException.class, () -> dotProductField.createKnnQuery(
            new float[]{0.3f, 0.1f, 1.0f}, 10));
        assertThat(e.getMessage(), containsString("The [dot_product] similarity can only be used with unit-length vectors."));
    }
}
