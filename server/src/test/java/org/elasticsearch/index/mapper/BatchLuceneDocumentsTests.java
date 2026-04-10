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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

public class BatchLuceneDocumentsTests extends ESTestCase {

    public void testNewDocumentReturnsLuceneDocument() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            assertNotNull(doc);
            assertEquals(0, doc.getFields().size());
            assertEquals("", doc.getPath());
            assertNull(doc.getParent());
        }
    }

    public void testAddFieldsToDocument() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            IndexableField field1 = new StringField("_id", "doc1", Field.Store.YES);
            IndexableField field2 = new StringField("name", "test", Field.Store.YES);
            doc.add(field1);
            doc.add(field2);

            assertEquals(2, doc.getFields().size());
            assertSame(field1, doc.getFields().get(0));
            assertSame(field2, doc.getFields().get(1));
        }
    }

    public void testMultipleDocumentsSequential() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc1 = batchDocs.newDocument();
            IndexableField f1 = new StringField("_id", "doc1", Field.Store.YES);
            IndexableField f2 = new StringField("name", "first", Field.Store.YES);
            doc1.add(f1);
            doc1.add(f2);

            LuceneDocument doc2 = batchDocs.newDocument();
            IndexableField f3 = new StringField("_id", "doc2", Field.Store.YES);
            IndexableField f4 = new StringField("name", "second", Field.Store.YES);
            IndexableField f5 = new StringField("age", "30", Field.Store.YES);
            doc2.add(f3);
            doc2.add(f4);
            doc2.add(f5);

            // Verify doc1 fields are isolated
            assertEquals(2, doc1.getFields().size());
            assertSame(f1, doc1.getFields().get(0));
            assertSame(f2, doc1.getFields().get(1));

            // Verify doc2 fields are isolated
            assertEquals(3, doc2.getFields().size());
            assertSame(f3, doc2.getFields().get(0));
            assertSame(f4, doc2.getFields().get(1));
            assertSame(f5, doc2.getFields().get(2));
        }
    }

    public void testIterator() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            List<IndexableField> expected = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                IndexableField field = new StringField("field" + i, "value" + i, Field.Store.YES);
                doc.add(field);
                expected.add(field);
            }

            List<IndexableField> iterated = new ArrayList<>();
            for (IndexableField field : doc) {
                iterated.add(field);
            }
            assertEquals(expected, iterated);
        }
    }

    public void testManyDocumentsFillMultiplePages() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            int fieldsPerDoc = 50;
            // Create enough documents to span multiple pages (page size ~2048)
            int docCount = (PageCacheRecycler.OBJECT_PAGE_SIZE / fieldsPerDoc) + 10;

            List<LuceneDocument> docs = new ArrayList<>();
            for (int d = 0; d < docCount; d++) {
                LuceneDocument doc = batchDocs.newDocument();
                for (int f = 0; f < fieldsPerDoc; f++) {
                    doc.add(new StringField("field" + f, "doc" + d + "_val" + f, Field.Store.YES));
                }
                docs.add(doc);
            }

            // Verify each document has correct number of fields
            for (int d = 0; d < docCount; d++) {
                LuceneDocument doc = docs.get(d);
                assertEquals(fieldsPerDoc, doc.getFields().size());

                // Verify field names
                for (int f = 0; f < fieldsPerDoc; f++) {
                    assertEquals("field" + f, doc.getFields().get(f).name());
                }
            }
        }
    }

    public void testDocumentFieldsSpanPageBoundary() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            int pageSize = PageCacheRecycler.OBJECT_PAGE_SIZE;

            // Fill the first page almost completely
            LuceneDocument doc1 = batchDocs.newDocument();
            for (int i = 0; i < pageSize - 5; i++) {
                doc1.add(new StringField("_f" + i, "v", Field.Store.YES));
            }

            // Create a second document whose fields span the page boundary
            LuceneDocument doc2 = batchDocs.newDocument();
            int doc2FieldCount = 20;
            for (int i = 0; i < doc2FieldCount; i++) {
                doc2.add(new StringField("_g" + i, "v", Field.Store.YES));
            }

            // Verify doc2's fields are readable across the page boundary
            assertEquals(doc2FieldCount, doc2.getFields().size());
            for (int i = 0; i < doc2FieldCount; i++) {
                assertEquals("_g" + i, doc2.getFields().get(i).name());
            }

            // Verify iteration works
            int count = 0;
            for (IndexableField ignored : doc2) {
                count++;
            }
            assertEquals(doc2FieldCount, count);
        }
    }

    public void testAddAllFields() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            List<IndexableField> fields = new ArrayList<>();
            fields.add(new StringField("_a", "1", Field.Store.YES));
            fields.add(new StringField("_b", "2", Field.Store.YES));
            fields.add(new StringField("_c", "3", Field.Store.YES));

            doc.addAll(fields);

            assertEquals(3, doc.getFields().size());
            assertEquals("_a", doc.getFields().get(0).name());
            assertEquals("_b", doc.getFields().get(1).name());
            assertEquals("_c", doc.getFields().get(2).name());
        }
    }

    public void testGetFieldsByName() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            doc.add(new StringField("_id", "1", Field.Store.YES));
            doc.add(new StringField("name", "first", Field.Store.YES));
            doc.add(new StringField("name", "second", Field.Store.YES));
            doc.add(new StringField("age", "30", Field.Store.YES));

            // getField returns first match
            assertEquals("_id", doc.getField("_id").name());
            assertEquals("name", doc.getField("name").name());

            // getFields returns all matches
            List<IndexableField> nameFields = doc.getFields("name");
            assertEquals(2, nameFields.size());

            // get returns string value
            assertEquals("1", doc.get("_id"));
        }
    }

    public void testKeyedFields() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            IndexableField field = new StringField("_id", "doc1", Field.Store.YES);
            doc.addWithKey("_id_key", field);

            assertSame(field, doc.getByKey("_id_key"));
            assertEquals(1, doc.getFields().size());
        }
    }

    public void testCloseReleasesPages() {
        BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE);

        // Create documents and add fields
        LuceneDocument doc = batchDocs.newDocument();
        doc.add(new StringField("_id", "doc1", Field.Store.YES));

        // Close should not throw
        batchDocs.close();
    }

    public void testEmptyDocument() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            assertEquals(0, doc.getFields().size());
            assertFalse(doc.iterator().hasNext());
        }
    }

    public void testAddSortedNumericDocValuesField() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            batchDocs.addSortedNumericDocValuesField(doc, "timestamp", 1234567890L);
            batchDocs.addSortedNumericDocValuesField(doc, "metric_value", 42L);

            assertEquals(2, doc.getFields().size());
            assertEquals("timestamp", doc.getFields().get(0).name());
            assertEquals(1234567890L, doc.getFields().get(0).numericValue().longValue());
            assertEquals("metric_value", doc.getFields().get(1).name());
            assertEquals(42L, doc.getFields().get(1).numericValue().longValue());
        }
    }

    public void testAddNumericDocValuesField() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            batchDocs.addNumericDocValuesField(doc, "_version", 1L);
            batchDocs.addNumericDocValuesField(doc, "_seq_no", 100L);

            assertEquals(2, doc.getFields().size());
            assertEquals("_version", doc.getFields().get(0).name());
            assertEquals(1L, doc.getFields().get(0).numericValue().longValue());
            assertEquals("_seq_no", doc.getFields().get(1).name());
            assertEquals(100L, doc.getFields().get(1).numericValue().longValue());
        }
    }

    public void testPooledFieldsAcrossDocuments() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc1 = batchDocs.newDocument();
            batchDocs.addSortedNumericDocValuesField(doc1, "value", 10L);

            LuceneDocument doc2 = batchDocs.newDocument();
            batchDocs.addSortedNumericDocValuesField(doc2, "value", 20L);

            // Each document has its own field instance with correct values
            assertEquals(1, doc1.getFields().size());
            assertEquals(10L, doc1.getFields().get(0).numericValue().longValue());
            assertEquals(1, doc2.getFields().size());
            assertEquals(20L, doc2.getFields().get(0).numericValue().longValue());

            // They should be different instances
            assertNotSame(doc1.getFields().get(0), doc2.getFields().get(0));
        }
    }

    public void testPooledFieldsReusedAcrossBatches() {
        IndexableField firstBatchField;
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            batchDocs.addSortedNumericDocValuesField(doc, "old_name", 1L);
            firstBatchField = doc.getFields().get(0);
        }

        // After close, the pool resets. The next batch reuses the same field instances.
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();
            batchDocs.addSortedNumericDocValuesField(doc, "new_name", 2L);
            IndexableField secondBatchField = doc.getFields().get(0);

            // Same pooled instance, but with updated name and value
            assertSame(firstBatchField, secondBatchField);
            assertEquals("new_name", secondBatchField.name());
            assertEquals(2L, secondBatchField.numericValue().longValue());
        }
    }

    public void testPoolOverflowFallsBackToNewAllocation() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            // Exhaust the sorted numeric pool
            for (int i = 0; i < FieldPool.SORTED_NUMERIC_POOL_SIZE + 10; i++) {
                batchDocs.addSortedNumericDocValuesField(doc, "field" + i, i);
            }

            // All fields should be present with correct values
            assertEquals(FieldPool.SORTED_NUMERIC_POOL_SIZE + 10, doc.getFields().size());

            // Overflow fields are fresh allocations, not pooled instances
            IndexableField lastField = doc.getFields().get(FieldPool.SORTED_NUMERIC_POOL_SIZE + 9);
            assertEquals("field" + (FieldPool.SORTED_NUMERIC_POOL_SIZE + 9), lastField.name());
        }
    }

    public void testMixedPooledAndRegularFields() {
        try (BatchLuceneDocuments batchDocs = new BatchLuceneDocuments(PageCacheRecycler.NON_RECYCLING_INSTANCE)) {
            LuceneDocument doc = batchDocs.newDocument();

            // Mix pooled fields with regular fields
            doc.add(new StringField("_id", "doc1", Field.Store.YES));
            batchDocs.addSortedNumericDocValuesField(doc, "timestamp", 1000L);
            batchDocs.addNumericDocValuesField(doc, "_version", 1L);
            doc.add(new StringField("name", "test", Field.Store.YES));

            assertEquals(4, doc.getFields().size());
            assertEquals("_id", doc.getFields().get(0).name());
            assertEquals("timestamp", doc.getFields().get(1).name());
            assertEquals("_version", doc.getFields().get(2).name());
            assertEquals("name", doc.getFields().get(3).name());
        }
    }
}
