/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;

/**
 * A pool of shared object arrays from {@link PageCacheRecycler} used to back the field lists
 * of multiple {@link LuceneDocument} instances in a batch. Instead of each document allocating
 * its own {@code ArrayList<IndexableField>}, all fields across all documents in the batch are
 * stored in shared {@code Object[]} pages obtained from the recycler.
 * <p>
 * Documents are created via {@link #newDocument()} which returns a standard {@link LuceneDocument}
 * backed by a {@link PooledFieldList}. Fields are written sequentially via a global cursor and
 * can span page boundaries transparently.
 * <p>
 * Additionally, this class provides access to a thread-local {@link FieldPool} of pre-allocated
 * Lucene field instances (e.g., {@link org.apache.lucene.document.SortedNumericDocValuesField},
 * {@link org.apache.lucene.document.NumericDocValuesField}) that can be reused across batches
 * to further reduce allocation pressure.
 * <p>
 * This object must be {@link #close() closed} after all documents have been indexed to return
 * the pages to the recycler and reset the field pool for the next batch.
 */
public final class BatchLuceneDocuments implements Releasable {

    private static final ThreadLocal<FieldPool> FIELD_POOL = ThreadLocal.withInitial(FieldPool::new);

    private final PageCacheRecycler recycler;
    private final List<Recycler.V<Object[]>> pages;
    private final int pageSize;
    private final FieldPool fieldPool;
    private int globalOffset;

    public BatchLuceneDocuments(PageCacheRecycler recycler) {
        this.recycler = recycler;
        this.pageSize = PageCacheRecycler.OBJECT_PAGE_SIZE;
        this.pages = new ArrayList<>();
        this.globalOffset = 0;
        this.fieldPool = FIELD_POOL.get();
        // Acquire the first page
        pages.add(recycler.objectPage());
    }

    /**
     * Creates a new {@link LuceneDocument} backed by pooled storage. The document's field list
     * starts at the current global offset and grows as fields are added.
     */
    public LuceneDocument newDocument() {
        PooledFieldList fieldList = new PooledFieldList(this, globalOffset);
        return new LuceneDocument(fieldList, "", null);
    }

    /**
     * Returns the thread-local field pool for reusing Lucene field instances across documents
     * in this batch.
     */
    public FieldPool fieldPool() {
        return fieldPool;
    }

    /**
     * Adds a pooled sorted numeric doc values field to the given document.
     * The field instance is obtained from the thread-local pool and reused across batches.
     *
     * @param doc   the document to add the field to
     * @param name  the field name
     * @param value the 64-bit long value
     */
    public void addSortedNumericDocValuesField(LuceneDocument doc, String name, long value) {
        doc.add(fieldPool.nextSortedNumeric(name, value));
    }

    /**
     * Adds a pooled numeric doc values field to the given document.
     * The field instance is obtained from the thread-local pool and reused across batches.
     *
     * @param doc   the document to add the field to
     * @param name  the field name
     * @param value the 64-bit long value
     */
    public void addNumericDocValuesField(LuceneDocument doc, String name, long value) {
        doc.add(fieldPool.nextNumeric(name, value));
    }

    /**
     * Adds a field to the current position in the pooled storage. If the current page is full,
     * a new page is acquired from the recycler.
     */
    void addField(IndexableField field) {
        int pageIndex = globalOffset / pageSize;
        int offsetInPage = globalOffset % pageSize;
        if (pageIndex >= pages.size()) {
            pages.add(recycler.objectPage());
        }
        pages.get(pageIndex).v()[offsetInPage] = field;
        globalOffset++;
    }

    /**
     * Retrieves the field at the given global index, translating to the appropriate page and offset.
     */
    IndexableField getField(int globalIndex) {
        int pageIndex = globalIndex / pageSize;
        int offsetInPage = globalIndex % pageSize;
        return (IndexableField) pages.get(pageIndex).v()[offsetInPage];
    }

    @Override
    public void close() {
        fieldPool.reset();
        for (Recycler.V<Object[]> page : pages) {
            page.close();
        }
        pages.clear();
    }
}
