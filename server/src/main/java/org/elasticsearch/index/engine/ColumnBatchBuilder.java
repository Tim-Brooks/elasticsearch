/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Column;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.ColumnWriter;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Two-phase builder for an {@link ElasticsearchBatch}.
 * <p>
 * Phase 1 (parsing): user field columns and the {@code _id} column are added via
 * {@link #addUserColumn} and {@link #addIdData}.
 * <p>
 * Phase 2 (engine): metadata columns ({@code _seq_no}, {@code _primary_term}, {@code _version})
 * are written via {@link #writeSeqNo}, {@link #writePrimaryTerm}, {@link #writeVersion}
 * after sequence numbers are assigned.
 * <p>
 * Finally, {@link #build()} assembles all columns into an {@link ElasticsearchBatch}.
 */
public final class ColumnBatchBuilder implements Releasable {

    private final Recycler<BytesRef> recycler;
    private int numDocs;

    // Phase 1 user columns (already finished — ReleasableBytesReference + metadata)
    private final List<ColumnInfo> userColumns = new ArrayList<>();

    // Phase 1 _id column
    private ColumnInfo idColumn;

    // Phase 2 metadata column writers (created lazily on first write)
    private ColumnWriter seqNoWriter;
    private ColumnWriter primaryTermWriter;
    private ColumnWriter versionWriter;

    public ColumnBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    public void setNumDocs(int numDocs) {
        this.numDocs = numDocs;
    }

    public int numDocs() {
        return numDocs;
    }

    /**
     * Add a finished user field column.
     */
    public void addUserColumn(String name, IndexableFieldType fieldType, boolean isLong, ReleasableBytesReference data, int entryCount) {
        userColumns.add(new ColumnInfo(name, fieldType, isLong, data, entryCount));
    }

    /**
     * Add the finished {@code _id} column data.
     */
    public void addIdData(String name, IndexableFieldType fieldType, ReleasableBytesReference data, int entryCount) {
        idColumn = new ColumnInfo(name, fieldType, false, data, entryCount);
    }

    /**
     * Write a {@code _seq_no} entry for a batch document.
     */
    public void writeSeqNo(int docId, long seqNo) {
        if (seqNoWriter == null) {
            seqNoWriter = new ColumnWriter(SeqNoFieldMapper.NAME, NumericDocValuesField.TYPE, true, recycler);
        }
        seqNoWriter.writeLong(docId, seqNo);
    }

    /**
     * Write a {@code _primary_term} entry for a batch document.
     */
    public void writePrimaryTerm(int docId, long primaryTerm) {
        if (primaryTermWriter == null) {
            primaryTermWriter = new ColumnWriter(SeqNoFieldMapper.PRIMARY_TERM_NAME, NumericDocValuesField.TYPE, true, recycler);
        }
        primaryTermWriter.writeLong(docId, primaryTerm);
    }

    /**
     * Write a {@code _version} entry for a batch document.
     */
    public void writeVersion(int docId, long version) {
        if (versionWriter == null) {
            versionWriter = new ColumnWriter(VersionFieldMapper.NAME, NumericDocValuesField.TYPE, true, recycler);
        }
        versionWriter.writeLong(docId, version);
    }

    /**
     * Build the final {@link ElasticsearchBatch}. After this call, the builder should not be used.
     */
    public ElasticsearchBatch build() {
        List<Column> columns = new ArrayList<>();
        List<Releasable> resources = new ArrayList<>();

        // Add _id column (must be first for processRowColumns in Lucene)
        if (idColumn != null) {
            columns.add(new BytesRefBinaryColumn(idColumn.name, idColumn.fieldType, idColumn.data, idColumn.entryCount));
            resources.add(idColumn.data);
        }

        // Add user field columns
        for (ColumnInfo info : userColumns) {
            if (info.isLong) {
                columns.add(new BytesRefLongColumn(info.name, info.fieldType, info.data, info.entryCount));
            } else {
                columns.add(new BytesRefBinaryColumn(info.name, info.fieldType, info.data, info.entryCount));
            }
            resources.add(info.data);
        }

        // Finish and add metadata columns
        if (seqNoWriter != null) {
            ReleasableBytesReference data = seqNoWriter.finish();
            columns.add(new BytesRefLongColumn(seqNoWriter.fieldName(), seqNoWriter.fieldType(), data, seqNoWriter.entryCount()));
            resources.add(data);
            seqNoWriter = null;
        }
        if (primaryTermWriter != null) {
            ReleasableBytesReference data = primaryTermWriter.finish();
            columns.add(
                new BytesRefLongColumn(primaryTermWriter.fieldName(), primaryTermWriter.fieldType(), data, primaryTermWriter.entryCount())
            );
            resources.add(data);
            primaryTermWriter = null;
        }
        if (versionWriter != null) {
            ReleasableBytesReference data = versionWriter.finish();
            columns.add(new BytesRefLongColumn(versionWriter.fieldName(), versionWriter.fieldType(), data, versionWriter.entryCount()));
            resources.add(data);
            versionWriter = null;
        }

        return new ElasticsearchBatch(numDocs, columns, Releasables.wrap(resources));
    }

    @Override
    public void close() {
        Releasables.close(seqNoWriter, primaryTermWriter, versionWriter);
        for (ColumnInfo info : userColumns) {
            Releasables.close(info.data);
        }
        if (idColumn != null) {
            Releasables.close(idColumn.data);
        }
    }

    private record ColumnInfo(String name, IndexableFieldType fieldType, boolean isLong, ReleasableBytesReference data, int entryCount) {}
}
