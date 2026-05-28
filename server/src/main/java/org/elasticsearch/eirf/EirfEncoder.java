/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Facade over {@link EirfDocumentParser}, {@link BufferedRow}, {@link EirfPartitionWriter}, and
 * {@link ColumnPathCache} for single-schema encoding sessions.
 *
 * <p>Two usage modes are supported:
 *
 * <p><b>Single partition</b>:
 * <pre>
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.addDocument(source1, XContentType.JSON, 0);
 *     encoder.addDocument(source2, XContentType.JSON, 0);
 *     EirfBatch batch = encoder.buildPartition(0);
 * }
 * </pre>
 *
 * <p><b>Multi-partition</b>:
 * <pre>
 * try (EirfEncoder encoder = new EirfEncoder()) {
 *     encoder.parseToScratch(source, XContentType.JSON, sink);
 *     int rowIndex = encoder.commitScratchTo(shardId);
 *     EirfBatch shardBatch = encoder.buildPartition(shardId);
 * }
 * </pre>
 *
 * @see EirfDocumentParser
 * @see EirfPartitionWriter
 * @see EirfPartitionWriter
 */
public class EirfEncoder implements Releasable {

    /**
     * Sentinel {@link Index} used as the partition map key for the single-index convenience APIs
     * ({@link #commitScratchTo(int)}, {@link #buildPartition(int)}).
     */
    static final Index DEFAULT_INDEX = new Index("_eirf_default_", "_na_");

    /**
     * Top-level data-stream timestamp field. Tracked as a literal (rather than imported from
     * {@code DataStream.TIMESTAMP_FIELD_NAME}) to keep the EIRF package independent of the
     * {@code cluster.metadata} package.
     */
    private static final String TIMESTAMP_FIELD = "@timestamp";

    private final EirfSchema schema;
    private final BufferedRow row;
    private final EirfPartitionWriter writer;
    private final ColumnPathCache pathCache;
    /** True after {@link #parseToScratch} returns and before {@link #commitScratchTo} is called. */
    private boolean rowStaged;
    /**
     * Leaf column index for the top-level {@code @timestamp} field, or {@code -1} until the
     * field appears in the cumulative schema.
     */
    private int timestampColumnIndex = -1;

    public EirfEncoder() {
        this.schema = new EirfSchema();
        this.row = new BufferedRow();
        this.writer = new EirfPartitionWriter(schema);
        this.pathCache = new ColumnPathCache();
    }

    /**
     * Adds a single document to the default partition identified by {@code partition}.
     * Equivalent to {@code parseToScratch(source, xContentType, LeafSink.NO_OP); commitScratchTo(partition)}.
     */
    public void addDocument(BytesReference source, XContentType xContentType, int partition) throws IOException {
        parseToScratch(source, xContentType, LeafSink.NO_OP);
        commitScratchTo(partition);
    }

    /**
     * Parses {@code source} into the encoder's per-document row and fires {@code sink} for every
     * primitive leaf value. The parsed row is held until the next {@link #commitScratchTo(int)} call.
     *
     * <p>Calling this method twice without an intervening {@code commitScratchTo} discards the
     * previously staged row.
     */
    public void parseToScratch(BytesReference source, XContentType xContentType, LeafSink sink) throws IOException {
        row.reset(schema.leafCount());
        EirfDocumentParser.parseXContent(source, xContentType, schema, row, sink, pathCache);
        if (timestampColumnIndex < 0) {
            timestampColumnIndex = schema.findLeaf(TIMESTAMP_FIELD, 0);
        }
        rowStaged = true;
    }

    /**
     * Flushes the staged row into the partition identified by {@code partitionKey} under the
     * {@link #DEFAULT_INDEX} sentinel.
     *
     * @throws IllegalStateException if no row is currently staged.
     */
    public int commitScratchTo(int partitionKey) throws IOException {
        return commitScratchTo(DEFAULT_INDEX, partitionKey);
    }

    /**
     * Flushes the staged row into the partition identified by ({@code concreteIndex},
     * {@code shardNum}), returning the row's index within that partition.
     *
     * @throws IllegalStateException if no row is currently staged.
     */
    public int commitScratchTo(Index concreteIndex, int shardNum) throws IOException {
        if (rowStaged == false) {
            throw new IllegalStateException("commitScratchTo called without a staged row");
        }
        int rowIndex = writer.commit(row, concreteIndex, shardNum);
        rowStaged = false;
        return rowIndex;
    }

    /**
     * Builds an {@link EirfBatch} for the partition identified by {@code partitionKey} under
     * the {@link #DEFAULT_INDEX} sentinel.
     */
    public EirfBatch buildPartition(int partitionKey) {
        return buildPartition(DEFAULT_INDEX, partitionKey);
    }

    /**
     * Builds an {@link EirfBatch} for the partition identified by ({@code concreteIndex},
     * {@code shardNum}). Producing a batch consumes that partition's row data.
     */
    public EirfBatch buildPartition(Index concreteIndex, int shardNum) {
        return writer.buildPartition(concreteIndex, shardNum);
    }

    /**
     * Returns the number of rows committed to the partition identified by {@code partitionKey}
     * under the {@link #DEFAULT_INDEX} sentinel.
     */
    public int docCount(int partitionKey) {
        return writer.docCount(DEFAULT_INDEX, partitionKey);
    }

    /**
     * Returns true if at least one row has been committed to the partition identified by
     * {@code partitionKey} under the {@link #DEFAULT_INDEX} sentinel.
     */
    public boolean hasPartition(int partitionKey) {
        return writer.hasPartition(DEFAULT_INDEX, partitionKey);
    }

    /**
     * Returns the dotted path for the given leaf column. Result is memoized: callers may use the
     * column index as a stable key for their own per-column state.
     */
    public String columnPath(int columnIndex) {
        return pathCache.get(columnIndex, schema);
    }

    @Override
    public void close() {
        writer.close();
    }

    /** Convenience method: encodes a list of sources into a single-partition batch. */
    public static EirfBatch encode(List<BytesReference> sources, XContentType xContentType) throws IOException {
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BytesReference source : sources) {
                encoder.addDocument(source, xContentType, 0);
            }
            return encoder.buildPartition(0);
        }
    }
}
