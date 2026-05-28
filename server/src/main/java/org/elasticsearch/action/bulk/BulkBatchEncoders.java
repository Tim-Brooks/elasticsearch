/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingExtractionException;
import org.elasticsearch.cluster.routing.RoutingExtractor;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.eirf.ColumnPathCache;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfDocumentParser;
import org.elasticsearch.eirf.EirfPartitionWriter;
import org.elasticsearch.eirf.BufferedRow;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.eirf.LeafSink;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Per-bulk helper that performs single-pass {@code XContent → EIRF} encoding, resolves the
 * concrete write index, and computes shard routing — accumulating one row per item directly into
 * the destination shard's row partition.
 *
 * <p>There is one {@link TargetState} per {@link IndexAbstraction} encountered in the bulk. Each
 * target owns a cumulative {@link EirfSchema}, a {@link ColumnPathCache}, and an
 * {@link EirfPartitionWriter} that fans rows out to many ({@link Index}, shard) partitions. A
 * single {@link BufferedRow} is shared across all targets for the lifetime of the session; it is reset
 * before each parse and its arrays grow to the largest schema seen.
 *
 * <p>Per-concrete-index {@link RoutingExtractor} instances are cached inside each target so their
 * column-level bitmask — which maps column indices to "matches routing predicate" — is reused
 * across all documents targeting the same concrete index. Only newly added schema columns trigger
 * a fresh {@link RoutingExtractor#matchesField} call; existing columns are answered from the
 * bitmask in O(1).
 *
 * <p>Two encoding paths feed routing:
 * <ul>
 *   <li><b>Pre-resolved concrete index</b> — for non-data-stream targets, non-TSDB data streams,
 *       and TSDB writes that already carry {@link IndexRequest#getRawTimestamp()} from the ingest
 *       pipeline, the concrete write index is known before parsing. The cached
 *       {@link RoutingExtractor} is attached as a {@link LeafSink} during the parse.</li>
 *   <li><b>Deferred concrete index</b> — for TSDB data streams without a pre-extracted timestamp,
 *       the source is parsed first with a no-op sink. The {@code @timestamp} value is read from
 *       the row via {@link BufferedRow#readTimestamp(int)} and used to pick the backing index; the
 *       routing extractor then computes the shard id by replaying the row via
 *       {@link BufferedRow#replayTo}. Routing strategies that operate on raw text
 *       ({@link IndexRouting.ExtractFromSource.ForRoutingPath}) cannot replay from the row and
 *       trigger a per-item fall back to the inline-source path.</li>
 * </ul>
 *
 * <p>Bulk-wide all-or-nothing: if a runtime encoder failure happens mid-grouping,
 * {@link #tryEncodeAndRoute} signals that via {@link #disabled()} and subsequent items skip
 * encoding. {@link #finalizeBatches} returns an empty map when disabled.
 */
final class BulkBatchEncoders implements Releasable {

    private static final Logger logger = LogManager.getLogger(BulkBatchEncoders.class);

    /** State held per {@link IndexAbstraction} encountered in the bulk. */
    private static final class TargetState {
        final IndexAbstraction abstraction;
        /** Cumulative schema for this index abstraction, growing across all documents in the bulk. */
        final EirfSchema schema = new EirfSchema();
        /** Memoized column-index-to-dotted-path mapping, shared with the parser and extractors. */
        final ColumnPathCache pathCache = new ColumnPathCache();
        /** Partition writer for per-(concreteIndex, shard) output buffers. */
        final EirfPartitionWriter writer;
        /**
         * Cached extractor per concrete write index. Preserves the column-level bitmask across
         * documents so {@link RoutingExtractor#matchesField} is invoked at most once per column.
         */
        final Map<Index, RoutingExtractor> extractorByIndex = new HashMap<>();
        /**
         * Leaf column index of the {@code @timestamp} field in this target's schema, or {@code -1}
         * until the field first appears.
         */
        int timestampColumnIndex = -1;
        /** Pending row attachments per destination shard, used by {@link #finalizeBatches}. */
        final Map<ShardId, List<PendingAttachment>> pendingByShard = new HashMap<>();

        TargetState(IndexAbstraction abstraction) {
            this.abstraction = abstraction;
            this.writer = new EirfPartitionWriter(schema);
        }
    }

    private record PendingAttachment(IndexRequest indexRequest, int rowIndex) {}

    private final Map<String, TargetState> targets = new HashMap<>();
    /** Shared row for the bulk session; grows to accommodate the largest schema seen. */
    private final BufferedRow row = new BufferedRow();
    private boolean disabled;
    private boolean closed;

    /**
     * Returns true if every item in {@code bulkRequest} is structurally eligible to be EIRF-encoded:
     * an {@link IndexRequest} with inline source bytes, a known content type, and no pre-attached
     * EIRF row. If false, the bulk goes through the inline-source path end-to-end and no encoder
     * helper is created.
     */
    static boolean isBulkBatchEligible(BulkRequest bulkRequest) {
        if (bulkRequest.isSimulated()) {
            return false;
        }
        for (DocWriteRequest<?> request : bulkRequest.requests) {
            if (request instanceof IndexRequest indexRequest) {
                if (isItemBatchEligible(indexRequest) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return bulkRequest.requests.isEmpty() == false;
    }

    /**
     * Per-item batch eligibility. Exposed for tests so the pre-scan logic can be exercised in
     * isolation.
     */
    static boolean isItemBatchEligible(IndexRequest request) {
        return request.indexSource().hasSource() && request.getContentType() != null && request.indexSource().hasEirfRow() == false;
    }

    /**
     * True after {@link #tryEncodeAndRoute} has hit a runtime encoder failure. Once disabled, the
     * helper returns null for every subsequent item and no batches are produced by
     * {@link #finalizeBatches}.
     */
    boolean disabled() {
        return disabled;
    }

    /**
     * Encode {@code request} into the per-{@link IndexAbstraction} target, resolve the concrete
     * write index, compute the destination shard, commit the row to that shard's partition, and
     * return the resulting {@link ShardId}.
     *
     * <p>Calls {@code preRoutingProcess}/{@code postRoutingProcess} on the request internally.
     * Callers must not duplicate those calls for batched items.
     *
     * @return the destination {@link ShardId}, or {@code null} if the item is not batchable — the
     *         caller must route the item via the inline-source path.
     */
    ShardId tryEncodeAndRoute(
        IndexRequest request,
        IndexAbstraction ia,
        ProjectMetadata project,
        Function<Index, IndexRouting> routingResolver
    ) {
        if (disabled) {
            return null;
        }
        TargetState target = targets.computeIfAbsent(ia.getName(), k -> new TargetState(ia));

        // Pre-resolve the concrete write index when it doesn't depend on the document contents.
        Index concreteIndex = tryPreResolveConcreteIndex(request, ia, project);

        // When the concrete index is known up front, attach the cached routing extractor as a live
        // LeafSink so routing data is accumulated in a single parse pass.
        IndexRouting indexRouting = null;
        RoutingExtractor liveExtractor = null;
        if (concreteIndex != null) {
            indexRouting = routingResolver.apply(concreteIndex);
            // preRoutingProcess may auto-generate the document id; routing strategies that need the
            // id read it during computeShardId. Run before the parse so the extractor has a valid id.
            request.preRoutingProcess(indexRouting);
            liveExtractor = getOrCreateExtractor(target, concreteIndex, indexRouting);
            if (liveExtractor != null) {
                liveExtractor.reset();
            }
        }

        LeafSink sink = liveExtractor != null ? liveExtractor : LeafSink.NO_OP;
        XContentType contentType = request.getContentType();

        // Parse the source into the shared row, growing the target's schema as new fields appear.
        row.reset(target.schema.leafCount());
        try {
            EirfDocumentParser.parseXContent(request.indexSource().bytes(), contentType, target.schema, row, sink, target.pathCache);
        } catch (Exception e) {
            // Parse failure (rare — source already passed BulkRequestParser validation) or extractor
            // threw (e.g. array at a matched routing column, or CBOR in duplicate-key mode).
            // Abandon the entire bulk's batch.
            logger.debug("EIRF encoding / routing extraction failed; abandoning batch for the rest of this bulk", e);
            disabled = true;
            return null;
        }

        // Deferred path: concrete index wasn't pre-resolved. Read @timestamp from the row to pick
        // the backing index, then materialize IndexRouting / extractor for it.
        if (concreteIndex == null) {
            if (target.timestampColumnIndex < 0) {
                target.timestampColumnIndex = target.schema.findLeaf("@timestamp", 0);
            }
            Object rawTimestamp = request.getRawTimestamp();
            if (rawTimestamp == null && target.timestampColumnIndex >= 0) {
                rawTimestamp = row.readTimestamp(target.timestampColumnIndex);
            }
            if (rawTimestamp == null) {
                // Let DataStream produce the canonical "missing @timestamp" error via its parsing path.
                return null;
            }
            DataStream dataStream = DataStream.resolveDataStream(ia, project);
            assert dataStream != null && dataStream.getIndexMode() == IndexMode.TIME_SERIES
                : "deferred path is only reachable for TSDB data streams; ia=" + ia.getName();
            concreteIndex = dataStream.selectTimeSeriesWriteIndexFromValue(rawTimestamp, project);
            indexRouting = routingResolver.apply(concreteIndex);
            request.preRoutingProcess(indexRouting);
            liveExtractor = getOrCreateExtractor(target, concreteIndex, indexRouting);
            if (liveExtractor != null) {
                if (liveExtractor.passRawText()) {
                    // Legacy TSDB (ForRoutingPath): raw-text hashing cannot be replayed from the row
                    // because numeric/boolean bytes were not retained. Fall back per-item.
                    return null;
                }
                liveExtractor.reset();
                row.replayTo(target.schema.leafCount(), target.schema, target.pathCache, liveExtractor);
            }
        }

        int shardNum;
        if (liveExtractor != null) {
            try {
                shardNum = liveExtractor.computeShardId(request);
            } catch (RoutingExtractionException e) {
                logger.debug("EIRF routing computeShardId failed; abandoning batch for the rest of this bulk", e);
                disabled = true;
                return null;
            }
        } else {
            // No extractor — id-and-routing-only strategies. id was set by preRoutingProcess above.
            shardNum = indexRouting.indexShard(request);
        }

        request.postRoutingProcess(indexRouting);

        ShardId destShardId = new ShardId(concreteIndex, shardNum);
        try {
            int rowIndex = target.writer.commit(row, concreteIndex, shardNum);
            target.pendingByShard.computeIfAbsent(destShardId, k -> new ArrayList<>()).add(new PendingAttachment(request, rowIndex));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to commit EIRF row for item to shard " + destShardId, e);
        }
        return destShardId;
    }

    /**
     * Builds the EIRF batch for every shard that received committed rows, attaches the EIRF row
     * reference to each item, and returns the batches keyed by ShardId. Returns an empty map when
     * {@link #disabled()} is true.
     */
    Map<ShardId, EirfBatch> finalizeBatches() {
        if (disabled) {
            return Collections.emptyMap();
        }
        Map<ShardId, EirfBatch> batchesByShard = new HashMap<>();
        for (TargetState target : targets.values()) {
            for (Map.Entry<ShardId, List<PendingAttachment>> entry : target.pendingByShard.entrySet()) {
                List<PendingAttachment> pending = entry.getValue();
                if (pending.isEmpty()) {
                    continue;
                }
                ShardId shardId = entry.getKey();
                EirfBatch batch = target.writer.buildPartition(shardId.getIndex(), shardId.getId());
                batchesByShard.put(shardId, batch);
                for (PendingAttachment attachment : pending) {
                    attachment.indexRequest.indexSource().setEirfRow(batch, attachment.rowIndex);
                }
            }
        }
        return batchesByShard;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (TargetState target : targets.values()) {
            target.writer.close();
        }
        targets.clear();
    }

    /**
     * Returns the cached {@link RoutingExtractor} for the given concrete index, creating and
     * caching it on the first call. Returns null if the routing strategy doesn't use a
     * source-inspecting extractor.
     *
     * <p>The cached extractor is bound to this target's schema column space. Its per-column
     * "matched" bitmask is preserved across documents so {@link RoutingExtractor#matchesField} is
     * called at most once per column per concrete index over the lifetime of the bulk.
     */
    private static RoutingExtractor getOrCreateExtractor(TargetState target, Index concreteIndex, IndexRouting indexRouting) {
        return target.extractorByIndex.computeIfAbsent(concreteIndex, k -> indexRouting.newRoutingExtractor());
    }

    /**
     * Returns the concrete write index for {@code request} when it can be determined without
     * inspecting the document source, or {@code null} when the abstraction requires the document's
     * {@code @timestamp} to pick a backing index. The deferred case is exactly TSDB data streams
     * with an op type of {@code create} and no {@link IndexRequest#getRawTimestamp()} already set
     * by the ingest pipeline.
     */
    private static Index tryPreResolveConcreteIndex(IndexRequest request, IndexAbstraction ia, ProjectMetadata project) {
        if (request.isWriteToFailureStore() == false
            && request.opType() == DocWriteRequest.OpType.CREATE
            && request.getRawTimestamp() == null) {
            DataStream dataStream = DataStream.resolveDataStream(ia, project);
            if (dataStream != null && dataStream.getIndexMode() == IndexMode.TIME_SERIES) {
                return null;
            }
        }
        return request.getConcreteWriteIndex(ia, project);
    }
}
