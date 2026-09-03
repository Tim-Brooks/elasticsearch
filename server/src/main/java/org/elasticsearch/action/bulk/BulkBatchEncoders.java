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
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.RoutingExtractor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.LeafSink;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceBatchEncoder;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Per-bulk helper that performs single-pass {@code XContent → ESCF} encoding into a single
 * index-level partition (partition 0) per concrete write index. Shard routing is deferred to
 * {@link #finishRouting}, which runs after all items have been encoded.
 *
 * <p>For indices whose routing strategy {@link IndexRouting#supportsBatchRouting() supports the
 * batch-routing contract} (currently {@code index.dimensions} / TSDB), the columnar trio
 * ({@code preProcess} / {@link IndexRouting#indexShard(IndexRequest[], SourceBatch)} /
 * {@code postProcess}) is invoked once per index in {@link #finishRouting}, computing tsids via
 * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator} without any source parsing
 * during the encode phase.
 *
 * <p>For indices that require a per-parse {@link RoutingExtractor} (currently {@code routing_path}
 * / LogsDB), the extractor is fed during the encode pass, its shard id recorded, and then routing
 * pre/post-processing is applied per item — the same single-pass flow as before, with rows
 * committing to partition 0 instead of the destination shard's partition.
 *
 * <p>Bulk-wide all-or-nothing: if a runtime encoder failure happens mid-grouping, {@link #disabled}
 * is set and subsequent items are routed inline by the caller. Items encoded before the failure
 * are handled in {@link #finishRouting} via per-item fallback (columnar path) or their
 * pre-computed shard ids (extractor path).
 */
final class BulkBatchEncoders implements Releasable {

    private static final Logger logger = LogManager.getLogger(BulkBatchEncoders.class);

    /** Sentinel returned from {@link #tryEncode} when the item cannot be batch-encoded. */
    static final int NOT_BATCHABLE = -1;

    /** All rows for a concrete index go into a single index-level partition before scatter. */
    private static final int INDEX_PARTITION = 0;

    private static final class IndexState {
        final SourceBatchEncoder encoder = new EscfEncoder();
        final IndexRouting routing;
        final Index concreteIndex;
        final int shardCount;
        /**
         * Non-null only for strategies that cannot use the columnar-routing trio
         * ({@code routing_path} indices where {@link IndexRouting#supportsBatchRouting()}
         * returns {@code false}). When null the columnar trio runs in {@link #finishRouting}.
         */
        @Nullable
        final RoutingExtractor extractor;
        /** Items in row-commit order; {@code items.get(i)} is at row {@code i} in the encoder. */
        final List<BulkItemRequest> items = new ArrayList<>();
        /**
         * Per-row destination shard id. For the extractor path, populated in {@link #tryEncode};
         * for the columnar path, populated by the columnar trio in {@link #finishRouting}.
         */
        int[] shardIds = new int[16];

        IndexState(IndexRouting routing, Index concreteIndex, int shardCount) {
            this.routing = routing;
            this.concreteIndex = concreteIndex;
            this.shardCount = shardCount;
            this.extractor = routing.supportsBatchRouting() ? null : routing.newRoutingExtractor();
        }
    }

    private final Map<Index, IndexState> indexStates = new HashMap<>();
    private boolean disabled;
    private boolean closed;

    /**
     * Per-item batch eligibility. An item is eligible if it has inline source bytes, a known content
     * type, and does not already carry a source-row reference (which would indicate a pre-built batch
     * path instead).
     */
    static boolean isItemBatchEligible(IndexRequest request) {
        return request.indexSource().hasSource() && request.getContentType() != null && request.indexSource().hasSourceRow() == false;
    }

    /**
     * True after {@link #tryEncode} has hit a runtime encoder failure. Once disabled, the
     * helper still accepts items (returning {@link #NOT_BATCHABLE}) so the router can forward them
     * to inline routing; any rows already committed are handled in {@link #finishRouting}.
     */
    boolean disabled() {
        return disabled;
    }

    /**
     * Encode {@code request} into the per-concrete-index encoder, committing the staged row to a
     * single index-level partition. For routing strategies that need a per-parse
     * {@link RoutingExtractor}, the shard id is also computed and stored here so that routing
     * pre/post processing can be applied. For strategies that support the columnar batch-routing
     * contract ({@link IndexRouting#supportsBatchRouting()}), shard assignment is deferred to
     * {@link #finishRouting}.
     *
     * @return the row index within this index's encoder on success, or {@link #NOT_BATCHABLE} if
     *         encoding failed — in which case {@link #disabled()} becomes {@code true} and the
     *         caller must route the item inline.
     */
    int tryEncode(BulkItemRequest item, IndexRequest request, Index concreteIndex, IndexRouting routing, ProjectMetadata project) {
        if (disabled) {
            return NOT_BATCHABLE;
        }
        IndexState state = indexStates.computeIfAbsent(
            concreteIndex,
            idx -> new IndexState(routing, concreteIndex, project.getIndexSafe(concreteIndex).getNumberOfShards())
        );
        LeafSink sink;
        if (state.extractor != null) {
            state.extractor.reset();
            sink = state.extractor;
        } else {
            sink = LeafSink.NO_OP;
        }
        try {
            state.encoder.parseToScratch(request.indexSource().bytes(), request.getContentType(), sink);
        } catch (Exception e) {
            // Either the source bytes failed the encoder's parse (rare — they already passed
            // BulkRequestParser validation), or the extractor threw because it can't handle the
            // input (e.g. an array at a matched routing column). Either way, abandon the entire
            // bulk's batch encoding.
            logger.debug("batch encoding / routing extraction failed; abandoning batch for the rest of this bulk", e);
            disabled = true;
            return NOT_BATCHABLE;
        }
        int rowIndex;
        try {
            rowIndex = state.encoder.commitScratchTo(INDEX_PARTITION);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to commit batch row for item to partition " + INDEX_PARTITION, e);
        }
        assert rowIndex == state.items.size() : "row index mismatch: expected " + state.items.size() + " but got " + rowIndex;
        if (state.extractor != null) {
            // Extractor path (routing_path / LogsDB): compute shard from the hash accumulated during
            // parse, applying pre/post processing now so the routing hash or time-based id is set
            // before the item leaves tryEncode.
            request.preRoutingProcess(routing);
            int shardId = state.extractor.computeShardId(request);
            request.postRoutingProcess(routing);
            if (rowIndex >= state.shardIds.length) {
                state.shardIds = ArrayUtil.grow(state.shardIds, rowIndex + 1);
            }
            state.shardIds[rowIndex] = shardId;
        }
        state.items.add(item);
        return rowIndex;
    }

    /**
     * Build the per-index batches, run shard routing for indices that support the columnar contract,
     * scatter into per-shard batches, attach source-row references on items, and populate
     * {@code requestsByShard} and {@code shardBatchesOut}. Items whose index falls back to per-item
     * routing (on a columnar-routing failure) are routed via their inline source and forwarded to
     * {@code onItemFailure} if routing itself fails.
     *
     * <p>When {@link #disabled()} is true, rows committed before the failure are handled: extractor-
     * path rows use their pre-computed shard ids (re-routing via inline source would violate
     * preconditions set by {@code postProcess}); columnar-path rows are routed per-item from inline
     * source since no routing work was done on them.
     */
    void finishRouting(
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        Map<ShardId, SourceBatch> shardBatchesOut,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        if (disabled) {
            for (IndexState state : indexStates.values()) {
                finishDisabledState(state, requestsByShard, onItemFailure);
            }
            return;
        }
        for (IndexState state : indexStates.values()) {
            finishActiveState(state, requestsByShard, shardBatchesOut, onItemFailure);
        }
    }

    private void finishDisabledState(
        IndexState state,
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        if (state.items.isEmpty()) {
            return;
        }
        if (state.extractor != null) {
            // Routing was done per-item in tryEncode (preRoutingProcess + computeShardId + postRoutingProcess).
            // Cannot call route() again: checkNoRouting would reject the routing hash already embedded.
            // Use the pre-computed shard ids directly; items retain inline source (setSourceRow not called).
            for (int row = 0; row < state.items.size(); row++) {
                int shardId = state.shardIds[row];
                requestsByShard.computeIfAbsent(new ShardId(state.concreteIndex, shardId), k -> new ArrayList<>())
                    .add(state.items.get(row));
            }
        } else {
            // Columnar path: no routing work was done yet. Route per-item from inline source.
            routePerItem(state, requestsByShard, onItemFailure, false);
        }
    }

    private void finishActiveState(
        IndexState state,
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        Map<ShardId, SourceBatch> shardBatchesOut,
        BiConsumer<BulkItemRequest, Exception> onItemFailure
    ) {
        if (state.items.isEmpty()) {
            return;
        }
        // preProcessed tracks whether routing pre-processing has been applied to the items
        // so that routePerItem can skip re-applying preRoutingProcess on fallback.
        // For the extractor path it was done per-item in tryEncode, so starts true.
        boolean preProcessed = state.extractor != null;
        EscfBatch batch = null;
        try {
            batch = (EscfBatch) state.encoder.buildPartition(INDEX_PARTITION);
            if (state.extractor == null) {
                // Columnar path (TSDB / index.dimensions): run the batch-routing trio.
                IndexRequest[] requests = indexRequests(state.items);
                state.routing.preProcess(requests);
                preProcessed = true;
                int[] shards = state.routing.indexShard(requests, batch);
                state.routing.postProcess(requests);
                if (state.shardIds.length < shards.length) {
                    state.shardIds = ArrayUtil.grow(state.shardIds, shards.length);
                }
                System.arraycopy(shards, 0, state.shardIds, 0, shards.length);
            }
            validateShardIds(state);
            if (state.shardCount == 1) {
                // Single-shard fast path: no scatter needed; transfer batch ownership to shardBatchesOut.
                ShardId shardId = new ShardId(state.concreteIndex, 0);
                List<BulkItemRequest> list = requestsByShard.computeIfAbsent(shardId, k -> new ArrayList<>());
                int n = state.items.size();
                for (int row = 0; row < n; row++) {
                    list.add(state.items.get(row));
                    ((IndexRequest) state.items.get(row).request()).indexSource().setSourceRow(batch, row);
                }
                shardBatchesOut.put(shardId, batch);
                batch = null; // ownership transferred — do not close
            } else {
                scatterAndAttach(state, batch, requestsByShard, shardBatchesOut);
                batch = null; // closed inside scatterAndAttach after scatter
            }
        } catch (Exception e) {
            Releasables.close(batch); // null-safe; no-op if already transferred or closed
            if (state.extractor != null) {
                // Extractor path: routing was done in tryEncode; cannot re-route via inline source.
                // Fall back by grouping on pre-computed shard ids (no batch for this index).
                logger.debug(
                    () -> "batch scatter failed for index ["
                        + state.concreteIndex.getName()
                        + "]; falling back to pre-computed shard grouping (no batch)",
                    e
                );
                for (int row = 0; row < state.items.size(); row++) {
                    requestsByShard.computeIfAbsent(new ShardId(state.concreteIndex, state.shardIds[row]), k -> new ArrayList<>())
                        .add(state.items.get(row));
                }
            } else {
                logger.debug(
                    () -> "columnar batch routing failed for index ["
                        + state.concreteIndex.getName()
                        + "]; per-item fallback from inline source",
                    e
                );
                routePerItem(state, requestsByShard, onItemFailure, preProcessed);
            }
        }
    }

    private static void scatterAndAttach(
        IndexState state,
        EscfBatch batch,
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        Map<ShardId, SourceBatch> shardBatchesOut
    ) {
        int n = state.items.size();
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(batch, state.shardIds, state.shardCount);
        } finally {
            // Scatterer copied bytes out; close the index-level batch. Parts are owned by shardBatchesOut.
            batch.close();
        }
        int[] nextRow = new int[state.shardCount];
        for (int row = 0; row < n; row++) {
            int shard = state.shardIds[row];
            ShardId shardId = new ShardId(state.concreteIndex, shard);
            shardBatchesOut.putIfAbsent(shardId, parts[shard]);
            requestsByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(state.items.get(row));
            ((IndexRequest) state.items.get(row).request()).indexSource().setSourceRow(parts[shard], nextRow[shard]++);
        }
    }

    private static void validateShardIds(IndexState state) {
        for (int row = 0; row < state.items.size(); row++) {
            int shardId = state.shardIds[row];
            if (shardId < 0 || shardId >= state.shardCount) {
                throw new IllegalArgumentException(
                    "shard id "
                        + shardId
                        + " at row "
                        + row
                        + " is outside valid range [0, "
                        + state.shardCount
                        + ") for index ["
                        + state.concreteIndex.getName()
                        + "]"
                );
            }
        }
    }

    /**
     * Per-item fallback for the columnar path: route each item from its inline source. Items whose
     * routing fails are forwarded to {@code onItemFailure} rather than throwing, preserving the
     * per-item isolation of the non-batch routing path.
     *
     * @param preProcessed if true, {@code preRoutingProcess} was already called on these items
     *                     (e.g. the columnar trio ran {@code preProcess} before failing); skip
     *                     calling it again to avoid violating the "id == null" precondition.
     */
    private static void routePerItem(
        IndexState state,
        Map<ShardId, List<BulkItemRequest>> requestsByShard,
        BiConsumer<BulkItemRequest, Exception> onItemFailure,
        boolean preProcessed
    ) {
        for (BulkItemRequest bulkItem : state.items) {
            IndexRequest request = (IndexRequest) bulkItem.request();
            try {
                if (preProcessed == false) {
                    request.preRoutingProcess(state.routing);
                }
                int shardId = request.route(state.routing);
                request.postRoutingProcess(state.routing);
                requestsByShard.computeIfAbsent(new ShardId(state.concreteIndex, shardId), k -> new ArrayList<>()).add(bulkItem);
            } catch (Exception ex) {
                onItemFailure.accept(bulkItem, ex);
            }
        }
    }

    private static IndexRequest[] indexRequests(List<BulkItemRequest> items) {
        IndexRequest[] requests = new IndexRequest[items.size()];
        for (int i = 0; i < items.size(); i++) {
            requests[i] = (IndexRequest) items.get(i).request();
        }
        return requests;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (IndexState state : indexStates.values()) {
            state.encoder.close();
        }
        indexStates.clear();
    }
}
