/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchScatterer;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Per-bulk router: decides each item's destination shard and builds the per-shard {@link SourceBatch}.
 *
 * <p>Both modes converge on the same post-scan flow in {@link #buildGrouping}: the columnar
 * batch-routing trio ({@code preProcess} / {@link IndexRouting#indexShard(IndexRequest[], SourceBatch)} /
 * {@code postProcess}) runs once per concrete index, shard ids are assigned, and batches are
 * scattered into per-shard slices. The only difference between modes is where the {@link EscfBatch}
 * comes from:
 * <ul>
 *   <li><b>Provided-batch mode</b>: the caller pre-built the batch; rows are recorded in
 *       {@link #route} and the batch is scattered in {@link #buildGrouping}.</li>
 *   <li><b>X-content mode</b>: {@link BulkBatchEncoders} parses each document into a single
 *       index-level partition during {@link #route}; {@link BulkBatchEncoders#finishRouting} runs
 *       the columnar trio and scatters in {@link #buildGrouping}.</li>
 * </ul>
 */
final class BatchModeRouter implements Releasable {

    // provided-batch mode state (null in x-content mode)
    // TODO: As a first pass we are restricting to a single concrete index. We will expand to multi index support.
    @Nullable
    private final String indexAbstractionName;
    @Nullable
    private final EscfBatch source;
    @Nullable
    private final int[] partitionIds;
    @Nullable
    private final BulkItemRequest[] items;
    @Nullable
    private Index concreteIndex;
    @Nullable
    private IndexRouting deferredRouting;
    private int shardCount;
    private int lastRow = -1;
    private int routedCount;
    private boolean groupingBuilt;
    private boolean scattered;

    // x-content mode state (null in provided-batch mode)
    @Nullable
    private final BulkBatchEncoders encoders;

    // Router-owned grouping (populated in route() for the inline-encode-disabled path and in
    // buildGrouping() for the columnar path). Shared across both modes.
    private final Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();

    // Shard batches produced by buildGrouping() in x-content mode; null until resolved.
    @Nullable
    private Map<ShardId, SourceBatch> xContentShardBatches;

    private BatchModeRouter(String indexAbstractionName, EscfBatch source) {
        this.indexAbstractionName = indexAbstractionName;
        this.source = source;
        this.partitionIds = new int[source.docCount()];
        this.items = new BulkItemRequest[source.docCount()];
        this.encoders = null;
    }

    private BatchModeRouter(BulkBatchEncoders encoders) {
        this.indexAbstractionName = null;
        this.source = null;
        this.partitionIds = null;
        this.items = null;
        this.encoders = encoders;
    }

    /** Returns the router for this bulk, or {@code null} when batch indexing does not apply. */
    @Nullable
    static BatchModeRouter create(BulkRequest bulkRequest, boolean batchIndexingSupported) {
        Map<String, SourceBatch> provided = bulkRequest.getPreBuiltBatches();
        boolean hasProvidedBatch = provided != null && provided.isEmpty() == false;

        if (hasProvidedBatch) {
            if (batchIndexingSupported == false) {
                throw new IllegalStateException(
                    "pre-built source batch submitted but batch indexing is not supported"
                        + " (setting disabled, feature flag off, or mixed-version cluster)"
                );
            }
            if (provided.size() > 1) {
                throw new IllegalArgumentException(
                    "pre-built source batch bulk carries "
                        + provided.size()
                        + " batches, but at most one is supported in step 1; multi-batch support will be added in a follow-up"
                );
            }
        } else if (batchIndexingSupported == false || bulkRequest.isSimulated() || bulkRequest.requests().isEmpty()) {
            return null;
        }

        // Single scan: both paths require all items to be IndexRequests; the provided-batch path
        // additionally requires every item to carry a source-row reference; the x-content path
        // requires each item to carry inline source with a known content type.
        for (DocWriteRequest<?> request : bulkRequest.requests()) {
            if (request instanceof IndexRequest indexRequest) {
                if (hasProvidedBatch) {
                    if (indexRequest.indexSource().hasSourceRow() == false) {
                        throw new IllegalArgumentException(
                            "item targeting index ["
                                + request.index()
                                + "] must carry a source-row reference when a pre-built batch is attached"
                        );
                    }
                } else if (BulkBatchEncoders.isItemBatchEligible(indexRequest) == false) {
                    return null;
                }
            } else {
                if (hasProvidedBatch) {
                    throw new IllegalArgumentException(
                        "["
                            + request.opType()
                            + "] operation on index ["
                            + request.index()
                            + "] cannot be mixed with pre-built source batches; every item of such a bulk must be an index"
                            + " request carrying a source-row reference"
                    );
                }
                return null;
            }
        }

        if (hasProvidedBatch) {
            Map.Entry<String, SourceBatch> only = provided.entrySet().iterator().next();
            String name = only.getKey();
            SourceBatch batch = only.getValue();
            if (batch instanceof EscfBatch escfBatch) {
                return new BatchModeRouter(name, escfBatch);
            }
            throw new IllegalArgumentException(
                "pre-built batch for index [" + name + "] must be an EscfBatch but was [" + batch.getClass().getName() + "]"
            );
        }

        return new BatchModeRouter(new BulkBatchEncoders());
    }

    /**
     * Records one item for batch routing. In x-content mode, encodes the item and — if encoding is
     * refused for the whole bulk — routes inline and adds directly to the router's
     * {@link #requestsByShard}. In provided-batch mode, records the item's row for deferred shard
     * assignment in {@link #buildGrouping}. Owns pre/post routing processing for the extractor
     * path (handled inside {@link BulkBatchEncoders#tryEncode}); the columnar path defers pre/post
     * to {@link #buildGrouping}.
     */
    void route(
        BulkItemRequest bulkItem,
        DocWriteRequest<?> request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        if (encoders != null) {
            if (encoders.tryEncode(bulkItem, (IndexRequest) request, concreteIndex, routing, project) == BulkBatchEncoders.NOT_BATCHABLE) {
                // Encoding was abandoned for this bulk; route inline, matching the non-batch path.
                request.preRoutingProcess(routing);
                int shardId = request.route(routing);
                request.postRoutingProcess(routing);
                requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(bulkItem);
            }
            return;
        }
        // Provided-batch mode
        prepareRouting((IndexRequest) request, abstraction, concreteIndex, routing, project);
        recordDeferredItem(bulkItem, ((IndexRequest) request).indexSource().rowIndex());
    }

    /**
     * Validates that this item can be accepted and binds the concrete index on the first item.
     * Must run before any routing in provided-batch mode.
     */
    private void prepareRouting(
        IndexRequest request,
        IndexAbstraction abstraction,
        Index concreteIndex,
        IndexRouting routing,
        ProjectMetadata project
    ) {
        if (indexAbstractionName.equals(abstraction.getName()) == false) {
            throw new IllegalArgumentException(
                "item targeting index ["
                    + request.index()
                    + "] carries a source-row reference but no pre-built batch was supplied under that name;"
                    + " batches must be keyed by the name set on the requests whose rows they hold"
            );
        }
        if (this.concreteIndex == null) {
            assignConcrete(concreteIndex, routing, project);
        } else if (this.concreteIndex.equals(concreteIndex) == false) {
            throw new IllegalArgumentException(
                "pre-built batch for ["
                    + indexAbstractionName
                    + "] resolved to concrete index ["
                    + concreteIndex.getName()
                    + "] in addition to ["
                    + this.concreteIndex.getName()
                    + "]; batches spanning multiple concrete indices (e.g. TSDB data streams with"
                    + " multiple backing indices) are not yet supported and will be added in a follow-up"
            );
        }
    }

    private void assignConcrete(Index index, IndexRouting routing, ProjectMetadata project) {
        if (routing instanceof IndexRouting.ExtractFromSource
            && routing instanceof IndexRouting.ExtractFromSource.ForIndexDimensions == false) {
            throw new IllegalArgumentException(
                "index ["
                    + index.getName()
                    + "] routes by extracting fields from _source, but this bulk supplies a pre-built source batch"
                    + " with no inline source; supply a pre-computed _tsid or use an index whose routing depends"
                    + " only on _id/_routing"
            );
        }
        shardCount = project.getIndexSafe(index).getNumberOfShards();
        concreteIndex = index;
        deferredRouting = routing;
    }

    private void recordDeferredItem(BulkItemRequest bulkItem, int rowIndex) {
        int docCount = source.docCount();
        if (rowIndex < 0 || rowIndex >= docCount) {
            throw new IllegalArgumentException(
                "rowIndex " + rowIndex + " is out of range [0, " + docCount + ") for pre-built batch [" + bulkItem.request().index() + "]"
            );
        }
        if (rowIndex <= lastRow) {
            throw new IllegalArgumentException(
                "rowIndex "
                    + rowIndex
                    + " is not strictly greater than the previous row "
                    + lastRow
                    + " of pre-built batch ["
                    + bulkItem.request().index()
                    + "]; rows must arrive in ascending order"
            );
        }
        partitionIds[rowIndex] = 0; // placeholder; real shard assigned in buildGrouping
        items[rowIndex] = bulkItem;
        lastRow = rowIndex;
        routedCount++;
    }

    /**
     * Resolves all deferred shard assignments and returns the final shard → items grouping. Both
     * modes run the columnar batch-routing trio here (or delegate to
     * {@link BulkBatchEncoders#finishRouting} which does so). Must be called exactly once, after
     * all {@link #route} calls.
     *
     * @param onItemFailure receives items for which the columnar routing pass failed and the
     *                      per-item fallback also failed, preserving per-item failure isolation
     */
    Map<ShardId, List<BulkItemRequest>> buildGrouping(BiConsumer<BulkItemRequest, Exception> onItemFailure) {
        if (encoders != null) {
            // X-content mode: BulkBatchEncoders runs the columnar trio and scatters.
            Map<ShardId, SourceBatch> shardBatches = new HashMap<>();
            encoders.finishRouting(requestsByShard, shardBatches, onItemFailure);
            xContentShardBatches = shardBatches;
            return requestsByShard;
        }
        // Provided-batch mode (mirrors the prototype's buildGrouping)
        return buildProvidedBatchGrouping();
    }

    private Map<ShardId, List<BulkItemRequest>> buildProvidedBatchGrouping() {
        if (groupingBuilt) {
            return requestsByShard;
        }
        groupingBuilt = true;
        if (routedCount == 0) {
            return requestsByShard;
        }
        if (routedCount != source.docCount()) {
            throw new IllegalStateException(
                "pre-built batch ["
                    + indexAbstractionName
                    + "] had "
                    + source.docCount()
                    + " rows but only "
                    + routedCount
                    + " were routed; dropped rows in pre-built batches are not yet supported and will be added in a follow-up"
            );
        }
        IndexRequest[] requests = buildRequestArray();
        deferredRouting.preProcess(requests);
        int[] shards = deferredRouting.indexShard(requests, source);
        deferredRouting.postProcess(requests);
        for (int i = 0; i < requests.length; i++) {
            int shardId = shards[i];
            partitionIds[i] = shardId;
            requestsByShard.computeIfAbsent(new ShardId(concreteIndex, shardId), k -> new ArrayList<>()).add(items[i]);
        }
        return requestsByShard;
    }

    private IndexRequest[] buildRequestArray() {
        IndexRequest[] requests = new IndexRequest[routedCount];
        for (int i = 0; i < routedCount; i++) {
            requests[i] = (IndexRequest) items[i].request();
        }
        return requests;
    }

    /**
     * Returns the per-shard batches. In provided-batch mode scatters on the first call and returns
     * empty on subsequent calls — the failure-store redirect pass must not re-scatter batches already
     * in flight. In x-content mode returns the map populated by {@link #buildGrouping}.
     */
    Map<ShardId, SourceBatch> shardBatches() {
        if (encoders != null) {
            // X-content mode: batches were built in buildGrouping(); return them once.
            Map<ShardId, SourceBatch> result = xContentShardBatches != null ? xContentShardBatches : Map.of();
            xContentShardBatches = null; // clear so the redirect pass gets an empty map
            return result;
        }
        return providedBatchShardBatches();
    }

    private Map<ShardId, SourceBatch> providedBatchShardBatches() {
        if (scattered || routedCount == 0) {
            return Map.of();
        }
        if (shardCount == 1) {
            // Single-shard: the source batch is already the shard batch; no scatter needed.
            return Map.of(new ShardId(concreteIndex, 0), source);
        }
        return scatter();
    }

    private Map<ShardId, SourceBatch> scatter() {
        scattered = true;
        EscfBatch[] parts;
        try (EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            parts = scatterer.scatter(source, partitionIds, shardCount);
        }
        Map<ShardId, SourceBatch> result = new HashMap<>();
        int[] nextRow = new int[shardCount];
        for (int row = 0; row < routedCount; row++) {
            int partition = partitionIds[row];
            EscfBatch part = parts[partition];
            assert part != null : "null partition " + partition + " for row " + row;
            result.putIfAbsent(new ShardId(concreteIndex, partition), part);
            IndexRequest req = (IndexRequest) items[row].request();
            req.indexSource().setSourceRow(part, nextRow[partition]++, req.indexSource().contentType());
        }
        return result;
    }

    /**
     * Verifies 1:1 alignment between shard items and their batch rows. The wire format rebuilds row
     * numbers from item ordinal, so misalignment would silently index the wrong source.
     */
    static void validateBatchAlignment(Map<ShardId, List<BulkItemRequest>> requestsByShard, Map<ShardId, SourceBatch> shardBatches) {
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            List<BulkItemRequest> items = entry.getValue();
            SourceBatch shardBatch = shardBatches.get(entry.getKey());
            if (shardBatch == null) {
                for (BulkItemRequest item : items) {
                    if (item.request() instanceof IndexRequest indexRequest && indexRequest.indexSource().hasSourceRow()) {
                        throw new IllegalStateException(
                            "item ["
                                + item.id()
                                + "] of shard ["
                                + entry.getKey()
                                + "] holds a source-row reference but its shard request has no batch attached;"
                                + " it would be indexed with an empty source"
                        );
                    }
                }
            } else if (BulkShardBatch.rowsAlignWithItems(shardBatch, items) == false) {
                throw new IllegalStateException(
                    "batch for shard ["
                        + entry.getKey()
                        + "] does not align with its items (batch rows: "
                        + shardBatch.docCount()
                        + ", items: "
                        + items.size()
                        + "); this indicates a bug in the scatter logic"
                );
            }
        }
    }

    @Override
    public void close() {
        if (encoders != null) {
            encoders.close();
        }
    }
}
