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
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;
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
 * the destination shard's row partition inside an {@link EirfEncoder}. There is one encoder per
 * {@link IndexAbstraction} (data stream or single index) encountered in the bulk; each encoder
 * fans rows out to many ({@link Index}, shard) partitions when a bulk straddles a data-stream
 * rollover.
 *
 * <p>Lifecycle: created at the start of a {@link BulkOperation#doRun() bulk run} only when
 * {@link #isBulkBatchEligible} returns true (every item in the bulk is structurally eligible for
 * EIRF encoding), used inside the initial-pass shard grouping, finalized via
 * {@link #finalizeBatches} just before per-shard {@code BulkShardRequest}s are constructed, and
 * {@link #close closed} when the bulk operation tears down.
 *
 * <p>Two encoding paths feed routing:
 * <ul>
 *   <li><b>Pre-resolved concrete index</b> — for non-data-stream targets, non-TSDB data streams,
 *       and TSDB writes that already carry {@link IndexRequest#getRawTimestamp()} from the ingest
 *       pipeline, the concrete write index is known before parsing. The routing strategy's
 *       {@link RoutingExtractor} is attached as a {@link EirfEncoder.LeafSink} during the parse,
 *       producing the shard id in the same pass.</li>
 *   <li><b>Deferred concrete index</b> — for TSDB data streams without a pre-extracted timestamp,
 *       the source is parsed first into scratch with a no-op sink. The {@code @timestamp} value
 *       is read from scratch via {@link EirfEncoder#stagedTimestampValue()} and used to pick the
 *       backing index; the routing strategy then computes the shard id by replaying scratch via
 *       {@link RoutingExtractor#computeShardIdFromScratch}. Routing strategies that operate on
 *       raw text ({@link IndexRouting.ExtractFromSource.ForRoutingPath}) cannot replay from
 *       scratch and trigger a per-item fall back to the inline-source path; in practice this
 *       only affects legacy TSDB indices created before
 *       {@code TSID_CREATED_DURING_ROUTING}.</li>
 * </ul>
 *
 * <p>Bulk-wide all-or-nothing: the decision to use EIRF encoding is made once for the whole bulk by
 * the pre-scan in {@link BulkOperation#doRun()}. If a runtime encoder failure happens mid-grouping
 * — typically because the source bytes that already passed {@code BulkRequestParser} validation
 * fail the encoder's full parse, or the routing extractor sees an array at a matched column —
 * {@link #tryEncodeAndRoute} signals that via {@link #disabled()} and the rest of the bulk goes
 * through the inline-source path. {@link #finalizeBatches} returns an empty map when disabled, so
 * previously-committed rows are simply discarded and items keep their inline source.
 */
final class BulkBatchEncoders implements Releasable {

    private static final Logger logger = LogManager.getLogger(BulkBatchEncoders.class);

    private static final class TargetState {
        final IndexAbstraction abstraction;
        final EirfEncoder encoder;
        /** Pending row attachments per destination (concreteIndex, shardId). Used by {@link #finalizeBatches}. */
        final Map<ShardId, List<PendingAttachment>> pendingByShard = new HashMap<>();

        TargetState(IndexAbstraction abstraction, EirfEncoder encoder) {
            this.abstraction = abstraction;
            this.encoder = encoder;
        }
    }

    private record PendingAttachment(IndexRequest indexRequest, int rowIndex) {}

    private final Map<String, TargetState> targets = new HashMap<>();
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
     * Per-item batch eligibility. Used by {@link #isBulkBatchEligible}; exposed for tests so the
     * pre-scan logic can be exercised in isolation.
     */
    static boolean isItemBatchEligible(IndexRequest request) {
        return request.indexSource().hasSource() && request.getContentType() != null && request.indexSource().hasEirfRow() == false;
    }

    /**
     * True after {@link #tryEncodeAndRoute} has hit a runtime encoder failure. Once disabled, the
     * helper still returns null for every subsequent item (so grouping can continue normally via
     * the inline-source path) and no batches are produced by {@link #finalizeBatches}.
     */
    boolean disabled() {
        return disabled;
    }

    /**
     * Encode {@code request} into the per-{@link IndexAbstraction} encoder, resolve the concrete
     * write index (using {@code @timestamp} extracted from scratch when this is a TSDB data stream
     * write without a pre-extracted timestamp), compute the destination shard, commit the staged
     * row to that shard's partition, and return the resulting {@link ShardId}.
     *
     * <p>Calls {@code preRoutingProcess}/{@code postRoutingProcess} on the request internally once
     * the concrete-index {@link IndexRouting} is known — callers must not duplicate those calls
     * for batched items.
     *
     * @param routingResolver bulk-scoped cache of {@link IndexRouting} per concrete index; supplied
     *                        by {@link BulkOperation}'s {@code ConcreteIndices} helper so the
     *                        per-index routing strategy is computed at most once.
     * @return the destination {@link ShardId}, or {@code null} if the item is not batchable — the
     *         caller must route the item via the inline-source path. {@code null} signals either
     *         a per-item fall back (e.g. routing strategy requires raw-text replay we can't do
     *         from scratch) or that the helper has been {@link #disabled()} by a prior failure.
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
        TargetState target = targets.computeIfAbsent(ia.getName(), k -> new TargetState(ia, new EirfEncoder()));
        EirfEncoder encoder = target.encoder;

        // Pre-resolve the concrete write index when it doesn't depend on the document contents.
        Index concreteIndex = tryPreResolveConcreteIndex(request, ia, project);

        // Decide whether to attach the routing extractor as a live LeafSink during the parse: only
        // when concreteIndex is known up front. Otherwise we parse with NO_OP and replay scratch
        // post-resolution.
        IndexRouting indexRouting = null;
        RoutingExtractor liveExtractor = null;
        if (concreteIndex != null) {
            indexRouting = routingResolver.apply(concreteIndex);
            // preProcess may auto-generate the document id; routing strategies that need the id
            // (IdAndRoutingOnly) read it during indexShard(...). Run before parseToScratch so
            // that the live extractor — if any — has a valid id by the time it's queried.
            request.preRoutingProcess(indexRouting);
            liveExtractor = indexRouting.newRoutingExtractor();
            if (liveExtractor != null) {
                liveExtractor.reset();
            }
        }

        EirfEncoder.LeafSink sink = liveExtractor != null ? liveExtractor : EirfEncoder.LeafSink.NO_OP;
        XContentType contentType = request.getContentType();
        try {
            encoder.parseToScratch(request.indexSource().bytes(), contentType, sink);
        } catch (Exception e) {
            // Either the source bytes failed the encoder's parse (rare — they already passed
            // BulkRequestParser validation), or the extractor threw because it can't handle the
            // input (e.g. an array at a matched routing column, or CBOR sources which the encoder
            // can't switch into duplicate-key-tolerant mode). Either way, abandon the entire
            // bulk's batch: items already committed are discarded by finalizeBatches returning
            // empty, and subsequent items skip encoding (see the disabled check at entry).
            logger.debug("EIRF encoding / routing extraction failed; abandoning batch for the rest of this bulk", e);
            disabled = true;
            return null;
        }

        // Deferred path: concrete index wasn't pre-resolved. Pull @timestamp from scratch (or
        // fall through to DataStream.getWriteIndex which will throw TimestampError if missing)
        // to pick the backing index, then materialize IndexRouting / extractor for it.
        if (concreteIndex == null) {
            DataStream dataStream = DataStream.resolveDataStream(ia, project);
            assert dataStream != null && dataStream.getIndexMode() == IndexMode.TIME_SERIES
                : "deferred path is only reachable for TSDB data streams; ia=" + ia.getName();
            Object rawTimestamp = request.getRawTimestamp();
            if (rawTimestamp == null) {
                rawTimestamp = encoder.stagedTimestampValue();
            }
            if (rawTimestamp == null) {
                // Let DataStream produce the canonical "missing @timestamp" error via its parsing
                // path. Returning null sends this item to the inline-source slow path, where
                // getWriteIndex(IndexRequest, ProjectMetadata) will throw TimestampError that the
                // BulkOperation loop already knows how to surface.
                return null;
            }
            concreteIndex = dataStream.selectTimeSeriesWriteIndexFromValue(rawTimestamp, project);
            indexRouting = routingResolver.apply(concreteIndex);
            request.preRoutingProcess(indexRouting);
            liveExtractor = indexRouting.newRoutingExtractor();
        }

        int shardNum;
        if (liveExtractor != null && sink == liveExtractor) {
            // Live sink ran during parse — the extractor has accumulated state and can produce
            // the shard id directly.
            try {
                shardNum = liveExtractor.computeShardId(request);
            } catch (RoutingExtractionException e) {
                logger.debug("EIRF routing computeShardId failed; abandoning batch for the rest of this bulk", e);
                disabled = true;
                return null;
            }
        } else if (liveExtractor != null) {
            // Deferred extraction: walk scratch. Only typed-mode strategies support this.
            // Raw-text strategies (ForRoutingPath) on a deferred-resolution path (TSDB without
            // rawTimestamp) fall back per-item; in practice this only affects legacy TSDB
            // indices created before TSID_CREATED_DURING_ROUTING.
            if (liveExtractor.passRawText()) {
                return null;
            }
            try {
                shardNum = liveExtractor.computeShardIdFromScratch(encoder, request);
            } catch (RoutingExtractionException e) {
                logger.debug("EIRF routing replay failed; abandoning batch for the rest of this bulk", e);
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
            int rowIndex = encoder.commitScratchTo(concreteIndex, shardNum);
            target.pendingByShard.computeIfAbsent(destShardId, k -> new ArrayList<>()).add(new PendingAttachment(request, rowIndex));
        } catch (Exception e) {
            // commitScratchTo failure indicates internal-state corruption (IO error on the
            // underlying stream). Surface it; the per-item catch in groupRequestsByShards turns
            // it into a per-item failure response.
            throw new IllegalStateException("Failed to commit EIRF row for item to shard " + destShardId, e);
        }
        return destShardId;
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

    /**
     * Build the EIRF batch for every shard that received committed rows, set the EIRF row reference
     * on each item routed there (replacing inline source bytes with a row reference), and return
     * the resulting batches keyed by ShardId. Returns an empty map when {@link #disabled()} is true.
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
                EirfBatch batch = target.encoder.buildPartition(shardId.getIndex(), shardId.getId());
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
            target.encoder.close();
        }
        targets.clear();
    }
}
