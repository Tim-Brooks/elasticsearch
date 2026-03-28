/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.RowBatchDocumentParser;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

import static org.elasticsearch.core.Strings.format;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = TransportBulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

    // Represents the maximum memory overhead factor for an operation when processed for indexing.
    // This accounts for potential increases in memory usage due to document expansion, including:
    // 1. If the document source is not stored in a contiguous byte array, it will be copied to ensure contiguity.
    // 2. If the document contains strings, Jackson uses char arrays (2 bytes per character) to parse string fields, doubling memory usage.
    // 3. Parsed string fields create new copies of their data, further increasing memory consumption.
    private static final int MAX_EXPANDED_OPERATION_MEMORY_OVERHEAD_FACTOR = 4;

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final Consumer<Runnable> postWriteAction;

    private final DocumentParsingProvider documentParsingProvider;

    @Inject
    public TransportShardBulkAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        MappingUpdatedAction mappingUpdatedAction,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        ProjectResolver projectResolver,
        DocumentParsingProvider documentParsingProvider
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BulkShardRequest::new,
            BulkShardRequest::new,
            ExecutorSelector.getWriteExecutorForShard(threadPool),
            PrimaryActionExecution.RejectOnOverload,
            indexingPressure,
            systemIndices,
            projectResolver,
            ReplicaActionExecution.SubjectToCircuitBreaker
        );
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.postWriteAction = WriteAckDelay.create(settings, threadPool);
        this.documentParsingProvider = documentParsingProvider;
    }

    private static final TransportRequestOptions TRANSPORT_REQUEST_OPTIONS = TransportRequestOptions.of(
        null,
        TransportRequestOptions.Type.BULK
    );

    @Override
    protected TransportRequestOptions transportOptions() {
        return TRANSPORT_REQUEST_OPTIONS;
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener
    ) {
        primary.ensureMutable(listener.delegateFailure((l, ignored) -> super.shardOperationOnPrimary(request, primary, l)), true);
    }

    @Override
    protected Map<ShardId, BulkShardRequest> splitRequestOnPrimary(BulkShardRequest request, ProjectMetadata project) {
        return ShardBulkSplitHelper.splitRequests(request, project);
    }

    @Override
    protected Tuple<BulkShardResponse, Exception> combineSplitResponses(
        BulkShardRequest originalRequest,
        Map<ShardId, BulkShardRequest> splitRequests,
        Map<ShardId, Tuple<BulkShardResponse, Exception>> responses
    ) {
        return ShardBulkSplitHelper.combineResponses(originalRequest, splitRequests, responses);
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> outerListener
    ) {
        var listener = ActionListener.releaseBefore(
            indexingPressure.trackPrimaryOperationExpansion(
                primaryOperationCount(request),
                getMaxOperationMemoryOverhead(request),
                force(request)
            ),
            outerListener
        );
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis, (update, shardId, mappingListener) -> {
            assert update != null;
            assert shardId != null;
            mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
        }, (mappingUpdateListener, initialMappingVersion) -> observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                mappingUpdateListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
            }
        }, clusterState -> {
            var index = primary.shardId().getIndex();
            var indexMetadata = clusterState.metadata().lookupProject(index).map(p -> p.index(index)).orElse(null);
            return indexMetadata == null || (indexMetadata.mapping() != null && indexMetadata.getMappingVersion() != initialMappingVersion);
        }), listener, executor(primary), postWriteRefresh, postWriteAction, documentParsingProvider);
    }

    @Override
    protected long primaryOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    @Override
    protected int primaryOperationCount(BulkShardRequest request) {
        return request.items().length;
    }

    @Override
    protected long primaryLargestOperationSize(BulkShardRequest request) {
        return request.largestOperationSize();
    }

    @Override
    protected boolean primaryAllowsOperationsBeyondSizeLimit(BulkShardRequest request) {
        return false;
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        Executor executor
    ) {
        performOnPrimary(
            request,
            primary,
            updateHelper,
            nowInMillisSupplier,
            mappingUpdater,
            waitForMappingUpdate,
            listener,
            executor,
            null,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE
        );
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        Executor executor,
        @Nullable PostWriteRefresh postWriteRefresh,
        @Nullable Consumer<Runnable> postWriteAction,
        DocumentParsingProvider documentParsingProvider
    ) {
        new ActionRunnable<>(listener) {

            private static final int MAX_BATCH_MAPPING_RETRIES = 3;

            private final BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);

            final long startBulkTime = System.nanoTime();

            private final ActionListener<Void> onMappingUpdateDone = ActionListener.wrap(v -> executor.execute(this), this::onRejection);

            private int batchMappingRetries = 0;
            private boolean batchFallbackToSerial = false;

            @Override
            protected void doRun() throws Exception {
                // Try batch path if the coordinating node determined this request is batch-eligible
                if (request.isRowBatchMode() && batchFallbackToSerial == false) {
                    try {
                        var batchResult = performRowBatchOnPrimary(request, primary, postWriteRefresh, postWriteAction);
                        if (batchResult != null) {
                            primary.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
                            listener.onResponse(batchResult);
                            return;
                        }
                    } catch (BatchDynamicMappingUpdateRequired mappingRequired) {
                        if (batchMappingRetries >= MAX_BATCH_MAPPING_RETRIES) {
                            logger.debug("Row batch dynamic mapping update retries exhausted, falling back to serial path");
                            batchFallbackToSerial = true;
                            runSerialLoop();
                            return;
                        }
                        batchMappingRetries++;
                        // Batch needs a dynamic mapping update — submit async and retry
                        final long initialMappingVersion = primary.mapperService().mappingVersion();
                        mappingUpdater.updateMappings(mappingRequired.getMappingUpdate(), primary.shardId(), new ActionListener<>() {
                            @Override
                            public void onResponse(Void v) {
                                // Mapping update accepted — wait for it to propagate, then re-execute
                                waitForMappingUpdate.accept(onMappingUpdateDone, initialMappingVersion);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Row batch dynamic mapping update failed, falling back to serial path", e);
                                batchFallbackToSerial = true;
                                onMappingUpdateDone.onResponse(null);
                            }
                        });
                        return; // async — will re-enter via listener
                    } catch (Exception e) {
                        // Batch failed entirely — fall through to serial path
                        logger.error("Row batch execution failed, falling back to serial path", e);
                    }
                }

                runSerialLoop();
            }

            private void runSerialLoop() throws Exception {
                while (context.hasMoreOperationsToExecute()) {
                    if (executeBulkItemRequest(
                        context,
                        updateHelper,
                        nowInMillisSupplier,
                        mappingUpdater,
                        waitForMappingUpdate,
                        onMappingUpdateDone,
                        documentParsingProvider
                    ) == false) {
                        // We are waiting for a mapping update on another thread, that will invoke this action again once its done
                        // so we just break out here.
                        return;
                    }
                    assert context.isInitial(); // either completed and moved to next or reset
                }
                primary.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
                // We're done, there's no more operations to execute so we resolve the wrapped listener
                finishRequest();
            }

            @Override
            public void onRejection(Exception e) {
                // We must finish the outstanding request. Finishing the outstanding request can include
                // refreshing and fsyncing. Therefore, we must force execution on the WRITE thread.
                executor.execute(new ActionRunnable<>(listener) {

                    @Override
                    protected void doRun() {
                        // Fail all operations after a bulk rejection hit an action that waited for a mapping update and finish the request
                        while (context.hasMoreOperationsToExecute()) {
                            context.setRequestToExecute(context.getCurrent());
                            final DocWriteRequest<?> docWriteRequest = context.getRequestToExecute();
                            onComplete(
                                exceptionToResult(
                                    e,
                                    primary,
                                    docWriteRequest.opType() == DocWriteRequest.OpType.DELETE,
                                    docWriteRequest.version(),
                                    docWriteRequest.id()
                                ),
                                context,
                                null
                            );
                        }
                        finishRequest();
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
            }

            private void finishRequest() {
                ActionListener.completeWith(
                    listener,
                    () -> new WritePrimaryResult<>(
                        context.getBulkShardRequest(),
                        context.buildShardResponse(),
                        context.getLocationToSync(),
                        context.getPrimary(),
                        logger,
                        postWriteRefresh,
                        postWriteAction
                    )
                );
            }
        }.run();
    }

    /**
     * Attempts row-oriented batch execution of all items on the primary shard.
     * Returns a WritePrimaryResult on success, or null if the batch should fall back to the serial path.
     */
    @Nullable
    static WritePrimaryResult<BulkShardRequest, BulkShardResponse> performRowBatchOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        @Nullable PostWriteRefresh postWriteRefresh,
        @Nullable Consumer<Runnable> postWriteAction
    ) throws IOException {
        final BulkItemRequest[] items = request.items();
        final ShardId shardId = request.shardId();
        final MapperService mapperService = primary.mapperService();
        final MappingLookup mappingLookup = mapperService.mappingLookup();
        final RowDocumentBatch rowBatch = request.getRowDocumentBatch();

        // Phase 1: Collect items — if any are aborted, fall back to serial
        final int batchDocCount = items.length;
        final java.util.List<IndexRequest> indexRequests = new java.util.ArrayList<>(batchDocCount);

        for (int i = 0; i < items.length; i++) {
            BulkItemRequest item = items[i];
            if (item.getPrimaryResponse() != null) {
                return null; // aborted item — fall back to serial path
            }
            indexRequests.add((IndexRequest) item.request());
        }

        if (batchDocCount == 0) {
            return null;
        }

        // Collect all field names that have named dynamic templates from any index request
        java.util.Set<String> dynamicTemplateFields = new java.util.HashSet<>();
        for (IndexRequest ir : indexRequests) {
            if (ir.getDynamicTemplates() != null) {
                dynamicTemplateFields.addAll(ir.getDynamicTemplates().keySet());
            }
        }

        // Check if any schema columns require dynamic mapping
        DocBatchSchema schema = rowBatch.schema();
        java.util.List<Integer> dynamicColumns = new java.util.ArrayList<>();
        for (int col = 0; col < schema.columnCount(); col++) {
            String columnName = schema.getColumnName(col);
            if (RowBatchDocumentParser.requiresDynamicMapping(columnName, mappingLookup)) {
                dynamicColumns.add(col);
            } else if (dynamicTemplateFields.contains(columnName)
                && RowBatchDocumentParser.resolveMapper(columnName, mappingLookup) == null
                && mappingLookup.getFieldType(columnName) == null) {
                    // Field has a named dynamic template but the mapping hierarchy didn't flag it
                    // (e.g. passthrough objects with subobjects:false may not be in objectMappers).
                    dynamicColumns.add(col);
                }
        }

        if (dynamicColumns.isEmpty() == false) {
            // Build a dynamic mapping update from the batch data
            var batchParser = mapperService.createRowBatchDocumentParser();
            CompressedXContent mappingUpdate = batchParser.buildDynamicMappingUpdate(
                rowBatch,
                dynamicColumns,
                indexRequests,
                mappingLookup
            );
            if (mappingUpdate == null) {
                return null; // couldn't determine types — fall back to serial
            }

            // Preflight merge to validate the mapping update
            CompressedXContent mergedSource = mapperService.merge(
                MapperService.SINGLE_MAPPING_NAME,
                mappingUpdate,
                MapperService.MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT
            ).mappingSource();

            DocumentMapper existingMapper = mapperService.documentMapper();
            if (existingMapper != null && mergedSource.equals(existingMapper.mappingSource())) {
                // Mapping already up to date (concurrent update?) — proceed with refreshed lookup
                // Fall through to parse with the current mappingLookup which should now have the fields
            } else {
                // Mapping update required — return special marker to trigger async update
                throw new BatchDynamicMappingUpdateRequired(mappingUpdate);
            }
        }

        // Phase 1.5: Extract tsid from batch data for ForIndexDimensions indices.
        // The tsid may have been pre-computed during BulkOperation encoding, but if dimension
        // fields were unmapped at that time (common during dynamic mapping bootstrapping),
        // the tsid will be null. Re-extract here using the current (post-mapping-update) mapping.
        if (primary.indexSettings().getMode() == org.elasticsearch.index.IndexMode.TIME_SERIES) {
            extractTsidsFromBatch(rowBatch, indexRequests, mappingLookup, primary.indexSettings().getIndexVersionCreated());
        }

        // Phase 2: Parse batch using RowBatchDocumentParser
        var batchParser = mapperService.createRowBatchDocumentParser();
        var parseResult = batchParser.parseRowBatch(rowBatch, indexRequests, mappingLookup);

        // Check if any parsed document needs a dynamic mapping update that wasn't predicted
        // by the upfront requiresDynamicMapping check (mirrors serial path's check in
        // IndexShard.applyIndexOperation after parsing)
        for (int i = 0; i < batchDocCount; i++) {
            if (parseResult.isSuccess(i)) {
                CompressedXContent docMappingUpdate = parseResult.getDocument(i).dynamicMappingsUpdate();
                if (docMappingUpdate != null) {
                    // A parsed document discovered unmapped fields during parsing — need mapping update
                    throw new BatchDynamicMappingUpdateRequired(docMappingUpdate);
                }
            }
        }

        // Phase 3: Build Engine.Index operations for successfully parsed documents
        final java.util.List<Engine.Index> engineOps = new java.util.ArrayList<>(batchDocCount);
        final long startTimeNanos = primary.getRelativeTimeInNanos();
        final long operationPrimaryTerm = primary.getOperationPrimaryTerm();

        for (int i = 0; i < batchDocCount; i++) {
            if (parseResult.isSuccess(i) == false) {
                continue;
            }

            var parsedDoc = parseResult.getDocument(i);
            IndexRequest indexRequest = indexRequests.get(i);

            Engine.Index engineIndex = new Engine.Index(
                org.elasticsearch.index.mapper.Uid.encodeId(parsedDoc.id()),
                parsedDoc,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                operationPrimaryTerm,
                indexRequest.version(),
                indexRequest.versionType(),
                Engine.Operation.Origin.PRIMARY,
                startTimeNanos,
                indexRequest.getAutoGeneratedTimestamp(),
                indexRequest.isRetry(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );

            engineOps.add(engineIndex);
        }

        // Phase 4: Execute on engine
        final java.util.List<Engine.IndexResult> engineResults = primary.indexBatch(engineOps, rowBatch.getDataReference());

        // Phase 5: Build BulkItemResponse for each item
        final BulkItemResponse[] responses = new BulkItemResponse[items.length];
        Translog.Location locationToSync = null;

        // Handle parse failures first
        for (int i = 0; i < batchDocCount; i++) {
            if (parseResult.isSuccess(i) == false) {
                BulkItemRequest item = items[i];
                Exception parseException = parseResult.getException(i);
                responses[i] = BulkItemResponse.failure(
                    item.id(),
                    item.request().opType(),
                    new BulkItemResponse.Failure(request.index(), item.request().id(), parseException)
                );
                item.setPrimaryResponse(responses[i]);
            }
        }

        // Handle engine results for successfully parsed documents
        int engineResultIdx = 0;
        for (int i = 0; i < batchDocCount; i++) {
            if (parseResult.isSuccess(i) == false) {
                continue;
            }

            BulkItemRequest item = items[i];
            Engine.IndexResult result = engineResults.get(engineResultIdx);
            engineResultIdx++;

            if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                IndexRequest indexRequest = indexRequests.get(i);
                IndexResponse indexResponse = new IndexResponse(
                    shardId,
                    result.getId(),
                    result.getSeqNo(),
                    result.getTerm(),
                    result.getVersion(),
                    result.isCreated(),
                    indexRequest.getExecutedPipelines()
                );
                indexResponse.setShardInfo(org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo.EMPTY);
                responses[i] = BulkItemResponse.success(item.id(), item.request().opType(), indexResponse);
                if (result.getTranslogLocation() != null) {
                    locationToSync = result.getTranslogLocation();
                }
            } else {
                responses[i] = BulkItemResponse.failure(
                    item.id(),
                    item.request().opType(),
                    new BulkItemResponse.Failure(request.index(), result.getId(), result.getFailure(), result.getSeqNo(), result.getTerm())
                );
            }
            item.setPrimaryResponse(responses[i]);
        }

        BulkShardResponse shardResponse = new BulkShardResponse(shardId, responses);

        return new WritePrimaryResult<>(request, shardResponse, locationToSync, primary, logger, postWriteRefresh, postWriteAction);
    }

    /**
     * Extracts tsid for each IndexRequest from the row batch data using the current mapping's dimension fields.
     * This is needed because the tsid may not have been set during BulkOperation encoding if dimension fields
     * were not yet mapped at that time.
     */
    private static void extractTsidsFromBatch(
        RowDocumentBatch rowBatch,
        java.util.List<IndexRequest> indexRequests,
        MappingLookup mappingLookup,
        org.elasticsearch.index.IndexVersion indexVersion
    ) {
        // Collect dimension field names from the current mapping
        java.util.Set<String> dimensionFields = new java.util.HashSet<>();
        for (var entry : mappingLookup.fieldMappers()) {
            if (entry instanceof org.elasticsearch.index.mapper.FieldMapper fm && fm.fieldType().isDimension()) {
                dimensionFields.add(fm.fieldType().name());
            }
        }
        if (dimensionFields.isEmpty()) {
            return;
        }

        // Map dimension fields to batch schema columns
        DocBatchSchema schema = rowBatch.schema();
        int[] dimColIndices = new int[dimensionFields.size()];
        String[] dimColNames = new String[dimensionFields.size()];
        int dimColCount = 0;
        for (int col = 0; col < schema.columnCount(); col++) {
            String colName = schema.getColumnName(col);
            if (dimensionFields.contains(colName)) {
                dimColIndices[dimColCount] = col;
                dimColNames[dimColCount] = colName;
                dimColCount++;
            }
        }
        if (dimColCount == 0) {
            return;
        }

        // Extract tsid for each document that doesn't already have one.
        // The OTLP path pre-computes tsid from protobuf data; don't overwrite it.
        for (int docIdx = 0; docIdx < indexRequests.size(); docIdx++) {
            IndexRequest request = indexRequests.get(docIdx);
            if (request.tsid() != null) {
                continue;
            }
            int rowIndex = request.batchRowIndex() >= 0 ? request.batchRowIndex() : docIdx;
            DocBatchRowReader rowReader = rowBatch.getRowReader(rowIndex);

            org.elasticsearch.cluster.routing.TsidBuilder tsidBuilder = new org.elasticsearch.cluster.routing.TsidBuilder(dimColCount);
            for (int i = 0; i < dimColCount; i++) {
                int colIdx = dimColIndices[i];
                String fieldName = dimColNames[i];
                byte baseType = rowReader.getBaseType(colIdx);

                switch (baseType) {
                    case RowType.STRING -> tsidBuilder.addStringDimension(fieldName, rowReader.getStringValue(colIdx));
                    case RowType.LONG -> tsidBuilder.addLongDimension(fieldName, rowReader.getLongValue(colIdx));
                    case RowType.DOUBLE -> tsidBuilder.addDoubleDimension(fieldName, rowReader.getDoubleValue(colIdx));
                    case RowType.TRUE -> tsidBuilder.addBooleanDimension(fieldName, true);
                    case RowType.FALSE -> tsidBuilder.addBooleanDimension(fieldName, false);
                    case RowType.ARRAY -> {
                        byte[] arrayData = rowReader.getArrayValue(colIdx);
                        SmallArrayReader arrayReader = new SmallArrayReader(arrayData);
                        while (arrayReader.next()) {
                            if (arrayReader.isNull()) continue;
                            switch (arrayReader.type()) {
                                case RowType.STRING -> tsidBuilder.addStringDimension(
                                    fieldName,
                                    arrayReader.stringBytes(),
                                    arrayReader.stringOffset(),
                                    arrayReader.stringLength()
                                );
                                case RowType.LONG -> tsidBuilder.addLongDimension(fieldName, arrayReader.longValue());
                                case RowType.DOUBLE -> tsidBuilder.addDoubleDimension(fieldName, arrayReader.doubleValue());
                                case RowType.TRUE -> tsidBuilder.addBooleanDimension(fieldName, true);
                                case RowType.FALSE -> tsidBuilder.addBooleanDimension(fieldName, false);
                                default -> {
                                }
                            }
                        }
                    }
                    case RowType.NULL -> {
                    }
                    default -> {
                    }
                }
            }
            if (tsidBuilder.size() > 0) {
                request.tsid(tsidBuilder.buildTsid(indexVersion));
            }
        }
    }

    /**
     * Executes bulk item requests and handles request execution exceptions.
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     *                      a mapping update that will finish and invoke the listener on a different thread
     */
    static boolean executeBulkItemRequest(
        BulkPrimaryExecutionContext context,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<Void> itemDoneListener,
        DocumentParsingProvider documentParsingProvider
    ) throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        // Translate update requests into index or delete requests which can be executed directly
        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(
                    updateRequest,
                    context.getPrimary(),
                    nowInMillisSupplier,
                    // Include inference fields so that partial updates can still retrieve embeddings for fields that weren't updated.
                    FetchSourceContext.FETCH_ALL_SOURCE
                );
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result = new Engine.IndexResult(failure, updateRequest.version(), updateRequest.id());
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            if (updateResult.getResponseResult() == DocWriteResponse.Result.NOOP) {
                context.markOperationAsNoOp(updateResult.action());
                context.markAsCompleted(context.getExecutionResult());
                context.getPrimary().noopUpdate();
                return true;
            }
            context.setRequestToExecute(updateResult.action());
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToExecute().version();
        final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
        final Engine.Result result;
        if (isDelete) {
            final DeleteRequest request = context.getRequestToExecute();
            result = primary.applyDeleteOperationOnPrimary(
                version,
                request.id(),
                request.versionType(),
                request.ifSeqNo(),
                request.ifPrimaryTerm()
            );
        } else {
            final IndexRequest request = context.getRequestToExecute();

            XContentMeteringParserDecorator meteringParserDecorator = documentParsingProvider.newMeteringParserDecorator(request);
            final SourceToParse sourceToParse = new SourceToParse(
                request.id(),
                request.source(),
                request.getContentType(),
                request.routing(),
                request.getDynamicTemplates(),
                request.getDynamicTemplateParams(),
                request.getIncludeSourceOnError(),
                meteringParserDecorator,
                request.tsid()
            );
            result = primary.applyIndexOperationOnPrimary(
                version,
                request.versionType(),
                sourceToParse,
                request.ifSeqNo(),
                request.ifPrimaryTerm(),
                request.getAutoGeneratedTimestamp(),
                request.isRetry()
            );
            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                return handleMappingUpdateRequired(
                    context,
                    mappingUpdater,
                    waitForMappingUpdate,
                    itemDoneListener,
                    primary,
                    result,
                    version,
                    updateResult
                );
            }
        }
        onComplete(result, context, updateResult);
        return true;
    }

    private static boolean handleMappingUpdateRequired(
        BulkPrimaryExecutionContext context,
        MappingUpdatePerformer mappingUpdater,
        ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<Void> itemDoneListener,
        IndexShard primary,
        Engine.Result result,
        long version,
        UpdateHelper.Result updateResult
    ) {
        final var mapperService = primary.mapperService();
        final long initialMappingVersion = mapperService.mappingVersion();
        try {
            if (mapperService.isNoOpUpdate(result.getRequiredMappingUpdate())) {
                context.resetForNoopMappingUpdateRetry(mapperService.mappingVersion());
                return true;
            }
        } catch (Exception e) {
            logger.info(() -> format("%s mapping update rejected by primary", primary.shardId()), e);
            assert result.getId() != null;
            onComplete(exceptionToResult(e, primary, false, version, result.getId()), context, updateResult);
            return true;
        }

        mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(), new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                context.markAsRequiringMappingUpdate();
                waitForMappingUpdate.accept(ActionListener.runAfter(new ActionListener<>() {
                    @Override
                    public void onResponse(Void v) {
                        assert context.requiresWaitingForMappingUpdate();
                        context.resetForMappingUpdateRetry();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        context.failOnMappingUpdate(e);
                    }
                }, () -> itemDoneListener.onResponse(null)), initialMappingVersion);
            }

            @Override
            public void onFailure(Exception e) {
                onComplete(exceptionToResult(e, primary, false, version, result.getId()), context, updateResult);
                // Requesting mapping update failed, so we don't have to wait for a cluster state update
                assert context.isInitial();
                itemDoneListener.onResponse(null);
            }
        });
        return false;
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version, String id) {
        assert id != null;
        return isDelete ? primary.getFailedDeleteResult(e, version, id) : primary.getFailedIndexResult(e, version, id);
    }

    private static void onComplete(Engine.Result r, BulkPrimaryExecutionContext context, UpdateHelper.Result updateResult) {
        context.markOperationAsExecuted(r);
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        if (isUpdate
            && isFailed
            && isConflictException(executionResult.getFailure().getCause())
            && context.getUpdateRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForUpdateRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            assert context.getPrimary().mapperService() != null;
            final MappingLookup mappingLookup = context.getPrimary().mapperService().mappingLookup();
            assert mappingLookup != null;

            response = processUpdateResponse(
                (UpdateRequest) docWriteRequest,
                context.getConcreteIndex(),
                mappingLookup,
                executionResult,
                updateResult
            );
        } else {
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                Level level;
                if (TransportShardBulkAction.isConflictException(failure)) {
                    level = Level.TRACE;
                } else {
                    level = Level.DEBUG;
                }
                logger.log(
                    level,
                    () -> Strings.format(
                        "%s failed to execute bulk item (%s) %s",
                        context.getPrimary().shardId(),
                        opType.getLowercase(),
                        docWriteRequest
                    ),
                    failure
                );

            }
            response = executionResult;
        }
        context.markAsCompleted(response);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    private static BulkItemResponse processUpdateResponse(
        final UpdateRequest updateRequest,
        final String concreteIndex,
        final MappingLookup mappingLookup,
        BulkItemResponse operationResponse,
        final UpdateHelper.Result translate
    ) {
        final BulkItemResponse response;
        if (operationResponse.isFailed()) {
            response = BulkItemResponse.failure(
                operationResponse.getItemId(),
                DocWriteRequest.OpType.UPDATE,
                operationResponse.getFailure()
            );
        } else {
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    indexResponse.getShardInfo(),
                    indexResponse.getShardId(),
                    indexResponse.getId(),
                    indexResponse.getSeqNo(),
                    indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(),
                    indexResponse.getResult()
                );

                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                        indexSourceAsBytes,
                        true,
                        updateIndexRequest.getContentType()
                    );
                    updateResponse.setGetResult(
                        UpdateHelper.extractGetResult(
                            updateRequest,
                            concreteIndex,
                            mappingLookup,
                            indexResponse.getSeqNo(),
                            indexResponse.getPrimaryTerm(),
                            indexResponse.getVersion(),
                            sourceAndContent.v2(),
                            sourceAndContent.v1(),
                            indexSourceAsBytes
                        )
                    );
                }
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    deleteResponse.getShardInfo(),
                    deleteResponse.getShardId(),
                    deleteResponse.getId(),
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    deleteResponse.getResult()
                );

                final GetResult getResult = UpdateHelper.extractGetResult(
                    updateRequest,
                    concreteIndex,
                    mappingLookup,
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(),
                    translate.updateSourceContentType(),
                    null
                );

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = BulkItemResponse.success(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }

    @Override
    protected void dispatchedShardOperationOnReplica(
        BulkShardRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> outerListener
    ) {
        var listener = ActionListener.releaseBefore(
            indexingPressure.trackReplicaOperationExpansion(getMaxOperationMemoryOverhead(request), force(request)),
            outerListener
        );
        ActionListener.completeWith(listener, () -> {
            final long startBulkTime = System.nanoTime();
            final Translog.Location location = performOnReplica(request, replica);
            replica.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
            return new WriteReplicaResult<>(request, location, null, replica, logger, postWriteAction);
        });
    }

    private static long getMaxOperationMemoryOverhead(BulkShardRequest request) {
        return request.maxOperationSizeInBytes() * MAX_EXPANDED_OPERATION_MEMORY_OVERHEAD_FACTOR;
    }

    @Override
    protected long replicaOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    @Override
    protected int replicaOperationCount(BulkShardRequest request) {
        return request.items().length;
    }

    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        if (request.isRowBatchMode()) {
            return performRowBatchOnReplica(request, replica);
        }

        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            final BulkItemRequest item = request.items()[i];
            final BulkItemResponse response = item.getPrimaryResponse();
            final Engine.Result operationResult;
            if (item.getPrimaryResponse().isFailed()) {
                if (response.getFailure().getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    continue; // ignore replication as we didn't generate a sequence number for this request.
                }

                final long primaryTerm;
                if (response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                    // primary is on older version, just take the current primary term
                    primaryTerm = replica.getOperationPrimaryTerm();
                } else {
                    primaryTerm = response.getFailure().getTerm();
                }
                operationResult = replica.markSeqNoAsNoop(
                    response.getFailure().getSeqNo(),
                    primaryTerm,
                    response.getFailure().getMessage()
                );
            } else {
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    continue; // ignore replication as it's a noop
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                operationResult = performOpOnReplica(response.getResponse(), item.request(), replica);
            }
            assert operationResult != null : "operation result must never be null when primary response has no failure";
            location = syncOperationResultOrThrow(operationResult, location);
        }
        return location;
    }

    private static Translog.Location performRowBatchOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        final RowDocumentBatch rowBatch = request.getRowDocumentBatch();
        final MapperService mapperService = replica.mapperService();
        final MappingLookup mappingLookup = mapperService.mappingLookup();
        final BulkItemRequest[] items = request.items();

        // Collect IndexRequests for the parser
        final java.util.List<IndexRequest> indexRequests = new java.util.ArrayList<>(items.length);
        for (BulkItemRequest item : items) {
            indexRequests.add((IndexRequest) item.request());
        }

        // Parse batch using RowBatchDocumentParser
        var batchParser = mapperService.createRowBatchDocumentParser();
        var parseResult = batchParser.parseRowBatch(rowBatch, indexRequests, mappingLookup);

        // Build Engine.Index operations for successfully parsed & non-failed docs
        final java.util.List<Engine.Index> engineOps = new java.util.ArrayList<>();
        final long startTimeNanos = replica.getRelativeTimeInNanos();

        for (int i = 0; i < items.length; i++) {
            BulkItemRequest item = items[i];
            BulkItemResponse response = item.getPrimaryResponse();

            if (response.isFailed()) {
                if (response.getFailure().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    replica.markSeqNoAsNoop(
                        response.getFailure().getSeqNo(),
                        response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM
                            ? replica.getOperationPrimaryTerm()
                            : response.getFailure().getTerm(),
                        response.getFailure().getMessage()
                    );
                }
                continue;
            }

            if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                continue;
            }

            if (parseResult.isSuccess(i) == false) {
                continue;
            }

            var parsedDoc = parseResult.getDocument(i);
            IndexRequest indexRequest = (IndexRequest) item.request();

            Engine.Index engineIndex = new Engine.Index(
                org.elasticsearch.index.mapper.Uid.encodeId(parsedDoc.id()),
                parsedDoc,
                response.getResponse().getSeqNo(),
                response.getResponse().getPrimaryTerm(),
                response.getResponse().getVersion(),
                null,
                Engine.Operation.Origin.REPLICA,
                startTimeNanos,
                indexRequest.getAutoGeneratedTimestamp(),
                indexRequest.isRetry(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );

            engineOps.add(engineIndex);
        }

        if (engineOps.isEmpty()) {
            return null;
        }

        java.util.List<Engine.IndexResult> replicaResults = replica.indexBatch(engineOps, rowBatch.getDataReference());

        Translog.Location replicaLocation = null;
        for (Engine.IndexResult result : replicaResults) {
            if (result.getTranslogLocation() != null) {
                replicaLocation = result.getTranslogLocation();
            }
        }
        return replicaLocation;
    }

    private static Engine.Result performOpOnReplica(
        DocWriteResponse primaryResponse,
        DocWriteRequest<?> docWriteRequest,
        IndexShard replica
    ) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE, INDEX -> {
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    indexRequest.source(),
                    indexRequest.getContentType(),
                    indexRequest.routing(),
                    Map.of(),
                    Map.of(),
                    true,
                    XContentMeteringParserDecorator.NOOP,
                    indexRequest.tsid()
                );
                result = replica.applyIndexOperationOnReplica(
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(),
                    sourceToParse
                );
            }
            case DELETE -> {
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    deleteRequest.id()
                );
            }
            default -> {
                assert false : "Unexpected request operation type on replica: " + docWriteRequest + ";primary result: " + primaryResponse;
                throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
            }
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // Even though the primary waits on all nodes to ack the mapping changes to the master
            // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
            // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
            // mapping update. Assume that that update is first applied on the primary, and only later on the replica
            // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
            // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
            // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
            // applied the new mapping, so there is no other option than to wait.
            throw new TransportReplicationAction.RetryOnReplicaException(
                replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate()
            );
        }
        return result;
    }

    /**
     * Exception used to signal that the batch path requires a dynamic mapping update before it can proceed.
     * Caught in doRun() to trigger the async mapping update flow.
     */
    static final class BatchDynamicMappingUpdateRequired extends RuntimeException {
        private final CompressedXContent mappingUpdate;

        BatchDynamicMappingUpdateRequired(CompressedXContent mappingUpdate) {
            super("Batch path requires dynamic mapping update");
            this.mappingUpdate = mappingUpdate;
        }

        CompressedXContent getMappingUpdate() {
            return mappingUpdate;
        }
    }
}
