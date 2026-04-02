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
import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MappingLookup;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.core.Strings.format;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = TransportBulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME);

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

    public static final FeatureFlag BATCH_INDEXING_FEATURE_FLAG = new FeatureFlag("batch_indexing");
    public static final Setting<Boolean> BATCH_INDEXING = boolSetting("indices.batch_indexing", false, value -> {
        if (value && BATCH_INDEXING_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalArgumentException(
                "[indices.batch_indexing] can only be enabled when the batch_indexing feature flag is enabled"
            );
        }
    }, Setting.Property.NodeScope);

    // Represents the maximum memory overhead factor for an operation when processed for indexing.
    // This accounts for potential increases in memory usage due to document expansion, including:
    // 1. If the document source is not stored in a contiguous byte array, it will be copied to ensure contiguity.
    // 2. If the document contains strings, Jackson uses char arrays (2 bytes per character) to parse string fields, doubling memory usage.
    // 3. Parsed string fields create new copies of their data, further increasing memory consumption.
    private static final int MAX_EXPANDED_OPERATION_MEMORY_OVERHEAD_FACTOR = 4;

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final Consumer<Runnable> postWriteAction;
    private final boolean batchIndexingEnabled;

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
        this.batchIndexingEnabled = BATCH_INDEXING.get(settings);
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
        final BulkPrimaryExecutionContext batchContext = new BulkPrimaryExecutionContext(request, primary);
        long startBatchTime = System.nanoTime();
        if (canUseBatchIndexing(request, batchIndexingEnabled)) {
            try {
                performBatchIndexOnPrimary(request, primary, documentParsingProvider, batchContext);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (batchContext.hasMoreOperationsToExecute() == false) {
                primary.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBatchTime);
                listener.onResponse(
                    new WritePrimaryResult<>(
                        request,
                        batchContext.buildShardResponse(),
                        batchContext.getLocationToSync(),
                        primary,
                        logger,
                        postWriteRefresh,
                        postWriteAction
                    )
                );
                return;
            }
        }
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
        }), listener, executor(primary), postWriteRefresh, postWriteAction, documentParsingProvider, batchContext, startBatchTime);
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
        performOnPrimary(
            request,
            primary,
            updateHelper,
            nowInMillisSupplier,
            mappingUpdater,
            waitForMappingUpdate,
            listener,
            executor,
            postWriteRefresh,
            postWriteAction,
            documentParsingProvider,
            new BulkPrimaryExecutionContext(request, primary),
            System.nanoTime()
        );
    }

    private static void performOnPrimary(
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
        DocumentParsingProvider documentParsingProvider,
        BulkPrimaryExecutionContext context,
        long startBulkTime
    ) {
        new ActionRunnable<>(listener) {

            private final ActionListener<Void> onMappingUpdateDone = ActionListener.wrap(v -> executor.execute(this), this::onRejection);

            @Override
            protected void doRun() throws Exception {
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
            final Translog.Location location;
            if (canUseBatchIndexing(request, batchIndexingEnabled)) {
                ReplicaBatchResult batchResult = performBatchIndexOnReplica(request, replica);
                if (batchResult.processedItems() < request.items().length) {
                    location = performOnReplica(request, replica, batchResult.processedItems(), batchResult.location());
                } else {
                    location = batchResult.location();
                }
            } else {
                location = performOnReplica(request, replica);
            }
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
        return performOnReplica(request, replica, 0, null);
    }

    static Translog.Location performOnReplica(
        BulkShardRequest request,
        IndexShard replica,
        int startIndex,
        @Nullable Translog.Location existingLocation
    ) throws Exception {
        Translog.Location location = existingLocation;
        for (int i = startIndex; i < request.items().length; i++) {
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

    /**
     * Checks whether the batch indexing path can be used for this request.
     * Returns true if batch indexing is enabled and all operations are index/create (no deletes, no updates).
     */
    static boolean canUseBatchIndexing(BulkShardRequest request, boolean batchIndexingEnabled) {
        if (batchIndexingEnabled == false) {
            return false;
        }
        for (BulkItemRequest item : request.items()) {
            final DocWriteRequest.OpType opType = item.request().opType();
            if (opType != DocWriteRequest.OpType.INDEX && opType != DocWriteRequest.OpType.CREATE) {
                return false;
            }
        }
        return true;
    }

    // Maximum number of operations to parse and index in a single pass to bound memory usage.
    static final int BATCH_CHUNK_SIZE = 32;

    /**
     * Attempts batch indexing on primary. Returns a fully-advanced context on success, or a partially-advanced
     * context (positioned at the first unprocessed item) if bailing out. The caller must check
     * {@link BulkPrimaryExecutionContext#hasMoreOperationsToExecute()} to determine whether the sequential
     * fallback path is needed for the remaining items.
     */
    static void performBatchIndexOnPrimary(
        final BulkShardRequest request,
        final IndexShard primary,
        final DocumentParsingProvider documentParsingProvider,
        final BulkPrimaryExecutionContext context
    ) throws IOException {
        final BulkItemRequest[] items = request.items();

        // Check for aborted items upfront
        for (BulkItemRequest item : items) {
            if (item.getPrimaryResponse() != null
                && item.getPrimaryResponse().isFailed()
                && item.getPrimaryResponse().getFailure().isAborted()) {
                return;
            }
        }

        // TODO: Required because VerionLock is re-entrant. We likely can switch that to be semaphore based and remove this protection
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));

        // Process in chunks to bound memory: parse + index BATCH_CHUNK_SIZE docs at a time,
        // allowing previous chunks' parsed docs to be GC'd before parsing the next chunk.
        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final int chunkSize = chunkEnd - chunkStart;
            final List<Engine.Index> operations = new ArrayList<>(chunkSize);

            for (int i = chunkStart; i < chunkEnd; i++) {
                final IndexRequest indexRequest = (IndexRequest) items[i].request();
                final XContentMeteringParserDecorator meteringParserDecorator = documentParsingProvider.newMeteringParserDecorator(
                    indexRequest
                );
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    indexRequest.source(),
                    indexRequest.getContentType(),
                    indexRequest.routing(),
                    indexRequest.getDynamicTemplates(),
                    indexRequest.getDynamicTemplateParams(),
                    indexRequest.getIncludeSourceOnError(),
                    meteringParserDecorator,
                    indexRequest.tsid()
                );
                Engine.Index operation;
                try {
                    operation = IndexShard.prepareIndex(
                        primary.mapperService(),
                        sourceToParse,
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        primary.getOperationPrimaryTerm(),
                        indexRequest.version(),
                        indexRequest.versionType(),
                        Engine.Operation.Origin.PRIMARY,
                        indexRequest.getAutoGeneratedTimestamp(),
                        indexRequest.isRetry(),
                        indexRequest.ifSeqNo(),
                        indexRequest.ifPrimaryTerm(),
                        primary.getRelativeTimeInNanos()
                    );
                } catch (Exception e) {
                    return;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    return;
                }
                if (seenUids.add(operation.uid()) == false) {
                    return;
                }
                operations.add(operation);
            }

            final List<Engine.IndexResult> results = primary.applyIndexOperationBatchOnPrimary(operations);

            for (Engine.IndexResult result : results) {
                assert context.hasMoreOperationsToExecute();
                context.setRequestToExecute(context.getCurrent());
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
            }
            seenUids.clear();
        }
    }

    /**
     * Performs a batch index on a replica. Returns the number of items processed from the start of the request's
     * items array. The caller should fall back to the item-by-item path for any remaining items.
     * The returned location may be null if no operations produced a translog location.
     */
    static ReplicaBatchResult performBatchIndexOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        final BulkItemRequest[] items = request.items();
        // TODO: Required because VerionLock is re-entrant. We likely can switch that to be semaphore based and remove this protection
        final Set<BytesRef> seenUids = new HashSet<>(Math.min(items.length, BATCH_CHUNK_SIZE));
        Translog.Location location = null;
        int processedItems = 0;

        for (int chunkStart = 0; chunkStart < items.length; chunkStart += BATCH_CHUNK_SIZE) {
            final int chunkEnd = Math.min(chunkStart + BATCH_CHUNK_SIZE, items.length);
            final List<Engine.Index> operations = new ArrayList<>(chunkEnd - chunkStart);

            int i = chunkStart;
            while (i < chunkEnd) {
                final BulkItemRequest item = items[i];
                final BulkItemResponse response = item.getPrimaryResponse();

                if (response.isFailed()) {
                    break;
                }
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    i++;
                    continue;
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;

                final IndexRequest indexRequest = (IndexRequest) item.request();
                final DocWriteResponse primaryResponse = response.getResponse();
                final SourceToParse sourceToParse = replicaSourceToParse(indexRequest);
                Engine.Index operation;
                try {
                    operation = IndexShard.prepareIndex(
                        replica.mapperService(),
                        sourceToParse,
                        primaryResponse.getSeqNo(),
                        primaryResponse.getPrimaryTerm(),
                        primaryResponse.getVersion(),
                        null,
                        Engine.Operation.Origin.REPLICA,
                        indexRequest.getAutoGeneratedTimestamp(),
                        indexRequest.isRetry(),
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0,
                        replica.getRelativeTimeInNanos()
                    );
                } catch (Exception e) {
                    break;
                }
                if (operation.parsedDoc().dynamicMappingsUpdate() != null) {
                    break;
                }
                if (seenUids.add(operation.uid()) == false) {
                    break;
                }
                operations.add(operation);
                i++;
            }

            if (operations.isEmpty() == false) {
                final List<Engine.IndexResult> results = replica.applyIndexOperationBatchOnReplica(operations);
                for (Engine.IndexResult result : results) {
                    location = syncOperationResultOrThrow(result, location);
                }
            }

            if (i < chunkEnd) {
                processedItems = i;
                break;
            }

            processedItems = chunkEnd;
            seenUids.clear();
        }

        return new ReplicaBatchResult(processedItems, location);
    }

    record ReplicaBatchResult(int processedItems, @Nullable Translog.Location location) {}

    private static Engine.Result performOpOnReplica(
        DocWriteResponse primaryResponse,
        DocWriteRequest<?> docWriteRequest,
        IndexShard replica
    ) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE, INDEX -> {
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final SourceToParse sourceToParse = replicaSourceToParse(indexRequest);
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

    private static SourceToParse replicaSourceToParse(IndexRequest indexRequest) {
        return new SourceToParse(
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
    }
}
