/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Performs shard-level bulk (index, delete or update) operations
 */
public class TransportBatchedShardBulkAction extends TransportReplicationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME, BulkShardResponse::new);

    private static final Logger logger = LogManager.getLogger(TransportBatchedShardBulkAction.class);

    private final BatchedShardExecutor batchedShardExecutor;

    @Inject
    public TransportBatchedShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                           MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper,
                                           ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.SAME, true, false);
        this.batchedShardExecutor = new BatchedShardExecutor(clusterService, threadPool, updateHelper, mappingUpdatedAction);
    }

    @Override
    protected ReplicationOperation.Replicas<BulkShardRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    @Override
    protected TransportRequestOptions transportOptions(Settings settings) {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     * and failure async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected void shardOperationOnPrimary(BulkShardRequest request, IndexShard primary,
                                           ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener) {
        String message = "FUCK";
//        String message = UUIDs.base64UUID();
        logger.error("1 " + message);
        CompletableContext<BatchedShardExecutor.FlushResult> flushContext = new CompletableContext<>();

        AtomicReference<BatchedShardExecutor.WriteResult> result1 = new AtomicReference<>();
        ActionListener<BatchedShardExecutor.WriteResult> writeListener = new ActionListener<>() {
            @Override
            public void onResponse(BatchedShardExecutor.WriteResult result) {
                logger.error("2 " + message);
                try {
                    result1.set(result);
                } catch (Exception e) {
                    logger.error("TERRIBLE WRITE LISTENER EXCEPTION", e);
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("3 " + message);
                listener.onFailure(e);
            }
        };

        ActionListener<BatchedShardExecutor.FlushResult> flushListener = new ActionListener<>() {
            @Override
            public void onResponse(BatchedShardExecutor.FlushResult flushResult) {
                logger.error("4 " + message);
                BatchedShardExecutor.WriteResult result = result1.get();
                if (result == null) {
                    logger.error("FUCK write result null");
                    throw new AssertionError("Null");
                }
                PrimaryResult<BulkShardRequest, BulkShardResponse> result2 = new PrimaryResult<>(result.getReplicaRequest(),
                    result.getResponse(), null);
                result2.finalResponseIfSuccessful.setForcedRefresh(flushResult.forcedRefresh());
                listener.onResponse(result2);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("5 " + message);
                listener.onFailure(e);
//                flushContext.completeExceptionally(e);
            }
        };

        batchedShardExecutor.primary(request, primary, writeListener, flushListener);
    }

    /**
     * Result of taking the action on the primary.
     *
     * NOTE: public for testing
     */
    public static class WritePrimaryResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
        Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> {

        private final CompletableContext<BatchedShardExecutor.FlushResult> flushContext;

        public WritePrimaryResult(ReplicaRequest request, Response finalResponse,
                                  CompletableContext<BatchedShardExecutor.FlushResult> flushContext) {
            super(request, finalResponse, null);
            this.flushContext = flushContext;
        }

        @Override
        public void runPostReplicationActions(ActionListener<Void> listener) {
            flushContext.addListener(ActionListener.toBiConsumer(new ActionListener<>() {
                @Override
                public void onResponse(BatchedShardExecutor.FlushResult result) {
                    try {
                        finalResponseIfSuccessful.setForcedRefresh(result.forcedRefresh());
                        listener.onResponse(null);
                    } catch (Exception e) {
                        logger.error("TERRIBLE UNCAUGHT POST REPLICATION EXCEPTION", e);
                        throw e;
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }));
        }
    }


    @Override
    public ReplicaResult shardOperationOnReplica(BulkShardRequest request, IndexShard replica) {
        throw new AssertionError("Override the async method, so the synchronous method should not be called");
    }


    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on replica
     */
    @Override
    protected void shardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener<Void> writeListener = new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                // Do not care about write result for replicas
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };

        ActionListener<BatchedShardExecutor.FlushResult> flushListener = new ActionListener<>() {
            @Override
            public void onResponse(BatchedShardExecutor.FlushResult flushResult) {
                listener.onResponse(new ReplicaResult());
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };

        batchedShardExecutor.replica(request, replica, writeListener, flushListener);
    }

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     *
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     */
    class WriteActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                      ActionListener<Void> listener) {
            if (TransportActions.isShardNotAvailableException(exception) == false) {
                logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            }
            shardStateAction.remoteShardFailed(
                replica.shardId(), replica.allocationId().getId(), primaryTerm, true, message, exception, listener);
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
