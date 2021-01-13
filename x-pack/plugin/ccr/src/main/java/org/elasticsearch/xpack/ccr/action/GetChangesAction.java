/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import joptsimple.internal.Strings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

public class GetChangesAction extends ActionType<GetChangesAction.Response> {

    public static final GetChangesAction INSTANCE = new GetChangesAction();
    public static final String NAME = "indices:data/read/xpack/not_ccr/shard_changes";

    private GetChangesAction() {
        super(NAME, GetChangesAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String index;
        private final String currentStateToken;
        private final TimeValue pollTimeout;
        private final long maxDocs;
        private final ByteSizeValue maxDocBytes;

        public Request(String index, String currentStateToken, TimeValue pollTimeout, long maxDocs, ByteSizeValue maxDocBytes) {
            this.index = index;
            this.currentStateToken = currentStateToken;
            this.pollTimeout = pollTimeout;
            this.maxDocs = maxDocs;
            this.maxDocBytes = maxDocBytes;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final String stateToken;
        private final Translog.Operation[] operations;

        Response(StreamInput in) throws IOException {
            super(in);
            stateToken = in.readString();
            operations = in.readArray(Translog.Operation::readOperation, Translog.Operation[]::new);
        }

        Response(String stateToken, Translog.Operation[] operations) {
            this.stateToken = stateToken;
            this.operations = operations;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(Translog.Operation::writeOperation, operations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("state_token", stateToken);
            builder.startArray("docs");
            for (Translog.Operation operation : operations) {
                builder.startObject();
                // TODO: Evaluate if we want to support more operation types
                builder.field("_id", ((Translog.Index) operation).id());
                XContentHelper.writeRawField(SourceFieldMapper.NAME, operation.getSource().source, builder, params);
                builder.endObject();
            }
            builder.endArray();
            return builder.endObject();
        }
    }

    public static class TransportGetChangesAction extends TransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final NodeClient client;

        @Inject
        public TransportGetChangesAction(final ActionFilters actionFilters, final ClusterService clusterService,
                                         final TransportService transportService, final NodeClient client) {
            super(NAME, actionFilters, transportService.getTaskManager());
            this.clusterService = clusterService;
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final IndexMetadata indexMetadata = clusterService.state().getMetadata().index(request.index);
            if (indexMetadata == null) {
                // Index not found
                listener.onFailure(new IndexNotFoundException(request.index));
                return;
            }

            final int numberOfShards = indexMetadata.getNumberOfShards();
            final String currentStateToken = request.currentStateToken;
            final long[] longSeqNos = new long[numberOfShards];
            if (currentStateToken == null) {
                for (int i = 0; i < numberOfShards; ++i) {
                    longSeqNos[i] = 0;
                }
            } else {
                final String[] currentStringSeqNos = currentStateToken.split(",");
                if (numberOfShards != currentStringSeqNos.length) {
                    listener.onFailure(new ElasticsearchException("Improper token format: unexpected number of shards"));
                    return;
                }
                int i = 0;
                for (String currentSeqNo : currentStringSeqNos) {
                    try {
                        longSeqNos[i++] = Long.parseLong(currentSeqNo);
                    } catch (NumberFormatException e) {
                        // Token improper format
                        listener.onFailure(new ElasticsearchException("Improper token format: cannot parse seqNos"));
                        return;
                    }
                }
            }
            final AtomicArray<ShardChangesAction.Response> responses = new AtomicArray<>(numberOfShards);
            final CountDown countDown = new CountDown(numberOfShards);
            for (int i = 0; i < numberOfShards; ++i) {
                final int shardIndex = i;
                ShardChangesAction.Request shardChangesRequest =
                    new ShardChangesAction.Request(new ShardId(indexMetadata.getIndex(), shardIndex), "irrelevant");
                shardChangesRequest.setFromSeqNo(longSeqNos[shardIndex] + 1);
                final int adjustedMaxDocs;
                if (numberOfShards > 1) {
                    adjustedMaxDocs = (int) request.maxDocs / numberOfShards;
                } else {
                    adjustedMaxDocs = (int) request.maxDocs;
                }
                shardChangesRequest.setMaxOperationCount(adjustedMaxDocs);
                final ByteSizeValue adjustedMaxBytes;
                if (numberOfShards > 1) {
                    adjustedMaxBytes = ByteSizeValue.ofBytes(request.maxDocBytes.getBytes() / numberOfShards);
                } else {
                    adjustedMaxBytes = request.maxDocBytes;
                }
                shardChangesRequest.setMaxBatchSize(adjustedMaxBytes);
                shardChangesRequest.setPollTimeout(request.pollTimeout);
                client.execute(ShardChangesAction.INSTANCE, shardChangesRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(ShardChangesAction.Response response) {
                        responses.set(shardIndex, response);
                        if (countDown.countDown()) {
                            String[] seqNos = new String[responses.length()];
                            int i = 0;
                            for (ShardChangesAction.Response r : responses.asList()) {
                                seqNos[i++] = Long.toString(r.getMaxSeqNo());
                            }
                            String newToken = Strings.join(seqNos, ",");
                            listener.onResponse(new Response(newToken, responses.asList().stream().flatMap(r -> {
                                Translog.Operation[] operations = r.getOperations();
                                // TODO: Only indexing operations. Could be a concern if not append-only
                                return Arrays.stream(operations).filter(op -> op instanceof Translog.Index);
                            }).toArray(Translog.Operation[]::new)));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (countDown.fastForward()) {
                            listener.onFailure(e);
                        }
                    }
                });
            }
        }
    }
}
