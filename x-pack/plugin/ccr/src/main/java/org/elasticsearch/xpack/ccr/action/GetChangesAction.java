/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetChangesAction extends ActionType<GetChangesAction.Response> {

    public static final GetChangesAction INSTANCE = new GetChangesAction();
    public static final String NAME = "indices:data/read/xpack/not_ccr/shard_changes";

    private GetChangesAction() {
        super(NAME, GetChangesAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private final String index;
        private final String currentStateToken;

        public Request(String index, String currentStateToken) {
            this.index = index;
            this.currentStateToken = currentStateToken;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<BytesReference> operations;

        Response(StreamInput in) throws IOException {
            super(in);
            operations = null;
        }

        Response(List<BytesReference> operations) {
            this.operations = operations;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
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
                    // Token improper format
                    listener.onFailure(new Exception());
                    return;
                }
                int i = 0;
                for (String currentSeqNo : currentStringSeqNos) {
                    try {
                        longSeqNos[i++] = Long.parseLong(currentSeqNo);
                    } catch (NumberFormatException e) {
                        // Token improper format
                        listener.onFailure(new Exception());
                        return;
                    }
                }
            }
            final AtomicArray<ShardChangesAction.Response> responses = new AtomicArray<>(numberOfShards);
            for (int i = 0; i < numberOfShards; ++i) {
                final int shardIndex = i;
                ShardChangesAction.Request shardChangesRequest =
                    new ShardChangesAction.Request(new ShardId(indexMetadata.getIndex(), shardIndex), "irrelevant");
                shardChangesRequest.setFromSeqNo(longSeqNos[shardIndex]);
                shardChangesRequest.setMaxOperationCount(128);
                shardChangesRequest.setMaxBatchSize(ByteSizeValue.ofMb(4));
                shardChangesRequest.setPollTimeout(TimeValue.timeValueSeconds(30));
                final CountDown countDown = new CountDown(numberOfShards);
                client.execute(ShardChangesAction.INSTANCE, shardChangesRequest, new ActionListener<>() {
                    @Override
                    public void onResponse(ShardChangesAction.Response response) {
                        responses.set(shardIndex, response);
                        if (countDown.countDown()) {
                            listener.onResponse(new Response(responses.asList().stream().flatMap(r -> {
                                Translog.Operation[] operations = r.getOperations();
                                return Arrays.stream(operations).map(operation -> operation.getSource().source);
                            }).collect(Collectors.toList())));
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
