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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;

public class GetChangesAction extends ActionType<GetChangesAction.Response> {

    public static final GetChangesAction INSTANCE = new GetChangesAction();
    public static final String NAME = "indices:data/read/xpack/not_ccr/shard_changes";

    private GetChangesAction() {
        super(NAME, GetChangesAction.Response::new);
    }

    public static class Request extends ActionRequest {

        private String index;
        private String currentStateToken;

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse {

        Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static class GetChangesTransportAction extends TransportAction<Request, Response> {


        private final ClusterService clusterService;

        protected GetChangesTransportAction(final ActionFilters actionFilters, final ClusterService clusterService,
                                            final TaskManager taskManager) {
            super(NAME, actionFilters, taskManager);
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            final IndexMetadata indexMetadata = clusterService.state().getMetadata().index(request.index);
            if (indexMetadata == null) {
                // Index not found
                listener.onFailure(new Exception());
                return;
            }

            final int numberOfShards = indexMetadata.getNumberOfShards();
            final String currentStateToken = request.currentStateToken;
            final long[] longSeqNos = new long[numberOfShards];
            if (currentStateToken == null) {
                for (int i = 0; i < numberOfShards; ++i) {
                    longSeqNos[i] = SequenceNumbers.NO_OPS_PERFORMED;
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
        }
    }
}
