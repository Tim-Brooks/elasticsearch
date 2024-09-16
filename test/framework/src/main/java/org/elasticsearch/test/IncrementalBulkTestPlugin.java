/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public class IncrementalBulkTestPlugin extends Plugin implements ActionPlugin {

    @Override
    public Collection<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(TestIncrementalBulkAction.TYPE, TestIncrementalBulkAction.class));
    }

    public static class TestIncrementalBulkAction extends TransportAction<TestIncrementalBulkAction.Request, BulkResponse> {

        public static final String NAME = "indices:data/write/test_incremental_bulk";
        public static final ActionType<BulkResponse> TYPE = new ActionType<>(NAME);
        private final IncrementalBulkService bulkService;
        private final ExecutorService executor;

        @Inject
        public TestIncrementalBulkAction(
            ActionFilters actionFilters,
            ThreadPool threadPool,
            TransportService transportService,
            IncrementalBulkService bulkService
        ) {
            super(NAME, actionFilters, transportService.getTaskManager(), threadPool.executor(ThreadPool.Names.GENERIC));
            this.bulkService = bulkService;
            executor = threadPool.executor(ThreadPool.Names.GENERIC);
        }

        @Override
        protected void doExecute(Task task, TestIncrementalBulkAction.Request request, ActionListener<BulkResponse> listener) {
            IncrementalBulkService.Handler handler = bulkService.newBulkRequest();

            ConcurrentLinkedQueue<IndexRequest> queue = new ConcurrentLinkedQueue<>();
            request.requestBuilders.forEach(b -> queue.add(b.request()));

            Runnable r = new Runnable() {

                @Override
                public void run() {
                    int toRemove = Math.min(request.batchSize, queue.size());
                    ArrayList<DocWriteRequest<?>> docs = new ArrayList<>();
                    for (int i = 0; i < toRemove; i++) {
                        docs.add(queue.poll());
                    }

                    if (queue.isEmpty()) {
                        handler.lastItems(docs, () -> {}, ActionListener.runAfter(listener, handler::close));
                    } else {
                        handler.addItems(docs, () -> {}, () -> executor.execute(this));
                    }
                }
            };
            r.run();
        }

        public static class Request extends ActionRequest {

            private final List<IndexRequestBuilder> requestBuilders;
            private final int batchSize;

            public Request(List<IndexRequestBuilder> requestBuilders, int batchSize) {
                this.requestBuilders = requestBuilders;
                this.batchSize = batchSize;
            }

            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        }
    }
}
