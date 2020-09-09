/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.network.CloseableChannel;

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class HttpPipeline implements Releasable {

    private static final ActionListener<Void> NO_OP = ActionListener.wrap(() -> {});

    private final HttpPipeliningAggregator<ActionListener<Void>> aggregator;
    private final CorsHandler corsHandler;
    private final BiConsumer<HttpChannel, HttpPipelinedRequest> messageHandler;

    public HttpPipeline(HttpPipeliningAggregator<ActionListener<Void>> aggregator, CorsHandler corsHandler,
                        BiConsumer<HttpChannel, HttpPipelinedRequest> requestHandler) {
        this.corsHandler = corsHandler;
        this.messageHandler = requestHandler;
        this.aggregator = aggregator;
    }

    public void handleHttpRequest(final HttpChannel httpChannel, final HttpRequest httpRequest) {
        boolean success = false;
        try {
            final HttpPipelinedRequest pipelinedRequest = aggregator.read(httpRequest);
            HttpResponse earlyCorsResponse = corsHandler.handleInbound(pipelinedRequest);
            if (earlyCorsResponse != null) {
                httpChannel.sendResponse(earlyCorsResponse, earlyResponseListener(httpRequest, httpChannel));
                httpRequest.release();
            } else {
                messageHandler.accept(httpChannel, pipelinedRequest);
            }
            success = true;
        } finally {
            if (success == false) {
                httpRequest.release();
            }
        }
    }

    public void sendHttpResponse(final HttpPipelinedResponse response, final ActionListener<Void> listener) {
        Supplier<List<Tuple<HttpPipelinedResponse, ActionListener<Void>>>> requests = () -> aggregator.write(response, listener);
        
    }

    @Override
    public void close() {
        final List<Tuple<HttpPipelinedResponse, ActionListener<Void>>> inflightResponses = aggregator.removeAllInflightResponses();
        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<HttpPipelinedResponse, ActionListener<Void>> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.v2().onFailure(closedChannelException);
                } catch (RuntimeException e) {
//                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
    }

    private static ActionListener<Void> earlyResponseListener(HttpRequest request, HttpChannel httpChannel) {
        if (HttpUtils.shouldCloseConnection(request)) {
            return ActionListener.wrap(() -> CloseableChannel.closeChannel(httpChannel));
        } else {
            return NO_OP;
        }
    }
}
