/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.ccr.action.GetChangesAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestGetChangesAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "fleet_get_changes_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_fleet/changes"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("index");
        String stateToken = restRequest.param("state_token");
        // TODO: Add max values
        long maxDocs = restRequest.paramAsLong("max_docs", 128);
        ByteSizeValue maxDocBytes = restRequest.paramAsSize("max_doc_bytes", ByteSizeValue.ofMb(4));
        TimeValue pollTimeout = restRequest.paramAsTime("poll_timeout", TimeValue.timeValueSeconds(30));
        GetChangesAction.Request request = new GetChangesAction.Request(index, stateToken, pollTimeout, maxDocs, maxDocBytes);
        return channel -> client.execute(GetChangesAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
