/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpPipeline;
import org.elasticsearch.http.HttpRequest;

class Netty4HttpRequestHandler extends SimpleChannelInboundHandler<HttpRequest> {

    private final HttpPipeline pipeline;
    private final Netty4HttpServerTransport serverTransport;

    Netty4HttpRequestHandler(HttpPipeline pipeline, Netty4HttpServerTransport serverTransport) {
        this.pipeline = pipeline;
        this.serverTransport = serverTransport;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest httpRequest) {
        final Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        boolean success = false;
        try {
            pipeline.handleHttpRequest(channel, httpRequest);
            success = true;
        } finally {
            if (success == false) {
                httpRequest.release();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        if (cause instanceof Error) {
            serverTransport.onException(channel, new Exception(cause));
        } else {
            serverTransport.onException(channel, (Exception) cause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Releasables.closeWhileHandlingException(pipeline);
        super.channelInactive(ctx);
    }
}
