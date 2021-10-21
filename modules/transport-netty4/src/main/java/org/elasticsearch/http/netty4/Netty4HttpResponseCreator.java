/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.http.HttpPipeline;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.netty4.NettyAllocator;

/**
 * Split up large responses to prevent batch compression {@link JdkZlibEncoder} down the pipeline.
 */
class Netty4HttpResponseCreator extends ChannelOutboundHandlerAdapter {

    private static final String DO_NOT_SPLIT = "es.unsafe.do_not_split_http_responses";

    private static final boolean DO_NOT_SPLIT_HTTP_RESPONSES;
    private static final int SPLIT_THRESHOLD;

    static {
        DO_NOT_SPLIT_HTTP_RESPONSES = Booleans.parseBoolean(System.getProperty(DO_NOT_SPLIT), false);
        // Netty will add some header bytes if it compresses this message. So we downsize slightly.
        SPLIT_THRESHOLD = (int) (NettyAllocator.suggestedMaxAllocationSize() * 0.99);
    }

    private final HttpPipeline httpPipeline;

    Netty4HttpResponseCreator(HttpPipeline httpPipeline) {
        this.httpPipeline = httpPipeline;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        HttpPipelinedResponse pipelinedResponse = (HttpPipelinedResponse) msg;
        for (Tuple<HttpResponse, ActionListener<Void>> readyResponse : httpPipeline.handleOutboundRequest(pipelinedResponse, null)) {
            assert readyResponse.v1() instanceof Netty4HttpResponse;
            Netty4HttpResponse response = (Netty4HttpResponse) readyResponse.v1();
            if (DO_NOT_SPLIT_HTTP_RESPONSES || response.content().readableBytes() <= SPLIT_THRESHOLD) {
                ctx.write(response.retain(), Netty4TcpChannel.addPromise(readyResponse.v2(), ctx.channel()));
            } else {
                ctx.write(new DefaultHttpResponse(response.protocolVersion(), response.status(), response.headers()));
                ByteBuf content = response.content();
                while (content.readableBytes() > SPLIT_THRESHOLD) {
                    ctx.write(new DefaultHttpContent(content.readRetainedSlice(SPLIT_THRESHOLD)));
                }
                ctx.write(new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes())),
                    Netty4TcpChannel.addPromise(readyResponse.v2(), ctx.channel()));
            }
        }
    }
}
