/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
<<<<<<< HEAD
 *     http://www.apache.org/licenses/LICENSE-2.0
=======
 *    http://www.apache.org/licenses/LICENSE-2.0
>>>>>>> upstream/master
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.http.HttpPipeline;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.transport.NettyAllocator;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;

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
