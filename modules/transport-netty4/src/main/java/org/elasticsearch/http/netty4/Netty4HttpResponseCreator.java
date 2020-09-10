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

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.http.HttpPipeline;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpResponse;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;

import java.util.List;

@ChannelHandler.Sharable
public class Netty4HttpResponseCreator extends MessageToMessageEncoder<HttpPipeline.HttpResponseContext> {
    @Override
    protected void encode(ChannelHandlerContext ctx, HttpPipeline.HttpResponseContext msg, List<Object> out) {
        for (Tuple<HttpPipelinedResponse, ActionListener<Void>> readyResponse : msg.get()) {
            HttpResponse response = readyResponse.v1().getDelegateRequest();
            assert response instanceof Netty4HttpResponse;
            ctx.write(response, Netty4TcpChannel.addPromise(readyResponse.v2(), ctx.channel()));
        }
    }
}
