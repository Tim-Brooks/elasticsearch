/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.nio;

import java.nio.channels.SocketChannel;
import java.util.function.Function;
import java.util.function.Predicate;

public class ChannelBuilder {

    private final NioSelector selector;

    private Predicate<NioSocketChannel> allowChannelPredicate = (c) -> true;
    private Function<SocketChannel, TransportLayer> transportLayerFunction;
    private ReadWriteHandler readWriteHandler;

    public ChannelBuilder(SocketChannel socketChannel, NioSelector selector) {
        this.selector = selector;
    }

    public static ChannelBuilder create(SocketChannel socketChannel, NioSelector selector) {
        return new ChannelBuilder(socketChannel, selector);
    }

    public ChannelBuilder setTransportLayer(Function<SocketChannel, TransportLayer> transportLayerFunction) {
        this.transportLayerFunction = transportLayerFunction;
        return this;
    }

    public ChannelBuilder setHandler(ReadWriteHandler handler) {
        this.readWriteHandler = readWriteHandler;
        return this;
    }

    public ChannelBuilder setAllowChannelPredicate(Predicate<NioSocketChannel> allowChannelPredicate) {
        this.allowChannelPredicate = allowChannelPredicate;
        return this;
    }

    public NewSocketChannelContext build() {
        return new NewSocketChannelContext(null, selector, null, null, null, null, allowChannelPredicate);
    }
}
