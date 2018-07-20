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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public abstract class TransportLayer {

    protected int readFromChannel(SocketChannel rawChannel, ByteBuffer buffer) throws IOException {
        int bytesRead = rawChannel.read(buffer);
        if (bytesRead < 0) {
            bytesRead = 0;
        }
        return bytesRead;
    }

    protected int readFromChannel(SocketChannel rawChannel, ByteBuffer[] buffers) throws IOException {
        int bytesRead = (int) rawChannel.read(buffers);
        if (bytesRead < 0) {
            bytesRead = 0;
        }
        return bytesRead;
    }

    protected int flushToChannel(SocketChannel rawChannel, ByteBuffer buffer) throws IOException {
        return rawChannel.write(buffer);
    }

    protected int flushToChannel(SocketChannel rawChannel, ByteBuffer[] buffers) throws IOException {
        return (int) rawChannel.write(buffers);
    }

    public abstract int read(SocketChannel socketChannel, InboundChannelBuffer channelBuffer) throws IOException;

    public abstract int write(FlushOperation flushOperation) throws IOException;

    public abstract boolean needsFlush();

    public abstract void flushChannel(SocketChannel socketChannel) throws IOException;

    public abstract void initiateClose();

    public abstract boolean shouldClose();

    public abstract void close() throws IOException;

}
