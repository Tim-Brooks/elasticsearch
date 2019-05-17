/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.FlushReadyWrite;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.ReadWriteHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

public class SSLReadWriteHandlerTests extends ESTestCase {


    private static class FakeHandler implements ReadWriteHandler {

        private boolean isClosed = false;
        private ArrayList<FlushOperation> outbound = new ArrayList<>();
        private ArrayList<ByteBuffer[]> inbound = new ArrayList<>();

        @Override
        public void channelRegistered() throws IOException {

        }

        @Override
        public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
            return new FlushReadyWrite(context, (ByteBuffer[]) message, listener);
        }

        @Override
        public List<FlushOperation> writeToBytes(WriteOperation writeOperation) throws IOException {
            return Collections.singletonList((FlushReadyWrite) writeOperation);
        }

        @Override
        public List<FlushOperation> pollFlushOperations() throws IOException {
            ArrayList<FlushOperation> flushOperations = new ArrayList<>(outbound);
            outbound.clear();
            return flushOperations;
        }

        @Override
        public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
            long readyBytes = channelBuffer.getIndex();
            inbound.add(channelBuffer.sliceBuffersTo(readyBytes));
            channelBuffer.release(readyBytes);
            return (int) readyBytes;
        }

        @Override
        public void initiateProtocolClose() throws IOException {
            isClosed = true;
        }

        @Override
        public boolean isProtocolClosed() {
            return isClosed;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
