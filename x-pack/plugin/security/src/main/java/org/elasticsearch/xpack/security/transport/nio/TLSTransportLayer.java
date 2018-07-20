/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.TransportLayer;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class TLSTransportLayer extends TransportLayer {

    private final SSLDriver sslDriver;

    public TLSTransportLayer(SSLDriver sslDriver) {
        this.sslDriver = sslDriver;
    }

    @Override
    public int read(SocketChannel socketChannel, InboundChannelBuffer channelBuffer) throws IOException {
        if (channelBuffer.getRemaining() == 0) {
            // Requiring one additional byte will ensure that a new page is allocated.
            channelBuffer.ensureCapacity(channelBuffer.getCapacity() + 1);
        }

        int bytesRead = readFromChannel(socketChannel, channelBuffer.sliceBuffersFrom(channelBuffer.getIndex()));

        if (bytesRead <= 0) {
            return bytesRead;
        }

        channelBuffer.incrementIndex(bytesRead);

        return bytesRead;
    }

    @Override
    public int write(FlushOperation flushOperation) throws IOException {
        assert flushOperation.isFullyFlushed() == false : "";
        if (sslDriver.readyForApplicationWrites()) {
            // Attempt to encrypt application write data. The encrypted data ends up in the
            // outbound write buffer.
            return sslDriver.applicationWrite(flushOperation.getBuffersToWrite());
        } else {
            return 0;
        }
    }

    @Override
    public boolean needsFlush() {
        return sslDriver.hasFlushPending();
    }

    @Override
    public void flushChannel(SocketChannel socketChannel) throws IOException {
        if (sslDriver.hasFlushPending()) {
            flushToChannel(socketChannel, sslDriver.getNetworkWriteBuffer());
        }
    }

    @Override
    public void initiateClose() {
        sslDriver.initiateClose();
    }

    @Override
    public boolean shouldClose() {
        return sslDriver.isClosed();
    }

    @Override
    public void close() throws IOException {
        sslDriver.close();
    }
}
