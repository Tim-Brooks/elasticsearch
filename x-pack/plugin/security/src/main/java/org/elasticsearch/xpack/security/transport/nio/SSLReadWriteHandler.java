/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.ReadWriteHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.WriteOperation;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;

public class SSLReadWriteHandler implements ReadWriteHandler {

    private final NioSelector selector;
    private final SSLDriver sslDriver;
    private final ReadWriteHandler delegate;
    private final LinkedList<FlushOperation> unencryptedBytes = new LinkedList<>();
    private final InboundChannelBuffer applicationBuffer;
    private boolean needsToInitiateClose = false;
    private boolean isCloseTimedOut = false;

    SSLReadWriteHandler(NioSelector selector, SSLDriver sslDriver, ReadWriteHandler delegate, InboundChannelBuffer applicationBuffer) {
        this.selector = selector;
        this.sslDriver = sslDriver;
        this.delegate = delegate;
        this.applicationBuffer = applicationBuffer;
    }

    @Override
    public void channelRegistered() throws IOException {
        sslDriver.init();
        delegate.channelRegistered();
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        return delegate.createWriteOperation(context, message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) throws IOException {
        unencryptedBytes.addAll(delegate.writeToBytes(writeOperation));
        return pollFlushOperations();
    }

    @Override
    public List<FlushOperation> pollFlushOperations() throws IOException {
        ArrayList<FlushOperation> encrypted = new ArrayList<>();
        unencryptedBytes.addAll(delegate.pollFlushOperations());
        maybeInitiateClose();

        SSLOutboundBuffer outboundBuffer = sslDriver.getOutboundBuffer();
        if (outboundBuffer.hasEncryptedBytesToFlush()) {
            encrypted.add(outboundBuffer.buildNetworkFlushOperation());
        }

        if (sslDriver.readyForApplicationData() == false) {
            return encrypted;
        }

        FlushOperation unencryptedFlush;
        while ((unencryptedFlush = unencryptedBytes.peekFirst()) != null) {
            try {
                // Attempt to encrypt application write data. The encrypted data ends up in the
                // outbound write buffer.
                sslDriver.write(unencryptedFlush);
                if (outboundBuffer.hasEncryptedBytesToFlush() == false) {
                    break;
                }
                if (unencryptedFlush.isFullyFlushed()) {
                    encrypted.add(outboundBuffer.buildNetworkFlushOperation(unencryptedBytes.removeFirst().getListener()));
                } else {
                    encrypted.add(outboundBuffer.buildNetworkFlushOperation());
                    break;
                }
            } catch (IOException e) {
                FlushOperation flushOperation = unencryptedBytes.removeFirst();
                selector.executeFailedListener(flushOperation.getListener(), e);
                throw e;
            }
        }

        return encrypted;
    }

    @Override
    public boolean readyForFlush() {
        return sslDriver.readyForApplicationData() && (unencryptedBytes.isEmpty() == false || delegate.readyForFlush());
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        sslDriver.read(channelBuffer, applicationBuffer);

        int bytesConsumed = Integer.MAX_VALUE;
        int totalBytesConsumed = 0;
        while (delegate.isProtocolClosed() == false && bytesConsumed > 0 && applicationBuffer.getIndex() > 0) {
            bytesConsumed = delegate.consumeReads(applicationBuffer);
            totalBytesConsumed += bytesConsumed;
        }

        maybeInitiateClose();

        return totalBytesConsumed;
    }

    @Override
    public void initiateProtocolClose() throws IOException {
        delegate.initiateProtocolClose();
        if (delegate.isProtocolClosed()) {
            sslDriver.initiateClose();
        } else {
            needsToInitiateClose = true;
        }
    }

    private void maybeInitiateClose() throws SSLException {
        if (needsToInitiateClose && delegate.isProtocolClosed()) {
            needsToInitiateClose = false;
            sslDriver.initiateClose();
        }
    }

    @Override
    public boolean isProtocolClosed() {
        return isCloseTimedOut || (delegate.isProtocolClosed() && sslDriver.isClosed());
    }

    @Override
    public void close() throws IOException {
        FlushOperation flushOperation;
        while ((flushOperation = unencryptedBytes.pollFirst()) != null) {
            selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
        }

        IOUtils.close(delegate::close, applicationBuffer::close, sslDriver::close);
    }
}
