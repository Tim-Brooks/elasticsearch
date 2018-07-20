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

import org.elasticsearch.common.concurrent.CompletableContext;
import org.elasticsearch.nio.utils.ExceptionsHelper;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class NewSocketChannelContext extends ChannelContext<SocketChannel> {

    protected final NioSocketChannel channel;
    protected final InboundChannelBuffer channelBuffer;
    protected final AtomicBoolean isClosing = new AtomicBoolean(false);
    private final TransportLayer transportLayer;
    private final ReadWriteHandler readWriteHandler;
    private final Predicate<NioSocketChannel> allowChannelPredicate;
    private final NioSelector selector;
    private final CompletableContext<Void> connectContext = new CompletableContext<>();
    private final LinkedList<FlushOperation> pendingFlushes = new LinkedList<>();
    private boolean closeNow;
    private Exception connectException;

    protected NewSocketChannelContext(NioSocketChannel channel, NioSelector selector, Consumer<Exception> exceptionHandler,
                                      TransportLayer transportLayer, ReadWriteHandler readWriteHandler, InboundChannelBuffer channelBuffer,
                                      Predicate<NioSocketChannel> allowChannelPredicate) {
        super(channel.getRawChannel(), exceptionHandler);
        this.selector = selector;
        this.channel = channel;
        this.transportLayer = transportLayer;
        this.readWriteHandler = readWriteHandler;
        this.channelBuffer = channelBuffer;
        this.allowChannelPredicate = allowChannelPredicate;
    }

    public int read() throws IOException {
        int bytesRead = 0;
        if (closeNow()) {
            return bytesRead;
        }
        bytesRead = transportLayer.read(channel.getRawChannel(), channelBuffer);
        if (bytesRead == 0) {
            return bytesRead;
        }

        handleReadBytes();

        return bytesRead;
    }

    public void flushChannel() throws IOException {
        if (closeNow()) {
            return;
        }
        // If there is currently data in the outbound transport layer, flush the transport layer.
        if (transportLayer.needsFlush()) {
            // If the data is not completely flushed, exit. We cannot produce new write data until the
            // existing data has been fully flushed.
            transportLayer.flushChannel(channel.getRawChannel());
            if (transportLayer.needsFlush()) {
                return;
            }
        }

        // If the driver is ready for new writes, we can attempt to proceed with any queued writes.
        if (transportLayer.readyForNewWrite()) {
            FlushOperation currentFlush;
            while (transportLayer.needsFlush() == false && (currentFlush = getPendingFlush()) != null) {
                // If the current operation has been fully consumed (encrypted) we now know that it has been
                // sent (as we only get to this point if the write buffer has been fully flushed).
                if (currentFlush.isFullyFlushed()) {
                    currentFlushOperationComplete();
                } else {
                    try {
                        // Attempt to write application data to the transport layer.
                        int bytesWritten = transportLayer.write(currentFlush);
                        if (bytesWritten == 0) {
                            break;
                        }
                        currentFlush.incrementIndex(bytesWritten);
                        // Flush the write buffer to the channel
                        transportLayer.flushChannel(channel.getRawChannel());
                    } catch (IOException e) {
                        currentFlushOperationFailed(e);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * This method indicates if a selector should close this channel.
     *
     * @return a boolean indicating if the selector should close
     */
    public boolean selectorShouldClose() {
        return closeNow() || transportLayer.shouldClose();
    }

    public void closeChannel() {

    }

    @Override
    public NioSelector getSelector() {
        return selector;
    }

    @Override
    public NioSocketChannel getChannel() {
        return channel;
    }

    public void addConnectListener(BiConsumer<Void, Exception> listener) {
        connectContext.addListener(listener);
    }

    public boolean isConnectComplete() {
        return connectContext.isDone() && connectContext.isCompletedExceptionally() == false;
    }

    /**
     * This method will attempt to complete the connection process for this channel. It should be called for
     * new channels or for a channel that has produced a OP_CONNECT event. If this method returns true then
     * the connection is complete and the channel is ready for reads and writes. If it returns false, the
     * channel is not yet connected and this method should be called again when a OP_CONNECT event is
     * received.
     *
     * @return true if the connection process is complete
     * @throws IOException if an I/O error occurs
     */
    public boolean connect() throws IOException {
        if (isConnectComplete()) {
            return true;
        } else if (connectContext.isCompletedExceptionally()) {
            Exception exception = connectException;
            if (exception == null) {
                throw new AssertionError("Should have received connection exception");
            } else if (exception instanceof IOException) {
                throw (IOException) exception;
            } else {
                throw (RuntimeException) exception;
            }
        }

        boolean isConnected = rawChannel.isConnected();
        if (isConnected == false) {
            try {
                isConnected = rawChannel.finishConnect();
            } catch (IOException | RuntimeException e) {
                connectException = e;
                connectContext.completeExceptionally(e);
                throw e;
            }
        }
        if (isConnected) {
            connectContext.complete(null);
        }
        return isConnected;
    }

    public void sendMessage(Object message, BiConsumer<Void, Exception> listener) {
        if (isClosing.get()) {
            listener.accept(null, new ClosedChannelException());
            return;
        }

        WriteOperation writeOperation = readWriteHandler.createWriteOperation(null, message, listener);

        NioSelector selector = getSelector();
        if (selector.isOnCurrentThread() == false) {
            selector.queueWrite(writeOperation);
            return;
        }

        selector.writeToChannel(writeOperation);
    }

    public void queueWriteOperation(WriteOperation writeOperation) {
        getSelector().assertOnSelectorThread();
        pendingFlushes.addAll(readWriteHandler.writeToBytes(writeOperation));
    }

    protected void currentFlushOperationFailed(IOException e) {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeFailedListener(flushOperation.getListener(), e);
    }

    protected void currentFlushOperationComplete() {
        FlushOperation flushOperation = pendingFlushes.pollFirst();
        getSelector().executeListener(flushOperation.getListener(), null);
    }

    protected FlushOperation getPendingFlush() {
        return pendingFlushes.peekFirst();
    }

    @Override
    protected void register() throws IOException {
        super.register();
        if (allowChannelPredicate.test(channel) == false) {
            closeNow = true;
        }
    }

    @Override
    public void closeFromSelector() throws IOException {
        getSelector().assertOnSelectorThread();
        if (isOpen()) {
            ArrayList<IOException> closingExceptions = new ArrayList<>(3);
            try {
                super.closeFromSelector();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            // Set to true in order to reject new writes before queuing with selector
            isClosing.set(true);

            // Poll for new flush operations to close
            pendingFlushes.addAll(readWriteHandler.pollFlushOperations());
            FlushOperation flushOperation;
            while ((flushOperation = pendingFlushes.pollFirst()) != null) {
                selector.executeFailedListener(flushOperation.getListener(), new ClosedChannelException());
            }

            try {
                readWriteHandler.close();
            } catch (IOException e) {
                closingExceptions.add(e);
            }
            channelBuffer.close();

            if (closingExceptions.isEmpty() == false) {
                ExceptionsHelper.rethrowAndSuppress(closingExceptions);
            }
        }
    }

    protected void handleReadBytes() throws IOException {
        int bytesConsumed = Integer.MAX_VALUE;
        while (bytesConsumed > 0 && channelBuffer.getIndex() > 0) {
            bytesConsumed = readWriteHandler.consumeReads(channelBuffer);
            channelBuffer.release(bytesConsumed);
        }

        // Some protocols might produce messages to flush during a read operation.
        pendingFlushes.addAll(readWriteHandler.pollFlushOperations());
    }

    public boolean readyForFlush() {
        getSelector().assertOnSelectorThread();
        // TODO: Maybe add the ability for transport layer to indicate if we should look at pending flushes
//        if (transportLayer.readyForNewWrite()) {
        if (true) {
            return pendingFlushes.isEmpty() == false;
        } else {
            return transportLayer.needsFlush();
        }
    }

    protected boolean closeNow() {
        return closeNow;
    }
}
