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

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SocketChannelContextTests extends ESTestCase {

    private SocketChannel rawChannel;
    private TestSocketChannelContext context;
    private Consumer<Exception> exceptionHandler;
    private NioSocketChannel channel;
    private BiConsumer<Void, Exception> listener;
    private NioSelector selector;
    private ReadWriteHandler readWriteHandler;
    private ByteBuffer ioBuffer = ByteBuffer.allocate(1024);
    private int messageLength;
    private InboundChannelBuffer channelBuffer;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        super.setUp();

        messageLength = randomInt(96) + 20;
        rawChannel = mock(SocketChannel.class);
        channel = mock(NioSocketChannel.class);
        listener = mock(BiConsumer.class);
        when(channel.getRawChannel()).thenReturn(rawChannel);
        exceptionHandler = mock(Consumer.class);
        selector = mock(NioSelector.class);
        readWriteHandler = mock(ReadWriteHandler.class);
        channelBuffer = InboundChannelBuffer.allocatingInstance();
        context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, channelBuffer);

        when(selector.isOnCurrentThread()).thenReturn(true);
        when(selector.getIoBuffer()).thenAnswer(invocationOnMock -> {
            ioBuffer.clear();
            return ioBuffer;
        });
    }

    public void testIOExceptionMeansReadyToClose() throws IOException {
        when(rawChannel.write(any(ByteBuffer[].class), anyInt(), anyInt())).thenThrow(new IOException());
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(new IOException());
        when(rawChannel.read(any(ByteBuffer[].class), anyInt(), anyInt())).thenThrow(new IOException());
        when(rawChannel.read(any(ByteBuffer.class))).thenThrow(new IOException());
        assertFalse(context.selectorShouldClose());
        expectThrows(IOException.class, () -> {
            if (randomBoolean()) {
                context.read();
            } else {
                ByteBuffer[] byteBuffers = {ByteBuffer.allocate(10)};
                FlushReadyWrite flushOperation = new FlushReadyWrite(context, byteBuffers, listener);
                when(readWriteHandler.writeToBytes(flushOperation)).thenReturn(Collections.singletonList(flushOperation));
                context.writeToChannel(flushOperation);
                context.flushChannel();
            }
        });
        assertTrue(context.selectorShouldClose());
    }

    public void testEOFMeansReadyToClose() throws IOException {
        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(-1);
        assertFalse(context.selectorShouldClose());
        context.read();
        assertTrue(context.selectorShouldClose());
    }

    public void testValidateInRegisterCanSucceed() throws IOException {
        InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
        context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, channelBuffer, (c) -> true);
        assertFalse(context.selectorShouldClose());
        context.register();
        assertFalse(context.selectorShouldClose());
    }

    public void testValidateInRegisterCanFail() throws IOException {
        InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
        context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, channelBuffer, (c) -> false);
        assertFalse(context.selectorShouldClose());
        context.register();
        assertTrue(context.selectorShouldClose());
    }

    public void testRegisterCallsHandlerAndQueuesFlushes() throws IOException {
        boolean produceFlushesInRegister = randomBoolean();
        assertFalse(context.readyForFlush());

        if (produceFlushesInRegister) {
            when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.singletonList(mock(FlushOperation.class)));
        } else {
            when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.emptyList());
        }
        context.register();

        verify(readWriteHandler).channelRegistered();
        assertEquals(produceFlushesInRegister, context.readyForFlush());
    }

    public void testConnectSucceeds() throws IOException {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        when(rawChannel.finishConnect()).thenReturn(false, true);

        context.addConnectListener((v, t) -> {
            if (t == null) {
                listenerCalled.compareAndSet(false, true);
            } else {
                throw new AssertionError("Connection should not fail");
            }
        });

        assertFalse(context.connect());
        assertFalse(context.isConnectComplete());
        assertFalse(listenerCalled.get());
        assertTrue(context.connect());
        assertTrue(context.isConnectComplete());
        assertTrue(listenerCalled.get());
    }

    public void testConnectFails() throws IOException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        IOException ioException = new IOException("boom");
        when(rawChannel.finishConnect()).thenReturn(false).thenThrow(ioException);

        context.addConnectListener((v, t) -> {
            if (t == null) {
                throw new AssertionError("Connection should not succeed");
            } else {
                exception.set(t);
            }
        });

        assertFalse(context.connect());
        assertFalse(context.isConnectComplete());
        assertNull(exception.get());
        expectThrows(IOException.class, context::connect);
        assertFalse(context.isConnectComplete());
        assertSame(ioException, exception.get());
    }

    public void testWriteFailsIfClosing() {
        context.closeChannel();

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        context.sendMessage(buffers, listener);

        verify(listener).accept(isNull(Void.class), any(ClosedChannelException.class));
    }

    public void testSendMessageFromDifferentThreadIsQueuedWithSelector() throws Exception {
        ArgumentCaptor<WriteOperation> writeOpCaptor = ArgumentCaptor.forClass(WriteOperation.class);

        when(selector.isOnCurrentThread()).thenReturn(false);

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        WriteOperation writeOperation = mock(WriteOperation.class);
        when(readWriteHandler.createWriteOperation(context, buffers, listener)).thenReturn(writeOperation);
        context.sendMessage(buffers, listener);

        verify(selector).queueWrite(writeOpCaptor.capture());
        WriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(writeOperation, writeOp);
    }

    public void testSendMessageFromSameThreadIsQueuedInChannel() {
        ArgumentCaptor<WriteOperation> writeOpCaptor = ArgumentCaptor.forClass(WriteOperation.class);

        ByteBuffer[] buffers = {ByteBuffer.wrap(createMessage(10))};
        WriteOperation writeOperation = mock(WriteOperation.class);
        when(readWriteHandler.createWriteOperation(context, buffers, listener)).thenReturn(writeOperation);
        context.sendMessage(buffers, listener);

        verify(selector).writeToChannel(writeOpCaptor.capture());
        WriteOperation writeOp = writeOpCaptor.getValue();

        assertSame(writeOperation, writeOp);
    }

    public void testQueuedWriteIsFlushedInFlushCall() throws Exception {
        assertFalse(context.readyForFlush());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};

        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushOperation)).thenReturn(Collections.singletonList(flushOperation));
        context.writeToChannel(flushOperation);

        assertTrue(context.readyForFlush());

        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(flushOperation.isFullyFlushed()).thenReturn(false, true);
        when(flushOperation.getListener()).thenReturn(listener);
        context.flushChannel();

        verify(rawChannel).write(eq(ioBuffer));
        verify(selector).executeListener(listener, null);
        assertFalse(context.readyForFlush());
    }

    public void testFlushCallWillPollForNewFlushes() throws Exception {
        assertFalse(context.readyForFlush());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};

        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushOperation)).thenReturn(Collections.singletonList(flushOperation));
        context.writeToChannel(flushOperation);

        assertTrue(context.readyForFlush());

        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(flushOperation.isFullyFlushed()).thenReturn(false, true);
        when(flushOperation.getListener()).thenReturn(listener);
        when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.singletonList(mock(FlushOperation.class)));
        context.flushChannel();

        verify(rawChannel).write(eq(ioBuffer));
        assertTrue(context.readyForFlush());
    }

    public void testPartialFlush() throws IOException {
        assertFalse(context.readyForFlush());
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushOperation)).thenReturn(Collections.singletonList(flushOperation));
        context.writeToChannel(flushOperation);
        assertTrue(context.readyForFlush());

        when(flushOperation.isFullyFlushed()).thenReturn(false);
        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        context.flushChannel();

        verify(listener, times(0)).accept(null, null);
        assertTrue(context.readyForFlush());
    }

    @SuppressWarnings("unchecked")
    public void testMultipleWritesPartialFlushes() throws IOException {
        assertFalse(context.readyForFlush());

        BiConsumer<Void, Exception> listener2 = mock(BiConsumer.class);
        FlushReadyWrite flushOperation1 = mock(FlushReadyWrite.class);
        FlushReadyWrite flushOperation2 = mock(FlushReadyWrite.class);
        when(flushOperation1.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        when(flushOperation2.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[] {ByteBuffer.allocate(3)});
        when(flushOperation1.getListener()).thenReturn(listener);
        when(flushOperation2.getListener()).thenReturn(listener2);

        when(readWriteHandler.writeToBytes(flushOperation1)).thenReturn(Collections.singletonList(flushOperation1));
        when(readWriteHandler.writeToBytes(flushOperation2)).thenReturn(Collections.singletonList(flushOperation2));
        context.writeToChannel(flushOperation1);
        context.writeToChannel(flushOperation2);

        assertTrue(context.readyForFlush());

        when(flushOperation1.isFullyFlushed()).thenReturn(false, true);
        when(flushOperation2.isFullyFlushed()).thenReturn(false);
        context.flushChannel();

        verify(selector).executeListener(listener, null);
        verify(listener2, times(0)).accept(null, null);
        assertTrue(context.readyForFlush());

        when(flushOperation2.isFullyFlushed()).thenReturn(false, true);

        context.flushChannel();

        verify(selector).executeListener(listener2, null);
        assertFalse(context.readyForFlush());
    }

    public void testWhenIOExceptionThrownListenerIsCalled() throws IOException {
        assertFalse(context.readyForFlush());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        FlushReadyWrite flushOperation = mock(FlushReadyWrite.class);
        when(readWriteHandler.writeToBytes(flushOperation)).thenReturn(Collections.singletonList(flushOperation));
        context.writeToChannel(flushOperation);

        assertTrue(context.readyForFlush());

        IOException exception = new IOException();
        when(flushOperation.getBuffersToWrite(anyInt())).thenReturn(buffers);
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(exception);
        when(flushOperation.getListener()).thenReturn(listener);
        expectThrows(IOException.class, () -> context.flushChannel());

        verify(selector).executeFailedListener(listener, exception);
        assertFalse(context.readyForFlush());
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });

        when(readWriteHandler.consumeReads(channelBuffer)).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            buffer.release(messageLength);
            return messageLength;
        });

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        verify(readWriteHandler, times(1)).consumeReads(channelBuffer);
    }

    public void testMultipleReadsConsumed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });

        when(readWriteHandler.consumeReads(channelBuffer)).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            buffer.release(messageLength);
            return messageLength;
        });

        assertEquals(bytes.length, context.read());

        assertEquals(0, channelBuffer.getIndex());
        verify(readWriteHandler, times(2)).consumeReads(channelBuffer);
    }

    public void testPartialRead() throws IOException {
        byte[] bytes = createMessage(messageLength);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });


        when(readWriteHandler.consumeReads(channelBuffer)).thenReturn(0);

        assertEquals(messageLength, context.read());

        assertEquals(bytes.length, channelBuffer.getIndex());
        verify(readWriteHandler, times(1)).consumeReads(channelBuffer);

        when(readWriteHandler.consumeReads(channelBuffer)).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            buffer.release(messageLength * 2);
            return messageLength;
        });

        assertEquals(messageLength, context.read());

        assertEquals(0, channelBuffer.getIndex());
        verify(readWriteHandler, times(2)).consumeReads(channelBuffer);
    }

    public void testWillStopConsumingReadsIfProtocolClosed() throws IOException {
        byte[] bytes = createMessage(messageLength * 2);

        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            ByteBuffer buffer = (ByteBuffer) invocationOnMock.getArguments()[0];
            buffer.put(bytes);
            return bytes.length;
        });

        AtomicBoolean isClosed = new AtomicBoolean(false);

        when(readWriteHandler.consumeReads(channelBuffer)).thenAnswer(invocationOnMock -> {
            InboundChannelBuffer buffer = (InboundChannelBuffer) invocationOnMock.getArguments()[0];
            buffer.release(messageLength);
            isClosed.set(true);
            return messageLength;
        });
        when(readWriteHandler.isProtocolClosed()).thenAnswer((i) -> isClosed.get());

        assertEquals(bytes.length, context.read());

        assertEquals(messageLength, channelBuffer.getIndex());
        verify(readWriteHandler, times(1)).consumeReads(channelBuffer);
    }

    public void testHandleReadBytesWillCheckForNewFlushOperations() throws IOException {
        assertFalse(context.readyForFlush());
        when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.singletonList(mock(FlushOperation.class)));
        context.handleReadBytes();
        assertTrue(context.readyForFlush());
    }

    public void testCloseChannelSchedulesCloseWithSelector() {
        context.closeChannel();
        verify(selector).queueChannelClose(channel);
    }

    public void testInitiateCloseNotifiesHandlerAndQueuesFlushOperations() throws IOException {
        assertFalse(context.readyForFlush());
        when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.singletonList(mock(FlushOperation.class)));
        context.initiateClose();
        verify(readWriteHandler).initiateProtocolClose();
        assertTrue(context.readyForFlush());
    }

    public void testSelectorShouldCloseWhenReadWriteHandlerReturnsTrueAndNoPendingFlushOperations() throws IOException {
        FlushOperation closeOperation = mock(FlushOperation.class);
        when(closeOperation.getBuffersToWrite(anyInt())).thenReturn(new ByteBuffer[0]);
        when(closeOperation.isFullyFlushed()).thenReturn(false, true);
        when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.singletonList(closeOperation));
        context.initiateClose();

        when(readWriteHandler.isProtocolClosed()).thenReturn(false, true, true);
        assertFalse(context.selectorShouldClose());
        when(readWriteHandler.pollFlushOperations()).thenReturn(Collections.emptyList());
        context.flushChannel();
        assertTrue(context.selectorShouldClose());
    }

    @SuppressWarnings("unchecked")
    public void testPendingFlushOpsClearedOnClose() throws Exception {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
            context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, channelBuffer);

            assertFalse(context.readyForFlush());

            ByteBuffer[] buffer = {ByteBuffer.allocate(10)};
            WriteOperation writeOperation = mock(WriteOperation.class);
            BiConsumer<Void, Exception> listener2 = mock(BiConsumer.class);
            when(readWriteHandler.writeToBytes(writeOperation)).thenReturn(Arrays.asList(new FlushOperation(buffer, listener),
                new FlushOperation(buffer, listener2)));
            context.writeToChannel(writeOperation);

            assertTrue(context.readyForFlush());

            when(channel.isOpen()).thenReturn(true);
            context.closeFromSelector();

            verify(selector, times(1)).executeFailedListener(same(listener), any(ClosedChannelException.class));
            verify(selector, times(1)).executeFailedListener(same(listener2), any(ClosedChannelException.class));

            assertFalse(context.readyForFlush());
        }
    }

    public void testCloseClosesReadWriteHandler() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            when(channel.isOpen()).thenReturn(true);
            InboundChannelBuffer buffer = InboundChannelBuffer.allocatingInstance();
            SocketChannelContext context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, buffer);
            context.closeFromSelector();
            verify(readWriteHandler).close();
        }
    }

    public void testCloseClosesChannelBuffer() throws IOException {
        try (SocketChannel realChannel = SocketChannel.open()) {
            when(channel.getRawChannel()).thenReturn(realChannel);
            when(channel.isOpen()).thenReturn(true);
            Runnable closer = mock(Runnable.class);
            IntFunction<Page> pageAllocator = (n) -> new Page(ByteBuffer.allocate(n), closer);
            InboundChannelBuffer buffer = new InboundChannelBuffer(pageAllocator);
            buffer.ensureCapacity(1);
            TestSocketChannelContext context = new TestSocketChannelContext(channel, selector, exceptionHandler, readWriteHandler, buffer);
            context.closeFromSelector();
            verify(closer).run();
        }
    }

    public void testReadToChannelBufferWillReadAsMuchAsIOBufferAllows() throws IOException {
        when(rawChannel.read(any(ByteBuffer.class))).thenAnswer(completelyFillBufferAnswer());

        InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
        int bytesRead = context.readFromChannel(channelBuffer);
        assertEquals(ioBuffer.capacity(), bytesRead);
        assertEquals(ioBuffer.capacity(), channelBuffer.getIndex());
    }

    public void testReadToChannelBufferHandlesIOException() throws IOException  {
        when(rawChannel.read(any(ByteBuffer.class))).thenThrow(new IOException());

        InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
        expectThrows(IOException.class, () -> context.readFromChannel(channelBuffer));
        assertTrue(context.selectorShouldClose());
        assertEquals(0, channelBuffer.getIndex());
    }

    public void testReadToChannelBufferHandlesEOF() throws IOException {
        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(-1);

        InboundChannelBuffer channelBuffer = InboundChannelBuffer.allocatingInstance();
        context.readFromChannel(channelBuffer);
        assertTrue(context.selectorShouldClose());
        assertEquals(0, channelBuffer.getIndex());
    }

    public void testFlushBuffersHandlesZeroFlush() throws IOException {
        when(rawChannel.write(any(ByteBuffer.class))).thenAnswer(consumeBufferAnswer(0));

        ByteBuffer[] buffers = {ByteBuffer.allocate(1023), ByteBuffer.allocate(1023)};
        FlushOperation flushOperation = new FlushOperation(buffers, listener);
        context.flushToChannel(flushOperation);
        assertEquals(2, flushOperation.getBuffersToWrite().length);
        assertEquals(0, flushOperation.getBuffersToWrite()[0].position());
    }

    public void testFlushBuffersHandlesPartialFlush() throws IOException {
        AtomicBoolean first = new AtomicBoolean(true);
        when(rawChannel.write(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            if (first.compareAndSet(true, false)) {
                return consumeBufferAnswer(1024).answer(invocationOnMock);
            } else {
                return consumeBufferAnswer(3).answer(invocationOnMock);
            }
        });

        ByteBuffer[] buffers = {ByteBuffer.allocate(1023), ByteBuffer.allocate(1023)};
        FlushOperation flushOperation = new FlushOperation(buffers, listener);
        context.flushToChannel(flushOperation);
        assertEquals(1, flushOperation.getBuffersToWrite().length);
        assertEquals(4, flushOperation.getBuffersToWrite()[0].position());
    }

    public void testFlushBuffersHandlesFullFlush() throws IOException {
        AtomicBoolean first = new AtomicBoolean(true);
        when(rawChannel.write(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            if (first.compareAndSet(true, false)) {
                return consumeBufferAnswer(1024).answer(invocationOnMock);
            } else {
                return consumeBufferAnswer(1022).answer(invocationOnMock);
            }
        });

        ByteBuffer[] buffers = {ByteBuffer.allocate(1023), ByteBuffer.allocate(1023)};
        FlushOperation flushOperation = new FlushOperation(buffers, listener);
        context.flushToChannel(flushOperation);
        assertTrue(flushOperation.isFullyFlushed());
    }

    public void testFlushBuffersHandlesIOException() throws IOException {
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(new IOException());

        ByteBuffer[] buffers = {ByteBuffer.allocate(10), ByteBuffer.allocate(10)};
        FlushOperation flushOperation = new FlushOperation(buffers, listener);
        expectThrows(IOException.class, () -> context.flushToChannel(flushOperation));
        assertTrue(context.selectorShouldClose());
    }

    public void testFlushBuffersHandlesIOExceptionSecondTimeThroughLoop() throws IOException {
        AtomicBoolean first = new AtomicBoolean(true);
        when(rawChannel.write(any(ByteBuffer.class))).thenAnswer(invocationOnMock -> {
            if (first.compareAndSet(true, false)) {
                return consumeBufferAnswer(1024).answer(invocationOnMock);
            } else {
                throw new IOException();
            }
        });

        ByteBuffer[] buffers = {ByteBuffer.allocate(1023), ByteBuffer.allocate(1023)};
        FlushOperation flushOperation = new FlushOperation(buffers, listener);
        expectThrows(IOException.class, () -> context.flushToChannel(flushOperation));
        assertTrue(context.selectorShouldClose());
        assertEquals(1, flushOperation.getBuffersToWrite().length);
        assertEquals(1, flushOperation.getBuffersToWrite()[0].position());
    }

    private static class TestSocketChannelContext extends SocketChannelContext {

        private TestSocketChannelContext(NioSocketChannel channel, NioSelector selector, Consumer<Exception> exceptionHandler,
                                         ReadWriteHandler readWriteHandler, InboundChannelBuffer channelBuffer) {
            this(channel, selector, exceptionHandler, readWriteHandler, channelBuffer, ALWAYS_ALLOW_CHANNEL);
        }

        private TestSocketChannelContext(NioSocketChannel channel, NioSelector selector, Consumer<Exception> exceptionHandler,
                                         ReadWriteHandler readWriteHandler, InboundChannelBuffer channelBuffer,
                                         Predicate<NioSocketChannel> allowChannelPredicate) {
            super(channel, selector, exceptionHandler, readWriteHandler, channelBuffer, allowChannelPredicate);
        }

        @Override
        void doSelectorRegister() {
            // We do not want to call the actual register with selector method as it will throw a NPE
        }
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private Answer<Integer> completelyFillBufferAnswer() {
        return invocationOnMock -> {
            ByteBuffer b = (ByteBuffer) invocationOnMock.getArguments()[0];
            int bytesRead = b.remaining();
            while (b.hasRemaining()) {
                b.put((byte) 1);
            }
            return bytesRead;
        };
    }

    private Answer<Object> consumeBufferAnswer(int bytesToConsume) {
        return invocationOnMock -> {
            ByteBuffer b = (ByteBuffer) invocationOnMock.getArguments()[0];
            b.position(b.position() + bytesToConsume);
            return bytesToConsume;
        };
    }
}
