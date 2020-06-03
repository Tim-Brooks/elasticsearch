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

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.elasticsearch.common.CheckedSupplier;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;

public class Netty4ChunkedWriteHandler extends ChannelDuplexHandler {

    private final Queue<WriteOperation> queuedWrites = new ArrayDeque<>();
    private WriteOperation currentWrite;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        assert msg instanceof CheckedSupplier;
        @SuppressWarnings("unchecked")
        final boolean queued = queuedWrites.offer(new WriteOperation((CheckedSupplier<ByteBuf, IOException>) msg, promise));
        assert queued;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws IOException {
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws IOException {
        Channel channel = ctx.channel();
        if (channel.isWritable() || channel.isActive() == false) {
            doFlush(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
        super.channelInactive(ctx);
    }

    private void doFlush(ChannelHandlerContext ctx) throws IOException {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            if (currentWrite != null) {
                currentWrite.promise.tryFailure(new ClosedChannelException());
            }
            failQueuedWrites();
            return;
        }
        while (channel.isWritable()) {
            if (currentWrite == null) {
                currentWrite = queuedWrites.poll();
            }
            if (currentWrite == null) {
                break;
            }
            final WriteOperation write = currentWrite;
            final ByteBuf currentBuffer = write.buffer();
            if (currentBuffer.readableBytes() == 0) {
                write.promise.trySuccess();
                currentWrite = null;
                continue;
            }
            final int readableBytes = currentBuffer.readableBytes();
            final int bufferSize = Math.min(readableBytes, 1 << 18);
            final int readerIndex = currentBuffer.readerIndex();
            final boolean sliced = readableBytes != bufferSize;
            final ByteBuf writeBuffer;
            if (sliced) {
                writeBuffer = currentBuffer.retainedSlice(readerIndex, bufferSize);
                currentBuffer.readerIndex(readerIndex + bufferSize);
            } else {
                writeBuffer = currentBuffer;
            }
            final ChannelFuture writeFuture = ctx.write(writeBuffer);
            if (sliced == false || currentBuffer.readableBytes() == 0) {
                currentWrite = null;
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess()) {
                        write.promise.trySuccess();
                    } else {
                        write.promise.tryFailure(future.cause());
                    }
                });
            } else {
                writeFuture.addListener(future -> {
                    assert ctx.executor().inEventLoop();
                    if (future.isSuccess() == false) {
                        write.promise.tryFailure(future.cause());
                    }
                });
            }
            ctx.flush();
            if (channel.isActive() == false) {
                failQueuedWrites();
                return;
            }
        }
    }

    private void failQueuedWrites() {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.promise.tryFailure(new ClosedChannelException());
        }
    }

    private static final class WriteOperation {

        private ByteBuf buf;
        private CheckedSupplier<ByteBuf, IOException> context;
        private final ChannelPromise promise;

        WriteOperation(CheckedSupplier<ByteBuf, IOException> context, ChannelPromise promise) {
            this.context = context;
            this.promise = promise;
        }

        ByteBuf buffer() throws IOException {
            if (buf == null) {
                buf = context.get();
                context = null;
            }
            assert context == null;
            return buf;
        }
    }
}
