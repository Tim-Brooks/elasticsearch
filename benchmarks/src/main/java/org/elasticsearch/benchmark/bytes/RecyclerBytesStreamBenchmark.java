/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NewRecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.OldRecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class RecyclerBytesStreamBenchmark {

    private final AtomicReference<BytesRef> bytesRef = new AtomicReference<>(new BytesRef(16384));
    private NewRecyclerBytesStreamOutput streamOutput;
    private String shortString;
    private String longString;
    private String nonAsciiString;
    private String veryLongString;
    private byte[] bytes1;
    private byte[] bytes2;
    private byte[] bytes3;
    private byte[] bytes4;

    @Setup
    public void initResults() throws IOException {
        streamOutput = new NewRecyclerBytesStreamOutput(new BenchmarkRecycler(bytesRef));
        shortString = "short string";
        longString = "long string that is completely ascii text and nothing else for copying into buffer";
        nonAsciiString = "long string that is comp € letely ascii text and nothing else for copying into buffer";
        veryLongString =
            "long string that is completely ascii text and nothing else for copying into buffer long string that is completely ascii text and nothing else for copying into buffer long string that is completely ascii text and nothing else for copying into buffer";
        ThreadLocalRandom random = ThreadLocalRandom.current();
        bytes1 = new byte[random.nextInt(100, 500)];
        bytes1 = new byte[327];
        bytes2 = new byte[random.nextInt(600, 800)];
        bytes2 = new byte[712];
        bytes3 = new byte[random.nextInt(1000, 1800)];
        bytes3 = new byte[1678];
        bytes4 = new byte[16387 * 4];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
        random.nextBytes(bytes3);
        random.nextBytes(bytes4);
    }

    // @Benchmark
    // public void writeByte() throws IOException {
    // streamOutput.seek(1);
    // for (byte item : bytes1) {
    // streamOutput.writeByte(item);
    // }
    // for (byte item : bytes2) {
    // streamOutput.writeByte(item);
    // }
    // for (byte item : bytes3) {
    // streamOutput.writeByte(item);
    // }
    // }

    @Benchmark
    public void writeBytes() throws IOException {
        streamOutput.seek(1);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesCrossPage() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesMultiPage() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(bytes4, 0, bytes4.length);
    }

    // @Benchmark
    // public void writeString() throws IOException {
    // streamOutput.seek(1);
    // streamOutput.writeString(shortString);
    // streamOutput.writeString(longString);
    // streamOutput.writeString(nonAsciiString);
    // streamOutput.writeString(veryLongString);
    // }

    private record BenchmarkRecycler(AtomicReference<BytesRef> bytesRef) implements Recycler<BytesRef> {

        @Override
        public V<BytesRef> obtain() {
            BytesRef recycledBytesRef = bytesRef.getAndSet(null);
            final BytesRef localBytesRef;
            final boolean recycled;
            if (recycledBytesRef != null) {
                recycled = true;
                localBytesRef = recycledBytesRef;
            } else {
                recycled = false;
                localBytesRef = new BytesRef(16384);
            }
            return new V<>() {
                @Override
                public BytesRef v() {
                    return localBytesRef;
                }

                @Override
                public boolean isRecycled() {
                    return recycled;
                }

                @Override
                public void close() {
                    if (recycled) {
                        bytesRef.set(localBytesRef);
                    }
                }
            };
        }

        @Override
        public int pageSize() {
            return 16384;
        }
    }
}
