/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.seqNo;

import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("unused")
public class LocalCheckpointTrackerBenchmark {

    @State(Scope.Benchmark)
    public static class TrackerState {
        LocalCheckpointTracker tracker;
        AtomicLong seqNoCounter;

        @Param({ "sequential", "scattered" })
        String pattern;

        @Setup(Level.Iteration)
        public void setup() {
            tracker = new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
            seqNoCounter = new AtomicLong(0);
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        long threadSeqNo;

        @Setup(Level.Iteration)
        public void setup() {
            threadSeqNo = 0;
        }
    }

    /**
     * Baseline single-threaded sequential processing
     */
    @Benchmark
    @Threads(1)
    public void markSeqNoSequential_01(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * 2 threads marking sequential sequence numbers
     */
    @Benchmark
    @Threads(2)
    public void markSeqNoSequential_02(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * 4 threads marking sequential sequence numbers
     */
    @Benchmark
    @Threads(4)
    public void markSeqNoSequential_04(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * 8 threads marking sequential sequence numbers
     */
    @Benchmark
    @Threads(8)
    public void markSeqNoSequential_08(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * 16 threads marking sequential sequence numbers
     */
    @Benchmark
    @Threads(16)
    public void markSeqNoSequential_16(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * 32 threads marking sequential sequence numbers
     */
    @Benchmark
    @Threads(32)
    public void markSeqNoSequential_32(TrackerState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * Test with scattered sequence numbers (thread-local counters with gaps)
     * This simulates real-world scenarios where sequence numbers may arrive out of order
     */
    @Benchmark
    @Threads(1)
    public void markSeqNoScattered_01(TrackerState state, ThreadState threadState) {
        if (state.pattern.equals("scattered")) {
            // Each thread maintains its own sequence number space with gaps
            long seqNo = threadState.threadSeqNo;
            threadState.threadSeqNo += 100; // Large gap to prevent checkpoint advancement
            state.tracker.markSeqNoAsProcessed(seqNo);
        } else {
            long seqNo = state.seqNoCounter.getAndIncrement();
            state.tracker.markSeqNoAsProcessed(seqNo);
        }
    }

    @Benchmark
    @Threads(4)
    public void markSeqNoScattered_04(TrackerState state, ThreadState threadState) {
        if (state.pattern.equals("scattered")) {
            long seqNo = threadState.threadSeqNo;
            threadState.threadSeqNo += 100;
            state.tracker.markSeqNoAsProcessed(seqNo);
        } else {
            long seqNo = state.seqNoCounter.getAndIncrement();
            state.tracker.markSeqNoAsProcessed(seqNo);
        }
    }

    @Benchmark
    @Threads(8)
    public void markSeqNoScattered_08(TrackerState state, ThreadState threadState) {
        if (state.pattern.equals("scattered")) {
            long seqNo = threadState.threadSeqNo;
            threadState.threadSeqNo += 100;
            state.tracker.markSeqNoAsProcessed(seqNo);
        } else {
            long seqNo = state.seqNoCounter.getAndIncrement();
            state.tracker.markSeqNoAsProcessed(seqNo);
        }
    }

    @Benchmark
    @Threads(16)
    public void markSeqNoScattered_16(TrackerState state, ThreadState threadState) {
        if (state.pattern.equals("scattered")) {
            long seqNo = threadState.threadSeqNo;
            threadState.threadSeqNo += 100;
            state.tracker.markSeqNoAsProcessed(seqNo);
        } else {
            long seqNo = state.seqNoCounter.getAndIncrement();
            state.tracker.markSeqNoAsProcessed(seqNo);
        }
    }

    /**
     * Test checkpoint advancement performance with continuous sequential writes
     * This measures the cost of checkpoint updates as they happen
     */
    @State(Scope.Benchmark)
    public static class CheckpointAdvancementState {
        LocalCheckpointTracker tracker;
        AtomicLong seqNoCounter;

        @Setup(Level.Iteration)
        public void setup() {
            tracker = new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
            seqNoCounter = new AtomicLong(0);
        }
    }

    /**
     * Benchmark checkpoint advancement with 1 thread
     * This simulates the best-case scenario where all sequence numbers arrive in order
     */
    @Benchmark
    @Threads(1)
    public void checkpointAdvancement_01(CheckpointAdvancementState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    @Benchmark
    @Threads(4)
    public void checkpointAdvancement_04(CheckpointAdvancementState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    @Benchmark
    @Threads(8)
    public void checkpointAdvancement_08(CheckpointAdvancementState state) {
        long seqNo = state.seqNoCounter.getAndIncrement();
        state.tracker.markSeqNoAsProcessed(seqNo);
    }

    /**
     * Test performance across bitset boundaries
     * This measures the cost when the tracker needs to allocate new bitsets
     */
    @State(Scope.Thread)
    public static class BitsetBoundaryState {
        LocalCheckpointTracker tracker;
        long seqNo;

        @Setup(Level.Invocation)
        public void setup() {
            tracker = new LocalCheckpointTracker(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED);
            // Pre-advance to near bitset boundary (1024 is BIT_SET_SIZE)
            seqNo = 1020;
        }
    }

    @Benchmark
    @Threads(1)
    public void bitsetBoundaryCrossing(BitsetBoundaryState state) {
        // Mark several seqnos that cross bitset boundary
        for (long i = state.seqNo; i < state.seqNo + 10; i++) {
            state.tracker.markSeqNoAsProcessed(i);
        }
    }

    /**
     * Test with mixed pattern: some threads do sequential writes, some do scattered
     * This simulates a more realistic workload
     */
    @State(Scope.Thread)
    public static class MixedPatternState {
        long threadId;
        long counter;

        @Setup(Level.Iteration)
        public void setup() {
            threadId = Thread.currentThread().threadId();
            counter = threadId * 10000; // Each thread starts at different base
        }
    }

    @Benchmark
    @Threads(8)
    public void mixedPattern_08(TrackerState state, MixedPatternState threadState) {
        // Even threads do sequential, odd threads do scattered
        long seqNo;
        if (threadState.threadId % 2 == 0) {
            seqNo = state.seqNoCounter.getAndIncrement();
        } else {
            seqNo = threadState.counter;
            threadState.counter += 50;
        }
        state.tracker.markSeqNoAsProcessed(seqNo);
    }
}
