/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeRateLimiter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * A {@link MergeScheduler} that runs each merge on a thread pool.
 *
 * <p>Specify the max number of threads that may run at once, and the maximum number of simultaneous
 * merges with {@link #setMaxMergesAndThreads}.
 *
 * <p>If the number of merges exceeds the max number of threads then the largest merges are paused
 * until one of the smaller merges completes.
 *
 * <p>If more than {@link #getMaxMergeCount} merges are requested then this class will forcefully
 * throttle the incoming threads by pausing until one more merges complete.
 *
 * <p>This class sets defaults based on Java's view of the cpu count, and it assumes a solid state
 * disk (or similar). If you have a spinning disk and want to maximize performance, use {@link
 * #setDefaultMaxMergesAndThreads(boolean)}.
 */
abstract class AbstractThreadPoolMergeScheduler extends MergeScheduler {

    /**
     * Dynamic default for {@code maxThreadCount} and {@code maxMergeCount}, based on CPU core count.
     * {@code maxThreadCount} is set to {@code max(1, min(4, cpuCoreCount/2))}. {@code maxMergeCount}
     * is set to {@code maxThreadCount + 5}.
     */
    public static final int AUTO_DETECT_MERGES_AND_THREADS = -1;

    /**
     * Used for testing.
     *
     * @lucene.internal
     */
    public static final String DEFAULT_CPU_CORE_COUNT_PROPERTY = "lucene.cms.override_core_count";

    private final ThreadLocal<MergeTask> localThreadMerge = new ThreadLocal<>();

    /**
     * List of currently active {@link MergeTask}s.
     */
    protected final List<MergeTask> pendingMerges = new ArrayList<>();

    // Max number of merge threads allowed to be running at
    // once. When there are more merges then this, we
    // forcefully pause the larger ones, letting the smaller
    // ones run, up until maxMergeCount merges at which point
    // we forcefully pause incoming threads (that presumably
    // are the ones causing so much merging).
    private int maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;

    // Max number of merges we accept before forcefully
    // throttling the incoming threads
    private int maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;

    /**
     * How many {@link MergeTask}s have kicked off (this is used to name them).
     */
    protected int mergeTaskCount;

    /**
     * Floor for IO write rate limit (we will never go any lower than this)
     */
    private static final double MIN_MERGE_MB_PER_SEC = 5.0;

    /**
     * Ceiling for IO write rate limit (we will never go any higher than this)
     */
    private static final double MAX_MERGE_MB_PER_SEC = 10240.0;

    /**
     * Initial value for IO write rate limit when doAutoIOThrottle is true
     */
    private static final double START_MB_PER_SEC = 20.0;

    /**
     * Merges below this size are not counted in the maxThreadCount, i.e. they can freely run in their
     * own thread (up until maxMergeCount).
     */
    private static final double MIN_BIG_MERGE_MB = 50.0;

    /**
     * Current IO writes throttle rate
     */
    protected double targetMBPerSec = START_MB_PER_SEC;

    /**
     * true if we should rate-limit writes for each merge
     */
    private boolean doAutoIOThrottle = false;

    private double forceMergeMBPerSec = Double.POSITIVE_INFINITY;

    /**
     * The executor provided for intra-merge parallelization
     */
    protected CachedExecutor intraMergeExecutor;

    /**
     * Sole constructor, with all settings set to default values.
     */
    AbstractThreadPoolMergeScheduler() {}

    /**
     * Expert: directly set the maximum number of merge threads and simultaneous merges allowed.
     *
     * @param maxMergeCount  the max # simultaneous merges that are allowed. If a merge is necessary
     *                       yet we already have this many threads running, the incoming thread (that is calling
     *                       add/updateDocument) will block until a merge thread has completed. Note that we will only
     *                       run the smallest <code>maxThreadCount</code> merges at a time.
     * @param maxThreadCount the max # simultaneous merge threads that should be running at once. This
     *                       must be &lt;= <code>maxMergeCount</code>
     */
    public synchronized void setMaxMergesAndThreads(int maxMergeCount, int maxThreadCount) {
        if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS && maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            // OK
            this.maxMergeCount = AUTO_DETECT_MERGES_AND_THREADS;
            this.maxThreadCount = AUTO_DETECT_MERGES_AND_THREADS;
        } else if (maxMergeCount == AUTO_DETECT_MERGES_AND_THREADS) {
            throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
        } else if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            throw new IllegalArgumentException("both maxMergeCount and maxThreadCount must be AUTO_DETECT_MERGES_AND_THREADS");
        } else {
            if (maxThreadCount < 1) {
                throw new IllegalArgumentException("maxThreadCount should be at least 1");
            }
            if (maxMergeCount < 1) {
                throw new IllegalArgumentException("maxMergeCount should be at least 1");
            }
            if (maxThreadCount > maxMergeCount) {
                throw new IllegalArgumentException("maxThreadCount should be <= maxMergeCount (= " + maxMergeCount + ")");
            }
            this.maxThreadCount = maxThreadCount;
            this.maxMergeCount = maxMergeCount;
        }
    }

    /**
     * Sets max merges and threads to proper defaults for rotational or non-rotational storage.
     *
     * @param spins true to set defaults best for traditional rotational storage (spinning disks),
     *              else false (e.g. for solid-state disks)
     */
    public synchronized void setDefaultMaxMergesAndThreads(boolean spins) {
        if (spins) {
            maxThreadCount = 1;
            maxMergeCount = 6;
        } else {
            int coreCount = Runtime.getRuntime().availableProcessors();

            // Let tests override this to help reproducing a failure on a machine that has a different
            // core count than the one where the test originally failed:
            try {
                String value = System.getProperty(DEFAULT_CPU_CORE_COUNT_PROPERTY);
                if (value != null) {
                    coreCount = Integer.parseInt(value);
                }
            } catch (@SuppressWarnings("unused") Throwable ignored) {}

            // If you are indexing at full throttle, how many merge threads do you need to keep up? It
            // depends: for most data structures, merging is cheaper than indexing/flushing, but for knn
            // vectors, merges can require about as much work as the initial indexing/flushing. Plus
            // documents are indexed/flushed only once, but may be merged multiple times.
            // Here, we assume an intermediate scenario where merging requires about as much work as
            // indexing/flushing overall, so we give half the core count to merges.

            maxThreadCount = Math.max(1, coreCount / 2);
            maxMergeCount = maxThreadCount + 5;
        }
    }

    /**
     * Set the per-merge IO throttle rate for forced merges (default: {@code
     * Double.POSITIVE_INFINITY}).
     */
    public synchronized void setForceMergeMBPerSec(double v) {
        forceMergeMBPerSec = v;
        updatePendingMerge();
    }

    /**
     * Get the per-merge IO throttle rate for forced merges.
     */
    public synchronized double getForceMergeMBPerSec() {
        return forceMergeMBPerSec;
    }

    /**
     * Turn on dynamic IO throttling, to adaptively rate limit writes bytes/sec to the minimal rate
     * necessary so merges do not fall behind. By default this is disabled and writes are not
     * rate-limited.
     */
    public synchronized void enableAutoIOThrottle() {
        doAutoIOThrottle = true;
        targetMBPerSec = START_MB_PER_SEC;
        updatePendingMerge();
    }

    /**
     * Turn off auto IO throttling.
     *
     * @see #enableAutoIOThrottle
     */
    public synchronized void disableAutoIOThrottle() {
        doAutoIOThrottle = false;
        updatePendingMerge();
    }

    /**
     * Returns true if auto IO throttling is currently enabled.
     */
    public synchronized boolean getAutoIOThrottle() {
        return doAutoIOThrottle;
    }

    /**
     * Returns the currently set per-merge IO writes rate limit, if {@link #enableAutoIOThrottle} was
     * called, else {@code Double.POSITIVE_INFINITY}.
     */
    public synchronized double getIORateLimitMBPerSec() {
        if (doAutoIOThrottle) {
            return targetMBPerSec;
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    /**
     * Returns {@code maxThreadCount}.
     *
     * @see #setMaxMergesAndThreads(int, int)
     */
    public synchronized int getMaxThreadCount() {
        return maxThreadCount;
    }

    /**
     * See {@link #setMaxMergesAndThreads}.
     */
    public synchronized int getMaxMergeCount() {
        return maxMergeCount;
    }

    /**
     * Removes the merge from the pending merges.
     */
    synchronized void removePendingMerge(MergeTask mergeToRemove) {
        // Paranoia: don't trust Thread.equals:
        for (int i = 0; i < pendingMerges.size(); i++) {
            if (pendingMerges.get(i) == mergeToRemove) {
                pendingMerges.remove(i);
                return;
            }
        }
    }

    @Override
    public Executor getIntraMergeExecutor(OneMerge merge) {
        assert intraMergeExecutor != null : "scaledExecutor is not initialized";
        // don't do multithreaded merges for small merges
        if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB * 1024 * 1024) {
            return super.getIntraMergeExecutor(merge);
        }
        return intraMergeExecutor;
    }

    @Override
    public Directory wrapForMerge(OneMerge merge, Directory in) {
        MergeTask currentTask = localThreadMerge.get();

        // Return a wrapped Directory which has rate-limited output.
        // Note: the rate limiter is only per thread. So, if there are multiple merge threads running
        // and throttling is required, each thread will be throttled independently.
        // The implication of this, is that the total IO rate could be higher than the target rate.
        RateLimiter rateLimiter = currentTask.rateLimiter;
        return new FilterDirectory(in) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                ensureOpen();

                // This Directory is only supposed to be used during merging,
                // so all writes should have MERGE context, else there is a bug
                // somewhere that is failing to pass down the right IOContext:
                assert context.context() == IOContext.Context.MERGE : "got context=" + context.context();

                return new RateLimitedIndexOutput(rateLimiter, in.createOutput(name, context));
            }
        };
    }

    /**
     * Called whenever the running merges have changed, to set merge IO limits. This method sorts the
     * merge threads by their merge size in descending order and then pauses/unpauses threads from
     * first to last -- that way, smaller merges are guaranteed to run before larger ones.
     */
    protected synchronized void updatePendingMerge() {

        // Only look at threads that are alive & not in the
        // process of stopping (ie have an active merge):
        final List<MergeTask> activeMerges = new ArrayList<>();

        int mergeIdx = 0;
        while (mergeIdx < pendingMerges.size()) {
            final MergeTask mergeTask = pendingMerges.get(mergeIdx);
            if (mergeTask.isPending() == false) {
                // Prune any dead threads
                pendingMerges.remove(mergeIdx);
                continue;
            }
            activeMerges.add(mergeTask);
            mergeIdx++;
        }

        // Sort the merge threads, largest first:
        CollectionUtil.timSort(activeMerges);

        final int activeMergeCount = activeMerges.size();

        int bigMergeCount = 0;

        for (mergeIdx = activeMergeCount - 1; mergeIdx >= 0; mergeIdx--) {
            MergeTask mergeTask = activeMerges.get(mergeIdx);
            if (mergeTask.merge.estimatedMergeBytes > MIN_BIG_MERGE_MB * 1024 * 1024) {
                bigMergeCount = 1 + mergeIdx;
                break;
            }
        }

        long now = System.nanoTime();

        StringBuilder message;
        if (verbose()) {
            message = new StringBuilder();
            message.append(
                String.format(Locale.ROOT, "updateMergeThreads ioThrottle=%s targetMBPerSec=%.1f MB/sec", doAutoIOThrottle, targetMBPerSec)
            );
        } else {
            message = null;
        }

        for (mergeIdx = 0; mergeIdx < activeMergeCount; mergeIdx++) {
            MergeTask mergeTask = activeMerges.get(mergeIdx);

            OneMerge merge = mergeTask.merge;

            // pause the thread if maxThreadCount is smaller than the number of merge threads.
            final boolean doPause = mergeIdx < bigMergeCount - maxThreadCount;

            double newMBPerSec;
            if (doPause) {
                newMBPerSec = 0.0;
            } else if (merge.getStoreMergeInfo().mergeMaxNumSegments() != -1) {
                newMBPerSec = forceMergeMBPerSec;
            } else if (doAutoIOThrottle == false) {
                newMBPerSec = Double.POSITIVE_INFINITY;
            } else if (merge.estimatedMergeBytes < MIN_BIG_MERGE_MB * 1024 * 1024) {
                // Don't rate limit small merges:
                newMBPerSec = Double.POSITIVE_INFINITY;
            } else {
                newMBPerSec = targetMBPerSec;
            }

            MergeRateLimiter rateLimiter = mergeTask.rateLimiter;
            double curMBPerSec = rateLimiter.getMBPerSec();

            if (verbose()) {
                long mergeStartNS = -1; // merge.mergeStartNS;
                if (mergeStartNS == -1) {
                    // IndexWriter didn't start the merge yet:
                    mergeStartNS = now;
                }
                message.append('\n');
                message.append(
                    String.format(
                        Locale.ROOT,
                        "merge task %s estSize=%.1f MB (written=%.1f MB) runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s\n",
                        mergeTask.getName(),
                        bytesToMB(merge.estimatedMergeBytes),
                        bytesToMB(rateLimiter.getTotalBytesWritten()),
                        nsToSec(now - mergeStartNS),
                        nsToSec(rateLimiter.getTotalStoppedNS()),
                        nsToSec(rateLimiter.getTotalPausedNS()),
                        rateToString(rateLimiter.getMBPerSec())
                    )
                );

                if (newMBPerSec != curMBPerSec) {
                    if (newMBPerSec == 0.0) {
                        message.append("  now stop");
                    } else if (curMBPerSec == 0.0) {
                        if (newMBPerSec == Double.POSITIVE_INFINITY) {
                            message.append("  now resume");
                        } else {
                            message.append(String.format(Locale.ROOT, "  now resume to %.1f MB/sec", newMBPerSec));
                        }
                    } else {
                        message.append(
                            String.format(Locale.ROOT, "  now change from %.1f MB/sec to %.1f MB/sec", curMBPerSec, newMBPerSec)
                        );
                    }
                } else if (curMBPerSec == 0.0) {
                    message.append("  leave stopped");
                } else {
                    message.append(String.format(Locale.ROOT, "  leave running at %.1f MB/sec", curMBPerSec));
                }
            }

            rateLimiter.setMBPerSec(newMBPerSec);
        }
        if (verbose()) {
            message(message.toString());
        }
    }

    private synchronized void initDynamicDefaults() throws IOException {
        if (maxThreadCount == AUTO_DETECT_MERGES_AND_THREADS) {
            setDefaultMaxMergesAndThreads(false);
            if (verbose()) {
                message("initDynamicDefaults maxThreadCount=" + maxThreadCount + " maxMergeCount=" + maxMergeCount);
            }
        }
    }

    private static String rateToString(double mbPerSec) {
        if (mbPerSec == 0.0) {
            return "stopped";
        } else if (mbPerSec == Double.POSITIVE_INFINITY) {
            return "unlimited";
        } else {
            return String.format(Locale.ROOT, "%.1f MB/sec", mbPerSec);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            sync();
        } finally {
            if (intraMergeExecutor != null) {
                intraMergeExecutor.shutdown();
            }
        }
    }

    /**
     * Wait for any running merge threads to finish. This call is not interruptible as used by {@link
     * #close()}.
     */
    public void sync() {
        boolean interrupted = false;
        try {
            while (true) {
                MergeTask toSync = null;
                synchronized (this) {
                    MergeTask localThreadMerge = this.localThreadMerge.get();
                    for (MergeTask task : pendingMerges) {
                        // In case a merge thread is calling us, don't try to sync on
                        // itself, since that will never finish!
                        if (task != localThreadMerge) {
                            toSync = task;
                            break;
                        }
                    }
                }
                if (toSync != null) {
                    try {
                        toSync.await();
                    } catch (@SuppressWarnings("unused") InterruptedException ie) {
                        // ignore this Exception, we will retry until all threads are dead
                        interrupted = true;
                    }
                } else {
                    break;
                }
            }
        } finally {
            // finally, restore interrupt status:
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the number of merges that are pending. Note that this number is &le; {@link #pendingMerges} size.
     *
     * @lucene.internal
     */
    public synchronized int pendingMergeCount() {
        int count = 0;
        for (MergeTask pendingMerge : pendingMerges) {
            if (pendingMerge.isPending() && pendingMerge.merge.isAborted() == false) {
                count++;
            }
        }
        return count;
    }

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    public synchronized void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        if (initialized.get() == false && initialized.compareAndSet(false, true)) {
            initDynamicDefaults();
            if (intraMergeExecutor == null) {
                intraMergeExecutor = new CachedExecutor();
            }
        }

        if (trigger == MergeTrigger.CLOSING) {
            // Disable throttling on close:
            targetMBPerSec = MAX_MERGE_MB_PER_SEC;
            updatePendingMerge();
        }

        // First, quickly run through the newly proposed merges
        // and add any orthogonal merges (ie a merge not
        // involving segments already pending to be merged) to
        // the queue. If we are way behind on merging, many of
        // these newly proposed merges will likely already be
        // registered.

        if (verbose()) {
            message("now merge");
            message("  index(source): " + mergeSource.toString());
        }

        // Iterate, pulling from the IndexWriter's queue of
        // pending merges, until it's empty:
        while (true) {

            if (maybeStall(mergeSource) == false) {
                break;
            }

            OneMerge merge = mergeSource.getNextMerge();
            if (merge == null) {
                if (verbose()) {
                    message("  no more merges pending; now return");
                }
                return;
            }

            boolean success = false;
            try {
                final MergeTask newMergeTask = new MergeTask(mergeSource, merge, getMergeTaskName(mergeSource, merge));
                pendingMerges.add(newMergeTask);

                updateIOThrottle(newMergeTask.merge, newMergeTask.rateLimiter);

                if (verbose()) {
                    message("    schedule new merge task [" + newMergeTask.getName() + "]");
                }

                dispatchMerge(newMergeTask);
                updatePendingMerge();

                success = true;
            } finally {
                if (success == false) {
                    mergeSource.onMergeFinished(merge);
                }
            }
        }
    }

    protected abstract void dispatchMerge(MergeTask newMergeTask) throws IOException;

    /**
     * This is invoked by {@link #merge} to possibly stall the incoming thread when there are too many
     * merges running or pending. The default behavior is to force this thread, which is producing too
     * many segments for merging to keep up, to wait until merges catch up. Applications that can take
     * other less drastic measures, such as limiting how many threads are allowed to index, can do
     * nothing here and throttle elsewhere.
     *
     * <p>If this method wants to stall but the calling thread is a merge thread, it should return
     * false to tell caller not to kick off any new merges.
     */
    protected synchronized boolean maybeStall(MergeSource mergeSource) {
        long startStallTime = 0;
        while (mergeSource.hasPendingMerges() && pendingMergeCount() >= maxMergeCount) {

            // This means merging has fallen too far behind: we
            // have already created maxMergeCount threads, and
            // now there's at least one more merge pending.
            // Note that only maxThreadCount of
            // those created merge threads will actually be
            // running; the rest will be paused (see
            // updateMergeThreads). We stall this producer
            // thread to prevent creation of new segments,
            // until merging has caught up:

            boolean isOnMergeThread = localThreadMerge.get() != null;
            if (isOnMergeThread) {
                // Never stall a merge thread since this blocks the thread from
                // finishing and calling updateMergeThreads, and blocking it
                // accomplishes nothing anyway (it's not really a segment producer):
                return false;
            }

            if (startStallTime == 0) {
                startStallTime = System.currentTimeMillis();
                if (verbose()) {
                    message("    too many merges; stalling...");
                }
            }
            doStall();
        }

        if (verbose() && startStallTime != 0) {
            message("  stalled for " + (System.currentTimeMillis() - startStallTime) + " ms");
        }

        return true;
    }

    /**
     * Called from {@link #maybeStall} to pause the calling thread for a bit.
     */
    protected synchronized void doStall() {
        try {
            // Defensively wait for only .25 seconds in case we are missing a .notify/All somewhere:
            wait(250);
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
        }
    }

    /**
     * Does the actual merge, by calling {@link
     * org.apache.lucene.index.MergeScheduler.MergeSource#merge}
     */
    protected void doMerge(MergeSource mergeSource, OneMerge merge) throws IOException {
        mergeSource.merge(merge);
    }

    /**
     * Create and return a name for a new merge task
     */
    protected synchronized String getMergeTaskName(MergeSource mergeSource, OneMerge merge) {
        return "Lucene Merge #" + mergeTaskCount++;
    }

    synchronized void runOnMergeFinished(MergeSource mergeSource, MergeTask mergeTask) {
        // the merge call as well as the merge thread handling in the finally
        // block must be sync'd on CMS otherwise stalling decisions might cause
        // us to miss pending merges
        assert pendingMerges.contains(mergeTask) : "caller is not a merge thread";
        // Let CMS run new merges if necessary:
        try {
            merge(mergeSource, MergeTrigger.MERGE_FINISHED);
        } catch (@SuppressWarnings("unused") AlreadyClosedException ace) {
            // OK
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        } finally {
            removePendingMerge(mergeTask);
            updatePendingMerge();
            // In case we had stalled indexing, we can now wake up
            // and possibly unstall:
            notifyAll();
        }
    }

    /**
     * Runs a merge thread to execute a single merge, then exits.
     */
    protected class MergeTask implements Runnable, Comparable<MergeTask> {
        final MergeSource mergeSource;
        final OneMerge merge;
        final MergeRateLimiter rateLimiter;
        private final String name;
        private volatile long mergeStartNS = -1;
        final CountDownLatch mergeDoneLatch = new CountDownLatch(1);

        /**
         * Sole constructor.
         */
        public MergeTask(MergeSource mergeSource, OneMerge merge, String name) {
            this.mergeSource = mergeSource;
            this.merge = merge;
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
            this.name = name;
        }

        @Override
        public int compareTo(MergeTask other) {
            // Larger merges sort first:
            return Long.compare(other.merge.estimatedMergeBytes, merge.estimatedMergeBytes);
        }

        private void await() throws InterruptedException {
            mergeDoneLatch.await();
        }

        private boolean isPending() {
            return mergeDoneLatch.getCount() != 0;
        }

        @Override
        public void run() {
            mergeStartNS = System.nanoTime();
            try {
                localThreadMerge.set(this);
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s start", getName()));
                }

                doMerge(mergeSource, merge);
                if (verbose()) {
                    message(
                        String.format(
                            Locale.ROOT,
                            "merge task %s merge segment [%s] done estSize=%.1f MB (written=%.1f MB) runTime=%.1fs "
                                + "(stopped=%.1fs, paused=%.1fs) rate=%s",
                            getName(),
                            getSegmentName(merge),
                            bytesToMB(merge.estimatedMergeBytes),
                            bytesToMB(rateLimiter.getTotalBytesWritten()),
                            nsToSec(System.nanoTime() - mergeStartNS),
                            nsToSec(rateLimiter.getTotalStoppedNS()),
                            nsToSec(rateLimiter.getTotalPausedNS()),
                            rateToString(rateLimiter.getMBPerSec())
                        )
                    );
                }

                runOnMergeFinished(mergeSource, this);

                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", getName()));
                }
            } catch (Throwable exc) {
                if (exc instanceof MergePolicy.MergeAbortedException) {
                    // OK to ignore
                } else if (suppressExceptions == false) {
                    // suppressExceptions is normally only set during
                    // testing.
                    handleMergeException(exc);
                }
            } finally {
                localThreadMerge.remove();
                mergeDoneLatch.countDown();
            }
        }

        private String getName() {
            return name;
        }
    }

    /**
     * Called when an exception is hit in a background merge thread
     */
    protected void handleMergeException(Throwable exc) {
        throw new MergePolicy.MergeException(exc);
    }

    private boolean suppressExceptions;

    /**
     * Used for testing
     */
    void setSuppressExceptions() {
        if (verbose()) {
            message("will suppress merge exceptions");
        }
        suppressExceptions = true;
    }

    /**
     * Used for testing
     */
    void clearSuppressExceptions() {
        if (verbose()) {
            message("will not suppress merge exceptions");
        }
        suppressExceptions = false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + ": "
            + "maxThreadCount="
            + maxThreadCount
            + ", "
            + "maxMergeCount="
            + maxMergeCount
            + ", "
            + "ioThrottle="
            + doAutoIOThrottle;
    }

    private boolean isBacklog(long now, OneMerge merge) {
        double mergeMB = bytesToMB(merge.estimatedMergeBytes);
        for (MergeTask pendingMerge : pendingMerges) {
            long mergeStartNS = pendingMerge.mergeStartNS;
            if (pendingMerge.isPending()
                && pendingMerge.merge != merge
                && mergeStartNS != -1
                && pendingMerge.merge.estimatedMergeBytes >= MIN_BIG_MERGE_MB * 1024 * 1024
                && nsToSec(now - mergeStartNS) > 3.0) {
                double otherMergeMB = bytesToMB(pendingMerge.merge.estimatedMergeBytes);
                double ratio = otherMergeMB / mergeMB;
                if (ratio > 0.3 && ratio < 3.0) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Tunes IO throttle when a new merge starts.
     */
    private synchronized void updateIOThrottle(OneMerge newMerge, MergeRateLimiter rateLimiter) throws IOException {
        if (doAutoIOThrottle == false) {
            return;
        }

        double mergeMB = bytesToMB(newMerge.estimatedMergeBytes);
        if (mergeMB < MIN_BIG_MERGE_MB) {
            // Only watch non-trivial merges for throttling; this is safe because the MP must eventually
            // have to do larger merges:
            return;
        }

        long now = System.nanoTime();

        // Simplistic closed-loop feedback control: if we find any other similarly
        // sized merges running, then we are falling behind, so we bump up the
        // IO throttle, else we lower it:
        boolean newBacklog = isBacklog(now, newMerge);

        boolean curBacklog = false;

        if (newBacklog == false) {
            if (pendingMerges.size() > maxThreadCount) {
                // If there are already more than the maximum merge threads allowed, count that as backlog:
                curBacklog = true;
            } else {
                // Now see if any still-running merges are backlog'd:
                for (MergeTask mergeThread : pendingMerges) {
                    if (isBacklog(now, mergeThread.merge)) {
                        curBacklog = true;
                        break;
                    }
                }
            }
        }

        double curMBPerSec = targetMBPerSec;

        if (newBacklog) {
            // This new merge adds to the backlog: increase IO throttle by 20%
            targetMBPerSec *= 1.20;
            if (targetMBPerSec > MAX_MERGE_MB_PER_SEC) {
                targetMBPerSec = MAX_MERGE_MB_PER_SEC;
            }
            if (verbose()) {
                if (curMBPerSec == targetMBPerSec) {
                    message(
                        String.format(Locale.ROOT, "io throttle: new merge backlog; leave IO rate at ceiling %.1f MB/sec", targetMBPerSec)
                    );
                } else {
                    message(String.format(Locale.ROOT, "io throttle: new merge backlog; increase IO rate to %.1f MB/sec", targetMBPerSec));
                }
            }
        } else if (curBacklog) {
            // We still have an existing backlog; leave the rate as is:
            if (verbose()) {
                message(String.format(Locale.ROOT, "io throttle: current merge backlog; leave IO rate at %.1f MB/sec", targetMBPerSec));
            }
        } else {
            // We are not falling behind: decrease IO throttle by 10%
            targetMBPerSec /= 1.10;
            if (targetMBPerSec < MIN_MERGE_MB_PER_SEC) {
                targetMBPerSec = MIN_MERGE_MB_PER_SEC;
            }
            if (verbose()) {
                if (curMBPerSec == targetMBPerSec) {
                    message(
                        String.format(Locale.ROOT, "io throttle: no merge backlog; leave IO rate at floor %.1f MB/sec", targetMBPerSec)
                    );
                } else {
                    message(String.format(Locale.ROOT, "io throttle: no merge backlog; decrease IO rate to %.1f MB/sec", targetMBPerSec));
                }
            }
        }

        double rate;

        if (newMerge.getStoreMergeInfo().mergeMaxNumSegments() != -1) {
            rate = forceMergeMBPerSec;
        } else {
            rate = targetMBPerSec;
        }
        rateLimiter.setMBPerSec(rate);
        targetMBPerSecChanged();
    }

    /**
     * Subclass can override to tweak targetMBPerSec.
     */
    protected void targetMBPerSecChanged() {}

    private static double nsToSec(long ns) {
        return ns / (double) TimeUnit.SECONDS.toNanos(1);
    }

    private static double bytesToMB(long bytes) {
        return bytes / 1024. / 1024.;
    }

    private static String getSegmentName(MergePolicy.OneMerge merge) {
        return merge.getMergeInfo() != null ? merge.getMergeInfo().info.name : "_na_";
    }

    /**
     * This executor provides intra-merge threads for parallel execution of merge tasks. It provides a
     * limited number of threads to execute merge tasks. In particular, if the number of
     * `mergeThreads` is equal to `maxThreadCount`, then the executor will execute the merge task in
     * the calling thread.
     */
    private class CachedExecutor implements Executor {

        private final AtomicInteger activeCount = new AtomicInteger(0);
        private final ThreadPoolExecutor executor;

        private CachedExecutor() {
            this.executor = new ThreadPoolExecutor(0, 1024, 1L, TimeUnit.MINUTES, new SynchronousQueue<>());
        }

        void shutdown() {
            executor.shutdown();
        }

        @Override
        public void execute(Runnable command) {
            final boolean isThreadAvailable;
            // we need to check if a thread is available before submitting the task to the executor
            // synchronize on CMS to get an accurate count of current threads
            synchronized (AbstractThreadPoolMergeScheduler.this) {
                int max = maxThreadCount - pendingMerges.size() - 1;
                int value = activeCount.get();
                if (value < max) {
                    activeCount.incrementAndGet();
                    assert activeCount.get() > 0 : "active count must be greater than 0 after increment";
                    isThreadAvailable = true;
                } else {
                    isThreadAvailable = false;
                }
            }
            if (isThreadAvailable) {
                executor.execute(() -> {
                    try {
                        command.run();
                    } catch (Throwable exc) {
                        if (suppressExceptions == false) {
                            // suppressExceptions is normally only set during
                            // testing.
                            handleMergeException(exc);
                        }
                    } finally {
                        activeCount.decrementAndGet();
                        assert activeCount.get() >= 0 : "unexpected negative active count";
                    }
                });
            } else {
                command.run();
            }
        }
    }
}
