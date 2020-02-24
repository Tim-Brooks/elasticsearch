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

package org.elasticsearch.action.bulk;

import org.HdrHistogram.Recorder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.RecordJFR;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class BatchedShardExecutor implements IndexEventListener {

    private static final long SYNC_SCHEDULE_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(5);
    private static final long MAX_EXECUTE_NANOS = TimeUnit.MILLISECONDS.toNanos(25);
    private static final String CLOSED_SHARD_MESSAGE = "Cannot perform operation, IndexShard is closed.";

    private static final Logger logger = LogManager.getLogger(BatchedShardExecutor.class);

    private final CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler;
    private final CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler;
    private final ThreadPool threadPool;
    private final int numberOfWriteThreads;

    private final ConcurrentHashMap<IndexShard, ShardState> shardStateMap = new ConcurrentHashMap<>();

    private final AtomicReference<MeanMetric> meanMetric = new AtomicReference<>(new MeanMetric());
    private final Recorder timeSliceRecorder = new Recorder(1, TimeUnit.SECONDS.toMicros(60), 3);


    @Inject
    public BatchedShardExecutor(ClusterService clusterService, ThreadPool threadPool, UpdateHelper updateHelper,
                                MappingUpdatedAction mappingUpdatedAction) {
        this(primaryOpHandler(clusterService, threadPool, updateHelper, mappingUpdatedAction), replicaOpHandler(), threadPool);
    }

    public BatchedShardExecutor(CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler,
                                CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler, ThreadPool threadPool) {
        this.primaryOpHandler = primaryOpHandler;
        this.replicaOpHandler = replicaOpHandler;
        this.threadPool = threadPool;
        this.numberOfWriteThreads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        RecordJFR.scheduleMeanSample("BatchedShardExecutor#NumberOfOps", threadPool, meanMetric);
        RecordJFR.scheduleHistogramSample("BatchedShardExecutor#TimeSlice", threadPool, new AtomicReference<>(timeSliceRecorder));
    }

    @Override
    public synchronized void afterIndexShardCreated(IndexShard indexShard) {
        shardStateMap.put(indexShard, new ShardState(numberOfWriteThreads, indexShard));
    }

    @Override
    public synchronized void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            ShardState state = shardStateMap.remove(indexShard);
            if (state != null) {
                state.close();
                ShardOp shardOp;
                ArrayList<ActionListener<Void>> listenersToFail = new ArrayList<>();
                while ((shardOp = state.pollPreIndexed()) != null) {
                    listenersToFail.add(shardOp.getWriteListener());
                }
                while ((shardOp = state.pollPostIndexed()) != null) {
                    listenersToFail.add(shardOp.getFlushListener());
                }
                onFailure(listenersToFail.stream(), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
            }
        }
    }

    public void primary(BulkShardRequest request, IndexShard primary, ActionListener<WriteResult> writeListener,
                        ActionListener<FlushResult> flushListener) {
        PrimaryOp shardOp = new PrimaryOp(request, primary, writeListener, flushListener);
        enqueueAndScheduleWrite(shardOp, true);
    }

    public void replica(BulkShardRequest request, IndexShard replica, ActionListener<Void> writeListener,
                        ActionListener<FlushResult> flushListener) {
        ReplicaOp shardOp = new ReplicaOp(request, replica, writeListener, flushListener);
        enqueueAndScheduleWrite(shardOp, true);
    }

    ShardState getShardState(IndexShard indexShard) {
        return shardStateMap.get(indexShard);
    }

    private void enqueueAndScheduleWrite(ShardOp shardOp, boolean allowReject) {
        IndexShard indexShard = shardOp.getIndexShard();
        ShardState shardState = shardStateMap.get(indexShard);

        if (shardState == null) {
            onFailure(Stream.of(shardOp.getWriteListener()), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
            return;
        }

        if (shardState.attemptPreIndexedEnqueue(shardOp, allowReject)) {
            // If the ShardState was closed after we enqueued, attempt to remove our operation and finish it
            // to ensure that it does not get lost.
            if (shardState.isClosed() && shardState.removePreIndexed(shardOp)) {
                onFailure(Stream.of(shardOp.getWriteListener()), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
            } else {
                maybeSchedule(shardState);
            }
        } else {
            throw new EsRejectedExecutionException("rejected execution of shard operation", false);
        }
    }

    private void enqueueFlush(ShardState shardState, ShardOp shardOp) {
        shardState.postIndexedEnqueue(shardOp);
        if (shardState.isClosed() && shardState.removePostIndexed(shardOp)) {
            onFailure(Stream.of(shardOp.getFlushListener()), new AlreadyClosedException(CLOSED_SHARD_MESSAGE));
        }
    }

    private void maybeSchedule(ShardState shardState) {
        if (shardState.shouldScheduleWriteTask()) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(new AbstractRunnable() {

                @Override
                public void onFailure(Exception e) {
                    logger.error("Uncaught exception when handling shard operations", e);
                    assert false : e;
                }

                @Override
                protected void doRun() {
                    shardState.markTaskStarted();
                    performShardOperations(shardState);
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }

                @Override
                public void onRejection(Exception e) {
                    assert false : e;
                }

                @Override
                public void onAfter() {
                    if (shardState.preIndexedQueue.isEmpty() == false || shardState.postIndexedQueue.isEmpty() == false) {
                        maybeSchedule(shardState);
                    }
                }
            });
        }
    }

    private void performShardOperations(ShardState shardState) {
        IndexShard indexShard = shardState.indexShard;

        ArrayList<ShardOp> needRefresh = new ArrayList<>(0);
        boolean immediateRefresh = false;
        int opsExecuted = 0;
        long startNanos = System.nanoTime();
        try {
            ShardOp shardOp;
            long lastSyncCheckNanos = 0;
            long nanosSpentExecuting = 0;
            boolean continueExecuting = true;
            while (continueExecuting && (shardOp = shardState.pollPreIndexed()) != null) {
                boolean opCompleted = false;
                try {
                    if (shardOp instanceof PrimaryOp) {
                        PrimaryOp primaryOp = (PrimaryOp) shardOp;
                        Runnable rescheduler = () -> enqueueAndScheduleWrite(primaryOp, false);
                        opCompleted = primaryOpHandler.apply(primaryOp, rescheduler);
                    } else {
                        opCompleted = replicaOpHandler.apply((ReplicaOp) shardOp);
                    }
                } catch (Exception e) {
                    onFailure(Stream.of(shardOp.getWriteListener()), e);
                } finally {
                    ++opsExecuted;
                    if (opCompleted) {
                        // Complete the write listener
                        onResponse(Stream.of(shardOp.getWriteListener()), null);

                        enqueueFlush(shardState, shardOp);

                        WriteRequest.RefreshPolicy refreshPolicy = shardOp.getRequest().getRefreshPolicy();
                        if (refreshPolicy == WriteRequest.RefreshPolicy.WAIT_UNTIL) {
                            needRefresh.add(shardOp);
                        } else if (refreshPolicy == WriteRequest.RefreshPolicy.IMMEDIATE) {
                            needRefresh.add(shardOp);
                            immediateRefresh = true;
                            shardOp.getFlushListener().setForcedRefresh(true);
                        }
                        afterWrite(indexShard);
                    }

                    // Update nanosSpentExecuting every 8 operations
                    if ((opsExecuted & (8 - 1)) == 0) {
                        nanosSpentExecuting = System.nanoTime() - startNanos;
                        if ((nanosSpentExecuting - lastSyncCheckNanos) > SYNC_SCHEDULE_INTERVAL_NANOS) {
                            lastSyncCheckNanos = nanosSpentExecuting;
                            boolean performedSyncs = maybeExecuteSync(shardState);
                            if (performedSyncs) {
                                continueExecuting = false;
                            }
                        }
                        if (nanosSpentExecuting >= MAX_EXECUTE_NANOS) {
                            continueExecuting = false;
                        }
                    }
                }
            }
        } finally {
            if (opsExecuted > 0) {
                long tenSeconds = TimeUnit.SECONDS.toMicros(10);
                timeSliceRecorder.recordValue(Math.min(TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos), tenSeconds));
                meanMetric.get().inc(opsExecuted);
            }
            if (needRefresh.isEmpty() == false) {
                performRefreshes(indexShard, needRefresh, immediateRefresh);
            }
            maybeExecuteSync(shardState);
        }
    }

    private boolean maybeExecuteSync(ShardState shardState) {
        if (shardState.shouldStartSyncing()) {
            IndexShard indexShard = shardState.indexShard;
            try {
                while (true) {
                    ArrayList<ShardOp> completedOpsAlreadySynced = new ArrayList<>();
                    ArrayList<ShardOp> completedOpsNeedSync = new ArrayList<>();

                    Translog.Location maxLocation = null;
                    Translog.Location syncedLocation = null;
                    try {
                        syncedLocation = indexShard.getTranslogLastSyncedLocation();
                    } catch (Exception e) {
                        // The Translog might have closed. Ignore.
                    }

                    ShardOp indexedOp;
                    int opsToHandle = 0;
                    while ((indexedOp = shardState.pollPostIndexed()) != null) {
                        ++opsToHandle;
                        Translog.Location location = indexedOp.locationToSync();
                        if (location != null) {
                            if (syncedLocation == null || needsSync(location, syncedLocation)) {
                                completedOpsNeedSync.add(indexedOp);
                                if (maxLocation == null) {
                                    maxLocation = location;
                                } else if (location.compareTo(maxLocation) > 0) {
                                    maxLocation = location;
                                }
                            } else {
                                completedOpsAlreadySynced.add(indexedOp);
                            }
                        } else {
                            completedOpsAlreadySynced.add(indexedOp);
                        }
                    }

                    if (opsToHandle == 0) {
                        break;
                    }

                    onResponse(completedOpsAlreadySynced.stream().map(ShardOp::getFlushListener), null);

                    if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST && maxLocation != null) {
                        syncOperations(indexShard, maxLocation, completedOpsNeedSync);
                    } else {
                        onResponse(completedOpsNeedSync.stream().map(ShardOp::getFlushListener), null);
                    }
                }
            } finally {
                shardState.markDoneSyncing();
            }
            return true;
        } else {
            return false;
        }
    }

    private static boolean needsSync(Translog.Location location, Translog.Location syncedLocation) {
        // if we have a new one it's already synced
        if (location.generation == syncedLocation.generation) {
            return (location.translogLocation + location.size) > syncedLocation.translogLocation;
        } else {
            return false;
        }
    }

    private void performRefreshes(IndexShard indexShard, ArrayList<ShardOp> needRefresh, boolean immediateRefresh) {
        assert needRefresh.isEmpty() == false;
        if (immediateRefresh) {
            immediateRefresh(indexShard, needRefresh);
        } else {
            waitUntilRefresh(indexShard, needRefresh);
        }
    }

    private void waitUntilRefresh(IndexShard indexShard, ArrayList<ShardOp> needRefresh) {

        Translog.Location maxLocation = null;
        for (ShardOp indexedOp : needRefresh) {
            Translog.Location location = indexedOp.locationToSync();
            if (maxLocation == null) {
                maxLocation = location;
            } else if (location != null && location.compareTo(maxLocation) > 0) {
                maxLocation = location;
            }
        }

        if (maxLocation != null) {
            addRefreshListeners(indexShard, maxLocation, needRefresh);
        } else {
            onResponse(needRefresh.stream().map(ShardOp::getFlushListener), null);
        }
    }

    private void immediateRefresh(IndexShard indexShard, ArrayList<ShardOp> needRefresh) {
        boolean refreshed = false;
        try {
            indexShard.refresh("refresh_flag_index");
            refreshed = true;
        } catch (Exception ex) {
            logger.warn("exception while forcing immediate refresh for shard operation", ex);
            onFailure(needRefresh.stream().map(ShardOp::getFlushListener), null);
        } finally {
            if (refreshed) {
                onResponse(needRefresh.stream().map(ShardOp::getFlushListener), null);
            }
        }
    }

    // TODO: Confirm if we want WARN log level. A lot of these will be thrown when the shard is closed/closing

    private void syncOperations(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> operations) {
        try {
            indexShard.sync(maxLocation, (ex) -> {
                if (ex == null) {
                    onResponse(operations.stream().map(ShardOp::getFlushListener), null);
                } else {
                    onFailure(operations.stream().map(ShardOp::getFlushListener), ex);
                }
            });
        } catch (Exception ex) {
            logger.warn("exception while syncing shard operations", ex);
            onFailure(operations.stream().map(ShardOp::getFlushListener), ex);
        }
    }

    private void addRefreshListeners(IndexShard indexShard, Translog.Location maxLocation, ArrayList<ShardOp> operations) {
        try {
            // TODO: Do we want to add each listener individually?
            indexShard.addRefreshListener(maxLocation, forcedRefresh -> {
                if (forcedRefresh) {
                    logger.warn("block until refresh ran out of slots and forced a refresh");
                }

                operations.forEach(op -> op.flushListener.setForcedRefresh(forcedRefresh));
                onResponse(operations.stream().map(ShardOp::getFlushListener), null);
            });
        } catch (Exception ex) {
            logger.warn("exception while adding refresh listener for shard operations", ex);
            onFailure(operations.stream().map(ShardOp::getFlushListener), ex);
        }
    }

    private void afterWrite(IndexShard indexShard) {
        try {
            indexShard.afterWriteOperation();
        } catch (Exception e) {
            logger.warn("exception while triggering post write operations", e);
        }
    }

    private <T> void onResponse(Stream<ActionListener<T>> listenerStream, T value) {
        try {
            ActionListener.onResponse(listenerStream, value);
        } catch (Exception e) {
            logger.error("uncaught exception when notifying shard operation listeners", e);
        }
    }

    private <T> void onFailure(Stream<ActionListener<T>> listenerStream, Exception ex) {
        try {
            ActionListener.onFailure(listenerStream, ex);
        } catch (Exception e) {
            logger.error("uncaught exception when notifying shard operation listeners", e);
        }
    }

    private static CheckedBiFunction<PrimaryOp, Runnable, Boolean, Exception> primaryOpHandler(
        ClusterService clusterService,
        ThreadPool threadPool,
        UpdateHelper updateHelper,
        MappingUpdatedAction mappingUpdatedAction) {
        return (primaryOp, rescheduler) -> {
            TimeValue timeout = primaryOp.getRequest().timeout();
            ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
            return TransportShardBulkAction.executeBulkItemRequests(primaryOp.context, updateHelper, threadPool::absoluteTimeInMillis,
                (update, shardId, mappingListener) -> {
                    assert update != null;
                    assert shardId != null;
                    mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
                }, TransportShardBulkAction.waitForMappingUpdate(observer, clusterService), rescheduler);
        };
    }

    private static CheckedFunction<ReplicaOp, Boolean, Exception> replicaOpHandler() {
        return (replicaOp) -> {
            Translog.Location location = TransportShardBulkAction.performOnReplica(replicaOp.getRequest(), replicaOp.getIndexShard());
            replicaOp.setLocation(location);
            return true;
        };
    }

    static class ShardState {

        private static final int MAX_QUEUED = 400;

        private final AtomicInteger pendingOps = new AtomicInteger(0);
        private final ConcurrentLinkedQueue<ShardOp> preIndexedQueue = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<ShardOp> postIndexedQueue = new ConcurrentLinkedQueue<>();
        private final IndexShard indexShard;
        private final int maxScheduledTasks;
        private final Semaphore scheduleTaskSemaphore;
        private final AtomicBoolean syncLock = new AtomicBoolean(false);
        private volatile boolean isClosed = false;

        private ShardState(int maxScheduledTasks, IndexShard indexShard) {
            this.maxScheduledTasks = maxScheduledTasks;
            this.scheduleTaskSemaphore = new Semaphore(maxScheduledTasks);
            this.indexShard = indexShard;
        }

        private boolean attemptPreIndexedEnqueue(ShardOp shardOp, boolean allowReject) {
            if (allowReject && pendingOps.get() >= MAX_QUEUED) {
                return false;
            } else {
                pendingOps.incrementAndGet();
                preIndexedQueue.add(shardOp);
                return true;
            }
        }

        private boolean removePreIndexed(ShardOp shardOp) {
            if (preIndexedQueue.remove(shardOp)) {
                pendingOps.getAndDecrement();
                return true;
            }
            return false;
        }

        private ShardOp pollPreIndexed() {
            ShardOp operation = preIndexedQueue.poll();
            if (operation != null) {
                pendingOps.getAndDecrement();
            }
            return operation;
        }

        private void postIndexedEnqueue(ShardOp shardOp) {
            postIndexedQueue.add(shardOp);
        }

        private boolean removePostIndexed(ShardOp shardOp) {
            return postIndexedQueue.remove(shardOp);
        }

        private ShardOp pollPostIndexed() {
            return postIndexedQueue.poll();
        }

        private boolean shouldStartSyncing() {
            return syncLock.get() == false && syncLock.compareAndSet(false, true);
        }

        private void markDoneSyncing() {
            assert syncLock.get();
            syncLock.set(false);
        }

        private boolean shouldScheduleWriteTask() {
            return scheduleTaskSemaphore.tryAcquire();
        }

        private void markTaskStarted() {
            scheduleTaskSemaphore.release();
        }

        int pendingOperations() {
            return pendingOps.get();
        }

        int scheduledTasks() {
            return maxScheduledTasks - scheduleTaskSemaphore.availablePermits();
        }

        boolean isClosed() {
            return isClosed;
        }

        void close() {
            isClosed = true;
        }
    }

    public static class WriteResult {

        @Nullable
        private final BulkShardRequest replicaRequest;
        private final BulkShardResponse response;


        public WriteResult(BulkShardRequest replicaRequest, BulkShardResponse response) {
            this.replicaRequest = replicaRequest;
            this.response = response;
        }

        public BulkShardRequest getReplicaRequest() {
            return replicaRequest;
        }

        public BulkShardResponse getResponse() {
            return response;
        }
    }

    public static class FlushResult {

        private final boolean forcedRefresh;

        public FlushResult(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }

        public boolean forcedRefresh() {
            return forcedRefresh;
        }
    }

    public abstract static class ShardOp {

        private final BulkShardRequest request;
        private final IndexShard indexShard;
        private final FlushListener flushListener;

        public ShardOp(BulkShardRequest request, IndexShard indexShard, ActionListener<FlushResult> flushListener) {
            this.request = request;
            this.indexShard = indexShard;
            this.flushListener = new FlushListener(request.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE, flushListener);
        }

        IndexShard getIndexShard() {
            return indexShard;
        }

        FlushListener getFlushListener() {
            return flushListener;
        }

        abstract ActionListener<Void> getWriteListener();

        abstract Translog.Location locationToSync();

        BulkShardRequest getRequest() {
            return request;
        }

        private static class FlushListener implements ActionListener<Void> {

            private final CountDown countDown;
            private final ActionListener<FlushResult> delegate;
            private volatile boolean forcedRefresh;

            private FlushListener(boolean waitOnRefresh, ActionListener<FlushResult> delegate) {
                this.delegate = delegate;
                if (waitOnRefresh) {
                    countDown = new CountDown(2);
                } else {
                    countDown = new CountDown(1);
                }
            }

            @Override
            public void onResponse(Void v) {
                if (countDown.countDown()) {
                    delegate.onResponse(new FlushResult(forcedRefresh));
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (countDown.fastForward()) {
                    delegate.onFailure(e);
                }
            }

            void setForcedRefresh(boolean forcedRefresh) {
                this.forcedRefresh = forcedRefresh;
            }
        }
    }

    public static class PrimaryOp extends ShardOp {

        private final BulkPrimaryExecutionContext context;
        private final ActionListener<Void> writeListener;

        public PrimaryOp(BulkShardRequest request, IndexShard indexShard, ActionListener<WriteResult> writeListener,
                         ActionListener<FlushResult> flushListener) {
            super(request, indexShard, flushListener);
            this.context = new BulkPrimaryExecutionContext(request, indexShard);
            this.writeListener = new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    writeListener.onResponse(new WriteResult(context.getBulkShardRequest(), context.buildShardResponse()));
                }

                @Override
                public void onFailure(Exception e) {
                    writeListener.onFailure(e);
                }
            };
        }

        @Override
        ActionListener<Void> getWriteListener() {
            return writeListener;
        }

        @Override
        Translog.Location locationToSync() {
            return context.getLocationToSync();
        }

        BulkPrimaryExecutionContext getContext() {
            return context;
        }
    }

    public static class ReplicaOp extends ShardOp {

        private final ActionListener<Void> listener;
        private Translog.Location location;

        public ReplicaOp(BulkShardRequest request, IndexShard indexShard, ActionListener<Void> listener,
                         ActionListener<FlushResult> flushListener) {
            super(request, indexShard, flushListener);
            this.listener = listener;
        }

        @Override
        ActionListener<Void> getWriteListener() {
            return listener;
        }

        @Override
        Translog.Location locationToSync() {
            return this.location;
        }

        void setLocation(Translog.Location location) {
            this.location = location;
        }
    }
}
