/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service.accord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.RoutingKey;
import accord.impl.AbstractAsyncExecutor;
import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SequentialAsyncExecutor;
import accord.local.cfk.CommandsForKey;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.MessageType.StandardMessage;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.IntrusivePriorityHeap;
import accord.utils.Invariants;
import accord.utils.QuadConsumer;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import accord.utils.TriConsumer;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncCallbacks.CallAndCallback;
import accord.utils.async.AsyncCallbacks.FlatCallAndCallback;
import accord.utils.async.AsyncCallbacks.RunAndCallback;
import accord.utils.async.AsyncCallbacks.RunOrFail;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.AccordConfig.QueuePriorityModel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.metrics.AccordExecutorMetrics;
import org.apache.cassandra.metrics.AccordReplicaMetrics;
import org.apache.cassandra.metrics.AccordSystemMetrics;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms.LogLinearDecayingHistogram;
import org.apache.cassandra.metrics.ShardedDecayingHistograms;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.DecayingHistogramsShard;
import org.apache.cassandra.service.accord.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.UniqueSave;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugSequentialExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import io.netty.util.concurrent.FastThreadLocal;

import static accord.utils.Invariants.createIllegalState;
import static org.apache.cassandra.config.AccordConfig.QueuePriorityModel.PHASE_HLC_FIFO;
import static org.apache.cassandra.service.accord.AccordCache.CommandAdapter.COMMAND_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.CommandsForKeyAdapter.CFK_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.registerJfrListener;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordTask.State.LOADING;
import static org.apache.cassandra.service.accord.AccordTask.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordTask.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_RUN;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * NOTE: We assume that NO BLOCKING TASKS are submitted to this executor AND WAITED ON by another task executing on this executor.
 *  (as we do not immediately schedule additional threads for submitted tasks, but schedule new threads only if necessary when the submitting execution completes)
 */
public abstract class AccordExecutor implements CacheSize, LoadExecutor<AccordTask<?>, Boolean>, SaveExecutor, Shutdownable, AbstractAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AccordExecutor.class);

    private static final long PRIORITY_BITS = 0x7000000000000000L;
    private static final QueuePriorityModel PRIORITY_MODEL = DatabaseDescriptor.getAccord().queue_priority_model;
    private static final long AGE_TO_FIFO = DatabaseDescriptor.getAccord().queue_priority_age_to_fifo.to(TimeUnit.MICROSECONDS);
    public static final ShardedDecayingHistograms HISTOGRAMS = new ShardedDecayingHistograms();
    private static final FastThreadLocal<Lock> paranoidPriorityInversionCheck = new FastThreadLocal<>();

    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveGlobalCaches extends GlobalCaches implements AutoCloseable
    {
        final AccordExecutor executor;

        public ExclusiveGlobalCaches(AccordExecutor executor, AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey)
        {
            super(global, commands, commandsForKey);
            this.executor = executor;
        }

        @Override
        public void close()
        {
            executor.beforeUnlockExternal();
            global.tryShrinkOrEvict(executor.lock);
            executor.unlock();
        }
    }

    public static class GlobalCaches
    {
        public final AccordCache global;
        public final AccordCache.Type<TxnId, Command, AccordSafeCommand> commands;
        public final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey;

        public GlobalCaches(AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey)
        {
            this.global = global;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
        }
    }

    private static class WaitForCompletion
    {
        final long position;
        long maybeNotify;
        final Runnable run;

        private WaitForCompletion(long position, Runnable run)
        {
            this.position = position;
            this.maybeNotify = position - 1;
            this.run = run;
        }

        public String toString()
        {
            return run.toString() + " @" + position;
        }
    }

    private final Lock lock;
    final Agent agent;
    final int executorId;
    private final AccordCache cache;

    private final TaskQueue<AccordTask<?>> scanningRanges = new TaskQueue<>(SCANNING_RANGES); // never queried, just parked here while scanning
    private final TaskQueue<AccordTask<?>> loading = new TaskQueue<>(LOADING); // never queried, just parked here while loading
    private final TaskQueue<Task> running = new TaskQueue<>(RUNNING);

    private final TaskQueue<AccordTask<?>> waitingToLoadRangeTxns = new TaskQueue<>(WAITING_TO_LOAD);
    private final TaskQueue<AccordTask<?>> waitingToLoad = new TaskQueue<>(WAITING_TO_LOAD);
    private final TaskQueue<Task> waitingToRun = new TaskQueue<>(WAITING_TO_RUN);

    private final ExclusiveGlobalCaches caches;

    private List<Condition> waitingForQuiescence;
    private Queue<WaitForCompletion> waitingForCompletion;

    final LogLinearDecayingHistograms histograms;
    final LogLinearDecayingHistogram elapsedPreparingToRun;
    final LogLinearDecayingHistogram elapsedWaitingToRun;
    final LogLinearDecayingHistogram elapsedRunning;
    final LogLinearDecayingHistogram elapsed;
    final LogLinearDecayingHistogram keys;
    public final AccordReplicaMetrics.Shard replicaMetrics;

    /**
     * The maximum total number of loads we can queue at once - this includes loads for range transactions,
     * which are subject to this limit as well as that imposed by {@link #maxQueuedRangeLoads}
      */
    private int maxQueuedLoads = 64;
    /**
     * The maximum number of loads exclusively for range transactions we can queue at once; the {@link #maxQueuedLoads} limit also applies.
     */
    private int maxQueuedRangeLoads = 8;

    private long maxWorkingSetSizeInBytes;
    private long maxWorkingCapacityInBytes;
    private long minPosition, nextPosition;
    private int activeLoads, activeRangeLoads;
    private boolean hasPausedLoading;
    int tasks;
    final DebugExecutor debug = DebugExecutor.maybeDebug();

    AccordExecutor(Lock lock, int executorId, Agent agent)
    {
        this.lock = lock;
        this.executorId = executorId;
        this.cache = new AccordCache(this, 0);
        this.agent = agent;

        final AccordCache.Type<TxnId, Command, AccordSafeCommand> commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey;
        commands = cache.newType(TxnId.class, COMMAND_ADAPTER, AccordCacheMetrics.CommandsCacheMetrics.newShard(lock));
        registerJfrListener(executorId, commands, "Command");

        commandsForKey = cache.newType(RoutingKey.class, CFK_ADAPTER, AccordCacheMetrics.CommandsForKeyCacheMetrics.newShard(lock));
        registerJfrListener(executorId, commandsForKey, "CommandsForKey");

        this.caches = new ExclusiveGlobalCaches(this, cache, commands, commandsForKey);

        DecayingHistogramsShard histogramsShard = HISTOGRAMS.newShard(lock);
        this.histograms = histogramsShard.unsafeGetInternal();
        this.elapsedPreparingToRun = AccordExecutorMetrics.INSTANCE.elapsedPreparingToRun.forShard(histogramsShard);
        this.elapsedWaitingToRun = AccordExecutorMetrics.INSTANCE.elapsedWaitingToRun.forShard(histogramsShard);
        this.elapsedRunning = AccordExecutorMetrics.INSTANCE.elapsedRunning.forShard(histogramsShard);
        this.elapsed = AccordExecutorMetrics.INSTANCE.elapsed.forShard(histogramsShard);
        this.keys = AccordExecutorMetrics.INSTANCE.keys.forShard(histogramsShard);
        this.replicaMetrics = new AccordReplicaMetrics.Shard(histogramsShard);
    }

    public int executorId()
    {
        return executorId;
    }

    public ExclusiveGlobalCaches lockCaches()
    {
        lock();
        return caches;
    }

    abstract boolean isInLoop();

    public final Lock unsafeLock()
    {
        return lock;
    }

    final void lock()
    {
        if (Invariants.isParanoid()) paranoidLockExclusive();
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        if (DEBUG_EXECUTION) debug.onEnterLock();
    }

    final void unlock()
    {
        if (Invariants.isParanoid()) paranoidUnlockExclusive();
        if (DEBUG_EXECUTION) debug.onExitLock();
        lock.unlock();
    }

    final void paranoidLockExclusive()
    {
        Lock locked = paranoidPriorityInversionCheck.getAndSet(lock);
        Invariants.require(locked == null || locked == lock, "Tried to take multiple AccordExecutor locks on same thread - this is dangerous for progress");
    }

    final void paranoidUnlockExclusive()
    {
        paranoidPriorityInversionCheck.set(null);
    }

    final boolean tryLock()
    {
       return onTryLock(lock.tryLock());
    }

    final boolean onTryLock(boolean result)
    {
        if (DEBUG_EXECUTION && result) debug.onEnterLock();
        if (Invariants.isParanoid())
        {
            if (result)
            {
                Lock locked = paranoidPriorityInversionCheck.getAndSet(lock);
                if (locked != null && locked != lock)
                {
                    lock.unlock();
                    paranoidPriorityInversionCheck.set(locked);
                    Invariants.require(false, "Tried to take multiple AccordExecutor locks on same thread - this is dangerous for progress");
                    return false;
                }
            }
            else
            {
                Lock locked = paranoidPriorityInversionCheck.get();
                Invariants.require(locked == null || locked == lock, "Tried to take multiple AccordExecutor locks on same thread - this is dangerous for progress");
            }
        }
        return result;
    }

    public AccordCache cacheExclusive()
    {
        Invariants.require(isOwningThread());
        return cache;
    }

    public AccordCache cacheUnsafe()
    {
        return cache;
    }

    final boolean hasWaitingToRun()
    {
        updateWaitingToRunExclusive();
        return hasAlreadyWaitingToRun();
    }

    final boolean hasAlreadyWaitingToRun()
    {
        return !waitingToRun.isEmpty();
    }

    void updateWaitingToRunExclusive()
    {
        // TODO (expected): this should not be invoked on every update of waiting to run
        maybeUnpauseLoading();
    }

    final Task pollWaitingToRunExclusive()
    {
        updateWaitingToRunExclusive();
        return pollAlreadyWaitingToRunExclusive();
    }

    final Task pollAlreadyWaitingToRunExclusive()
    {
        Task next = waitingToRun.poll();
        if (next != null)
        {
            if (DEBUG_EXECUTION) DebugTask.get(next).onPolled();
            next.addToQueue(running);
        }
        return next;
    }

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return Stream.of();
    }

    public void waitForQuiescence()
    {
        Condition condition;
        lock();
        try
        {
            if (tasks == 0)
                return;

            if (waitingForQuiescence == null)
                waitingForQuiescence = new ArrayList<>();
            condition = Condition.newOneTimeCondition();
            waitingForQuiescence.add(condition);
        }
        finally
        {
            unlock();
        }
        condition.awaitThrowUncheckedOnInterrupt();
    }

    protected void notifyQuiescentExclusive()
    {
        if (waitingForQuiescence != null)
        {
            waitingForQuiescence.forEach(Condition::signalAll);
            waitingForQuiescence = null;
        }
        if (waitingForCompletion != null)
        {
            logger.warn("{} processed all pending tasks (<{}) but found waiting: {}", this, nextPosition, waitingForCompletion);
            waitingForCompletion.forEach(w -> w.run.run());
            waitingForCompletion = null;
        }
    }

    public void afterSubmittedAndConsequences(Runnable run)
    {
        lock();
        try
        {
            if (tasks == 0)
            {
                run.run();
                return;
            }

            if (waitingForCompletion != null) // escape hatch for some bug that means we lose a notification for a given task's queue position
                maybeNotifyWaitingForCompletion();
            if (waitingForCompletion == null)
                waitingForCompletion = new ArrayDeque<>();

            long position = nextPosition;
            minPosition = position;
            waitingForCompletion.add(new WaitForCompletion(position, run));
        }
        finally
        {
            unlock();
        }
    }

    void maybeUnpauseLoading()
    {
        if (!hasPausedLoading)
            return;

        if (cache.weightedSize() < maxWorkingCapacityInBytes || (loading.isEmpty() && waitingToRun.isEmpty()))
        {
            hasPausedLoading = false;
            enqueueLoadsExclusive();
        }
    }

    public abstract boolean hasTasks();
    abstract void beforeUnlockExternal();
    abstract boolean isOwningThread();

    private void enqueueLoadsExclusive()
    {
        outer: while (true)
        {
            TaskQueue<AccordTask<?>> queue = waitingToLoadRangeTxns.isEmpty() || activeRangeLoads >= maxQueuedRangeLoads ? waitingToLoad : waitingToLoadRangeTxns;
            AccordTask<?> next = queue.peek();
            if (next == null)
                return;

            if (hasPausedLoading || cache.weightedSize() >= maxWorkingCapacityInBytes)
            {
                // we have too much in memory already, and we have work waiting to run, so let that complete before queueing more
                if (!loading.isEmpty() || !waitingToRun.isEmpty())
                {
                    AccordSystemMetrics.metrics.pausedExecutorLoading.inc();
                    hasPausedLoading = true;
                    return;
                }
            }

            switch (next.state())
            {
                default:
                {
                    failExclusive(next, createIllegalState("Unexpected state: " + next.toDescription()));
                    break;
                }
                case WAITING_TO_SCAN_RANGES:
                    if (activeRangeLoads >= maxQueuedRangeLoads)
                    {
                        parkRangeLoad(next);
                    }
                    else
                    {
                        ++activeRangeLoads;
                        ++activeLoads;
                        next.rangeScanner().start(this);
                        updateQueue(next);
                    }
                    break;

                case WAITING_TO_LOAD:
                    while (true)
                    {
                        AccordCacheEntry<?, ?> load = next.peekWaitingToLoad();
                        boolean isForRange = isForRange(next, load);
                        if (isForRange && activeRangeLoads >= maxQueuedRangeLoads)
                        {
                            parkRangeLoad(next);
                            continue outer;
                        }

                        Invariants.require(load != null);
                        ++activeLoads;
                        if (isForRange)
                            ++activeRangeLoads;

                        for (AccordTask<?> task : cache.load(this, next, isForRange, load))
                        {
                            if (task == next) continue;
                            if (task.onLoading(load))
                                updateQueue(task);
                        }
                        Object prev = next.pollWaitingToLoad();
                        Invariants.require(prev == load);
                        if (next.peekWaitingToLoad() == null)
                            break;

                        Invariants.require(next.state() == WAITING_TO_LOAD, "Invalid state: %s", next);
                        if (activeLoads >= maxQueuedLoads)
                            return;
                    }
                    Invariants.require(next.state().compareTo(LOADING) >= 0, "Invalid state: %s", next);
                    updateQueue(next);
            }
        }
    }

    private boolean isForRange(AccordTask<?> task, AccordCacheEntry<?, ?> load)
    {
        boolean isForRangeTxn = task.hasRanges();
        if (!isForRangeTxn)
            return false;

        for (AccordTask<?> t : load.loadingOrWaiting().waiters())
        {
            if (!t.hasRanges())
                return false;
        }
        return true;
    }

    @Override
    public Cancellable execute(RunOrFail runOrFail)
    {
        PlainChain submit = new PlainChain(runOrFail);
        return submit(submit);
    }

    public <T> AsyncChain<T> buildDebuggable(Callable<T> task, Object describe)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return submit(new DebuggableChain(new CallAndCallback<>(task, callback), null, 0, describe));
            }
        };
    }

    private void parkRangeLoad(AccordTask<?> task)
    {
        if (task.queued() != waitingToLoadRangeTxns)
        {
            task.unqueueIfQueued();
            task.addToQueue(waitingToLoadRangeTxns);
        }
    }

    private void updateQueue(AccordTask<?> task)
    {
        task.unqueueIfQueued();
        switch (task.state())
        {
            default: throw new AssertionError("Unexpected state: " + task.toDescription());
            case WAITING_TO_SCAN_RANGES:
            case WAITING_TO_LOAD:
                task.addToQueue(waitingToLoad);
                break;
            case SCANNING_RANGES:
                task.addToQueue(scanningRanges);
                break;
            case LOADING:
                task.addToQueue(loading);
                break;
            case WAITING_TO_RUN:
                waitingToRun(task);
                break;
        }
    }

    private void waitingToRun(AccordTask<?> task)
    {
        task.onWaitingToRun();
        task.addToQueue(task.commandStore.exclusiveExecutor);
    }

    private void waitingToRun(Task task, @Nullable SequentialExecutor queue)
    {
        task.onWaitingToRun();
        task.addToQueue(queue == null ? waitingToRun : queue);
    }

    public SequentialExecutor executor()
    {
        return new SequentialExecutor(this);
    }

    public SequentialExecutor executor(int commandStoreId)
    {
        return new SequentialExecutor(this, commandStoreId);
    }

    public SequentialAsyncExecutor newSequentialExecutor()
    {
        return new SequentialExecutor(this);
    }

    public <R> void cancel(AccordTask<R> task)
    {
        Invariants.require(task.commandStore.executor() == this,
                              "%s is a wrong command store for %s, should be %s",
                              this, task, task);
        submit(AccordExecutor::cancelExclusive, CancelTask::new, task);
    }

    @Override
    public <K, V> Cancellable load(AccordTask<?> parent, Boolean isForRange, AccordCacheEntry<K, V> entry)
    {
        return submitPlainExclusive(parent, newLoad(entry, isForRange));
    }

    @Override
    public Cancellable save(AccordCacheEntry<?, ?> entry, UniqueSave identity, Runnable save)
    {
        return submitPlainExclusive(null, new SaveRunnable(entry, identity, save));
    }

    private <P1> void submit(BiConsumer<AccordExecutor, P1> sync, Function<P1, Task> async, P1 p1)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a), (f, p1a, p2a, p3) -> f.apply(p1a), sync, async, p1, null, null);
    }

    private <P1, P2> void submit(TriConsumer<AccordExecutor, P1, P2> sync, BiFunction<P1, P2, Task> async, P1 p1, P2 p2)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a, p2a), (f, p1a, p2a, p3) -> f.apply(p1a, p2a), sync, async, p1, p2, null);
    }

    private <P1, P2, P3> void submit(QuadConsumer<AccordExecutor, P1, P2, P3> sync, TriFunction<P1, P2, P3, Task> async, P1 p1, P2 p2, P3 p3)
    {
        submit((e, c, p1a, p2a, p3a) -> c.accept(e, p1a, p2a, p3a), TriFunction::apply, sync, async, p1, p2, p3);
    }

    private <P1, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1, P2, P3, P4> sync, QuadFunction<P1, P2, P3, P4, Task> async, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        submit(sync, async, p1, p1, p2, p3, p4);
    }

    abstract <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Task> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    <R> void submit(AccordTask<R> operation)
    {
        submit(AccordExecutor::submitExclusive, i -> i, operation);
    }

    <R> void submitPriority(AccordTask<R> operation)
    {
        submit(AccordExecutor::submitPriorityExclusive, i -> i, operation);
    }

    void submitExclusive(AccordTask<?> task)
    {
        assignQueuePosition(task);
        submitInternalExclusive(task);
    }

    void submitPriorityExclusive(AccordTask<?> task)
    {
        assignMinQueuePosition(task);
        submitInternalExclusive(task);
    }

    private void submitInternalExclusive(AccordTask<?> task)
    {
        task.setupExclusive();
        ++tasks;
        updateQueue(task);
        enqueueLoadsExclusive();
    }

    public void submitExclusive(Runnable runnable)
    {
        submitPlainExclusive(new PlainRunnable(null, runnable));
    }

    private void submitPlainExclusive(Plain task)
    {
        ++tasks;
        assignQueuePosition(task);
        waitingToRun(task, task.executor());
    }

    Cancellable submitPlainExclusive(Task parent, AbstractIOTask task)
    {
        return submitPlainExclusive(parent, new WrappedIOTask(task));
    }

    <T extends Task> T submitPlainExclusive(Task parent, T task)
    {
        Invariants.require(isOwningThread());
        ++tasks;
        if (parent != null) inheritQueuePosition(parent, task);
        else assignFifoQueuePosition(task);
        task.onWaitingToRun();
        waitingToRun.append(task);
        return task;
    }

    private void assignQueuePosition(Task task)
    {
        if (task.queuePosition != 0) updateNextPosition(task);
        else assignFifoQueuePosition(task);
    }

    private void assignQueuePosition(AccordTask<?> task)
    {
        if (task.queuePosition != 0) updateNextPosition(task);
        else
        {
            long priority_bits = PRIORITY_BITS;
            TxnId txnId = null;
            switch (PRIORITY_MODEL)
            {
                case ORIG_PHASE_HLC_FIFO:
                case PHASE_HLC_FIFO:
                {
                    // TODO (expected): we should process messages for a TxnId together, to avoid processing delayed messages out of order
                    PreLoadContext context = task.preLoadContext();
                    if (context instanceof Request)
                    {
                        MessageType type = ((Request) context).type();
                        if (type instanceof StandardMessage)
                        {
                            TxnId txnId0 = context.primaryTxnId();
                            switch ((StandardMessage)type)
                            {
                                case APPLY_REQ:
                                {
                                    priority_bits = 0L;
                                    txnId = txnId0;
                                    break;
                                }
                                case READ_EPHEMERAL_REQ:
                                case READ_REQ:
                                case STABLE_THEN_READ_REQ:
                                {
                                    priority_bits = 1000000000000000L;
                                    txnId = txnId0;
                                    break;
                                }
                                case COMMIT_REQ:
                                {
                                    Commit commit = (Commit) context;
                                    if (PRIORITY_MODEL == PHASE_HLC_FIFO || commit.ballot.equals(Ballot.ZERO))
                                        txnId = commit.txnId;
                                    if (commit.kind.saveStatus == SaveStatus.Stable) priority_bits = 1000000000000L;
                                    else priority_bits = 2000000000000L;
                                    break;
                                }
                                case ACCEPT_REQ:
                                {
                                    Accept accept = (Accept) context;
                                    if (PRIORITY_MODEL == PHASE_HLC_FIFO || accept.ballot.equals(Ballot.ZERO))
                                        txnId = accept.txnId;
                                    priority_bits = 3000000000000L;
                                    break;
                                }
                                case GET_EPHEMERAL_READ_DEPS_REQ:
                                case PRE_ACCEPT_REQ:
                                {
                                    txnId = txnId0;
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case HLC_FIFO:
                {
                    txnId = task.preLoadContext().primaryTxnId();
                    break;
                }
                case FIFO:
                {
                    break;
                }
            }

            if (txnId != null)
            {
                long hlc = txnId.hlc();
                long delta = nextPosition - hlc;
                if (delta < AGE_TO_FIFO)
                {
                    long position = hlc;
                    if (delta <= 0) nextPosition = position + 1;
                    else if (position < minPosition) position = minPosition;
                    position |= priority_bits;
                    task.queuePosition = position;
                    return;
                }
            }

            assignFifoQueuePosition(task);
        }

    }

    private void assignMinQueuePosition(Task task)
    {
        task.queuePosition = minPosition | PRIORITY_BITS;
    }

    private void assignFifoQueuePosition(Task task)
    {
        task.queuePosition = nextPosition++ | PRIORITY_BITS;
    }

    private void updateNextPosition(Task task)
    {
        nextPosition = Math.max(nextPosition, (task.queuePosition & ~PRIORITY_BITS) + 1);
    }

    private void inheritQueuePosition(Task parent, Task task)
    {
        task.queuePosition = parent.queuePosition;
    }

    void completeTaskExclusive(Task task)
    {
        // for integration with SequentialExecutor, we must :
        //  - first take the position so that represents the just-executed task
        //  - call cleanup to submit any following task on the relevant sub-queue
        //  - remove the previous task from the running collection only if still present (SequentialExecutor will have removed it)
        long position = task.queuePosition;
        try
        {
            task.cleanupExclusive(this);
        }
        finally
        {
            --tasks;
            if (running.contains(task))
                running.remove(task);

            if (waitingForCompletion != null && waitingForCompletion.peek().maybeNotify <= position)
                maybeNotifyWaitingForCompletion();

            cache.tryShrinkOrEvict(lock);
        }
    }

    private void maybeNotifyWaitingForCompletion()
    {
        long min = minPosition(waitingToRun.peek(),
                    minPosition(waitingToLoad.peek(),
                      minPosition(waitingToLoadRangeTxns.peek(),
                        minPosition(running.peek(),
                          minPosition(loading.peek(),
                            minPosition(scanningRanges.peek(), Long.MAX_VALUE))))));

        while (!waitingForCompletion.isEmpty() && waitingForCompletion.peek().position - min <= 0)
            waitingForCompletion.poll().run.run();
        if (waitingForCompletion.isEmpty())
            waitingForCompletion = null;
        else
            waitingForCompletion.peek().maybeNotify = min;
    }

    private static long minPosition(@Nullable Task task, long min)
    {
        return task == null ? min : Long.min(task.queuePosition, min);
    }

    void cancelExclusive(AccordTask<?> task)
    {
        AccordTask.State state = task.state();
        switch (state)
        {
            default: throw new UnhandledEnum(state);
            case SCANNING_RANGES:
            case LOADING:
            case WAITING_TO_LOAD:
            case WAITING_TO_SCAN_RANGES:
            case WAITING_TO_RUN:
                task.unqueueIfQueued();
                try { task.cancelExclusive(); }
                finally { completeTaskExclusive(task); }
                break;

            case INITIALIZED: // TODO (expected): preferable to be able to cancel at this stage, even if unlikely to trigger at this phase
            case ASSIGNED:
            case RUNNING:
            case PERSISTING:
            case FINISHED:
            case CANCELLED:
            case FAILED:
                // cannot safely cancel
        }
    }

    void onScannedRangesExclusive(AccordTask<?> task, Throwable fail)
    {
        --activeLoads;
        --activeRangeLoads;
        // the task may have already been cancelled, in which case we don't need to fail it
        if (!task.state().isExecuted())
        {
            if (fail != null)
            {
                failExclusive(task, fail);
            }
            else
            {
                task.rangeScanner().scannedExclusive();
                updateQueue(task);
            }
        }
        enqueueLoadsExclusive();
    }

    private void failExclusive(AccordTask<?> task, Throwable fail)
    {
        if (task.state().isExecuted())
            return;

        try { task.failExclusive(fail); }
        catch (Throwable t) { agent.onException(t); }
        finally
        {
            task.unqueueIfQueued();
            completeTaskExclusive(task);
        }
    }

    private <K, V> void onSavedExclusive(AccordCacheEntry<K, V> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    private <K, V> void onLoadedExclusive(AccordCacheEntry<K, V> loaded, V value, Throwable fail, boolean isForRange)
    {
        --activeLoads;
        if (isForRange)
            --activeRangeLoads;

        if (loaded.status() != EVICTED)
        {
            try (BufferList<AccordTask<?>> tasks = loaded.loading().copyWaiters())
            {
                if (fail != null)
                {
                    for (AccordTask<?> task : tasks)
                        failExclusive(task, fail);
                    cache.failedToLoad(loaded);
                }
                else
                {
                    cache.loaded(loaded, value);
                    for (AccordTask<?> task : tasks)
                    {
                        if (task.onLoad(loaded))
                        {
                            Invariants.require(task.queued() == loading);
                            task.unqueue();
                            waitingToRun(task);
                        }
                    }
                }
            }
        }

        enqueueLoadsExclusive();
    }

    public Future<?> submit(Runnable run)
    {
        PlainRunnable task = new PlainRunnable(new AsyncPromise<>(), run);
        submit(task);
        return task.result;
    }

    public void execute(Runnable command)
    {
        submit(new PlainRunnable(null, command));
    }

    private Cancellable submit(Plain task)
    {
        submit(AccordExecutor::submitPlainExclusive, i -> i, task);
        return task;
    }

    public void executeDirectlyWithLock(Runnable command)
    {
        lock();
        try
        {
            command.run();
        }
        finally
        {
            beforeUnlockExternal();
            unlock();
        }
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.require(isOwningThread());
        cache.setCapacity(bytes);
        maxWorkingCapacityInBytes = cache.capacity() + maxWorkingSetSizeInBytes;
    }

    public void setWorkingSetSize(long bytes)
    {
        Invariants.require(isOwningThread());
        maxWorkingSetSizeInBytes = bytes;
        maxWorkingCapacityInBytes = cache.capacity() + maxWorkingSetSizeInBytes;
        if (maxWorkingCapacityInBytes < maxWorkingSetSizeInBytes)
            maxWorkingCapacityInBytes = Long.MAX_VALUE;
    }

    public void setMaxQueuedLoads(int total, int range)
    {
        Invariants.require(isOwningThread());
        Invariants.requireArgument(total >= 1, "Must permit at least one load");
        Invariants.requireArgument(range >= 1, "Must permit at least one range load");
        maxQueuedLoads = total;
        maxQueuedRangeLoads = range;
    }

    @Override
    public long capacity()
    {
        return cache.capacity();
    }

    @Override
    public int size()
    {
        return cache.size();
    }

    @Override
    public long weightedSize()
    {
        return cache.weightedSize();
    }

    protected static abstract class TaskRunner implements DebuggableTaskRunner
    {
        // TODO (desired): this probably doesn't need to be volatile
        private volatile Task running;
        private static final AtomicReferenceFieldUpdater<TaskRunner, Task> runningUpdater = AtomicReferenceFieldUpdater.newUpdater(TaskRunner.class, Task.class, "running");

        @Override
        public DebuggableTask running()
        {
            Task running = this.running;
            return running == null ? null : running.debuggable();
        }

        Task runningTask()
        {
            return running;
        }

        void setRunning(Task debuggable)
        {
            runningUpdater.lazySet(this, debuggable);
        }

        void clearRunning()
        {
            runningUpdater.lazySet(this, null);
        }
    }

    public static abstract class Task extends IntrusivePriorityHeap.Node
    {
        public final WithResources resources;
        Task next;
        long queuePosition;
        public long createdAt = nanoTime(), waitingToRunAt, runningAt, cleanupAt;

        protected Task()
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        }

        public final Task unwrap()
        {
            if (this instanceof SequentialQueueTask)
                return ((SequentialQueueTask) this).queue.task;
            return this;
        }

        final void setReadyToCleanup()
        {
            queuePosition |= Long.MIN_VALUE;
        }

        final boolean isReadyToCleanup()
        {
            return 0 != (queuePosition & Long.MIN_VALUE);
        }

        final void onRunning()
        {
            runningAt = nanoTime();
            if (DEBUG_EXECUTION) ((DebugTask)resources).onRunning();
        }

        final void onRunComplete()
        {
            if (DEBUG_EXECUTION) ((DebugTask)resources).onRunComplete();
        }

        final void onWaitingToRun()
        {
            waitingToRunAt = nanoTime();
        }

        static Task reverse(Task unqueued)
        {
            Task prev = null;
            Task cur = unqueued;
            while (cur != null)
            {
                Task next = cur.next;
                cur.next = prev;
                prev = cur;
                cur = next;
            }
            return prev;
        }

        public DebuggableTask debuggable() { return null; }

        abstract void submitExclusive(AccordExecutor owner);

        /**
         * Prepare to run while holding the state cache lock
         */
        abstract protected void preRunExclusive();

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        protected abstract void runInternal();

        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void fail(Throwable fail);

        /**
         * Cleanup the command while holding the state cache lock
         */
        protected void cleanupExclusive(AccordExecutor executor)
        {
            cleanupAt = nanoTime();
            if (runningAt != 0)
            {
                if (waitingToRunAt == 0)
                    waitingToRunAt = runningAt;
                executor.elapsedWaitingToRun.increment(runningAt - waitingToRunAt, runningAt);
                executor.elapsedPreparingToRun.increment(waitingToRunAt - createdAt, runningAt);
                executor.elapsedRunning.increment(cleanupAt - runningAt, cleanupAt);
                executor.elapsed.increment(cleanupAt - createdAt, cleanupAt);
            }
            if (DEBUG_EXECUTION) DebugTask.get(this).onCompleted(executor.debug);
        }

        void cancelExclusive(AccordExecutor owner) {}

        abstract protected void addToQueue(TaskQueue queue);
    }

    // run the task even on a stopped commandStore
    public interface Unstoppable extends PreLoadContext.Empty
    {
    }

    // run the task even on a terminated commandStore
    public interface Unterminatable extends Unstoppable
    {
    }

    static final class SequentialQueueTask extends Task
    {
        private final SequentialExecutor queue;

        SequentialQueueTask(SequentialExecutor queue)
        {
            super();
            this.queue = queue;
        }

        @Override void submitExclusive(AccordExecutor owner) { throw new UnsupportedOperationException(); }

        @Override
        protected void preRunExclusive()
        {
            queue.preRunTask();
        }

        @Override
        protected void runInternal()
        {
            queue.runTask();
        }

        @Override
        protected void fail(Throwable t)
        {
            queue.failTask(t);
        }

        @Override
        protected void cleanupExclusive(AccordExecutor executor)
        {
            queue.cleanupTask();
        }

        @Override
        protected void addToQueue(TaskQueue queue)
        {
            Invariants.require(queue.kind == RUNNING);
            queue.append(this);
        }

        protected boolean isInHeap()
        {
            return super.isInHeap();
        }
    }

    private static final AtomicReferenceFieldUpdater<SequentialExecutor, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(SequentialExecutor.class, Thread.class, "owner");
    public class SequentialExecutor extends TaskQueue<Task> implements SequentialAsyncExecutor
    {
        final int commandStoreId;
        final SequentialQueueTask selfTask;
        private Task task;
        private volatile Thread owner, waiting;
        private boolean stopped;
        private volatile boolean visibleStopped;
        private boolean terminated;

        final DebugSequentialExecutor debug;

        SequentialExecutor(AccordExecutor executor)
        {
            this(executor, -1);
        }

        SequentialExecutor(AccordExecutor executor, int commandStoreId)
        {
            super(WAITING_TO_RUN, commandStoreId < 0);
            this.commandStoreId = commandStoreId;
            this.selfTask = new SequentialQueueTask(this);
            this.debug = DebugSequentialExecutor.maybeDebug(executor.debug, commandStoreId);
        }

        void preRunTask()
        {
            Invariants.require(task != null);
            task.preRunExclusive();
        }

        void runTask()
        {
            Thread self = Thread.currentThread();
            if (!ownerUpdater.compareAndSet(this, null, self))
            {
                if (DEBUG_EXECUTION) debug.onWaiting();
                Invariants.require(self == Thread.currentThread());
                waiting = self;
                outer: do
                {
                    while (true)
                    {
                        Thread owner = this.owner;
                        if (owner == self) break outer;
                        if (owner == null) continue outer;
                        LockSupport.park();
                    }
                }
                while (!ownerUpdater.compareAndSet(this, null, self));
            }
            waiting = null;

            if (stopped && reject(task))
                task.fail(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + ((AccordTask<?>) task).preLoadContext()));
            else
                task.runInternal();
            // NOTE: cannot safely release owner here, in case an immediate-execution runs before we can release our references and store their changes to the cache
        }

        private boolean reject(Task task)
        {
            if (!(task instanceof AccordTask<?>))
                return true;

            PreLoadContext context = ((AccordTask<?>) task).preLoadContext();

            return !(terminated ? (context instanceof Unterminatable) : (context instanceof Unstoppable));
        }

        void failTask(Throwable t)
        {
            task.fail(t);
        }

        void cleanupTask()
        {
            try { task.cleanupExclusive(AccordExecutor.this); }
            finally
            {
                owner = null;
                task = super.poll();
                if (DEBUG_EXECUTION) debug.onSetTask(task);

                // it should only be possible for this method to be invoked once we're on the running queue
                AccordExecutor.this.running.remove(selfTask);
                if (task != null)
                {
                    selfTask.queuePosition = task.queuePosition;
                    waitingToRun.append(selfTask);
                }
            }
        }

        // invoked by removeAndUpdateNext; expect to already be next
        @Override
        protected void append(Task newTask)
        {
            if (task != null)
            {
                Invariants.require(selfTask.isInHeap());
                super.append(newTask);
            }
            else
            {
                Invariants.require(isEmpty());
                task = newTask;
                selfTask.queuePosition = newTask.queuePosition;
                waitingToRun.append(selfTask);
                if (DEBUG_EXECUTION) debug.onSetTask(newTask);
            }
        }

        @Override
        protected void remove(Task remove)
        {
            if (remove == task) removeCurrentTask(remove);
            else super.remove(remove);
        }

        @Override
        protected boolean removeIfContains(Task remove)
        {
            if (remove == task) return removeCurrentTask(remove);
            else return super.removeIfContains(remove);
        }

        private boolean removeCurrentTask(Node remove)
        {
            if (running.contains(selfTask))
                return false;

            Invariants.require(remove == task);
            // cannot overwrite task while it is being executed - this cannot happen for AccordTask
            // but can for other tasks that don't track their own state

            task = super.poll();
            if (DEBUG_EXECUTION) debug.onSetTask(task);
            if (waitingToRun.contains(selfTask))
            {
                if (task == null) waitingToRun.remove(selfTask);
                else
                {
                    selfTask.queuePosition = task.queuePosition;
                    waitingToRun.update(selfTask);
                }
            }
            else
            {
                Invariants.expect(false, "%s should have been queued to run as it had the task %s pending, that has now been cancelled", this, remove);
                if (task != null)
                {
                    selfTask.queuePosition = task.queuePosition;
                    waitingToRun.append(selfTask);
                }
            }
            Invariants.require(task == null || waitingToRun.contains(selfTask));
            return true;
        }

        public boolean inExecutor()
        {
            return owner == Thread.currentThread();
        }

        public boolean stopped()
        {
            return visibleStopped;
        }

        void stop()
        {
            Invariants.require(inExecutor());
            this.stopped = true;
            this.visibleStopped = true;
        }

        void terminate()
        {
            Invariants.require(inExecutor());
            this.visibleStopped = this.terminated = this.stopped = true;
        }

        @Override
        protected Task poll()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Task peek()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean contains(Task contains)
        {
            return super.contains(contains) || (task == contains && !running.contains(selfTask));
        }

        @Override
        public AsyncChain<Void> chain(Runnable run)
        {
            long position = inheritQueuePosition();
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super Void, Throwable> callback)
                {
                    return execute(new RunAndCallback(run, callback), position);
                }
            };
        }

        @Override
        public <T> AsyncChain<T> chain(Callable<T> call)
        {
            long position = inheritQueuePosition();
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    return execute(new CallAndCallback<>(call, callback), position);
                }
            };
        }

        @Override
        public <T> AsyncChain<T> flatChain(Callable<? extends AsyncChain<T>> call)
        {
            long position = inheritQueuePosition();
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    return execute(new FlatCallAndCallback<>(call, callback), position);
                }
            };
        }

        @Override
        public Cancellable execute(RunOrFail runOrFail)
        {
            return execute(runOrFail, inheritQueuePosition());
        }

        private long inheritQueuePosition()
        {
            return inExecutor() && task != null ? task.queuePosition : 0;
        }

        private Cancellable execute(RunOrFail runOrFail, long queuePosition)
        {
            PlainChain submit = new PlainChain(runOrFail, SequentialExecutor.this, queuePosition);
            return AccordExecutor.this.submit(submit);
        }

        @Override
        public void execute(Runnable run)
        {
            PlainRunnable submit = new PlainRunnable(null, run, this, inheritQueuePosition());
            AccordExecutor.this.submit(submit);
        }

        @Override
        public boolean tryExecuteImmediately(Runnable run)
        {
            Thread self = Thread.currentThread();
            Thread owner = this.owner;
            if (owner != null ? owner != self : !ownerUpdater.compareAndSet(this, null, self))
                return false;

            try { run.run(); }
            catch (Throwable t) { agent.onException(t); }
            finally
            {
                if (owner == null)
                {
                    Thread waiting = this.waiting;
                    Invariants.require(waiting != self);
                    this.owner = waiting;
                    if (waiting == null) // recheck, to ensure happens-before relation with a new waiter that expects any non-null owner to notify it
                        waiting = this.waiting;
                    if (waiting != null)
                        LockSupport.unpark(waiting);
                }
            }
            return true;
        }
    }

    static class TaskQueue<T extends Task> extends IntrusivePriorityHeap<T>
    {
        final AccordTask.State kind;

        TaskQueue(AccordTask.State kind)
        {
            this.kind = kind;
        }

        TaskQueue(AccordTask.State kind, boolean tiny)
        {
            super(tiny);
            this.kind = kind;
        }

        @Override
        public int compare(T o1, T o2)
        {
            return Long.compare(o1.queuePosition, o2.queuePosition);
        }

        protected void append(T task)
        {
            super.append(task);
        }

        protected void update(T task)
        {
            super.update(task);
        }

        protected T poll()
        {
            ensureHeapified();
            return pollNode();
        }

        protected T peek()
        {
            ensureHeapified();
            return peekNode();
        }

        protected T get(int index)
        {
            return super.get(index);
        }

        protected void remove(T remove)
        {
            super.remove(remove);
        }

        @Override
        protected boolean removeIfContains(T node)
        {
            return super.removeIfContains(node);
        }

        protected boolean contains(T contains)
        {
            return super.contains(contains);
        }
    }

    static class CancelTask extends Task
    {
        final Task cancel;
        private CancelTask(Task cancel) { this.cancel = cancel; }
        @Override void submitExclusive(AccordExecutor owner) { cancel.cancelExclusive(owner); }
        @Override protected void preRunExclusive() { throw new UnsupportedOperationException(); }
        @Override protected void runInternal() { throw new UnsupportedOperationException(); }
        @Override protected void fail(Throwable fail) { throw new UnsupportedOperationException(); }
        @Override protected void addToQueue(TaskQueue queue) { throw new UnsupportedOperationException(); }
    }

    static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    abstract class Plain extends Task implements Cancellable
    {
        abstract SequentialExecutor executor();

        @Override
        protected void preRunExclusive() {}

        @Override
        protected final void addToQueue(TaskQueue queue)
        {
            Invariants.require(queue.kind == WAITING_TO_RUN || queue.kind == RUNNING);
            queue.append(this);
        }

        @Override
        public void cancel()
        {
            submit((e, c) -> c.cancelExclusive(e), CancelTask::new, this);
        }

        void cancelExclusive(AccordExecutor owner)
        {
            SequentialExecutor executor = executor();
            TaskQueue queue = executor == null ? waitingToRun : executor;
            if (queue.contains(this))
            {
                queue.remove(this);
                completeTaskExclusive(this);
                try { fail(new CancellationException()); }
                catch (Throwable t) { agent.onException(t); }
            }
        }

        @Override
        final void submitExclusive(AccordExecutor owner)
        {
            owner.submitPlainExclusive(this);
        }
    }

    class PlainRunnable extends Plain implements Cancellable
    {
        final @Nullable AsyncPromise<Void> result;
        final Runnable run;
        final @Nullable SequentialExecutor executor;

        PlainRunnable(Runnable run)
        {
            this(null, run);
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run)
        {
            this(result, run, null, 0);
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run, @Nullable SequentialExecutor executor, long queuePosition)
        {
            this.result = result;
            this.run = run;
            this.executor = executor;
            this.queuePosition = queuePosition;
        }

        @Override
        protected void runInternal()
        {
            onRunning();
            try (Closeable close = resources.get())
            {
                run.run();
            }
            if (result != null)
                result.trySuccess(null);
            onRunComplete();
        }

        @Override
        protected void fail(Throwable t)
        {
            if (result != null)
                result.tryFailure(t);
            agent.onException(t);
        }

        @Override
        SequentialExecutor executor()
        {
            return executor;
        }
    }

    // a task that may be submitted to this executor or another
    abstract class IOTask extends Plain implements Cancellable, DebuggableTask
    {
        final long createdAtNanos = MonotonicClock.Global.approxTime.now();
        long startedAtNanos;

        abstract void postRunExclusive();

        @Override
        protected void preRunExclusive()
        {
            startedAtNanos = MonotonicClock.Global.approxTime.now();
        }

        @Override
        protected void cleanupExclusive(AccordExecutor executor)
        {
            super.cleanupExclusive(executor);
            postRunExclusive();
        }

        @Override
        SequentialExecutor executor()
        {
            return null;
        }

        @Override
        public long creationTimeNanos()
        {
            return createdAtNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return startedAtNanos;
        }
    }

    static class FailureHolder
    {
        static final FailureHolder NOT_STARTED = new FailureHolder(new RuntimeException("Not started"));

        final Throwable fail;

        FailureHolder(Throwable fail)
        {
            this.fail = fail;
        }
    }

    <K, V> LoadRunnable<K, V> newLoad(AccordCacheEntry<K, V> entry, boolean isForRange)
    {
        return isForRange ? new LoadRangeRunnable<>(entry) : new LoadRunnable<>(entry);
    }

    class LoadRunnable<K, V> extends IOTask
    {
        final AccordCacheEntry<K, V> entry;
        Object result = FailureHolder.NOT_STARTED;

        LoadRunnable(AccordCacheEntry<K, V> entry)
        {
            this.entry = entry;
        }

        boolean isForRange() { return false; }

        void postRunExclusive()
        {
            if (!(result instanceof FailureHolder)) onLoadedExclusive(entry, (V)result, null, isForRange());
            else onLoadedExclusive(entry, null, ((FailureHolder)result).fail, isForRange());
        }

        @Override
        public void runInternal()
        {
            onRunning();
            try (Closeable close = resources.get())
            {
                result = entry.owner.parent().adapter().load(entry.owner.commandStore, entry.key());
            }
            onRunComplete();
        }

        @Override
        protected void fail(Throwable t)
        {
            result = new FailureHolder(t);
        }

        @Override
        public String description()
        {
            return "Loading " + entry;
        }
    }

    final class LoadRangeRunnable<K, V> extends LoadRunnable<K, V>
    {
        LoadRangeRunnable(AccordCacheEntry<K, V> entry) { super(entry); }
        @Override boolean isForRange() { return true; }
    }

    static abstract class AbstractIOTask
    {
        abstract protected void runInternal();
        abstract protected void postRunExclusive();
        abstract protected void fail(Throwable t);
        abstract protected String description();
    }

    class WrappedIOTask extends IOTask
    {
        final AbstractIOTask wrapped;

        WrappedIOTask(AbstractIOTask wrap)
        {
            this.wrapped = wrap;
        }

        @Override
        protected void runInternal()
        {
            onRunning();
            try (Closeable close = resources.get())
            {
                wrapped.runInternal();
            }
            onRunComplete();
        }

        @Override
        void postRunExclusive()
        {
            wrapped.postRunExclusive();
        }

        @Override
        public String description()
        {
            return wrapped.description();
        }

        @Override
        protected void fail(Throwable fail)
        {
            wrapped.fail(fail);
        }
    }

    private static final Throwable NOT_STARTED = new Throwable();
    class SaveRunnable extends IOTask
    {
        final AccordCacheEntry<?, ?> entry;
        final UniqueSave identity;
        final Runnable run;
        Throwable failure = NOT_STARTED;

        SaveRunnable(AccordCacheEntry<?, ?> entry, UniqueSave identity, Runnable run)
        {
            this.entry = entry;
            this.identity = identity;
            this.run = run;
        }

        @Override
        void postRunExclusive()
        {
            onSavedExclusive(entry, identity, failure);
        }

        @Override
        public void runInternal()
        {
            onRunning();
            try (Closeable close = resources.get())
            {
                run.run();
            }
            onRunComplete();
            failure = null;
        }

        @Override
        protected void fail(Throwable t)
        {
            failure = t;
        }

        @Override
        public String description()
        {
            return "Save " + entry;
        }
    }

    class PlainChain extends Plain
    {
        final RunOrFail runOrFail;
        final @Nullable SequentialExecutor executor;

        PlainChain(RunOrFail runOrFail)
        {
            this(runOrFail, null, 0);
        }

        PlainChain(RunOrFail runOrFail, @Nullable SequentialExecutor executor, long queuePosition)
        {
            this.runOrFail = runOrFail;
            this.executor = executor;
            this.queuePosition = queuePosition;
        }

        @Override
        SequentialExecutor executor()
        {
            return executor;
        }

        @Override
        protected void runInternal()
        {
            onRunning();
            try (Closeable close = resources.get())
            {
                runOrFail.run();
            }
            catch (Throwable t)
            {
                // shouldn't throw exceptions
                agent.onException(t);
            }
            onRunComplete();
        }

        @Override
        protected void fail(Throwable fail)
        {
            try
            {
                runOrFail.fail(fail);
            }
            catch (Throwable t)
            {
                fail.addSuppressed(t);
                agent.onException(fail);
            }
        }
    }

    class DebuggableChain extends PlainChain implements DebuggableTask
    {
        final long createdAtNanos;
        long startedAtNanos;
        final Object describe;

        DebuggableChain(RunOrFail runOrFail, @Nullable SequentialExecutor executor, int queuePosition, Object describe)
        {
            super(runOrFail, executor, queuePosition);
            this.createdAtNanos = MonotonicClock.Global.approxTime.now();
            this.describe = Invariants.nonNull(describe);
        }

        @Override
        public long creationTimeNanos()
        {
            return createdAtNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return startedAtNanos;
        }

        @Override
        protected void preRunExclusive()
        {
            startedAtNanos = MonotonicClock.Global.approxTime.now();
        }

        @Override
        public String description()
        {
            return describe.toString();
        }

        @Override
        public DebuggableTask debuggable()
        {
            return this;
        }
    }


    public static class TaskInfo implements Comparable<TaskInfo>
    {
        // sorted in name order for reporting to virtual tables
        public enum Status { LOADING, RUNNING, SCANNING_RANGES, WAITING_TO_LOAD, WAITING_TO_RUN }

        final Status status;
        final int commandStoreId;

        final Task task;

        public TaskInfo(Status status, int commandStoreId, Task task)
        {
            this.status = status;
            this.commandStoreId = commandStoreId;
            this.task = task;
        }

        public Status status()
        {
            return status;
        }

        public Integer commandStoreId()
        {
            return commandStoreId >= 0 ? commandStoreId : null;
        }

        public long position()
        {
            return task.queuePosition;
        }

        public @Nullable String describe()
        {
            if (task instanceof AccordTask)
                return ((AccordTask<?>) task).preLoadContext().reason();

            if (task instanceof DebuggableTask)
                return ((DebuggableTask) task).description();

            return null;
        }

        public @Nullable PreLoadContext preLoadContext()
        {
            if (task instanceof AccordTask)
                return ((AccordTask<?>) task).preLoadContext();
            if (task instanceof WrappedIOTask && ((WrappedIOTask) task).wrapped instanceof AccordTask.RangeTxnScanner)
                return ((AccordTask<?>.RangeTxnScanner) ((WrappedIOTask) task).wrapped).preLoadContext();
            return null;
        }

        @Override
        public int compareTo(TaskInfo that)
        {
            int c = this.status.compareTo(that.status);
            if (c == 0) c = Long.compare(this.position(), that.position());
            return c;
        }
    }

    public List<TaskInfo> taskSnapshot()
    {
        List<TaskInfo> result = new ArrayList<>();
        lock();
        try
        {
            addToSnapshot(result, waitingToLoad, TaskInfo.Status.WAITING_TO_LOAD, TaskInfo.Status.WAITING_TO_LOAD);
            addToSnapshot(result, waitingToLoadRangeTxns, TaskInfo.Status.WAITING_TO_LOAD, TaskInfo.Status.WAITING_TO_LOAD);
            addToSnapshot(result, scanningRanges, TaskInfo.Status.SCANNING_RANGES, TaskInfo.Status.SCANNING_RANGES);
            addToSnapshot(result, loading, TaskInfo.Status.LOADING, TaskInfo.Status.LOADING);
            addToSnapshot(result, waitingToRun, TaskInfo.Status.WAITING_TO_RUN, TaskInfo.Status.WAITING_TO_RUN);
            addToSnapshot(result, running, TaskInfo.Status.RUNNING, TaskInfo.Status.WAITING_TO_RUN);
        }
        finally
        {
            unlock();
        }
        result.sort(TaskInfo::compareTo);
        return result;
    }

    private static void addToSnapshot(List<TaskInfo> snapshot, TaskQueue<?> queue, TaskInfo.Status ifCurrent, TaskInfo.Status ifQueued)
    {
        for (int i = 0 ; i < queue.size() ; ++i)
        {
            Task t = queue.get(i);
            if (t instanceof SequentialQueueTask)
            {
                SequentialExecutor q = ((SequentialQueueTask) t).queue;
                snapshot.add(new TaskInfo(ifCurrent, q.commandStoreId, q.task));
                for (int j = 0 ; j < q.size() ; ++j)
                    snapshot.add(new TaskInfo(ifQueued, q.commandStoreId, q.get(j)));
            }
            else
            {
                int commmandStoreId = t instanceof AccordTask ? ((AccordTask<?>) t).commandStore.id() : -1;
                snapshot.add(new TaskInfo(ifCurrent, commmandStoreId, t));
            }
        }
    }

    public int unsafePreparingToRunCount()
    {
        return waitingToLoad.size() + waitingToLoadRangeTxns.size() + scanningRanges.size() + loading.size();
    }

    public int unsafeWaitingToRunCount()
    {
        return waitingToRun.size();
    }

    public int unsafeRunningCount()
    {
        return running.size();
    }

}
