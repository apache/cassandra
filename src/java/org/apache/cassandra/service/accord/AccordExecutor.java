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
import accord.api.AsyncExecutor;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.SequentialAsyncExecutor;
import accord.local.cfk.CommandsForKey;
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
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.service.accord.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.UniqueSave;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.utils.Invariants.createIllegalState;
import static org.apache.cassandra.service.accord.AccordCache.CommandAdapter.COMMAND_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.CommandsForKeyAdapter.CFK_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.registerJfrListener;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordTask.State.LOADING;
import static org.apache.cassandra.service.accord.AccordTask.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordTask.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_RUN;

/**
 * NOTE: We assume that NO BLOCKING TASKS are submitted to this executor AND WAITED ON by another task executing on this executor.
 */
public abstract class AccordExecutor implements CacheSize, LoadExecutor<AccordTask<?>, Boolean>, SaveExecutor, Shutdownable, AsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AccordExecutor.class);
    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, AccordCacheMetrics metrics, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveGlobalCaches extends GlobalCaches implements AutoCloseable
    {
        final Lock lock;
        final AccordExecutor executor;

        public ExclusiveGlobalCaches(AccordExecutor executor, AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey)
        {
            super(global, commands, commandsForKey);
            this.lock = executor.lock;
            this.executor = executor;
        }

        @Override
        public void close()
        {
            executor.beforeUnlock();
            lock.unlock();
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

    final Lock lock;
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

    private static class WaitForCompletion
    {
        final int position;
        int maybeNotify;
        final Runnable run;

        private WaitForCompletion(int position, Runnable run)
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
    private int nextPosition;
    private int activeLoads, activeRangeLoads;
    private boolean hasPausedLoading;
    int tasks;
    int runningThreads;

    AccordExecutor(Lock lock, int executorId, AccordCacheMetrics metrics, Agent agent)
    {
        this.lock = lock;
        this.executorId = executorId;
        this.cache = new AccordCache(this, 0, metrics);
        this.agent = agent;

        final AccordCache.Type<TxnId, Command, AccordSafeCommand> commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey;
        commands = cache.newType(TxnId.class, COMMAND_ADAPTER);
        registerJfrListener(executorId, commands, "Command");

        commandsForKey = cache.newType(RoutingKey.class, CFK_ADAPTER);
        registerJfrListener(executorId, commandsForKey, "CommandsForKey");

        this.caches = new ExclusiveGlobalCaches(this, cache, commands, commandsForKey);
        ScheduledExecutors.scheduledFastTasks.scheduleAtFixedRate(() -> {
            executeDirectlyWithLock(cache::processNoEvictQueue);
        }, 1L, 1L, TimeUnit.SECONDS);
    }

    public int executorId()
    {
        return executorId;
    }

    public ExclusiveGlobalCaches lockCaches()
    {
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        return caches;
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

    boolean hasWaitingToRun()
    {
        updateWaitingToRunExclusive();
        return !waitingToRun.isEmpty();
    }

    Task pollWaitingToRunExclusive()
    {
        updateWaitingToRunExclusive();
        Task next = waitingToRun.poll();
        if (next != null)
            next.addToQueue(running);
        return next;
    }

    void updateWaitingToRunExclusive()
    {
        maybeUnpauseLoading();
    }

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return Stream.of();
    }

    public void waitForQuiescence()
    {
        Condition condition;
        lock.lock();
        try
        {
            if (tasks == 0 && runningThreads == 0)
                return;

            if (waitingForQuiescence == null)
                waitingForQuiescence = new ArrayList<>();
            condition = Condition.newOneTimeCondition();
            waitingForQuiescence.add(condition);
        }
        finally
        {
            lock.unlock();
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
        lock.lock();
        try
        {
            if (tasks == 0 && runningThreads == 0)
            {
                run.run();
                return;
            }

            if (waitingForCompletion != null) // escape hatch for some bug that means we lose a notification for a given task's queue position
                maybeNotifyWaitingForCompletion();
            if (waitingForCompletion == null)
                waitingForCompletion = new ArrayDeque<>();

            int position = nextPosition;
            waitingForCompletion.add(new WaitForCompletion(position, run));
        }
        finally
        {
            lock.unlock();
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
    abstract void beforeUnlock();
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
    public <T> AsyncChain<T> build(Callable<T> task)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return submit(new PlainChain<>(task, callback, null));
            }
        };
    }

    public <T> AsyncChain<T> buildDebuggable(Callable<T> task, Object describe)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                return submit(new DebuggableChain<>(task, callback, null, describe));
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

    void consumeExclusive(Submittable object)
    {
        try
        {
            if (object instanceof SubmittableTask) ((SubmittableTask) object).submitExclusive(this);
            else ((SubmitAsync) object).submitExclusive(this);
        }
        catch (Throwable t)
        {
            agent.onUncaughtException(t);
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

    private void waitingToRun(AccordTask task)
    {
        task.addToQueue(task.commandStore.exclusiveExecutor);
    }

    private void waitingToRun(SubmittableTask task, @Nullable SequentialExecutor queue)
    {
        task.addToQueue(queue == null ? waitingToRun : queue);
    }

    public SequentialExecutor executor()
    {
        return new SequentialExecutor();
    }

    public SequentialAsyncExecutor newSequentialExecutor()
    {
        return new SequentialExecutor();
    }

    public <R> void cancel(AccordTask<R> task)
    {
        Invariants.require(task.commandStore.executor() == this,
                              "%s is a wrong command store for %s, should be %s",
                              this, task, task);
        submit(AccordExecutor::cancelExclusive, CancelAsync::new, task);
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

    private <P1> void submit(BiConsumer<AccordExecutor, P1> sync, Function<P1, Submittable> async, P1 p1)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a), (f, p1a, p2a, p3) -> f.apply(p1a), sync, async, p1, null, null);
    }

    private <P1, P2> void submit(TriConsumer<AccordExecutor, P1, P2> sync, BiFunction<P1, P2, Submittable> async, P1 p1, P2 p2)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a, p2a), (f, p1a, p2a, p3) -> f.apply(p1a, p2a), sync, async, p1, p2, null);
    }

    private <P1, P2, P3> void submit(QuadConsumer<AccordExecutor, P1, P2, P3> sync, TriFunction<P1, P2, P3, Submittable> async, P1 p1, P2 p2, P3 p3)
    {
        submit((e, c, p1a, p2a, p3a) -> c.accept(e, p1a, p2a, p3a), TriFunction::apply, sync, async, p1, p2, p3);
    }

    private <P1, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1, P2, P3, P4> sync, QuadFunction<P1, P2, P3, P4, Submittable> async, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        submit(sync, async, p1, p1, p2, p3, p4);
    }

    abstract <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Submittable> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    <R> void submit(AccordTask<R> operation)
    {
        submit(AccordExecutor::submitExclusive, i -> i, operation);
    }

    void submitExclusive(AccordTask<?> task)
    {
        assignQueuePosition(task);
        task.setupExclusive();
        ++tasks;
        updateQueue(task);
        enqueueLoadsExclusive();
    }

    void submitExclusive(Runnable runnable)
    {
        submitPlainExclusive(new PlainRunnable(null, runnable, null));
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
        else assignNewQueuePosition(task);
        waitingToRun.append(task);
        return task;
    }

    private void assignQueuePosition(Task task)
    {
        if (task.queuePosition == 0)
            assignNewQueuePosition(task);
    }

    private void assignNewQueuePosition(Task task)
    {
        if (nextPosition == 0) nextPosition++;
        task.queuePosition = nextPosition++;
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
        int position = task.queuePosition;
        try
        {
            task.cleanupExclusive();
        }
        finally
        {
            --tasks;
            if (running.contains(task))
                running.remove(task);

            if (waitingForCompletion != null && waitingForCompletion.peek().maybeNotify - position >= 0)
                maybeNotifyWaitingForCompletion();
        }
    }

    private void maybeNotifyWaitingForCompletion()
    {
        int min = minPosition(waitingToRun.peek(),
                    minPosition(waitingToLoad.peek(),
                      minPosition(waitingToLoadRangeTxns.peek(),
                        minPosition(running.peek(),
                          minPosition(loading.peek(),
                            minPosition(scanningRanges.peek(), Integer.MAX_VALUE))))));

        while (!waitingForCompletion.isEmpty() && waitingForCompletion.peek().position - min <= 0)
            waitingForCompletion.poll().run.run();
        if (waitingForCompletion.isEmpty())
            waitingForCompletion = null;
        else
            waitingForCompletion.peek().maybeNotify = min;
    }

    private static int minPosition(@Nullable Task task, int min)
    {
        return task == null ? min : Integer.min(task.queuePosition, min);
    }

    void cancelExclusive(AccordTask<?> task)
    {
        switch (task.state())
        {
            default: throw new UnhandledEnum(task.state());
            case INITIALIZED:
                // we could be cancelled before we even reach the queue
                try { task.cancelExclusive(); }
                finally { task.cleanupExclusive(); }
                break;

            case SCANNING_RANGES:
            case LOADING:
            case WAITING_TO_LOAD:
            case WAITING_TO_SCAN_RANGES:
            case WAITING_TO_RUN:
                task.unqueueIfQueued();
                try { task.cancelExclusive(); }
                finally { completeTaskExclusive(task); }
                break;

            case FAILING:
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
        catch (Throwable t) { agent.onUncaughtException(t); }
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
        PlainRunnable task = new PlainRunnable(new AsyncPromise<>(), run, null);
        submit(task);
        return task.result;
    }

    public void execute(Runnable command)
    {
        submit(new PlainRunnable(null, command, null));
    }

    private Cancellable submit(Plain task)
    {
        submit(AccordExecutor::submitPlainExclusive, i -> i, task);
        return task;
    }

    public void executeDirectlyWithLock(Runnable command)
    {
        lock.lock();
        try
        {
            command.run();
        }
        finally
        {
            beforeUnlock();
            lock.unlock();
        }
    }

    public void execute(Runnable command, AccordCommandStore commandStore)
    {
        submit(new PlainRunnable(null, command, commandStore.exclusiveExecutor));
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
        int queuePosition;

        protected Task()
        {
        }

        public DebuggableTask debuggable() { return null; }

        /**
         * Prepare to run while holding the state cache lock
         */
        abstract protected void preRunExclusive();

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void runInternal();
        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void fail(Throwable fail);

        /**
         * Cleanup the command while holding the state cache lock
         */
        abstract protected void cleanupExclusive();

        void cancelExclusive(AccordExecutor owner) {}

        abstract protected void addToQueue(TaskQueue queue);
    }

    interface Submittable
    {
    }

    static abstract class SubmittableTask extends Task implements Submittable
    {
        final WithResources locals = ExecutorLocals.propagate();
        abstract void submitExclusive(AccordExecutor owner);
    }

    static class SequentialQueueTask extends Task
    {
        private final SequentialExecutor queue;

        SequentialQueueTask(SequentialExecutor queue)
        {
            super();
            this.queue = queue;
        }

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
        protected void cleanupExclusive()
        {
            queue.cleanupTask();
        }

        @Override
        protected void addToQueue(TaskQueue queue)
        {
            Invariants.require(queue.kind == RUNNING);
            queue.append(this);
        }
    }

    private static final AtomicReferenceFieldUpdater<SequentialExecutor, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(SequentialExecutor.class, Thread.class, "owner");
    public class SequentialExecutor extends TaskQueue<Task> implements SequentialAsyncExecutor
    {
        final SequentialQueueTask selfTask;
        private Task task;
        private volatile Thread owner, waiting;
        private boolean running;

        SequentialExecutor()
        {
            super(WAITING_TO_RUN);
            this.selfTask = new SequentialQueueTask(this);
        }

        void preRunTask()
        {
            Invariants.require(task != null);
            task.preRunExclusive();
            running = true;
        }

        void runTask()
        {
            Thread self = Thread.currentThread();
            while (!ownerUpdater.compareAndSet(this, null, self))
            {
                waiting = self;
                while (owner != null)
                    LockSupport.park();
                waiting = null;
            }
            task.runInternal();
        }

        void failTask(Throwable t)
        {
            task.fail(t);
        }

        void cleanupTask()
        {
            try { task.cleanupExclusive(); }
            finally
            {
                owner = null;
                running = false;
                task = super.poll();
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
                Invariants.require(running || waitingToRun.contains(selfTask));
                super.append(newTask);
            }
            else
            {
                Invariants.require(!running && isEmpty());
                task = newTask;
                selfTask.queuePosition = newTask.queuePosition;
                waitingToRun.append(selfTask);
            }
        }

        @Override
        protected void remove(Task remove)
        {
            Invariants.require(remove != null);
            if (remove != task)
            {
                super.remove(remove);
            }
            else if (!running)
            {
                // cannot overwrite task while it is being executed - this cannot happen for AccordTask
                // but can for other tasks that don't track their own state

                task = super.poll();
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
            }
            Invariants.require(task == null || running || waitingToRun.contains(selfTask));
        }

        public boolean inExecutor()
        {
            return owner == Thread.currentThread();
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
            return super.contains(contains) || (task == contains && !running);
        }

        @Override
        public <T> AsyncChain<T> build(Callable<T> call)
        {
            int position = inExecutor() && task != null ? task.queuePosition : 0;
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    PlainChain<T> submit = new PlainChain<>(call, callback, SequentialExecutor.this);
                    submit.queuePosition = position;
                    return AccordExecutor.this.submit(submit);
                }
            };
        }

        @Override
        public void execute(Runnable run)
        {
            PlainRunnable submit = new PlainRunnable(null, run, this);
            if (inExecutor() && this.task != null)
                submit.queuePosition = this.task.queuePosition;
            AccordExecutor.this.submit(submit);
        }

        @Override
        public void maybeExecuteImmediately(Runnable run)
        {
            Thread self = Thread.currentThread();
            Thread owner = this.owner;
            if (owner == self || (owner == null && ownerUpdater.compareAndSet(this, null, self)))
            {
                try { run.run(); }
                catch (Throwable t) { agent.onUncaughtException(t); }
                finally
                {
                    if (owner == null)
                    {
                        this.owner = null;
                        if (waiting != null)
                            LockSupport.unpark(waiting);
                    }
                }
            }
            else
            {
                execute(run);
            }
        }
    }

    static class TaskQueue<T extends Task> extends IntrusivePriorityHeap<T>
    {
        final AccordTask.State kind;

        TaskQueue(AccordTask.State kind)
        {
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

        protected void remove(T remove)
        {
            super.remove(remove);
        }

        protected boolean contains(T contains)
        {
            return super.contains(contains);
        }
    }

    private abstract static class SubmitAsync implements Submittable
    {
        abstract void submitExclusive(AccordExecutor executor);
    }

    private static class CancelAsync extends SubmitAsync
    {
        final Task cancel;

        private CancelAsync(Task cancel)
        {
            this.cancel = cancel;
        }

        @Override
        void submitExclusive(AccordExecutor executor)
        {
            cancel.cancelExclusive(executor);
        }
    }

    static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    abstract class Plain extends SubmittableTask implements Cancellable
    {
        abstract SequentialExecutor executor();

        @Override
        protected void preRunExclusive() {}

        @Override
        protected void cleanupExclusive() {}

        @Override
        protected final void addToQueue(TaskQueue queue)
        {
            Invariants.require(queue.kind == WAITING_TO_RUN || queue.kind == RUNNING);
            queue.append(this);
        }

        @Override
        public void cancel()
        {
            submit((e, c) -> c.cancelExclusive(e), CancelAsync::new, this);
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
                catch (Throwable t) { agent.onUncaughtException(t); }
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
            this(null, run, null);
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run, @Nullable SequentialExecutor executor)
        {
            this.result = result;
            this.run = run;
            this.executor = executor;
        }

        @Override
        protected void runInternal()
        {
            try (Closeable close = locals.get())
            {
                run.run();
            }
            if (result != null)
                result.trySuccess(null);
        }

        @Override
        protected void fail(Throwable t)
        {
            if (result != null)
                result.tryFailure(t);
            agent.onUncaughtException(t);
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
        protected void cleanupExclusive()
        {
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
            try (Closeable close = locals.get())
            {
                result = entry.owner.parent().adapter().load(entry.owner.commandStore, entry.key());
            }
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
            try (Closeable close = locals.get())
            {
                wrapped.runInternal();
            }
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
            try (Closeable close = locals.get())
            {
                run.run();
            }
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

    class PlainChain<T> extends Plain
    {
        final Callable<T> call;
        final BiConsumer<? super T, Throwable> callback;
        final @Nullable SequentialExecutor executor;

        PlainChain(Callable<T> call, BiConsumer<? super T, Throwable> callback, @Nullable SequentialExecutor executor)
        {
            this.call = call;
            this.callback = callback;
            this.executor = executor;
        }

        @Override
        SequentialExecutor executor()
        {
            return executor;
        }

        @Override
        protected void runInternal()
        {
            T success;
            try (Closeable close = locals.get())
            {
                success = call.call();
            }
            catch (Throwable t)
            {
                fail(t);
                return;
            }
            try
            {
                callback.accept(success, null);
            }
            catch (Throwable t)
            {
                agent.onUncaughtException(t);
            }
        }

        @Override
        protected void fail(Throwable fail)
        {
            try
            {
                callback.accept(null, fail);
            }
            catch (Throwable t)
            {
                fail.addSuppressed(t);
                agent.onUncaughtException(fail);
            }
        }
    }

    class DebuggableChain<T> extends PlainChain<T> implements DebuggableTask
    {
        final long createdAtNanos;
        long startedAtNanos;
        final Object describe;

        DebuggableChain(Callable<T> call, BiConsumer<? super T, Throwable> callback, @Nullable SequentialExecutor executor, Object describe)
        {
            super(call, callback, executor);
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
        protected void cleanupExclusive()
        {
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
}
