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

import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.RoutingKey;
import accord.impl.TimestampsForKey;
import accord.local.Command;
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
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.AccordCachingState.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordStateCache.registerJfrListener;
import static org.apache.cassandra.service.accord.AccordTask.State.LOADING;
import static org.apache.cassandra.service.accord.AccordTask.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AccordExecutor implements CacheSize, AccordCachingState.OnLoaded, AccordCachingState.OnSaved, Shutdownable
{
    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    public interface ExecutorFunction extends Function<Runnable, Future<?>> {}
    public interface ExecutorFunctionFactory extends Function<AccordExecutor, ExecutorFunction> {}

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveGlobalCaches implements AutoCloseable
    {
        final Lock lock;
        public final AccordStateCache global;
        public final AccordStateCache.Type<TxnId, Command, AccordSafeCommand> commands;
        public final AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKey;
        public final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey;

        public ExclusiveGlobalCaches(Lock lock, AccordStateCache global, AccordStateCache.Type<TxnId, Command, AccordSafeCommand> commands, AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKey, AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey)
        {
            this.lock = lock;
            this.global = global;
            this.commands = commands;
            this.timestampsForKey = timestampsForKey;
            this.commandsForKey = commandsForKey;
        }

        @Override
        public void close()
        {
            lock.unlock();
        }
    }

    private static final int MAX_QUEUED_LOADS_PER_EXECUTOR = 64;
    private static final int MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR = 8;

    final Lock lock;
    final Agent agent;
    final int executorId;
    private final AccordStateCache cache;
    private final ExecutorFunction loadExecutor;
    private final ExecutorFunction rangeLoadExecutor;

    private final TaskQueue<AccordTask<?>> scanningRanges = new TaskQueue<>(SCANNING_RANGES); // never queried, just parked here while scanning
    private final TaskQueue<AccordTask<?>> loading = new TaskQueue<>(LOADING); // never queried, just parked here while loading

    private final TaskQueue<AccordTask<?>> waitingToLoadRangeTxns = new TaskQueue<>(WAITING_TO_LOAD);

    private final TaskQueue<AccordTask<?>> waitingToLoad = new TaskQueue<>(WAITING_TO_LOAD);
    private final TaskQueue<Task> waitingToRun = new TaskQueue<>(WAITING_TO_RUN);
    private final Object2ObjectHashMap<AccordCommandStore, CommandStoreQueue> commandStoreQueues = new Object2ObjectHashMap<>();

    private final AccordCachingState.OnLoaded onRangeLoaded = this::onRangeLoaded;
    private final ExclusiveGlobalCaches locked;

    private long maxWorkingSetSizeInBytes;
    private long maxWorkingCapacityInBytes;
    private int nextPosition;
    private int activeLoads, activeRangeLoads;
    private boolean hasPausedLoading;
    int tasks;
    int running;

    AccordExecutor(Lock lock, int executorId, AccordStateCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        this.lock = lock;
        this.executorId = executorId;
        this.cache = new AccordStateCache(saveExecutor.apply(this), this, 0, metrics);
        this.loadExecutor = loadExecutor.apply(this);
        this.rangeLoadExecutor = rangeLoadExecutor.apply(this);
        this.agent = agent;

        final AccordStateCache.Type<TxnId, Command, AccordSafeCommand> commands;
        final AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKey;
        final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKey;
        commands = cache.newType(TxnId.class,
                                 AccordSafeCommand.class,
                                 AccordSafeCommand.safeRefFactory(),
                                 AccordCommandStore::loadCommand,
                                 AccordCommandStore::appendToKeyspace,
                                 AccordCommandStore::validateCommand,
                                 // TODO (expected): size diff method, so we only revisit portions that have changed
                                 AccordObjectSizes::command);
        registerJfrListener(executorId, commands, "Command");
        timestampsForKey = cache.newType(RoutingKey.class,
                                         AccordSafeTimestampsForKey.class,
                                         AccordSafeTimestampsForKey::new,
                                         AccordCommandStore::loadTimestampsForKey,
                                         AccordCommandStore::saveTimestampsForKey,
                                         AccordCommandStore::validateTimestampsForKey,
                                         AccordObjectSizes::timestampsForKey);
        registerJfrListener(executorId, timestampsForKey, "TimestampsForKey");
        commandsForKey = cache.newType(RoutingKey.class,
                                       AccordSafeCommandsForKey.class,
                                       AccordSafeCommandsForKey::new,
                                       AccordCommandStore::loadCommandsForKey,
                                       AccordCommandStore::saveCommandsForKey,
                                       AccordCommandStore::validateCommandsForKey,
                                       AccordObjectSizes::commandsForKey);
        registerJfrListener(executorId, commandsForKey, "CommandsForKey");

        this.locked = new ExclusiveGlobalCaches(lock, cache, commands, timestampsForKey, commandsForKey);
    }

    public int executorId()
    {
        return executorId;
    }

    public ExclusiveGlobalCaches lockCaches()
    {
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        return locked;
    }

    public AccordStateCache cacheUnsafe()
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
        return waitingToRun.poll();
    }

    void updateWaitingToRunExclusive()
    {
        maybeUnpauseLoading();
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

    abstract boolean hasTasks();
    abstract boolean isInThread();

    private void enqueueLoadsExclusive()
    {
        outer: while (true)
        {
            if (hasPausedLoading || cache.weightedSize() >= maxWorkingCapacityInBytes)
            {
                // we have too much in memory already, and we have work waiting to run, so let that complete before queueing more
                if (!loading.isEmpty() || !waitingToRun.isEmpty())
                {
                    hasPausedLoading = true;
                    return;
                }
            }

            TaskQueue<AccordTask<?>> queue = waitingToLoadRangeTxns.isEmpty() || activeRangeLoads >= MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR ? waitingToLoad : waitingToLoadRangeTxns;
            AccordTask<?> next = queue.peek();
            if (next == null)
                return;

            switch (next.state())
            {
                default:
                {
                    failExclusive(next, new AssertionError("Unexpected state: " + next.toDescription()));
                    break;
                }
                case WAITING_TO_SCAN_RANGES:
                    ++activeRangeLoads;
                    ++activeLoads;
                    next.rangeLoader().start(rangeLoadExecutor);
                    updateQueue(next);
                    break;

                case WAITING_TO_LOAD:
                    while (true)
                    {
                        AccordCachingState<?, ?> load = next.peekWaitingToLoad();
                        boolean isForRange = isForRange(next, load);
                        if (isForRange && activeRangeLoads >= MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR)
                        {
                            parkRangeLoad(next);
                            continue outer;
                        }

                        Invariants.checkState(load != null);
                        AccordCachingState.OnLoaded onLoaded = this;
                        ++activeLoads;
                        if (isForRange)
                        {
                            ++activeRangeLoads;
                            onLoaded = onRangeLoaded;
                        }

                        for (AccordTask<?> task : cache.load(loadExecutor, load, onLoaded))
                        {
                            if (task == next) continue;
                            if (task.onLoading(load))
                                updateQueue(task);
                        }
                        Object prev = next.pollWaitingToLoad();
                        Invariants.checkState(prev == load);
                        if (next.peekWaitingToLoad() == null)
                            break;

                        Invariants.checkState(next.state() == WAITING_TO_LOAD, "Invalid state: %s", next);
                        if (activeLoads >= MAX_QUEUED_LOADS_PER_EXECUTOR)
                            return;
                    }
                    Invariants.checkState(next.state().compareTo(LOADING) >= 0, "Invalid state: %s", next);
                    updateQueue(next);
            }
        }
    }

    private boolean isForRange(AccordTask<?> task, AccordCachingState<?, ?> load)
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

    private void parkRangeLoad(AccordTask<?> task)
    {
        if (task.queued() != waitingToLoadRangeTxns)
        {
            task.unqueueIfQueued();
            task.addToQueue(waitingToLoadRangeTxns);
        }
    }

    void consumeExclusive(Object object)
    {
        try
        {
            if (object instanceof AccordTask<?>)
                loadExclusive((AccordTask<?>) object);
            else
                ((SubmitAsync) object).acceptExclusive(this);
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
                task.runQueuedAt = nanoTime();
                commandStoreQueues.computeIfAbsent(task.commandStore, CommandStoreQueue::new)
                                  .append(task);
                break;
        }
    }
    
    private void waitingToRun(Task task)
    {
        if (task.commandStore == null)
        {
            waitingToRun.append(task);
        }
        else
        {
            commandStoreQueues.computeIfAbsent(task.commandStore, CommandStoreQueue::new)
                              .append(task);
        }
    }

    private void removeFromQueue(AccordTask<?> task)
    {
        TaskQueue<?> queue = task.unqueueIfQueued();
    }

    private Future<?> submitIOExclusive(Runnable run)
    {
        Invariants.checkState(isInThread());
        ++tasks;
        AsyncPromise<Void> result = new AsyncPromise<>();
        PlainRunnable task = new PlainRunnable(result, run, null);
        // TODO (expected): adopt queue position of the submitting task
        task.queuePosition = ++nextPosition;
        waitingToRun.append(task);
        return result;
    }

    public Executor executor(AccordCommandStore commandStore)
    {
        return task -> AccordExecutor.this.submit(task, commandStore);
    }

    public <R> void submit(AccordTask<R> operation)
    {
        submit(AccordExecutor::loadExclusive, Function.identity(), operation);
    }

    public <R> void cancel(AccordTask<R> operation)
    {
        submit(AccordExecutor::cancelExclusive, OnCancel::new, operation);
    }

    public void onScannedRanges(AccordTask<?> task, Throwable fail)
    {
        submit(AccordExecutor::onScannedRangesExclusive, OnScannedRanges::new, task, fail);
    }

    public <K, V> void onSaved(AccordCachingState<K, V> saved, Object identity, Throwable fail)
    {
        submit(AccordExecutor::onSavedExclusive, OnSaved::new, saved, identity, fail);
    }

    @Override
    public <K, V> void onLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        submit(AccordExecutor::onLoadedExclusive, OnLoaded::new, loaded, value, fail, false);
    }

    public <K, V> void onRangeLoaded(AccordCachingState<K, V> loaded, V value, Throwable fail)
    {
        submit(AccordExecutor::onLoadedExclusive, OnLoaded::new, loaded, value, fail, true);
    }

    private <P1> void submit(BiConsumer<AccordExecutor, P1> sync, Function<P1, ?> async, P1 p1)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a), (f, p1a, p2a, p3) -> f.apply(p1a), sync, async, p1, null, null);
    }

    private <P1, P2> void submit(TriConsumer<AccordExecutor, P1, P2> sync, BiFunction<P1, P2, ?> async, P1 p1, P2 p2)
    {
        submit((e, c, p1a, p2a, p3) -> c.accept(e, p1a, p2a), (f, p1a, p2a, p3) -> f.apply(p1a, p2a), sync, async, p1, p2, null);
    }

    private <P1, P2, P3> void submit(QuadConsumer<AccordExecutor, P1, P2, P3> sync, TriFunction<P1, P2, P3, ?> async, P1 p1, P2 p2, P3 p3)
    {
        submit((e, c, p1a, p2a, p3a) -> c.accept(e, p1a, p2a, p3a), TriFunction::apply, sync, async, p1, p2, p3);
    }

    private <P1, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1, P2, P3, P4> sync, QuadFunction<P1, P2, P3, P4, Object> async, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        submit(sync, async, p1, p1, p2, p3, p4);
    }

    abstract <P1s, P1a, P2, P3, P4> void submit(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4);

    private void submitExclusive(AsyncPromise<Void> result, Runnable run, AccordCommandStore commandStore)
    {
        ++tasks;
        PlainRunnable task = new PlainRunnable(result, run, commandStore);
        task.queuePosition = ++nextPosition;
        waitingToRun(task);
    }

    private void loadExclusive(AccordTask<?> task)
    {
        ++tasks;
        task.queuePosition = ++nextPosition;
        task.setupExclusive();
        updateQueue(task);
        enqueueLoadsExclusive();
    }

    private void cancelExclusive(AccordTask<?> task)
    {
        switch (task.state())
        {
            default:
            case INITIALIZED:
                throw new AssertionError("Unexpected state: " + task.toDescription());

            case LOADING:
            case WAITING_TO_LOAD:
            case WAITING_TO_SCAN_RANGES:
            case SCANNING_RANGES:
            case WAITING_TO_RUN:
                --tasks;
                task.unqueueIfQueued();
                task.cancelExclusive();
                break;

            case RUNNING:
            case PERSISTING:
            case FINISHED:
            case CANCELLED:
            case FAILED:
                // cannot safely cancel
        }
    }

    private void onScannedRangesExclusive(AccordTask<?> task, Throwable fail)
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
                task.rangeLoader().scanned();
                updateQueue(task);
            }
        }
        enqueueLoadsExclusive();
    }

    private void failExclusive(AccordTask<?> task, Throwable fail)
    {
        if (task.state().isExecuted())
            return;

        --tasks;
        try { task.fail(fail); }
        catch (Throwable t) { agent.onUncaughtException(t); }
        finally
        {
            task.unqueueIfQueued();
            task.cleanupExclusive();
        }
    }

    private <K, V> void onSavedExclusive(AccordCachingState<K, V> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    private <K, V> void onLoadedExclusive(AccordCachingState<K, V> loaded, V value, Throwable fail, boolean isForRange)
    {
        boolean enqueueLoads = activeLoads-- == MAX_QUEUED_LOADS_PER_EXECUTOR;
        if (isForRange) enqueueLoads |= activeRangeLoads-- == MAX_QUEUED_RANGE_LOADS_PER_EXECUTOR;

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
                            Invariants.checkState(task.queued() == loading);
                            task.unqueue();
                            waitingToRun(task);
                        }
                    }
                }
            }
        }

        if (enqueueLoads)
            enqueueLoadsExclusive();
    }

    public Future<?> submit(Runnable run)
    {
        return submit(run, null);
    }

    // TODO (expected): offer queue jumping/priorities
    public Future<?> submit(Runnable run, AccordCommandStore commandStore)
    {
        AsyncPromise<Void> result = new AsyncPromise<>();
        submit(AccordExecutor::submitExclusive, SubmitPlainRunnable::new, result, run, commandStore);
        return result;
    }

    public void execute(Runnable command)
    {
        submit(command);
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
            lock.unlock();
        }
    }

    public void execute(Runnable command, AccordCommandStore commandStore)
    {
        submit(command, commandStore);
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.checkState(isInThread());
        cache.setCapacity(bytes);
        maxWorkingCapacityInBytes = cache.capacity() + maxWorkingSetSizeInBytes;
    }

    public void setWorkingSetSize(long bytes)
    {
        Invariants.checkState(isInThread());
        maxWorkingSetSizeInBytes = bytes;
        maxWorkingCapacityInBytes = cache.capacity() + maxWorkingSetSizeInBytes;
        if (maxWorkingCapacityInBytes < maxWorkingSetSizeInBytes)
            maxWorkingCapacityInBytes = Long.MAX_VALUE;
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

    public static abstract class Task extends IntrusivePriorityHeap.Node
    {
        final AccordCommandStore commandStore;

        int queuePosition;

        protected Task(AccordCommandStore commandStore)
        {
            this.commandStore = commandStore;
        }

        /**
         * Prepare to run while holding the state cache lock
         */
        abstract protected void preRunExclusive();

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void run();
        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void fail(Throwable fail);

        /**
         * Cleanup the command while holding the state cache lock
         */
        abstract protected void cleanupExclusive();

        abstract protected void addToQueue(TaskQueue queue);
    }

    class PlainRunnable extends Task
    {   // TODO (expected): support cancellation
        final AsyncPromise<Void> result;
        final Runnable run;

        PlainRunnable(AsyncPromise<Void> result, Runnable run, AccordCommandStore commandStore)
        {
            super(commandStore);
            this.result = result;
            this.run = run;
        }

        @Override
        protected void preRunExclusive() {}

        @Override
        protected void run()
        {
            run.run();
            result.trySuccess(null);
        }

        @Override
        protected void fail(Throwable t)
        {
            result.tryFailure(t);
            agent.onUncaughtException(t);
        }

        @Override
        protected void cleanupExclusive() {}

        @Override
        protected void addToQueue(TaskQueue queue)
        {
            Invariants.checkState(queue.kind == WAITING_TO_RUN);
            queue.append(this);
        }
    }

    class CommandStoreQueue extends Task
    {
        final TaskQueue<Task> queue = new TaskQueue<>(WAITING_TO_RUN);
        Task next;

        CommandStoreQueue(AccordCommandStore commandStore)
        {
            super(commandStore);
        }

        @Override
        protected void preRunExclusive()
        {
            Invariants.checkState(next != null);
            Thread self = Thread.currentThread();
            commandStore.setOwner(self, self);
            next.preRunExclusive();
        }

        @Override
        protected void run()
        {
            next.run();
        }

        @Override
        protected void fail(Throwable t)
        {
            next.fail(t);
        }

        @Override
        protected void cleanupExclusive()
        {
            next.cleanupExclusive();
            commandStore.setOwner(null, Thread.currentThread());
            updateNext(queue.poll());
        }

        @Override
        protected void addToQueue(TaskQueue queue)
        {
            throw new UnsupportedOperationException();
        }

        void append(Task task)
        {   // TODO (expected): if the new task is higher priority, replace next
            if (next == null) updateNext(task);
            else task.addToQueue(queue);
        }

        void updateNext(Task task)
        {
            next = task;
            if (task != null)
            {
                queuePosition = task.queuePosition;
                waitingToRun.append(this);
            }
        }
    }

    static final class TaskQueue<T extends Task> extends IntrusivePriorityHeap<T>
    {
        final AccordTask.State kind;

        TaskQueue(AccordTask.State kind)
        {
            this.kind = kind;
        }

        @Override
        public int compare(T o1, T o2)
        {
            return Integer.compare(o1.queuePosition, o2.queuePosition);
        }
        public void append(T task)
        {
            super.append(task);
        }

        public T poll()
        {
            ensureHeapified();
            return pollNode();
        }

        public T peek()
        {
            ensureHeapified();
            return peekNode();
        }

        public void remove(T remove)
        {
            super.remove(remove);
        }

        public boolean contains(T contains)
        {
            return super.contains(contains);
        }
    }

    private abstract static class SubmitAsync
    {
        abstract void acceptExclusive(AccordExecutor executor);
    }

    private static class SubmitPlainRunnable extends SubmitAsync
    {
        final AsyncPromise<Void> result;
        final Runnable run;
        final AccordCommandStore commandStore;

        private SubmitPlainRunnable(AsyncPromise<Void> result, Runnable run, AccordCommandStore commandStore)
        {
            this.result = result;
            this.run = run;
            this.commandStore = commandStore;
        }

        @Override
        void acceptExclusive(AccordExecutor executor)
        {
            executor.submitExclusive(result, run, commandStore);
        }
    }

    private static class OnLoaded<K, V> extends SubmitAsync
    {
        static final int FAIL = 1;
        static final int RANGE = 2;
        final AccordCachingState<K, V> loaded;
        final Object result;
        final int flags;

        OnLoaded(AccordCachingState<K, V> loaded, V success, Throwable fail, boolean isForRange)
        {
            this.loaded = loaded;
            int flags = isForRange ? RANGE : 0;
            if (fail == null)
            {
                result = success;
            }
            else
            {
                result = fail;
                flags |= FAIL;
            }
            this.flags = flags;
        }

        V success()
        {
            return (flags & FAIL) == 0 ? (V) result : null;
        }

        Throwable fail()
        {
            return (flags & FAIL) == 0 ? null : (Throwable) result;
        }

        boolean isForRange()
        {
            return (flags & RANGE) != 0;
        }

        @Override
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onLoadedExclusive(loaded, success(), fail(), isForRange());
        }
    }

    private static class OnScannedRanges extends SubmitAsync
    {
        final AccordTask<?> scanned;
        final Throwable fail;

        private OnScannedRanges(AccordTask<?> scanned, Throwable fail)
        {
            this.scanned = scanned;
            this.fail = fail;
        }

        @Override
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onScannedRangesExclusive(scanned, fail);
        }
    }

    private static class OnSaved<K, V> extends SubmitAsync
    {
        final AccordCachingState<K, V> state;
        final Object identity;
        final Throwable fail;

        private OnSaved(AccordCachingState<K, V> state, Object identity, Throwable fail)
        {
            this.state = state;
            this.identity = identity;
            this.fail = fail;
        }

        @Override
        void acceptExclusive(AccordExecutor executor)
        {
            executor.onSavedExclusive(state, identity, fail);
        }
    }

    private static class OnCancel<R> extends SubmitAsync
    {
        final AccordTask<R> cancel;

        private OnCancel(AccordTask<R> cancel)
        {
            this.cancel = cancel;
        }

        @Override
        void acceptExclusive(AccordExecutor executor)
        {
            executor.cancelExclusive(cancel);
        }
    }

    static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    static ExecutorFunctionFactory constantFactory(ExecutorFunction exec)
    {
        return ignore -> exec;
    }

    static ExecutorFunctionFactory constantFactory(ExecutorPlus exec)
    {
        return ignore -> exec::submit;
    }

    public static ExecutorFunction submitIOToSelf(AccordExecutor executor)
    {
        return executor::submitIOExclusive;
    }
}
