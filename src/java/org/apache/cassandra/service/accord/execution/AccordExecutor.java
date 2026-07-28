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

package org.apache.cassandra.service.accord.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import accord.api.Agent;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.RoutingKey;
import accord.impl.AbstractAsyncExecutor;
import accord.local.Command;
import accord.local.cfk.CommandsForKey;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncCallbacks.CallAndCallback;
import accord.utils.async.AsyncCallbacks.RunOrFail;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.AccordConfig.QueueBalancingModel;
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
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.UniqueSave;
import org.apache.cassandra.service.accord.execution.ExclusiveExecutor.ExclusiveExecutorTask;
import org.apache.cassandra.service.accord.execution.IOTaskWrapper.WrappableIOTask;
import org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GlobalGroup;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.AccordCache.CommandAdapter.COMMAND_ADAPTER;
import static org.apache.cassandra.service.accord.execution.AccordCache.CommandsForKeyAdapter.CFK_ADAPTER;
import static org.apache.cassandra.service.accord.execution.AccordCache.registerJfrListener;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.OTHER;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_SCAN;
import static org.apache.cassandra.service.accord.execution.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.COUNTER_MASKS;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.overflowBit;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.selectByOverflowBits;
import static org.apache.cassandra.service.accord.execution.TaskQueueMulti.setOverflowWhenLessEqual;

/**
 * NOTE: We assume that NO BLOCKING TASKS are submitted to this executor AND WAITED ON by another task executing on this executor.
 *  (as we do not immediately schedule additional threads for submitted tasks, but schedule new threads only if necessary when the submitting execution completes)
 */
public abstract class AccordExecutor implements CacheSize, LoadExecutor<SafeTask<?>, Boolean>, SaveExecutor, Shutdownable, AbstractAsyncExecutor
{
    static final QueuePriorityModel PRIORITY_MODEL;
    static final QueueBalancingModel BALANCING_MODEL;
    static final long AGE_TO_FIFO;
    // PRIORITY_FAIR blends two strategies (flow: least fairly serviced; age: earliest-queued work) by deficit
    // round-robin; weights of BLEND_TOTAL come from a single imbalance ramp (onset..onset+width) trading age->flow.
    static final int BLEND_SHIFT = 6, BLEND_TOTAL = 1 << BLEND_SHIFT;
    static final int FLOW_ONSET, FLOW_WIDTH_SHIFT;
    static final boolean BALANCE_BY_POSITION;
    static final long GLOBAL_QUEUE_LIMITS, EXCLUSIVE_QUEUE_LIMITS;
    static final int NONSYNC_MIN_BATCH_SIZE, NONSYNC_MAX_BATCH_SIZE, NONSYNC_BLOCKED_LIMIT;
    static final boolean NONSYNC_ENABLED;
    private static final long LOADING_GROUPS = overflowBit(LOAD) | overflowBit(RANGE_LOAD) | overflowBit(RANGE_SCAN);

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        AGE_TO_FIFO = config.queue_priority_age_to_fifo.to(TimeUnit.MICROSECONDS);
        PRIORITY_MODEL = config.queue_priority_model != null ? config.queue_priority_model : QueuePriorityModel.HLC_FIFO;
        BALANCING_MODEL = config.queue_balancing_model != null ? config.queue_balancing_model : QueueBalancingModel.BLENDED_PRIORITY_PHASE_FAIR;
        FLOW_ONSET  = config.queue_flow_imbalance_onset == null ? 4  : config.queue_flow_imbalance_onset;
        FLOW_WIDTH_SHIFT  = config.queue_flow_imbalance_width_shift == null ? 5 : config.queue_flow_imbalance_width_shift;
        NONSYNC_MIN_BATCH_SIZE = config.queue_nonsync_min_batch_size == null ? 16 : config.queue_nonsync_min_batch_size;
        NONSYNC_MAX_BATCH_SIZE = config.queue_nonsync_max_batch_size == null ? 64 : config.queue_nonsync_max_batch_size;
        NONSYNC_ENABLED = config.queue_nonsync_enabled == null || config.queue_nonsync_enabled;
        NONSYNC_BLOCKED_LIMIT = config.queue_nonsync_blocked_limit == null ? 8 : config.queue_nonsync_blocked_limit;
        Invariants.require(FLOW_ONSET >= 0 && FLOW_WIDTH_SHIFT >= 0);
        switch (BALANCING_MODEL)
        {
            default: throw new UnhandledEnum(BALANCING_MODEL);
            case PRIORITY_ONLY:
            case BLENDED_PRIORITY_PHASE_FAIR:
                BALANCE_BY_POSITION = true;
                break;
            case PHASE_ONLY:
            case PHASE_FAIR:
                BALANCE_BY_POSITION = false;
        }

        {
            // TODO (required): pick default max loads/saves/range loads based on number of threads
            long global = COUNTER_MASKS, exclusive = COUNTER_MASKS;
            global ^= (0x7fL ^ 1) << (RANGE_SCAN.ordinal() * 8);
            if (config.queue_active_limits != null)
            {
                long[] limits = parseEnumParams(config.queue_active_limits, "queue_active_limits");
                global = selectByOverflowBits(setOverflowWhenLessEqual(limits[0], 0), global, limits[0]);
                exclusive = selectByOverflowBits(setOverflowWhenLessEqual(limits[1], 0), global, limits[1]);
            }
            GLOBAL_QUEUE_LIMITS = global;
            EXCLUSIVE_QUEUE_LIMITS = exclusive;
        }

    }

    public static final ShardedDecayingHistograms HISTOGRAMS = new ShardedDecayingHistograms();

    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    // WARNING: this is a shared object, so close is NOT idempotent
    public static final class ExclusiveGlobalCaches extends GlobalCaches implements AutoCloseable
    {
        final AccordExecutor executor;

        public ExclusiveGlobalCaches(AccordExecutor executor, AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey)
        {
            super(global, commands, commandsForKey);
            this.executor = executor;
        }

        @Override
        public void close()
        {
            executor.beforeUnlockExternal();
            global.tryShrinkOrEvict(executor.lock);
            executor.unlock(TaskRunner.get());
        }
    }

    public static class GlobalCaches
    {
        public final AccordCache global;
        public final AccordCache.Type<TxnId, Command, SaferCommand> commands;
        public final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey;

        public GlobalCaches(AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand> commands, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey)
        {
            this.global = global;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
        }
    }

    final LogLinearDecayingHistograms histograms;
    final LogLinearDecayingHistogram elapsedPreparingToRun;
    final LogLinearDecayingHistogram elapsedWaitingToRun;
    final LogLinearDecayingHistogram elapsedRunning;
    final LogLinearDecayingHistogram elapsed;
    final LogLinearDecayingHistogram keys;
    public final AccordReplicaMetrics.Shard replicaMetrics;

    private final Lock lock;
    final Agent agent;
    public final int executorId;
    private final AccordCache cache;
    private final ExclusiveGlobalCaches caches;
    final AtomicLong uniqueCreatedAt = new AtomicLong();
    final DebugExecutor debug = DebugExecutor.maybeDebug();

    private long maxWorkingSetSizeInBytes;
    private long maxWorkingCapacityInBytes;

    final TaskQueueStandalone<SafeTask<?>> loading = new TaskQueueStandalone<>(ExecutorQueue.LOADING);
    final TaskQueueStandalone<SafeTask<?>> waiting = new TaskQueueStandalone<>(ExecutorQueue.WAITING);
    final TaskQueueRunnable<Task> runnable = new TaskQueueRunnable<>();

    private final Tranches tranches = new Tranches(this);

    /**
     * Newly submitted work must take a position >= minPosition, but this condition does not apply to consequences of
     * previously submitted work; this inherits the originating task's position and tranche.
     * This is to ensure afterSubmittedAndConsequences functions correctly.
     */
    long minPosition = 1;
    long nextPosition = 1;
    int tasks;

    private boolean hasPausedLoading;

    private List<Condition> waitingForQuiescence;

    AccordExecutor(Lock lock, int executorId, Agent agent)
    {
        this.lock = lock;
        this.executorId = executorId;
        this.cache = new AccordCache(this, 0);
        this.agent = agent;

        final AccordCache.Type<TxnId, Command, SaferCommand> commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey> commandsForKey;
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

    abstract boolean isInLoop();
    abstract void beforeUnlockExternal();
    abstract <P1> void submit(Consumer<P1> sync, Function<P1, Task> async, P1 p1);
    public abstract boolean hasTasks();
    public abstract boolean isOwningThread();

    public final Lock unsafeLock()
    {
        return lock;
    }

    public final void lock(TaskRunner self)
    {
        long startAt = DEBUG_EXECUTION ? Clock.Global.nanoTime() : 0;
        if (!self.tryEnterAccordLockedExecutor(this))
            throw new UnsupportedOperationException("To ensure system performance, it is not permitted to utilise multiple AccordExecutor simultaneously with the same thread");
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        if (DEBUG_EXECUTION) debug.onEnterLock(startAt);
    }

    public final void unlock(TaskRunner self)
    {
        self.exitAccordLockedExecutor();
        if (DEBUG_EXECUTION) debug.onExitLock();
        lock.unlock();
    }

    public final boolean tryLock(TaskRunner self)
    {
        AccordExecutor active = self.accordActiveExecutor();
        if (active != null && active != this)
            return false;

       return onTryLock(self, lock.tryLock());
    }

    final boolean onTryLock(TaskRunner self, boolean result)
    {
        if (result && !self.tryEnterAccordLockedExecutor(this))
        {
            // shouldn't be possible, we should have checked this already
            lock.unlock();
            throw new IllegalStateException();
        }
        return result;
    }

    public ExclusiveGlobalCaches lockCaches()
    {
        lock(TaskRunner.get(Thread.currentThread()));
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

    final boolean hasAlreadyWaitingToRun()
    {
        return runnable.hasWaitingToRun();
    }

    void updateWaitingToRunExclusive()
    {
        // TODO (expected): this should not be invoked on every update of waiting to run
        maybeUnpauseLoading();
    }

    // drain only new work; specifically leave anything that would call completeTask queued.
    // this is to maintain invariants in Tranches.complete, where we may have some consequence of some earlier task
    // pending but unqueued, so that we have not incremented its tranche count, and in the interim we set the tranche
    // count to zero
    void drainUnqueuedNewWorkExclusive()
    {
    }

    final Task pollAlreadyWaitingToRunExclusive()
    {
        Task next = runnable.poll();
        if (DEBUG_EXECUTION && next != null) DebugTask.get(next).onPolled();
        return next;
    }

    public void waitForQuiescence()
    {
        TaskRunner self = TaskRunner.get();
        Condition condition;
        lock(self);
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
            unlock(self);
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
        tranches.finishAll(nextPosition);
    }

    public void afterSubmittedAndConsequences(Runnable run)
    {
        TaskRunner self = TaskRunner.get();

        lock(self);
        try
        {
            drainUnqueuedNewWorkExclusive();
            if (tasks == 0)
            {
                run.run();
                return;
            }

            tranches.registerWait(run);
        }
        finally
        {
            unlock(self);
        }
    }

    private boolean hasNonLoadingWaitingToRun()
    {
        return runnable.hasWaitingToRunExcluding(LOADING_GROUPS);
    }

    final void maybeUnpauseLoading()
    {
        if (hasPausedLoading && (cache.weightedSize() < maxWorkingCapacityInBytes || !hasNonLoadingWaitingToRun()))
        {
            hasPausedLoading = false;
            runnable.restart(LOADING_GROUPS);
        }
    }

    final void maybePauseLoading()
    {
        if (hasPausedLoading)
            return;

        if (!hasPausedLoading && cache.weightedSize() >= maxWorkingCapacityInBytes && hasNonLoadingWaitingToRun())
        {
            AccordSystemMetrics.metrics.pausedExecutorLoading.inc();
            hasPausedLoading = true;
            runnable.stop(LOADING_GROUPS);
        }
    }

    public ExclusiveExecutor newExclusiveExecutor(int commandStoreId)
    {
        return new ExclusiveExecutor(this, commandStoreId);
    }

    public ExclusiveAsyncExecutor newExclusiveExecutor()
    {
        return new ExclusiveExecutor(this);
    }

    @Override
    public <K, V> IOTaskLoad<?, ?> load(SafeTask<?> parent, Boolean isForRange, AccordCacheEntry<K, V, ?> entry)
    {
        IOTaskLoad<?, ?> result = newLoad(entry, isForRange);
        result.inherit(parent).submitExclusiveNoExcept();
        return result;
    }

    @Override
    public Cancellable save(AccordCacheEntry<?, ?, ?> entry, UniqueSave identity, Runnable save)
    {
        IOTaskSave task = new IOTaskSave(this, entry, identity, save);
        task.submitExclusiveNoExcept();
        return task;
    }

    void submitTask(Task task)
    {
        submit(Task::submitExclusiveNoExcept, i -> i, task);
    }

    public Future<?> submit(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = new PlainRunnable(this, new AsyncPromise<>(), run, OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else submitTask(task);
        return task.result;
    }

    public void execute(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = new PlainRunnable(this, null, run, OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else submitTask(task);
    }

    @Override
    public Cancellable execute(RunOrFail runOrFail)
    {
        Task inherit = inherit();
        PlainChain task = new PlainChain(this, runOrFail, null, ExclusiveGroup.OTHER);
        if (inherit != null) inherit.addConsequence(task);
        else submitTask(task);
        return task;
    }

    public <T> AsyncChain<T> buildDebuggable(Callable<T> call, Object describe)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                Task inherit = inherit();
                PlainChainDebuggable task = new PlainChainDebuggable(AccordExecutor.this, new CallAndCallback<>(call, callback), null, describe);
                if (inherit != null) inherit.addConsequence(task);
                else submitTask(task);
                return task;
            }
        };
    }

    public void submitExclusive(Runnable runnable)
    {
        new PlainRunnable(this, null, runnable, OTHER).submitExclusiveNoExcept();
    }

    Cancellable submitExclusive(Task parent, GlobalGroup group, WrappableIOTask wrap)
    {
        IOTaskWrapper task = new IOTaskWrapper(this, wrap, group);
        task.inherit(parent).submitExclusiveNoExcept();
        return task;
    }

    Task inherit()
    {
        TaskRunner self = TaskRunner.get();

        if (self.accordActiveExecutor() != this)
            return null;

        Task task = self.accordActiveSelfTask();
        if (task instanceof ExclusiveExecutorTask)
            task = ((ExclusiveExecutorTask)task).queue.task;
        return task;
    }

    void registerExclusive(Task task)
    {
        Invariants.require(isOwningThread());
        Invariants.require(task.is(UNREGISTERED));
        ++tasks;
        if (task.hasInherited())
        {
            tranches.addInherited(task.tranche(), task.position);
        }
        else
        {
            long position = task.position;
            if (position == 0)
            {
                task.position = position = nextPosition++;
            }
            else
            {
                long delta = nextPosition - position;
                if (delta >= AGE_TO_FIFO)
                    task.position = position = nextPosition++;
                else if (delta <= 0)
                    nextPosition = position + 1;
                else if (position < minPosition)
                    task.position = position = minPosition;
            }
            task.setTranche(tranches.addNew(position));
        }
    }

    final void completedTaskExclusive(Task task)
    {
        Invariants.require(task.compareTo(EXECUTED) >= 0);
        if (DEBUG_EXECUTION) DebugTask.get(task).onCompleted(debug);
        unregisterExclusive(task);
        runnable.cleanup(task);
        cache.tryShrinkOrEvict(lock);
        maybeUnpauseLoading();
    }

    final void unregisterExclusive(Task task)
    {
        int tranch = task.tranche();
        --tasks;
        tranches.complete(tranch);
    }

    void onScannedRangesExclusive(SafeTask<?> task, Throwable fail)
    {
        // the task may have already been cancelled, in which case we don't need to fail it
        if (task.state().isDone())
            return;

        SafeTask<?>.RangeTxnScanner scanner = task.rangeScanner();
        if (scanner == null && fail == null) fail = new CancellationException();
        if (fail != null) task.tryFailAndCompleteExclusive(fail, FAILED);
        else scanner.scannedExclusive();
    }

    <K, V> void onSavedExclusive(AccordCacheEntry<K, V, ?> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    <K, V> void onLoadedExclusive(AccordCacheEntry<K, V, ?> loaded, V value, Throwable fail)
    {
        if (loaded.status() == EVICTED)
            return;

        try (ArrayBuffers.BufferList<SafeTask<?>> tasks = loaded.drainWaitingToLoad())
        {
            if (fail != null)
            {
                for (SafeTask<?> task : tasks)
                    task.tryFailAndCompleteExclusive(fail, FAILED);
                cache.failedToLoad(loaded);
            }
            else
            {
                cache.loaded(loaded, value);
                for (SafeTask<?> task : tasks)
                    task.onLoadOneExclusive(loaded);
            }
        }

        maybePauseLoading();
    }

    public void executeDirectlyWithLock(Runnable command)
    {
        TaskRunner self = TaskRunner.get();
        lock(self);
        try
        {
            self.setAccordActiveExecutor(this);
            command.run();
        }
        finally
        {
            beforeUnlockExternal();
            self.setAccordActiveExecutor(null);
            unlock(self);
        }
    }

    @Override
    public void setCapacity(long bytes)
    {
        Invariants.require(isOwningThread());
        cache.setCapacity(bytes);
        refreshCapacity();
    }

    public void setWorkingSetSize(long bytes)
    {
        Invariants.require(isOwningThread());
        maxWorkingSetSizeInBytes = bytes;
        refreshCapacity();
    }

    private void refreshCapacity()
    {
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

    public int unsafePreparingToRunCount()
    {
        return loading.size();
    }

    public int unsafeWaitingToRunCount()
    {
        return runnable.waitingCount();
    }

    public int unsafeRunningCount()
    {
        return runnable.assigned.size();
    }

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return Stream.of();
    }

    public int executorId()
    {
        return executorId;
    }

    public static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    <K, V> IOTaskLoad<K, V> newLoad(AccordCacheEntry<K, V, ?> entry, boolean isForRange)
    {
        return new IOTaskLoad<>(this, entry, isForRange ? RANGE_LOAD : LOAD);
    }

    public List<TaskInfo> taskSnapshot()
    {
        List<TaskInfo> result = new ArrayList<>();
        TaskRunner self = TaskRunner.get();
        lock(self);
        try
        {
            addToSnapshot(result, loading, TaskInfo.Status.LOADING, TaskInfo.Status.LOADING);
            for (TaskQueue queue : runnable.queues)
            {
                if (queue != null)
                    addToSnapshot(result, queue, TaskInfo.Status.WAITING_TO_RUN, TaskInfo.Status.WAITING_TO_RUN);
            }
            addToSnapshot(result, runnable.assigned, TaskInfo.Status.RUNNING, TaskInfo.Status.WAITING_TO_RUN);
        }
        finally
        {
            unlock(self);
        }
        result.sort(TaskInfo::compareTo);
        return result;
    }

    private static void addToSnapshot(List<TaskInfo> snapshot, TaskQueue<?> queue, TaskInfo.Status ifCurrent, TaskInfo.Status ifQueued)
    {
        for (int i = 0 ; i < queue.size() ; ++i)
        {
            Task t = queue.getSingle(i);
            if (t instanceof ExclusiveExecutorTask)
            {
                ExclusiveExecutor q = ((ExclusiveExecutorTask) t).queue;
                snapshot.add(new TaskInfo(ifCurrent, q.commandStoreId, q.task));
                for (TaskQueue<?> q0 : q.queues)
                {
                    if (q0 != null)
                    {
                        for (int j = 0 ; j < q0.size() ; ++j)
                            snapshot.add(new TaskInfo(ifQueued, q.commandStoreId, q0.getSingle(j)));
                    }
                }
            }
            else
            {
                int commmandStoreId = t instanceof SafeTask ? ((SafeTask<?>) t).commandStore.id() : -1;
                snapshot.add(new TaskInfo(ifCurrent, commmandStoreId, t));
            }
        }
    }

    private static long[] parseEnumParams(String input, String describe)
    {
        String[] specs = input.split(";");
        if (specs.length != 2)
            throw new IllegalArgumentException("Invalid specifiers in " + describe + "; expect [GlobalGroup];[ExclusiveGroup] but got: " + input);
        long[] result = new long[2];
        result[0] = parseEnumParams(GlobalGroup::valueOf, specs[0], describe + " for GlobalGroup");
        result[1] = parseEnumParams(ExclusiveGroup::valueOf, specs[1], describe + " for ExclusiveGroup");
        return result;
    }

    private static long parseEnumParams(Function<String, ? extends Enum<?>> get, String input, String describe)
    {
        long result = 0;
        for (String spec : input.split(","))
        {
            if (spec.trim().isEmpty()) continue;

            String[] split = spec.split(":");
            if (split.length != 2)
                throw new IllegalArgumentException("Invalid specifier " + spec + " in " + describe + ": " + input);

            try
            {
                Enum<?> queue = get.apply(split[0]);
                long value = Long.parseLong(split[1]);
                if (value <= 0 || value >= 128)
                    throw new IllegalArgumentException("Invalid limit " + value + " in queue_active_limits: " + input);

                result |= value << (queue.ordinal() * 8);
            }
            catch (Throwable t)
            {
                throw new IllegalArgumentException("Invalid queue identifier " + split[0] + " in " + describe + ": " + input);
            }
        }

        return result;
    }

    protected static void prepareRunComplete(TaskRunner self, Task task)
    {
        if (task.prepareExclusiveNoExcept())
        {
            try { task.runNoExcept(self); }
            finally { task.completeExclusiveNoExcept(); }
        }
    }
}
