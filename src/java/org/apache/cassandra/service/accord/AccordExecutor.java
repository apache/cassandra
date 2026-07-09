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
import java.util.Map;
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
import accord.api.ExclusiveAsyncExecutor;
import accord.api.RoutingKey;
import accord.impl.AbstractAsyncExecutor;
import accord.local.Command;
import accord.local.ExecutionContext;
import accord.local.cfk.CommandsForKey;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.MessageType.StandardMessage;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.IntrusiveHeapNode;
import accord.utils.IntrusivePriorityHeap;
import accord.utils.Invariants;
import accord.utils.QuadConsumer;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import accord.utils.TinyEnumSet;
import accord.utils.TriConsumer;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncCallbacks.CallAndCallback;
import accord.utils.async.AsyncCallbacks.RunOrFail;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.concurrent.ExecutorLocals;
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
import org.apache.cassandra.service.accord.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.SaveExecutor;
import org.apache.cassandra.service.accord.AccordCacheEntry.UniqueSave;
import org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup;
import org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup;
import org.apache.cassandra.service.accord.AccordExecutor.Task.GroupKind;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExclusiveExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.primitives.Routable.Domain.Range;
import static accord.utils.Invariants.createIllegalState;
import static org.apache.cassandra.config.AccordConfig.QueuePriorityModel.ORIG_HLC_FIFO;
import static org.apache.cassandra.service.accord.AccordCache.CommandAdapter.COMMAND_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.CommandsForKeyAdapter.CFK_ADAPTER;
import static org.apache.cassandra.service.accord.AccordCache.registerJfrListener;
import static org.apache.cassandra.service.accord.AccordCacheEntry.Status.EVICTED;
import static org.apache.cassandra.service.accord.AccordExecutor.MultiTaskQueue.COUNTER_LOWBITS;
import static org.apache.cassandra.service.accord.AccordExecutor.MultiTaskQueue.COUNTER_MASKS;
import static org.apache.cassandra.service.accord.AccordExecutor.MultiTaskQueue.minCounterValue;
import static org.apache.cassandra.service.accord.AccordExecutor.MultiTaskQueue.selectByOverflowBits;
import static org.apache.cassandra.service.accord.AccordExecutor.MultiTaskQueue.setOverflowWhenLessEqual;
import static org.apache.cassandra.service.accord.AccordExecutor.RunnableTaskQueue.RUNNABLE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.ACCEPT;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.APPLY;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.COMMIT;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.PREACCEPT;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.RANGE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.ExclusiveGroup.STABLE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.COMMAND_STORE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.OTHER;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.RANGE_LOAD;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.RANGE_SCAN;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.GlobalGroup.SAVE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.MAX_TRANCHE;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.ASSIGNED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.INITIALIZED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.LOADING;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_TO_SCAN_RANGES;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * NOTE: We assume that NO BLOCKING TASKS are submitted to this executor AND WAITED ON by another task executing on this executor.
 *  (as we do not immediately schedule additional threads for submitted tasks, but schedule new threads only if necessary when the submitting execution completes)
 */
public abstract class AccordExecutor implements CacheSize, LoadExecutor<AccordTask<?>, Boolean>, SaveExecutor, Shutdownable, AbstractAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AccordExecutor.class);

    private static final QueuePriorityModel PRIORITY_MODEL;
    private static final QueueBalancingModel BALANCING_MODEL;
    private static final long AGE_TO_FIFO;
    // PRIORITY_FAIR blends two strategies (flow: least fairly serviced; age: earliest-queued work) by deficit
    // round-robin; weights of BLEND_TOTAL come from a single imbalance ramp (onset..onset+width) trading age->flow.
    private static final int BLEND_SHIFT = 6, BLEND_TOTAL = 1 << BLEND_SHIFT;
    private static final int FLOW_ONSET, FLOW_WIDTH_SHIFT;
    private static final boolean BALANCE_BY_POSITION;
    private static final long GLOBAL_QUEUE_LIMITS, GLOBAL_QUEUE_BUDGETS;
    private static final long EXCLUSIVE_QUEUE_LIMITS, EXCLUSIVE_QUEUE_BUDGETS;

    static
    {
        AccordConfig config = DatabaseDescriptor.getAccord();
        AGE_TO_FIFO = config.queue_priority_age_to_fifo.to(TimeUnit.MICROSECONDS);
        PRIORITY_MODEL = config.queue_priority_model != null ? config.queue_priority_model : QueuePriorityModel.HLC_FIFO;
        BALANCING_MODEL = config.queue_balancing_model != null ? config.queue_balancing_model : QueueBalancingModel.BLENDED_PRIORITY_PHASE_BUDGET_FAIR;
        FLOW_ONSET  = config.queue_flow_imbalance_onset == null ? 4  : config.queue_flow_imbalance_onset;
        FLOW_WIDTH_SHIFT  = config.queue_flow_imbalance_width_shift == null ? 5 : config.queue_flow_imbalance_width_shift;
        Invariants.require(FLOW_ONSET >= 0 && FLOW_WIDTH_SHIFT >= 0);
        switch (BALANCING_MODEL)
        {
            default: throw new UnhandledEnum(BALANCING_MODEL);
            case PRIORITY_ONLY:
            case BLENDED_PRIORITY_PHASE_FAIR:
            case BLENDED_PRIORITY_PHASE_BUDGET_FAIR:
            case PRIORITY_BUDGET:
                BALANCE_BY_POSITION = true;
                break;
            case PHASE_ONLY:
            case PHASE_FAIR:
            case PHASE_BUDGET:
            case PHASE_BUDGET_FAIR:
                BALANCE_BY_POSITION = false;
        }

        {
            long global = COUNTER_MASKS, exclusive = COUNTER_MASKS;
            global ^= (0x7f ^ 1) << RANGE_SCAN.ordinal();
            if (config.queue_active_limits != null)
            {
                long[] limits = parseEnumParams(config.queue_active_limits, "queue_active_limits");
                global = selectByOverflowBits(setOverflowWhenLessEqual(limits[0], 0), global, limits[0]);
                exclusive = selectByOverflowBits(setOverflowWhenLessEqual(limits[1], 0), global, limits[1]);
            }
            GLOBAL_QUEUE_LIMITS = global;
            EXCLUSIVE_QUEUE_LIMITS = exclusive;
        }

        {
            long global = encodeCounters(Map.of(COMMAND_STORE, 16L, LOAD, 16L, SAVE, 16L, OTHER, 16L, RANGE_LOAD, 2L, RANGE_SCAN, 1L));
            long exclusive = encodeCounters(Map.of(APPLY, 8L, STABLE, 7L, COMMIT, 6L, ACCEPT, 5L, OTHER, 4L, PREACCEPT, 4L, RANGE, 1L));
            if (config.queue_budgets != null)
            {
                long[] limits = parseEnumParams(config.queue_budgets, "queue_budgets");
                // any that are unset, set to the lowest value of any other specified
                if (limits[0] != 0)
                {
                    long min = minCounterValue(limits[0], 0);
                    long mins = min * COUNTER_LOWBITS;
                    if (min > 1)
                    {
                        mins ^= (min ^ 1) << (RANGE_SCAN.ordinal() * 8); // set the default RANGE_SCAN budget to 1
                        mins ^= (min ^ 2) << (RANGE_LOAD.ordinal() * 8); // set the default RANGE_LOAD budget to 2
                    }
                    global = selectByOverflowBits(setOverflowWhenLessEqual(limits[0], 0), mins, limits[0]);
                }
                if (limits[1] != 0)
                    exclusive = selectByOverflowBits(setOverflowWhenLessEqual(limits[1], 0), minCounterValue(limits[1], 0) * COUNTER_LOWBITS, limits[1]);
            }
            GLOBAL_QUEUE_BUDGETS = global;
            EXCLUSIVE_QUEUE_BUDGETS = exclusive;
        }
    }

    public static final ShardedDecayingHistograms HISTOGRAMS = new ShardedDecayingHistograms();

    public interface AccordExecutorFactory
    {
        AccordExecutor get(int executorId, Mode mode, int threads, IntFunction<String> name, Agent agent);
    }

    public enum Mode { RUN_WITH_LOCK, RUN_WITHOUT_LOCK }

    public interface AccordTaskRunner
    {
        AccordExecutor accordActiveExecutor();
        void setAccordActiveExecutor(AccordExecutor newExecutor);

        AccordExecutor accordLockedExecutor();
        boolean trySetAccordLockedExecutor(AccordExecutor newLockedExecutor);
        void clearAccordLockedExecutor();

        AccordExecutor.Task accordActiveTask();
        // to be called only by the thread itself, so can (eventually) avoid any memory barriers
        AccordExecutor.Task accordActiveSelfTask();
        void setAccordActiveTask(AccordExecutor.Task newActiveTask);


        static AccordTaskRunner get()
        {
            return get(Thread.currentThread());
        }

        static AccordTaskRunner get(Thread thread)
        {
            return thread instanceof CassandraThread ? (CassandraThread)thread : ThreadLocalAccordTaskRunner.threadLocal.get();
        }
    }

    public static final class ThreadLocalAccordTaskRunner implements AccordTaskRunner
    {
        private AccordExecutor lockedExecutor;
        private AccordExecutor activeExecutor;
        volatile Task activeTask;

        private static final ThreadLocal<ThreadLocalAccordTaskRunner> threadLocal = ThreadLocal.withInitial(ThreadLocalAccordTaskRunner::new);

        @Override
        public AccordExecutor accordActiveExecutor()
        {
            return activeExecutor;
        }

        @Override
        public void setAccordActiveExecutor(AccordExecutor newExecutor)
        {
            activeExecutor = newExecutor;
        }

        @Override
        public AccordExecutor accordLockedExecutor()
        {
            return lockedExecutor;
        }

        @Override
        public boolean trySetAccordLockedExecutor(AccordExecutor newLockedExecutor)
        {
            if (lockedExecutor != null)
                return false;
            lockedExecutor = newLockedExecutor;
            return true;
        }

        @Override
        public void clearAccordLockedExecutor()
        {
            lockedExecutor = null;
        }

        @Override
        public void setAccordActiveTask(Task newActiveTask)
        {
            activeTask = newActiveTask;
        }

        @Override
        public Task accordActiveTask()
        {
            return activeTask;
        }

        @Override
        public Task accordActiveSelfTask()
        {
            return activeTask;
        }
    }

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
            executor.unlock(AccordTaskRunner.get());
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

    /**
     * Tasks are separated into tranches based on their position.
     *
     * When we want to wait for all submitted tasks and their consequences to complete, we create a new tranche
     * and track the number of tasks still extant for all prior tranches - once these reach zero we can signal
     * the completion of the work.
     */
    private static final class Tranches
    {
        class WithDeferred implements Runnable
        {
            final Runnable run;
            final ArrayDeque<Runnable> deferred;

            WithDeferred(Runnable run, Runnable deferred)
            {
                this.run = run;
                this.deferred = new ArrayDeque<>(1);
                this.deferred.add(deferred);
            }

            WithDeferred(Runnable run, ArrayDeque<Runnable> deferred)
            {
                this.run = run;
                this.deferred = deferred;
            }

            @Override
            public void run()
            {
                Runnable register = deferred.poll();
                if (!deferred.isEmpty())
                {
                    Invariants.require(runs[firstIndex] != null);
                    Invariants.require(!(runs[firstIndex] instanceof WithDeferred));
                    runs[firstIndex] = new WithDeferred(runs[firstIndex], deferred);
                }
                registerWait(register);
                try { run.run(); }
                catch (Throwable t) { owner.agent.onException(t); }
            }
        }

        final AccordExecutor owner;

        int firstTranche;
        int firstIndex;
        long[] mins = new long[8];
        int[] counts = new int[8];
        Runnable[] runs = new Runnable[8];

        // the next tranche, that all new work is being collected against
        // (and will move to the array accounting once a new wait is registered)
        long nextMin;
        int nextTranche;
        int nextCount;

        private Tranches(AccordExecutor owner)
        {
            this.owner = owner;
        }

        private int trancheToIndex(int tranche)
        {
            return firstIndex + trancheToIndexOffset(tranche);
        }

        private int trancheToIndexOffset(int tranche)
        {
            int offset = tranche - firstTranche;
            if (offset < 0)
                offset += MAX_TRANCHE + 1;
            return offset;
        }

        int size()
        {
            return trancheToIndexOffset(nextTranche);
        }

        int capacity()
        {
            return counts.length;
        }

        int addNew(long position)
        {
            Invariants.require(position >= nextMin);
            ++nextCount;
            return nextTranche;
        }

        void addInherited(int tranche, long position)
        {
            if (tranche == nextTranche)
            {
                Invariants.require(position >= nextMin);
                ++nextCount;
            }
            else
            {
                int index = trancheToIndex(tranche);
                Invariants.require(counts[index] > 0);
                Invariants.require(mins[index] <= position);
                ++counts[index];
            }
        }

        void complete(int tranche)
        {
            if (tranche == nextTranche)
            {
                --nextCount;
            }
            else
            {

                if (counts[trancheToIndex(tranche)] == 1)
                    owner.drainUnqueuedNewWorkExclusive(); // make sure we don't have any pending

                if (--counts[trancheToIndex(tranche)] == 0 && tranche == firstTranche)
                {
                    do advance();
                    while (firstTranche != nextTranche && counts[firstIndex] == 0);
                }

                if (firstIndex >= counts.length / 2)
                    compact();
            }
        }

        public void finishAll(long nextPosition)
        {
            while (firstTranche != nextTranche)
            {
                logger.warn("{} processed all pending tasks (<{}) but found {} waiting for {}", this,
                            nextPosition, counts[firstIndex], size() == 1 ? nextMin : mins[firstIndex + 1]);
                advance();
            }
        }

        private void advance()
        {
            Runnable run = runs[firstIndex];
            runs[firstIndex] = null;
            ++firstIndex;
            if (firstTranche == MAX_TRANCHE) firstTranche = 0;
            else ++firstTranche;
            try { run.run(); }
            catch (Throwable t) { owner.agent.onException(t); }
        }

        public void registerWait(Runnable run)
        {
            int newNextTranche = (nextTranche + 1) % (MAX_TRANCHE + 1);
            if (newNextTranche == firstTranche)
            {
                Runnable cur = runs[firstIndex];
                if (cur instanceof WithDeferred) ((WithDeferred) cur).deferred.add(run);
                else runs[firstIndex] = new WithDeferred(runs[firstIndex], run);
                return;
            }

            if ((firstIndex + size()) == capacity())
                growOrCompact();

            int index = firstIndex + size();
            mins[index] = nextMin;
            counts[index] = nextCount;
            runs[index] = run;
            nextMin = owner.minPosition = owner.nextPosition;
            nextCount = 0;
            nextTranche = newNextTranche;
        }

        private void compact()
        {
            if (size() <= capacity()/4 && capacity() > 8) resize(capacity()/2);
            else compact(mins, counts, runs);
        }

        private void growOrCompact()
        {
            if (size() > capacity()/2) resize(capacity() * 2);
            else compact();
        }

        private void resize(int newSize)
        {
            Invariants.require(newSize > 0);
            long[] newMins = new long[newSize];
            int[] newCounts = new int[newSize];
            Runnable[] newRuns = new Runnable[newSize];
            compact(newMins, newCounts, newRuns);
            mins = newMins;
            counts = newCounts;
            runs = newRuns;
        }

        private void compact(long[] newMins, int[] newCounts, Runnable[] newRuns)
        {
            int size = size();
            System.arraycopy(mins, firstIndex, newMins, 0, size);
            System.arraycopy(counts, firstIndex, newCounts, 0, size);
            System.arraycopy(runs, firstIndex, newRuns, 0, size);
            firstIndex = 0;
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
    final int executorId;
    private final AccordCache cache;
    private final ExclusiveGlobalCaches caches;
    final DebugExecutor debug = DebugExecutor.maybeDebug();

    // TODO (expected): remove this
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

    private final StandaloneTaskQueue<AccordTask<?>> scanningRanges = new StandaloneTaskQueue<>(TinyEnumSet.encode(SCANNING_RANGES)); // never queried, just parked here while scanning
    private final StandaloneTaskQueue<AccordTask<?>> waitingToLoadRangeTxns = new StandaloneTaskQueue<>(TinyEnumSet.encode(WAITING_TO_LOAD));
    private final StandaloneTaskQueue<AccordTask<?>> waitingToLoad = new StandaloneTaskQueue<>(TinyEnumSet.encode(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD));
    private final StandaloneTaskQueue<AccordTask<?>> loading = new StandaloneTaskQueue<>(LOADING);
    private final RunnableTaskQueue<Task> runnable = new RunnableTaskQueue<>();

    private final Tranches tranches = new Tranches(this);

    /**
     * Newly submitted work must take a position >= minPosition, but this condition does not apply to consequences of
     * previously submitted work; this inherits the originating operation's position and tranche.
     * This is to ensure afterSubmittedAndConsequences functions correctly.
     */
    private long minPosition = 1;
    private long nextPosition = 1;
    int tasks;

    private int activeLoads, activeRangeLoads;
    private boolean hasPausedLoading;

    private List<Condition> waitingForQuiescence;

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
        lock(AccordTaskRunner.get(Thread.currentThread()));
        return caches;
    }

    abstract boolean isInLoop();

    public final Lock unsafeLock()
    {
        return lock;
    }

    final void lock(AccordTaskRunner self)
    {
        if (!self.trySetAccordLockedExecutor(this))
            throw new UnsupportedOperationException("To ensure system performance, it is not permitted to lock multiple AccordExecutor simultaneously with the same thread");
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        if (DEBUG_EXECUTION) debug.onEnterLock();
    }

    final void unlock(AccordTaskRunner self)
    {
        self.clearAccordLockedExecutor();
        if (DEBUG_EXECUTION) debug.onExitLock();
        lock.unlock();
    }

    final boolean tryLock(AccordTaskRunner self)
    {
        AccordExecutor locked = self.accordLockedExecutor();
        if (locked != null)
            return false;

       return onTryLock(self, lock.tryLock());
    }

    final boolean onTryLock(AccordTaskRunner self, boolean result)
    {
        if (result && !self.trySetAccordLockedExecutor(this))
        {
            // shouldn't be possible, we should have checked this already
            lock.unlock();
            throw new IllegalStateException();
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

    final boolean hasAlreadyWaitingToRun()
    {
        return runnable.hasWaiting();
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

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return Stream.of();
    }

    public void waitForQuiescence()
    {
        AccordTaskRunner self = AccordTaskRunner.get();
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
        AccordTaskRunner self = AccordTaskRunner.get();

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

    final void maybeUnpauseLoading()
    {
        if (!hasPausedLoading)
            return;

        if (cache.weightedSize() < maxWorkingCapacityInBytes || (loading.isEmpty() || runnable.hasWaiting()))
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
            StandaloneTaskQueue<AccordTask<?>> queue = waitingToLoadRangeTxns.isEmpty() || activeRangeLoads >= maxQueuedRangeLoads ? waitingToLoad : waitingToLoadRangeTxns;
            AccordTask<?> next = queue.peek();
            if (next == null)
                return;

            if (hasPausedLoading || cache.weightedSize() >= maxWorkingCapacityInBytes)
            {
                // we have too much in memory already, and we have work waiting to run, so let that complete before queueing more
                if (!loading.isEmpty() || runnable.hasWaiting())
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
        boolean isForRangeTxn = task.isRange();
        if (!isForRangeTxn)
            return false;

        for (AccordTask<?> t : load.loadingOrWaiting().waiters())
        {
            if (!t.isRange())
                return false;
        }
        return true;
    }

    private void parkRangeLoad(AccordTask<?> task)
    {
        if (task.queued() != waitingToLoadRangeTxns)
        {
            task.unqueue();
            waitingToLoadRangeTxns.enqueue(task);
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
                waitingToLoad.enqueue(task);
                break;
            case SCANNING_RANGES:
                scanningRanges.enqueue(task);
                break;
            case LOADING:
                loading.enqueue(task);
                break;
            case WAITING_TO_RUN:
                waitingToRun(task);
                break;
        }
    }

    private void waitingToRun(AccordTask<?> task)
    {
        task.onWaitingToRun();
        task.commandStore.exclusiveExecutor.enqueue(task);
    }

    private void waitingToRun(Task task, @Nullable ExclusiveExecutor queue)
    {
        task.onWaitingToRun();
        if (queue == null) runnable.enqueue(task);
        else queue.enqueue(task);
    }

    public ExclusiveExecutor executor()
    {
        return new ExclusiveExecutor(this);
    }

    public ExclusiveExecutor executor(int commandStoreId)
    {
        return new ExclusiveExecutor(this, commandStoreId);
    }

    public ExclusiveAsyncExecutor newExclusiveExecutor()
    {
        return new ExclusiveExecutor(this);
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

    void submitExclusive(AccordTask<?> task)
    {
        registerExclusive(task);
        task.setupExclusive();
        updateQueue(task);
        enqueueLoadsExclusive();
    }

    public void submitExclusive(Runnable runnable)
    {
        submitPlainExclusive(new PlainRunnable(null, runnable, OTHER));
    }

    private Cancellable submitPlain(Plain task)
    {
        submit(AccordExecutor::submitPlainExclusive, i -> i, task);
        return task;
    }

    private void submitPlainExclusive(Plain task)
    {
        registerExclusive(task);
        waitingToRun(task, task.executor());
    }

    Cancellable submitPlainExclusive(Task parent, GlobalGroup group, AbstractIOTask task)
    {
        return submitPlainExclusive(parent, new WrappedIOTask(task, group, parent.position, parent.tranche()));
    }

    final <T extends Task> T submitPlainExclusive(Task parent, T task)
    {
        Invariants.require(isOwningThread());
        if (parent == null) registerExclusive(task);
        else registerConsequenceExclusive(parent, task);
        task.onWaitingToRun();
        runnable.enqueue(task);
        return task;
    }

    private void registerConsequenceExclusive(Task parent, Task task)
    {
        ++tasks;
        int tranche = parent.tranche();
        tranches.addInherited(tranche, parent.position);
        task.position = parent.position;
        task.setInheritedTranche(tranche);
    }

    private void registerExclusive(Task task)
    {
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

    final void completeTaskExclusive(Task task)
    {
        runnable.complete(task);
        try { task.cleanupExclusive(this); }
        finally { cache.tryShrinkOrEvict(lock); }
    }

    final void unregisterExclusive(Task task)
    {
        int tranch = task.tranche();
        --tasks;
        tranches.complete(tranch);
    }

    final void cancelExclusive(AccordTask<?> task)
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
            try (ArrayBuffers.BufferList<AccordTask<?>> tasks = loaded.loading().copyWaiters())
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

    private Task inherit()
    {
        Thread thread = Thread.currentThread();
        AccordTaskRunner self = AccordTaskRunner.get(thread);

        if (self.accordActiveExecutor() != this)
            return null;

        Task task = self.accordActiveSelfTask();
        if (task instanceof ExclusiveExecutorTask)
            task = ((ExclusiveExecutorTask)task).queue.task;
        return task;
    }

    public Future<?> submit(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = inherit == null ? new PlainRunnable(new AsyncPromise<>(), run, OTHER)
                                             : new PlainRunnable(new AsyncPromise<>(), run, OTHER, inherit.position, inherit.tranche());
        submitPlain(task);
        return task.result;
    }

    public void execute(Runnable run)
    {
        Task inherit = inherit();
        PlainRunnable task = inherit == null ? new PlainRunnable(null, run, OTHER)
                                             : new PlainRunnable(null, run, OTHER, inherit.position, inherit.tranche());
        submitPlain(task);
    }

    @Override
    public Cancellable execute(RunOrFail runOrFail)
    {
        Task inherit = inherit();
        PlainChain submit = inherit == null ? new PlainChain(runOrFail, null, ExclusiveGroup.OTHER)
                                            : new PlainChain(runOrFail, null, ExclusiveGroup.OTHER, inherit.position, inherit.tranche());
        return submitPlain(submit);
    }

    public <T> AsyncChain<T> buildDebuggable(Callable<T> call, Object describe)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super T, Throwable> callback)
            {
                Task inherit = inherit();
                return submitPlain(inherit == null ? new DebuggableChain(new CallAndCallback<>(call, callback), null, describe)
                                                   : new DebuggableChain(new CallAndCallback<>(call, callback), null, inherit.position, inherit.tranche(), describe));
            }
        };
    }

    public void executeDirectlyWithLock(Runnable command)
    {
        AccordTaskRunner self = AccordTaskRunner.get();
        lock(self);
        try
        {
            self.setAccordActiveExecutor(this);
            command.run();
        }
        finally
        {
            beforeUnlockExternal();
            self.clearAccordLockedExecutor();
            unlock(self);
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
        private volatile AccordTaskRunner wrapped;

        protected void setWrapped(AccordTaskRunner wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public DebuggableTask running()
        {
            Task running = wrapped.accordActiveTask();
            return running == null ? null : running.debuggable();
        }
    }

    public static abstract class Task extends IntrusiveHeapNode
    {
        enum State
        {
            INITIALIZED(),
            WAITING_TO_SCAN_RANGES(INITIALIZED),
            SCANNING_RANGES(WAITING_TO_SCAN_RANGES),
            WAITING_TO_LOAD(INITIALIZED, SCANNING_RANGES),
            LOADING(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD),
            WAITING_TO_RUN(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD, LOADING),
            ASSIGNED(WAITING_TO_RUN),
            RUNNING(ASSIGNED),
            PERSISTING(RUNNING),
            FINISHED(RUNNING, PERSISTING),
            CANCELLED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN, ASSIGNED),
            FAILED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN, ASSIGNED, RUNNING, PERSISTING);

            private final int permittedFrom;
            static final State[] VALUES = values();

            State()
            {
                this.permittedFrom = 0;
            }

            State(State ... permittedFroms)
            {
                int permittedFrom = 0;
                for (State state : permittedFroms)
                    permittedFrom |= 1 << state.ordinal();
                this.permittedFrom = permittedFrom;
            }

            boolean isPermittedFrom(int prevOrdinal)
            {
                return (permittedFrom & (1 << prevOrdinal)) != 0;
            }

            boolean isExecuted()
            {
                return this.compareTo(PERSISTING) >= 0;
            }

            boolean isComplete()
            {
                return this.compareTo(FINISHED) >= 0;
            }

            static State forOrdinal(int ordinal)
            {
                return VALUES[ordinal];
            }
        }

        enum GlobalGroup
        {
            COMMAND_STORE,
            LOAD,
            SAVE,
            OTHER,
            RANGE_LOAD(true),
            RANGE_SCAN(true),
            ;

            final int bits;

            GlobalGroup()
            {
                this(false);
            }

            GlobalGroup(boolean isRange)
            {
                this.bits = (ordinal() << GLOBAL_GROUP_SHIFT) | (isRange ? RANGE_BIT : 0);
            }
        }

        enum ExclusiveGroup
        {
            APPLY,
            STABLE,
            COMMIT,
            ACCEPT,
            OTHER,
            PREACCEPT,
            RANGE(true),
            ;

            final int bits;

            ExclusiveGroup()
            {
                this(false);
            }

            ExclusiveGroup(boolean isRange)
            {
                this.bits = (ordinal() << EXCLUSIVE_GROUP_SHIFT) | (isRange ? RANGE_BIT : 0);
            }
        }

        public enum GroupKind
        {
            EXCLUSIVE(ExclusiveGroup.values().length, EXCLUSIVE_GROUP_SHIFT),
            GLOBAL(GlobalGroup.values().length, GLOBAL_GROUP_SHIFT),
            NONE(0, 0);

            final int count;
            final byte shift;
            GroupKind(int count, int shift)
            {
                this.count = count;
                this.shift = (byte) shift;
            }
        }

        private static final int STATE_MASK = 0xf;
        private static final int GROUP_MASK = 0x7;
        private static final int GLOBAL_GROUP_SHIFT = 7;
        private static final int EXCLUSIVE_GROUP_SHIFT = 4;
        private static final int RANGE_BIT = 1 << 10;
        private static final int CLEANUP_BIT = 1 << 11;
        private static final int HAS_TRANCHE_BIT = 1 << 12;
        private static final int HAS_INHERITED_BIT = 1 << 13;
        private static final int TRANCHE_SHIFT = 14;
        static final int MAX_TRANCHE = 0x3ff;

        public final WithResources resources;
        Task next;

        long position;
        private int info;

        // TODO (expected): do we need this? we should be able to determine the queue from state() if needed for e.g. cancellation
        private TaskQueue queued;

        // TODO (expected): expose via executors vtable
        // TODO (expected): use just one long and some flag bits to indicate which point it represents, and report incrementally
        public long createdAt = nanoTime(), waitingToRunAt, runningAt, cleanupAt;

        protected Task(GlobalGroup group, State state)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            info = init(group, ExclusiveGroup.OTHER, state);
        }

        protected Task(ExclusiveGroup group, State state)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            info = init(GlobalGroup.OTHER, group, state);
        }

        protected Task(GlobalGroup group, State state, long position, int tranche)
        {
            this(group, state);
            this.position = position;
            setInheritedTranche(tranche);
        }

        protected Task(ExclusiveGroup group, State state, long position, int tranche)
        {
            this(group, state);
            this.position = position;
            setInheritedTranche(tranche);
        }

        protected Task(ExecutionContext context)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            ExclusiveGroup group = ExclusiveGroup.OTHER;
            TxnId txnId = context.primaryTxnId();
            if (txnId != null)
            {
                if (txnId.is(Range)) group = ExclusiveGroup.RANGE;
                else
                {
                    switch (PRIORITY_MODEL)
                    {
                        case HLC_FIFO:
                        case ORIG_HLC_FIFO:
                        {
                            // TODO (expected): we should process messages for a TxnId together, to avoid processing delayed messages out of order
                            if (context instanceof Request)
                            {
                                MessageType type = ((Request) context).type();
                                if (type instanceof StandardMessage)
                                {
                                    switch ((StandardMessage)type)
                                    {
                                        case APPLY_REQ:
                                        {
                                            group = APPLY;
                                            break;
                                        }
                                        case READ_EPHEMERAL_REQ:
                                        case READ_REQ:
                                        case STABLE_THEN_READ_REQ:
                                        {
                                            group = STABLE;
                                            break;
                                        }
                                        case COMMIT_REQ:
                                        {
                                            Commit commit = (Commit) context;
                                            if (PRIORITY_MODEL == ORIG_HLC_FIFO && !commit.ballot.equals(Ballot.ZERO))
                                                txnId = null;
                                            if (commit.kind.saveStatus == SaveStatus.Stable) group = STABLE;
                                            else group = COMMIT;
                                            break;
                                        }
                                        case ACCEPT_REQ:
                                        {
                                            Accept accept = (Accept) context;
                                            if (PRIORITY_MODEL == ORIG_HLC_FIFO && !accept.ballot.equals(Ballot.ZERO))
                                                txnId = null;
                                            group = ExclusiveGroup.ACCEPT;
                                            break;
                                        }
                                        case GET_EPHEMERAL_READ_DEPS_REQ:
                                        case PRE_ACCEPT_REQ:
                                        {
                                            group = ExclusiveGroup.PREACCEPT;
                                            break;
                                        }
                                        default:
                                        {
                                            txnId = null;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                txnId = null;
                            }
                            break;
                        }
                        case FIFO:
                        {
                            txnId = null;
                            break;
                        }
                    }
                }
            }

            this.info = init(GlobalGroup.OTHER, group, INITIALIZED);
            if (txnId != null)
                this.position = txnId.hlc();
        }

        public final Task unwrap()
        {
            if (this instanceof ExclusiveExecutorTask)
                return ((ExclusiveExecutorTask) this).queue.task;
            return this;
        }

        final void setReadyToCleanup()
        {
            info |= CLEANUP_BIT;
        }

        final boolean isReadyToCleanup()
        {
            return 0 != (info & CLEANUP_BIT);
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

        abstract String toDescription();

        abstract void submitExclusive(AccordExecutor owner);

        /**
         * Prepare to run while holding the state cache lock
         */
        abstract protected void preRunExclusive();

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        protected abstract void run();

        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract protected void fail(Throwable fail);

        abstract protected boolean isNewWork();

        /**
         * Cleanup the command while holding the state cache lock
         */
        protected void cleanupExclusive(AccordExecutor executor)
        {
            executor.unregisterExclusive(this);
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

        public final State state()
        {
            return State.forOrdinal(stateOrdinal());
        }

        private int stateOrdinal()
        {
            return info & STATE_MASK;
        }

        final boolean is(State state)
        {
            return stateOrdinal() == state.ordinal();
        }

        final boolean is(GlobalGroup group)
        {
            return globalGroupOrdinal() == group.ordinal();
        }

        boolean isRange()
        {
            return 0 != (info & RANGE_BIT);
        }

        final int compareTo(State state)
        {
            return stateOrdinal() - state.ordinal();
        }

        final void setState(State state)
        {
            Invariants.require(state.isPermittedFrom(stateOrdinal()), "%s forbidden from %s", state, this, Task::reportBadStateTransition);
            setStateUnsafe(state);
        }

        private static String reportBadStateTransition(Task task)
        {
            return task.state() + " for " + task.toDescription();
        }

        final void setStateUnsafe(State state)
        {
            info = (info & ~STATE_MASK) | state.ordinal();
        }

        final int globalGroupOrdinal()
        {
            return (info >>> GLOBAL_GROUP_SHIFT) & GROUP_MASK;
        }

        final int exclusiveGroupOrdinal()
        {
            return (info >>> EXCLUSIVE_GROUP_SHIFT) & GROUP_MASK;
        }

        @Nullable
        final TaskQueue<?> queued()
        {
            return queued;
        }

        final void unqueueIfQueued()
        {
            if (queued != null)
                unqueue();
        }

        final void unqueue()
        {
            Invariants.require(queued != null);
            queued.unqueue(this);
            queued = null;
        }

        final void unsetQueue(TaskQueue<?> queue)
        {
            Invariants.require(queued == queue);
            queued = null;
        }

        final void setQueue(TaskQueue<?> queue)
        {
            Invariants.require(queued == null);
            Invariants.require(isCompatible(queue));
            queued = queue;
        }

        private boolean isCompatible(TaskQueue<?> queue)
        {
            int self = stateOrdinal();
            return TinyEnumSet.contains(queue.states, self);
        }

        private static int init(GlobalGroup global, ExclusiveGroup exclusive, State state)
        {
            return global.bits | exclusive.bits | state.ordinal();
        }

        final void setTranche(int tranche)
        {
            Invariants.require(tranche <= MAX_TRANCHE);
            info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT;
        }

        final void setInheritedTranche(int tranche)
        {
            Invariants.require(tranche <= MAX_TRANCHE);
            info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT | HAS_INHERITED_BIT;
        }

        final int tranche()
        {
            Invariants.require((info & HAS_TRANCHE_BIT) != 0);
            return info >>> TRANCHE_SHIFT;
        }

        final boolean hasInherited()
        {
            return (info & HAS_INHERITED_BIT) != 0;
        }
    }

    // run the task even on a stopped commandStore
    public interface Unstoppable extends ExecutionContext.Empty
    {
    }

    // run the task even on a terminated commandStore
    public interface Unterminatable extends Unstoppable
    {
    }

    static final class ExclusiveExecutorTask extends Task
    {
        private final ExclusiveExecutor queue;

        ExclusiveExecutorTask(ExclusiveExecutor queue)
        {
            super(COMMAND_STORE, INITIALIZED);
            this.queue = queue;
        }

        @Override
        String toDescription()
        {
            return queue.task.toDescription();
        }

        @Override void submitExclusive(AccordExecutor owner) { throw new UnsupportedOperationException(); }

        @Override
        protected void preRunExclusive()
        {
            queue.preRunTask();
        }

        @Override
        protected void run()
        {
            queue.runTask();
        }

        @Override
        protected void fail(Throwable t)
        {
            queue.failTask(t);
        }

        @Override
        protected boolean isNewWork()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void cleanupExclusive(AccordExecutor executor)
        {
            queue.cleanupTask();
        }

        protected boolean isInHeap()
        {
            return super.isInHeap();
        }
    }

    private static final AtomicReferenceFieldUpdater<ExclusiveExecutor, Thread> ownerUpdater = AtomicReferenceFieldUpdater.newUpdater(ExclusiveExecutor.class, Thread.class, "owner");
    public final class ExclusiveExecutor extends MultiTaskQueue<Task> implements ExclusiveAsyncExecutor
    {
        final int commandStoreId;
        final ExclusiveExecutorTask selfTask;
        private Task task;
        volatile Thread owner, waiting;
        private boolean stopped;
        private volatile boolean visibleStopped;
        private boolean terminated;

        final DebugExclusiveExecutor debug;

        ExclusiveExecutor(AccordExecutor executor)
        {
            this(executor, -1);
        }

        ExclusiveExecutor(AccordExecutor executor, int commandStoreId)
        {
            super(RUNNABLE, commandStoreId < 0 ? GroupKind.NONE : GroupKind.EXCLUSIVE, EXCLUSIVE_QUEUE_BUDGETS, EXCLUSIVE_QUEUE_LIMITS);
            this.commandStoreId = commandStoreId;
            this.selfTask = new ExclusiveExecutorTask(this);
            this.debug = DebugExclusiveExecutor.maybeDebug(executor.debug, commandStoreId);
        }

        void preRunTask()
        {
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
                task.fail(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + ((AccordTask<?>) task).executionContext()));
            else
                task.run();
            // NOTE: cannot safely release owner here, in case an immediate-execution runs before we can release our references and store their changes to the cache
        }

        private boolean reject(Task task)
        {
            if (!(task instanceof AccordTask<?>))
                return true;

            ExecutionContext context = ((AccordTask<?>) task).executionContext();
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
                active = 0;
                owner = null;
                task = super.pollMulti();
                if (DEBUG_EXECUTION) debug.onSetTask(task);
                if (task != null)
                {
                    selfTask.position = task.position;
                    selfTask.setStateUnsafe(WAITING_TO_RUN);
                    runnable.enqueue(selfTask);
                }
            }
        }

        void enqueue(Task newTask)
        {
            if (task != null)
            {
                Invariants.require(selfTask.isInHeap() || selfTask.isReadyToCleanup());
                super.enqueueMulti(newTask);
            }
            else
            {
                Invariants.require(isEmptySingle());
                incrementDispatches(newTask);
                task = newTask;
                task.setQueue(this);
                selfTask.position = newTask.position;
                selfTask.setStateUnsafe(WAITING_TO_RUN);
                runnable.enqueue(selfTask);
                if (DEBUG_EXECUTION) debug.onSetTask(newTask);
            }
        }

        @Override
        void unqueue(Task remove)
        {
            if (remove == task) removeCurrentTask(remove);
            else super.unqueueMulti(remove);
        }

        boolean tryUnqueueWaiting(Task remove)
        {
            if (remove == task) return tryRemoveCurrentTask(remove);
            else return super.tryUnqueueWaiting(remove);
        }

        private boolean tryRemoveCurrentTask(IntrusiveHeapNode remove)
        {
            if (runnable.isAssigned(selfTask))
                return false;

            removeCurrentTask(remove);
            return true;
        }

        private void removeCurrentTask(IntrusiveHeapNode remove)
        {
            Invariants.require(remove == task);
            // cannot overwrite task while it is being executed - this cannot happen for AccordTask
            // but can for other tasks that don't track their own state

            decrementDispatches(task);
            task.unsetQueue(this);
            task = pollMulti();
            if (DEBUG_EXECUTION) debug.onSetTask(task);
            if (runnable.isWaiting(selfTask))
            {
                if (task == null) runnable.unqueue(selfTask);
                else
                {
                    selfTask.position = task.position;
                    runnable.requeue(selfTask);
                }
            }
            else
            {
                Invariants.expect(false, "%s should have been queued to run as it had the task %s pending, that has now been cancelled", this, remove);
                if (task != null)
                {
                    selfTask.position = task.position;
                    selfTask.setStateUnsafe(WAITING_TO_RUN);
                    runnable.enqueue(selfTask);
                }
            }
            Invariants.require(task == null || runnable.isWaiting(selfTask));
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
        public AsyncChain<Void> chain(Runnable run)
        {
            return AsyncChains.chain(this, run);
        }

        @Override
        public <V> AsyncChain<V> chain(Callable<V> call)
        {
            return AsyncChains.chain(this, call);
        }

        @Override
        public <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call)
        {
            return AsyncChains.flatChain(this, call);
        }

        Task inherit()
        {
            Thread thread = Thread.currentThread();
            if (thread == owner)
                return task;
            return AccordTaskRunner.get(thread).accordActiveSelfTask();
        }

        @Override
        public void execute(Runnable run)
        {
            Task inherit = inherit();
            PlainRunnable submit = inherit == null ? new PlainRunnable(null, run, this, ExclusiveGroup.OTHER)
                                                   : new PlainRunnable(null, run, this, ExclusiveGroup.OTHER, inherit.position, inherit.tranche());
            submitPlain(submit);
        }

        @Override
        public Cancellable execute(RunOrFail runOrFail)
        {
            Task inherit = inherit();
            PlainChain submit = inherit == null ? new PlainChain(runOrFail, ExclusiveExecutor.this, ExclusiveGroup.OTHER)
                                                : new PlainChain(runOrFail, ExclusiveExecutor.this, ExclusiveGroup.OTHER, inherit.position, inherit.tranche());
            return submitPlain(submit);
        }

        @Override
        public boolean tryExecuteImmediately(Runnable run)
        {
            Thread thread = Thread.currentThread();
            Thread owner = this.owner;
            if (owner != null && owner != thread)
                return false;

            AccordTaskRunner self = AccordTaskRunner.get(thread);
            AccordExecutor active = self.accordActiveExecutor();
            if (active != null && active != AccordExecutor.this)
                return false; // prevent cross-executor locking/execution

            if (!ownerUpdater.compareAndSet(this, null, thread))
                return false;

            if (active == null)
                self.setAccordActiveExecutor(AccordExecutor.this);

            try { run.run(); }
            catch (Throwable t) { agent.onException(t); }
            finally
            {
                if (active == null)
                    self.setAccordActiveExecutor(null);

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

    // has xSingle methods to distinguish from within MultiTaskQueue whether we're invoking the multi or single variation
    static class TaskQueue<T extends Task> extends IntrusivePriorityHeap<T>
    {
        final int states;
        public TaskQueue(int states) {this.states = states; }

        void unqueue(T task)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public final int compare(T o1, T o2)
        {
            return Long.compare(o1.position, o2.position);
        }

        @Override
        protected final void ensureHeapified()
        {
            super.ensureHeapified();
        }

        protected final boolean requeueSingle(T requeue)
        {
            int oldIndex = updateNode(requeue);
            if (oldIndex < 0)
                return false;

            int newIndex = heapIndex(requeue);
            return Math.min(oldIndex, newIndex) == 0 && heapifiedSize() > 0;
        }

        final T peekSingle()
        {
            ensureHeapified();
            return peekNode();
        }

        final T pollSingle()
        {
            ensureHeapified();
            return pollNode();
        }

        final boolean isEmptySingle()
        {
            return isEmptyInternal();
        }

        final int enqueueSingle(T enqueue)
        {
            return insertNode(enqueue);
        }

        final boolean unqueueSingle(T unqueue)
        {
            int heapIndex = heapIndex(unqueue);
            removeNode(unqueue);
            return heapIndex == 0;
        }

        final boolean tryUnqueueSingle(T remove)
        {
            return removeNodeIfContains(remove);
        }

        final T getSingle(int index)
        {
            return super.getNode(index);
        }

        final boolean isQueuedSingle(T test)
        {
            return containsNode(test);
        }
    }

    static final class StandaloneTaskQueue<T extends Task> extends TaskQueue<T>
    {
        StandaloneTaskQueue(int states)
        {
            super(states);
        }

        StandaloneTaskQueue(Task.State state)
        {
            super(TinyEnumSet.encode(state));
        }

        void enqueue(T enqueue)
        {
            enqueue.setQueue(this);
            enqueueSingle(enqueue);
        }

        void unqueue(T unqueue)
        {
            unqueue.unsetQueue(this);
            removeNode(unqueue);
        }

        T peek()
        {
            return peekSingle();
        }

        T poll()
        {
            return pollSingle();
        }

        boolean isEmpty()
        {
            return isEmptySingle();
        }
    }

    /**
     * A {@link TaskQueue} sub-divided into up to eight per-group sub-queues (groups are phases for the exclusive
     * executor, or work classes globally) plus a policy for choosing which sub-queue to serve next. The policy balances
     * three competing concerns: ordering/priority (run the oldest / highest-priority work), fairness (do not let one
     * group monopolise the executor), and throughput (do not let an even split leave a busy group's backlog growing
     * without bound).
     *
     * <h2>Packed counters</h2>
     * Per-group state is held in {@code long}s, one 7-bit lane per group; bit 7 of each byte is a spare "guard" bit used
     * to run branch-free min/compare across all eight lanes at once (see {@code minCounters}). The lanes are:
     * <ul>
     *   <li>{@code positions[]} - the head task's position (HLC, or submission order) per group; drives FIFO/priority.</li>
     *   <li>{@code recent} - a windowed count of recently <i>served</i> tasks per group (service received).</li>
     *   <li>{@code arrivals} - a windowed, size-biased count of recent <i>arrivals</i> per group (demand offered).</li>
     *   <li>{@code current} - tasks currently in flight per group, used to enforce {@code queue_active_limits}.</li>
     *   <li>{@code hasWork}/{@code dirty} - guard-bit masks: which groups have queued work / need a position refresh.</li>
     * </ul>
     *
     * <h2>Windowing: a bounded memory of the past</h2>
     * {@code recent} and {@code arrivals} are not running totals. Whenever incrementing {@code recent} tips any lane to
     * its maximum (0x80), <em>both</em> {@code recent} and {@code arrivals} are halved (see {@code incrementRecents}),
     * and each lane also saturates at 0x7f. They are therefore an exponentially-decaying window keyed on service/time.
     * This is what stops a one-off burst of arrivals granting a group a permanent boost: the burst saturates the lane
     * briefly and then decays away as other work is served.
     *
     * <h2>The fair selection counter</h2>
     * The fair models pick the group with the smallest {@code effective} counter, where per lane
     * <pre>effective = min(0x7f, max(0, recent - arrivals) + bias)</pre>
     * <ul>
     *   <li>{@code max(0, recent - arrivals)} - a group whose arrivals have outpaced its recent service clamps to 0 and
     *       is therefore preferred; in steady state each group's service tracks its arrival rate, which bounds backlog.
     *       The clamp is symmetric, so a group cannot bank "credit" during a lull and later use it to monopolise service.</li>
     * </ul>
     *
     * <h2>Choosing a group ({@code minGroup} / {@code pollByBlend})</h2>
     * <ul>
     *   <li>{@code PRIORITY_ONLY} - always the lowest {@code positions} (oldest / highest priority), ignoring fairness.</li>
     *   <li>{@code PHASE_OVERRIDE} - strict phase order: the lowest-index group with work, always.</li>
     *   <li>{@code PHASE_FAIR} - pure fairness: the smallest {@code effective}; ties broken by index, within a group by
     *       position. It has no priority fallback, so it is deliberately completing-first under contention.</li>
     *   <li>{@code PRIORITY_FAIR} (default) - a deficit round-robin blend of two strategies, chosen per poll: <b>flow</b>
     *       (smallest {@code effective}, i.e. least fairly serviced) and <b>age</b> (oldest {@code positions}, i.e. FIFO).
     *       The flow weight ramps up with the flow imbalance ({@code max-min} of {@code effective}) over
     *       {@code queue_flow_imbalance_onset..+width}, trading against age zero-sum; when balanced it is pure age. The
     *       ramp is smooth (not a hard mode switch), so there is no boundary to oscillate around and no hysteresis is
     *       needed. Age also drains stale/standing backlogs for free: a backlog's items are the oldest work, so age
     *       clears them without a separate backlog-aware strategy (the {@code effective} flow counter tracks arrival
     *       <em>rate</em> and is deliberately blind to standing <em>stock</em>).</li>
     * </ul>
     *
     * <h2>Concurrency limits and eligibility</h2>
     * A group whose in-flight {@code current} count has reached its {@code queue_active_limits} limit is
     * {@code saturated} and excluded from selection ({@code disabled}), as is any group with no queued work.
     *
     * <h2>Note on {@code positions} freshness</h2>
     * {@code positions} is refreshed lazily from the sub-queue heads via the {@code dirty} mask; {@code dirty} must use
     * the same guard-bit layout as {@code hasWork} so the refresh loop in {@code minGroupByPriority} actually runs -
     * otherwise it silently degenerates to lowest-index-with-work and starves high-index / new-work groups.
     *
     * <p>NB it extends {@link TaskQueue} to keep the type hierarchy simple for method dispatch, and for efficiency for
     * anonymous ExclusiveExecutors which do not use multiple queues, while letting ExclusiveExecutor share a parent
     * class for both use cases.
     */
    static abstract class MultiTaskQueue<T extends Task> extends TaskQueue<T>
    {
        private static final TaskQueue[] NO_QUEUES = new TaskQueue[0];
        private static final long[] NO_POSITIONS = new long[0];
        static final long COUNTER_OVERFLOWS = 0x8080808080808080L;
        static final long COUNTER_MASKS = 0x7f7f7f7f7f7f7f7fL;
        static final long COUNTER_LOWBITS = 0x0101010101010101L;

        final TaskQueue<T>[] queues;
        final long[] positions;
        final byte groupShift;
        final long baseBudget, limits;

        /** sets overflow bits for each counter when it needs its position updated */
        long dirty;
        /** sets overflow bits for each counter when there's associated work */
        long hasWork;
        /** Stores recent dequeue counts for up to 8 sub queues. */
        long dispatches;
        /** Stores recent enqueue counts for up to 8 sub queues. */
        long arrivals;
        /** Stores currently-active counts for up to 8 sub queues. We can use this to impose limits on specific queues. */
        long active;
        /** Stores a biased budget for each task type, so that we may prefer to serve one type of task over another */
        long budget;
        /** deficit-round-robin credits for the two PRIORITY_FAIR strategies (flow/age). */
        int creditFlow, creditAge;

        int waitingCount;

        MultiTaskQueue(int waitingStates, GroupKind groups, long budget, long limits)
        {
            super(waitingStates);
            this.baseBudget = budget;
            this.limits = limits;
            int queueCount = groups.count;
            Invariants.require(queueCount <= 8);
            queues = queueCount == 0 ? NO_QUEUES : new TaskQueue[queueCount];
            positions = queueCount > 0 && BALANCE_BY_POSITION ? new long[queueCount] : NO_POSITIONS;
            groupShift = groups.shift;
        }

        final int group(Task task)
        {
            if (groupShift == 0)
                return -1;

            return (task.info >>> groupShift) & Task.GROUP_MASK;
        }

        final TaskQueue<T> queue(Task task)
        {
            int group = group(task);
            if (group < 0)
                return this;

            return queue(group);
        }

        final TaskQueue<T> queue(int group)
        {
            TaskQueue<T> queue = queues[group];
            if (queue == null)
                queues[group] = queue = new TaskQueue<>(0);

            return queue;
        }

        private int pollGroup()
        {
            if (hasWork == 0)
                return -1;

            switch (BALANCING_MODEL)
            {
                default: throw new UnhandledEnum(PRIORITY_MODEL);
                case PRIORITY_ONLY: return pollGroupByPriority();
                case PHASE_ONLY: return pollGroupByIndex();
                case PRIORITY_BUDGET: return pollGroupByPriorityWithBudget();
                case PHASE_BUDGET: return pollGroupByIndexWithBudget();
                case PHASE_FAIR: return pollGroupByPhaseFair();
                case PHASE_BUDGET_FAIR: return pollGroupByPhaseBudgetFair();
                case BLENDED_PRIORITY_PHASE_FAIR: return pollGroupByBlended();
                case BLENDED_PRIORITY_PHASE_BUDGET_FAIR: return pollGroupByBlendedWithBudget();
            }
        }

        private int pollGroupByPriority()
        {
            return pollGroupByPriority(unsaturatedWithWork());
        }

        private int pollGroupByPriority(long enabled)
        {
            long refresh = dirty & hasWork;
            while (refresh != 0)
            {
                int bitIndex = Long.numberOfTrailingZeros(refresh);
                int group = bitIndex / 8;
                positions[group] = queues[group].peekSingle().position;
                refresh ^= 1L << bitIndex;
            }
            dirty = 0;

            long minPosition = Long.MAX_VALUE;
            int minGroup = -1;
            long visit = enabled >>> 7;
            while (visit != 0)
            {
                int bitIndex = Long.numberOfTrailingZeros(visit);
                int group = bitIndex / 8;
                long position = positions[group];
                if (position < minPosition)
                {
                    minGroup = group;
                    minPosition = position;
                }
                visit ^= 1L << bitIndex;
            }

            return minGroup;
        }

        private int pollGroupByIndex()
        {
            long visit = (hasWork & unsaturated()) >>> 7;
            if (visit == 0)
                return -1;

            int bitIndex = Long.numberOfTrailingZeros(visit);
            return bitIndex / 8;
        }

        private int pollGroupByPriorityWithBudget()
        {
            return pollGroupByPriority(unsaturatedWithWorkAndBudget());
        }

        private int pollGroupByIndexWithBudget()
        {
            long visit = (hasWork & unsaturated());
            if (visit == 0)
                return -1;

            visit = withBudgetOrReset(visit);
            visit >>>= 7;
            int bitIndex = Long.numberOfTrailingZeros(visit);
            return bitIndex / 8;
        }

        private int pollGroupByPhaseFair()
        {
            return minCounterIndex(recentFlowImbalances());
        }

        private int pollGroupByPhaseBudgetFair()
        {
            return minCounterIndex(recentFlowImbalances(), saturatedOrWithoutWorkOrWithoutBudget());
        }

        // PRIORITY_FAIR selection: a deficit round-robin blend of two strategies, chosen per poll:
        //   flow -> minCounterIndex(recent - arrivals + bias)  (least fairly serviced)
        //   age  -> minGroupByPriority()  (earliest-queued work)
        // wFlow = ramp(flow imbalance F = max-min of the selection counters); wAge = BLEND_TOTAL - wFlow. As flow gets
        // uneven, polls trade from age to flow zero-sum; when balanced it is pure age (FIFO). A ramp (not a hard
        // threshold) means there is no mode cliff to oscillate around, so no anti-oscillation penalty is needed. Age is
        // also what drains a stale/standing backlog -- its items are the oldest -- so no separate stock strategy is needed.
        private int pollGroupByBlended()
        {
            return pollGroupByBlended(saturatedOrWithoutWork());
        }

        private int pollGroupByBlended(long disabled)
        {
            long withoutWork = hasWork ^ COUNTER_OVERFLOWS;
            long counters = recentFlowImbalances();
            long minMax = minMaxCounterValue(counters, withoutWork);
            long min = minMax & 0x7f;
            long max = minMax >>> 8;
            int flowImbalance = (int) (max - min);

            int flowWeight = flowWeight(flowImbalance);
            int priorityWeight = BLEND_TOTAL - flowWeight;

            creditFlow += flowWeight;
            creditAge += priorityWeight;

            if (creditFlow >= creditAge)
            {
                creditFlow -= BLEND_TOTAL;
                if (disabled != withoutWork)
                    min = minCounterValue(counters, disabled);

                return minCounterIndex(counters, min, disabled);
            }
            else
            {
                creditAge -= BLEND_TOTAL;
                return pollGroupByPriority(disabled ^ COUNTER_OVERFLOWS);
            }
        }

        private int pollGroupByBlendedWithBudget()
        {
            return pollGroupByBlended(saturatedOrWithoutWorkOrWithoutBudget());
        }

        private long saturated()
        {
            return ((active | COUNTER_OVERFLOWS) - limits) & COUNTER_OVERFLOWS;
        }

        private long unsaturated()
        {
            return saturated() ^ COUNTER_OVERFLOWS;
        }

        private long unsaturatedWithWork()
        {
            return hasWork & unsaturated();
        }

        private long unsaturatedWithWorkAndBudget()
        {
            return withBudgetOrReset(hasWork & unsaturated());
        }

        private long saturatedOrWithoutWorkOrWithoutBudget()
        {
            return withoutBudgetOrReset(saturatedOrWithoutWork());
        }

        private long withoutBudgetOrReset(long disabled)
        {
            return COUNTER_OVERFLOWS ^ withBudgetOrReset(COUNTER_OVERFLOWS ^ disabled);
        }

        private long withBudgetOrReset(long enabled)
        {
            long hasBudget = hasBudget();
            if ((hasBudget & enabled) != 0)
                return hasBudget & enabled;

            budget = baseBudget;
            return enabled;
        }

        private long hasBudget()
        {
            return setOverflowWhenLessEqual(budget, 0) ^ COUNTER_OVERFLOWS;
        }

        private long saturatedOrWithoutWork()
        {
            return (hasWork ^ COUNTER_OVERFLOWS) | saturated();
        }

        // arrivals is a windowed measure of ARRIVAL (incremented on enqueue, size-biased; decays on recent's overflow).
        // Combined with recent (service) as effective = max(0, recent - arrivals): a queue whose arrivals outpace its
        // service clamps to 0 and is preferred, so service converges to arrival rate (bounding the busy queue's backlog).
        private long recentFlowImbalances()
        {
            return clampedSubtract(dispatches, arrivals);
        }

        static long minCounterValue(long counters, long disabled)
        {
            long mins = counters;
            mins |= overflowsToLowMasks(disabled);
            mins = minCounters(mins, mins >>> 8); // each slot is min of slots [i..i+1]
            mins = minCounters(mins, mins >>> 16); // each slot is min of slots [i..i+3]
            mins = minCounters(mins, mins >>> 32); // each slot is min of slots [i..i+7]
            return mins & 0x7f;
        }

        static long minMaxCounterValue(long counters, long disabled)
        {
            long mins = counters;
            long maxs = counters ^ COUNTER_MASKS;
            long overflowMasks = overflowsToLowMasks(disabled);
            mins |= overflowMasks;
            maxs |= overflowMasks;
            mins = minCounters(mins, mins >>> 8) & 0x007f007f007f007fL; // each slot is min of slots [i..i+1]
            maxs = (minCounters(maxs, maxs << 8) & 0x7f007f007f007f00L); // each slot is min of slots ~[i..i+1]
            long minmaxs = mins | maxs;
            minmaxs = minCounters(minmaxs, minmaxs >>> 16); // each slot is min of slots [i..i+3]
            minmaxs = minCounters(minmaxs, minmaxs >>> 32); // each slot is min of slots [i..i+7]
            return (minmaxs ^ 0x7f00) & 0x7f7f;
        }

        /**
         * If provided two counters (containing 8 7 bit counters each),
         * returns the minimum of each matching counter
         */
        private static long minCounters(long a, long b)
        {
            // set overflow bits where a <= b
            long selecta = setOverflowWhenLessEqual (a, b);
            return selectByOverflowBits(selecta, a, b);
        }

        static long setOverflowWhenLessEqual(long a, long b)
        {
            return ((b | COUNTER_OVERFLOWS) - a) & COUNTER_OVERFLOWS;
        }

        // select a if overflow bit is set; b if it is unset
        static long selectByOverflowBits(long selecta, long a, long b)
        {
            selecta = overflowsToLowMasks(selecta);
            a &= selecta;
            b &= ~selecta;
            return a | b;
        }

        static long overflowsToLowMasks(long v)
        {
            return v - (v >>> 7);
        }

        private static int flowWeight(int flowImbalance)
        {
            if (flowImbalance <= FLOW_ONSET) return 0;
            return Math.min(BLEND_TOTAL, ((flowImbalance - FLOW_ONSET) << BLEND_SHIFT) >>> FLOW_WIDTH_SHIFT);
        }

        // per-lane max(0, a - b), carry-free: zero both a and b in lanes where a <= b, then subtract
        private static long clampedSubtract(long a, long b)
        {
            long keep = ~overflowsToLowMasks(setOverflowWhenLessEqual(a, b));
            return (a & keep) - (b & keep);
        }

        private int minCounterIndex(long counters)
        {
            return minCounterIndex(counters, saturatedOrWithoutWork());
        }

        private int minCounterIndex(long counters, long disabled)
        {
            return minCounterIndex(counters, minCounterValue(counters, disabled), disabled);
        }

        private int minCounterIndex(long counters, long minCounterValue, long disabled)
        {
            long mins = minCounterValue * COUNTER_LOWBITS;
            long select = ((mins | COUNTER_OVERFLOWS) - counters) & COUNTER_OVERFLOWS;
            // now unset those overflow bits associated with disabled queues
            select &= ~disabled;
            if (select == 0)
                return -1;
            return (Long.numberOfTrailingZeros(select) - 7) / 8;
        }

        final T pollMulti()
        {
            int group = pollGroup();
            if (group < 0)
            {
                // group < 0 can mean EITHER we don't have any nested queues OR those queues are either empty or DISABLED
                T result = pollSingle();
                if (result != null)
                    --waitingCount;
                return result;
            }

            --waitingCount;
            incrementActive(group);
            incrementDispatches(group);
            decrementBudget(group);

            TaskQueue<T> queue = queues[group];
            T head = queue.pollSingle();
            // NOTE: must clear dirty when emptied, symmetrically with unqueue(): the fair selection paths
            // never consume the dirty bit, so a group drained during a fairness episode would otherwise
            // retain a stale dirty bit and NPE in minGroupByPriority.peekSingle() when balance is restored.
            if (queue.isEmptySingle()) { unsetHasWork(group); unsetDirty(group); }
            else setDirty(group);
            return head;
        }

        final void enqueueMulti(T task)
        {
            task.setQueue(this);
            int group = group(task);
            if (group < 0)
            {
                enqueueSingle(task);
            }
            else
            {
                TaskQueue<T> queue = queue(group);
                int result = queue.enqueueSingle(task);
                incrementArrivals(group);
                if (result < 0) setHasWork(group);
                if (result != 0) setDirty(group);
            }
            ++waitingCount;
        }

        final void requeue(T task)
        {
            int group = group(task);
            if (group < 0) requeueSingle(task);
            else
            {
                TaskQueue<T> queue = queue(group);
                Invariants.require(queue != null && queue.isQueuedSingle(task));
                if (queue.requeueSingle(task))
                    setDirty(group);
            }
        }

        final void unqueueMulti(T task)
        {
            int group = group(task);
            TaskQueue<T> queue = group < 0 ? this : queue(task);
            Invariants.require(queue.isQueuedSingle(task));
            unqueue(task, group, queue);
        }

        // if there is an active collection, we return false and do not remove ourselves from it
        boolean tryUnqueueWaiting(T task)
        {
            int group = group(task);
            TaskQueue<T> queue = group < 0 ? this : queue(task);
            if (!queue.isQueuedSingle(task))
                return false;

            unqueue(task, group, queue);
            return true;
        }

        private void unqueue(T task, int group, TaskQueue<T> queue)
        {
            task.unsetQueue(this);
            boolean dirty = queue.unqueueSingle(task);
            --waitingCount;
            if (group >= 0)
            {
                if (queue.isEmptySingle())
                {
                    unsetHasWork(group);
                    unsetDirty(group);
                }
                else if (dirty) setDirty(group);
            }
        }

        final void incrementActive(int group)
        {
            active += lowBit(group);
        }

        final void decrementActive(int group)
        {
            active -= lowBit(group);
        }

        final void incrementDispatches(Task task)
        {
            int group = group(task);
            if (group >= 0)
                incrementDispatches(group);
        }

        final void incrementDispatches(int group)
        {
            dispatches += lowBit(group);
            if ((dispatches & COUNTER_OVERFLOWS) != 0)
            {
                dispatches = (dispatches >>> 1) & COUNTER_MASKS;
                arrivals = (arrivals >>> 1) & COUNTER_MASKS; // arrivals (arrival) decays on the service/time clock
            }
        }

        final void decrementBudget(int group)
        {
            // no need to manage underflow; if the budget is being used,
            // a queue should only dispatch work when there's non-zero budget
            budget -= lowBit(group);
        }

        final void decrementDispatches(Task task)
        {
            int group = group(task);
            if (group >= 0)
                decrementDispatches(group);
        }

        final void decrementDispatches(int group)
        {
            long lowBit = lowBit(group);
            dispatches -= lowBit;
            dispatches += (dispatches >>> 7) & lowBit;
        }

        final void incrementArrivals(int group)
        {
            int shift = group * 8;
            long overflowBit = 0x80L << shift;
            arrivals += 1L << shift;
            // if we overflow, unset the overflow bit and set all other bits for the counter
            long overflow = arrivals & overflowBit;
            arrivals ^= overflow;
            arrivals |= overflow - (overflow >>> 7);
        }

        final void setHasWork(int group)
        {
            hasWork |= overflowBit(group);
        }

        final void unsetHasWork(int group)
        {
            hasWork &= ~overflowBit(group);
        }

        final void setDirty(int group)
        {
            dirty |= overflowBit(group);
        }

        final void unsetDirty(int group)
        {
            dirty &= ~overflowBit(group);
        }

        final boolean hasWaiting()
        {
            return waitingCount > 0;
        }

        final boolean isWaiting(T task)
        {
            return queue(task).isQueuedSingle(task);
        }

        final int waitingCount()
        {
            return waitingCount;
        }

        final long lowBit(int group)
        {
            return 1L << (group * 8);
        }

        final long overflowBit(int group)
        {
            return 0x80L << (group * 8);
        }
    }

    static final class RunnableTaskQueue<T extends Task> extends MultiTaskQueue<T>
    {
        static final int RUNNABLE = TinyEnumSet.encode(WAITING_TO_RUN, ASSIGNED, RUNNING);

        final TaskQueue<T> assigned;

        RunnableTaskQueue()
        {
            super(RUNNABLE, GroupKind.GLOBAL, GLOBAL_QUEUE_BUDGETS, GLOBAL_QUEUE_LIMITS);
            this.assigned = new TaskQueue<>(0);
        }

        T poll()
        {
            T next = pollMulti();
            if (next == null)
                return null;

            next.setState(ASSIGNED);
            assigned.enqueueSingle(next);

            return next;
        }

        void enqueue(T enqueue)
        {
            enqueueMulti(enqueue);
        }

        void unqueue(T unqueue)
        {
            if (assigned.isQueuedSingle(unqueue))
            {
                int group = group(unqueue);
                if (group >= 0)
                    decrementActive(group);

                unqueue.unsetQueue(this);
                assigned.unqueueSingle(unqueue);
            }
            else
            {
                super.unqueueMulti(unqueue);
            }
        }

        int waitingOrAssignedCount()
        {
            return waitingCount + assigned.size();
        }

        boolean hasAssignedOrWaiting()
        {
            return waitingCount > 0 || !assigned.isEmptySingle();
        }

        boolean hasAssigned()
        {
            return !assigned.isEmptySingle();
        }

        boolean isAssigned(T task)
        {
            return assigned.isQueuedSingle(task);
        }

        void complete(T task)
        {
            if (assigned.tryUnqueueSingle(task))
            {
                int group = group(task);
                if (group >= 0)
                    decrementActive(group);
                task.unsetQueue(this);
            }
        }
    }

    static class CancelTask extends Task
    {
        final Task cancel;
        private CancelTask(Task cancel)
        {
            super(GlobalGroup.OTHER, WAITING_TO_RUN);
            this.cancel = cancel;
        }

        @Override void submitExclusive(AccordExecutor owner) { cancel.cancelExclusive(owner); }
        @Override protected void preRunExclusive() { throw new UnsupportedOperationException(); }
        @Override protected void run() { throw new UnsupportedOperationException(); }
        @Override protected void fail(Throwable fail) { throw new UnsupportedOperationException(); }
        @Override protected boolean isNewWork() { return false; }
        @Override String toDescription() { return "Cancel " + cancel.toDescription(); }
    }

    static <O> IntFunction<O> constant(O out)
    {
        return ignore -> out;
    }

    abstract class Plain extends Task implements Cancellable
    {
        Plain(GlobalGroup group, long position, int tranche)
        {
            super(group, WAITING_TO_RUN, position, tranche);
        }

        Plain(ExclusiveGroup group, long position, int tranche)
        {
            super(group, WAITING_TO_RUN, position, tranche);
        }

        Plain(GlobalGroup group)
        {
            super(group, WAITING_TO_RUN);
        }

        Plain(ExclusiveGroup group)
        {
            super(group, WAITING_TO_RUN);
        }

        abstract ExclusiveExecutor executor();

        @Override
        protected void preRunExclusive() {}

        @Override
        public void cancel()
        {
            submit((e, c) -> c.cancelExclusive(e), CancelTask::new, this);
        }

        void cancelExclusive(AccordExecutor owner)
        {
            ExclusiveExecutor executor = executor();
            if ((executor == null ? runnable : executor).tryUnqueueWaiting(this))
            {
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

        @Override
        protected boolean isNewWork()
        {
            return true;
        }
    }

    class PlainRunnable extends Plain implements Cancellable
    {
        final @Nullable AsyncPromise<Void> result;
        final Runnable run;
        final @Nullable ExclusiveExecutor executor;

        PlainRunnable(AsyncPromise<Void> result, Runnable run, GlobalGroup group, long position, int tranche)
        {
            super(group, position, tranche);
            this.result = result;
            this.run = run;
            this.executor = null;
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run, GlobalGroup group)
        {
            super(group);
            this.result = result;
            this.run = run;
            this.executor = null;
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run, ExclusiveExecutor executor, ExclusiveGroup group, long position, int tranche)
        {
            super(group, position, tranche);
            this.result = result;
            this.run = run;
            this.executor = executor;
        }

        PlainRunnable(AsyncPromise<Void> result, Runnable run, ExclusiveExecutor executor, ExclusiveGroup group)
        {
            super(group);
            this.result = result;
            this.run = run;
            this.executor = executor;
        }

        @Override
        String toDescription()
        {
            // TODO (expected): ensure this is usefully descriptive, or accept a separate description
            return run.toString();
        }

        @Override
        protected void run()
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
        ExclusiveExecutor executor()
        {
            return executor;
        }
    }

    // a task that may be submitted to this executor or another
    abstract class IOTask extends Plain implements Cancellable, DebuggableTask
    {
        IOTask(GlobalGroup group, long position, int tranche)
        {
            super(group, position, tranche);
        }

        IOTask(GlobalGroup group)
        {
            super(group);
        }

        abstract void postRunExclusive();

        @Override
        protected void cleanupExclusive(AccordExecutor executor)
        {
            super.cleanupExclusive(executor);
            postRunExclusive();
        }

        @Override
        ExclusiveExecutor executor()
        {
            return null;
        }

        @Override
        public long creationTimeNanos()
        {
            return createdAt;
        }

        @Override
        public long startTimeNanos()
        {
            return runningAt;
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
        return new LoadRunnable<>(entry, isForRange ? RANGE_LOAD : LOAD);
    }

    class LoadRunnable<K, V> extends IOTask
    {
        final AccordCacheEntry<K, V> entry;
        Object result = FailureHolder.NOT_STARTED;

        LoadRunnable(AccordCacheEntry<K, V> entry, GlobalGroup group)
        {
            super(group);
            Invariants.require(group == LOAD || group == RANGE_LOAD);
            this.entry = entry;
        }

        boolean isForRange() { return is(RANGE_LOAD); }

        void postRunExclusive()
        {
            if (!(result instanceof FailureHolder)) onLoadedExclusive(entry, (V)result, null, isForRange());
            else onLoadedExclusive(entry, null, ((FailureHolder)result).fail, isForRange());
        }

        @Override
        String toDescription()
        {
            return "Load " + entry.key();
        }

        @Override
        public void run()
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

        WrappedIOTask(AbstractIOTask wrap, GlobalGroup group, long position, int tranche)
        {
            super(group, position, tranche);
            this.wrapped = wrap;
        }

        @Override
        String toDescription()
        {
            return wrapped.description();
        }

        @Override
        protected void run()
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
            super(SAVE);
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
        String toDescription()
        {
            return "Save " + entry.key();
        }

        @Override
        public void run()
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
        final @Nullable ExclusiveExecutor executor;

        PlainChain(RunOrFail runOrFail, ExclusiveExecutor executor, ExclusiveGroup group)
        {
            super(group);
            this.runOrFail = runOrFail;
            this.executor = executor;
        }

        PlainChain(RunOrFail runOrFail, ExclusiveExecutor executor, ExclusiveGroup group, long position, int tranche)
        {
            super(group, position, tranche);
            this.runOrFail = runOrFail;
            this.executor = executor;
        }

        @Override
        ExclusiveExecutor executor()
        {
            return executor;
        }

        @Override
        String toDescription()
        {
            return runOrFail.toString();
        }

        @Override
        protected void run()
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
        final Object describe;

        DebuggableChain(RunOrFail runOrFail, @Nullable ExclusiveExecutor executor, Object describe)
        {
            super(runOrFail, executor, ExclusiveGroup.OTHER);
            this.describe = Invariants.nonNull(describe);
        }

        DebuggableChain(RunOrFail runOrFail, @Nullable ExclusiveExecutor executor, long position, int tranche, Object describe)
        {
            super(runOrFail, executor, ExclusiveGroup.OTHER, position, tranche);
            this.describe = Invariants.nonNull(describe);
        }

        @Override
        public long creationTimeNanos()
        {
            return createdAt;
        }

        @Override
        public long startTimeNanos()
        {
            return runningAt;
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
            return task.position;
        }

        public @Nullable String describe()
        {
            if (task instanceof AccordTask)
                return ((AccordTask<?>) task).executionContext().reason();

            if (task instanceof DebuggableTask)
                return ((DebuggableTask) task).description();

            return null;
        }

        public @Nullable ExecutionContext preLoadContext()
        {
            if (task instanceof AccordTask)
                return ((AccordTask<?>) task).executionContext();
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
        AccordTaskRunner self = AccordTaskRunner.get();
        lock(self);
        try
        {
            addToSnapshot(result, scanningRanges, TaskInfo.Status.SCANNING_RANGES, TaskInfo.Status.SCANNING_RANGES);
            addToSnapshot(result, waitingToLoadRangeTxns, TaskInfo.Status.WAITING_TO_LOAD, TaskInfo.Status.WAITING_TO_LOAD);
            addToSnapshot(result, waitingToLoad, TaskInfo.Status.WAITING_TO_LOAD, TaskInfo.Status.WAITING_TO_LOAD);
            addToSnapshot(result, loading, TaskInfo.Status.WAITING_TO_LOAD, TaskInfo.Status.WAITING_TO_LOAD);
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
                int commmandStoreId = t instanceof AccordTask ? ((AccordTask<?>) t).commandStore.id() : -1;
                snapshot.add(new TaskInfo(ifCurrent, commmandStoreId, t));
            }
        }
    }

    public int unsafePreparingToRunCount()
    {
        return waitingToLoad.size() + loading.size() + waitingToLoadRangeTxns.size() + scanningRanges.size();
    }

    public int unsafeWaitingToRunCount()
    {
        return runnable.waitingCount();
    }

    public int unsafeRunningCount()
    {
        return runnable.assigned.size();
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

    private static long encodeCounters(Map<? extends Enum<?>, Long> counters)
    {
        long result = 0;
        for (Map.Entry<? extends Enum<?>, Long> e : counters.entrySet())
            result |= e.getValue() << (e.getKey().ordinal() * 8);
        return result;
    }

}
