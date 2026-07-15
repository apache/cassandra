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
import java.util.concurrent.atomic.AtomicLong;
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
import accord.local.ExecutionContext.ExecutionSequence;
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
import org.apache.cassandra.service.accord.AccordExecutor.Task.State;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExclusiveExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugExecutor;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY;
import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY_ATOMIC;
import static accord.primitives.Routable.Domain.Range;
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
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.FAILED_TO_LOAD;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.RUNNING_OR_EXECUTED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_ON_OPTIONAL;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_ON_REQUIRED;
import static org.apache.cassandra.service.accord.AccordExecutor.Task.State.WAITING_TO_RUN;
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
    private static final long GLOBAL_QUEUE_LIMITS, EXCLUSIVE_QUEUE_LIMITS;
    static final int NONSYNC_MIN_BATCH_SIZE, NONSYNC_MAX_BATCH_SIZE, NONSYNC_BLOCKED_LIMIT;
    static final boolean NONSYNC_ENABLED;

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

    public interface AccordTaskRunner
    {
        AccordExecutor accordActiveExecutor();
        void setAccordActiveExecutor(AccordExecutor newExecutor);

        AccordExecutor accordLockedExecutor();
        boolean tryEnterAccordLockedExecutor(AccordExecutor newLockedExecutor);
        void exitAccordLockedExecutor();

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
        public boolean tryEnterAccordLockedExecutor(AccordExecutor newLockedExecutor)
        {
            if (lockedExecutor != null)
                return false;
            lockedExecutor = newLockedExecutor;
            return true;
        }

        @Override
        public void exitAccordLockedExecutor()
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
    final AtomicLong uniqueCreatedAt = new AtomicLong();
    final DebugExecutor debug = DebugExecutor.maybeDebug();

    private long maxWorkingSetSizeInBytes;
    private long maxWorkingCapacityInBytes;

    final StandaloneTaskQueue<AccordTask<?>> scanningRanges = new StandaloneTaskQueue<>(TinyEnumSet.encode(SCANNING_RANGES)); // never queried, just parked here while scanning
    final StandaloneTaskQueue<AccordTask<?>> loading = new StandaloneTaskQueue<>(TinyEnumSet.encode(LOADING_REQUIRED, LOADING_OPTIONAL));
    final StandaloneTaskQueue<AccordTask<?>> waitingOnCacheQueues = new StandaloneTaskQueue<>(TinyEnumSet.encode(WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL));
    final RunnableTaskQueue<Task> runnable = new RunnableTaskQueue<>();

    private final Tranches tranches = new Tranches(this);

    /**
     * Newly submitted work must take a position >= minPosition, but this condition does not apply to consequences of
     * previously submitted work; this inherits the originating operation's position and tranche.
     * This is to ensure afterSubmittedAndConsequences functions correctly.
     */
    private long minPosition = 1;
    private long nextPosition = 1;
    int tasks;

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
        long startAt = DEBUG_EXECUTION ? 0 : Clock.Global.nanoTime();
        if (!self.tryEnterAccordLockedExecutor(this))
            throw new UnsupportedOperationException("To ensure system performance, it is not permitted to lock multiple AccordExecutor simultaneously with the same thread");
        //noinspection LockAcquiredButNotSafelyReleased
        lock.lock();
        if (DEBUG_EXECUTION) debug.onEnterLock(startAt);
    }

    final void unlock(AccordTaskRunner self)
    {
        self.exitAccordLockedExecutor();
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
        if (result && !self.tryEnterAccordLockedExecutor(this))
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

        if (cache.weightedSize() < maxWorkingCapacityInBytes && !runnable.hasWaitingToRun())
        {
            hasPausedLoading = false;
            runnable.restart(RANGE_SCAN.ordinal());
            runnable.restart(LOAD.ordinal());
            runnable.restart(RANGE_LOAD.ordinal());
        }
    }

    final void maybePauseLoading()
    {
        if (hasPausedLoading)
            return;

        if (cache.weightedSize() >= maxWorkingCapacityInBytes || !runnable.hasWaitingToRun())
        {
            AccordSystemMetrics.metrics.pausedExecutorLoading.inc();
            hasPausedLoading = true;
            runnable.stop(RANGE_SCAN.ordinal());
            runnable.stop(LOAD.ordinal());
            runnable.stop(RANGE_LOAD.ordinal());
        }
    }

    public abstract boolean hasTasks();
    abstract void beforeUnlockExternal();
    abstract boolean isOwningThread();

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
    public <K, V> LoadRunnable<?, ?> load(AccordTask<?> parent, Boolean isForRange, AccordCacheEntry<K, V, ?> entry)
    {
        LoadRunnable<?, ?> load = newLoad(entry, isForRange);
        return submitPlainExclusive(parent, load);
    }

    @Override
    public Cancellable save(AccordCacheEntry<?, ?, ?> entry, UniqueSave identity, Runnable save)
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
        submit((self, task) -> task.submitExclusive(self), i -> i, operation);
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
        task.onLoaded();
        ExclusiveExecutor executor = task.executor();
        if (executor == null) runnable.enqueue(task, true);
        else executor.enqueue(task, true);
    }

    Cancellable submitPlainExclusive(Task parent, GlobalGroup group, AbstractIOTask task)
    {
        return submitPlainExclusive(parent, new WrappedIOTask(task, group, parent.position, parent.tranche()));
    }

    final <T extends Task> T submitPlainExclusive(Task parent, T task)
    {
        Invariants.require(isOwningThread());
        task.setStateExclusive(WAITING_TO_RUN);
        if (parent == null) registerExclusive(task);
        else registerConsequenceExclusive(parent, task);
        task.onLoaded();
        runnable.enqueue(task, true);
        return task;
    }

    private void registerConsequenceExclusive(Task parent, Task task)
    {
        ++tasks;
        int tranche = parent.tranche();
        tranches.addInherited(tranche, parent.position);
        task.position = parent.position;
        task.setInheritedWithTranche(tranche);
    }

    void registerExclusive(Task task)
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

    final void cleanupTaskExclusive(Task task, boolean executed)
    {
        runnable.cleanup(task);
        try { task.cleanupExclusive(this, executed); }
        finally
        {
            cache.tryShrinkOrEvict(lock);
            maybeUnpauseLoading();
        }
    }

    final void unregisterExclusive(Task task)
    {
        int tranch = task.tranche();
        --tasks;
        tranches.complete(tranch);
    }

    final void cancelExclusive(AccordTask<?> task)
    {
        State state = task.state();
        switch (state)
        {
            default: throw new UnhandledEnum(state);
            case SCANNING_RANGES:
            case LOADING_REQUIRED:
            case LOADING_OPTIONAL:
            case WAITING_ON_REQUIRED:
            case WAITING_ON_OPTIONAL:
            case WAITING_TO_RUN:
                if (!task.hasIncrementalStarted())
                {
                    task.unqueueIfQueued();
                    try { task.cancelExclusive(); }
                    finally { cleanupTaskExclusive(task, false); }
                    break;
                }
            case UNINITIALIZED: // TODO (expected): preferable to be able to cancel at this stage, even if unlikely to trigger at this phase
            case RUNNING:
            case INCOMPLETE:
            case EXECUTED:
            case CANCELLED:
            case FAILED_TO_LOAD:
                // cannot safely cancel
        }
    }

    void onScannedRangesExclusive(AccordTask<?> task, Throwable fail)
    {
        // the task may have already been cancelled, in which case we don't need to fail it
        if (task.state().isExecuted())
            return;

        if (fail != null) failExclusive(task, fail, FAILED_TO_LOAD);
        else task.rangeScanner().scannedExclusive();
    }

    private void failExclusive(AccordTask<?> task, Throwable fail, State newState)
    {
        if (task.state().isExecuted())
            return;

        try { task.failExclusive(fail, newState); }
        catch (Throwable t) { agent.onException(t); }
        finally
        {
            task.unqueueIfQueued();
            cleanupTaskExclusive(task, false);
        }
    }

    private <K, V> void onSavedExclusive(AccordCacheEntry<K, V, ?> state, Object identity, Throwable fail)
    {
        cache.saved(state, identity, fail);
    }

    private <K, V> void onLoadedExclusive(AccordCacheEntry<K, V, ?> loaded, V value, Throwable fail)
    {
        if (loaded.status() == EVICTED)
            return;

        try (ArrayBuffers.BufferList<AccordTask<?>> tasks = loaded.drainWaitingToLoad())
        {
            if (fail != null)
            {
                for (AccordTask<?> task : tasks)
                    failExclusive(task, fail, FAILED_TO_LOAD);
                cache.failedToLoad(loaded);
            }
            else
            {
                cache.loaded(loaded, value);
                for (AccordTask<?> task : tasks)
                    task.onLoadOneExclusive(loaded);
            }
        }

        maybePauseLoading();
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
            self.exitAccordLockedExecutor();
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
        private static final int WAITING_ON_OPTIONAL_BIT = 1 << 5;
        private static final int WAITING_TO_RUN_BIT = 1 << 6;
        private static final int INCOMPLETE_BIT = 1 << 9;

        enum State
        {
            UNINITIALIZED(),
            SCANNING_RANGES(UNINITIALIZED),
            LOADING_REQUIRED(UNINITIALIZED, SCANNING_RANGES),
            LOADING_OPTIONAL(UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED),
            WAITING_ON_REQUIRED(WAITING_ON_OPTIONAL_BIT | WAITING_TO_RUN_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
            WAITING_ON_OPTIONAL(WAITING_TO_RUN_BIT | INCOMPLETE_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED),
            WAITING_TO_RUN(INCOMPLETE_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL),
            RUNNING(WAITING_TO_RUN),
            EXECUTED(RUNNING),
            INCOMPLETE(RUNNING),
            FAILED_TO_LOAD(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
            FAILED_OTHER(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN),
            CANCELLED(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN, RUNNING),
            ;

            private final int permittedFrom;
            public static final int WAITING = TinyEnumSet.encode(WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN);
            public static final int WAITING_OR_RUNNING = WAITING | TinyEnumSet.encode(RUNNING);
            public static final int RUNNING_OR_EXECUTED = WAITING | TinyEnumSet.encode(RUNNING, EXECUTED);
            static final State[] VALUES = values();

            static
            {
                // hack to allow us to create loops in our enum transition declarations
                Invariants.require(INCOMPLETE_BIT == 1 << INCOMPLETE.ordinal());
            }

            State()
            {
                this.permittedFrom = 0;
            }

            State(State ... permittedFroms)
            {
                this(0, permittedFroms);
            }

            State(int additional, State ... permittedFroms)
            {
                int permittedFrom = additional;
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
                return this.compareTo(EXECUTED) >= 0;
            }

            boolean hasStarted()
            {
                return this.compareTo(RUNNING) >= 0;
            }

            static State forOrdinal(int ordinal)
            {
                return VALUES[ordinal];
            }
        }

        enum RunState
        {
            NONE, PERSISTING, SUCCESS, FAILED;

            private static final RunState[] VALUES = values();
            static RunState forOrdinal(int ordinal)
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
            RANGE_LOAD,
            RANGE_SCAN,
        }

        enum ExclusiveGroup
        {
            APPLY,
            STABLE,
            COMMIT,
            ACCEPT,
            OTHER,
            RECOVER,
            PREACCEPT,
            RANGE,
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
        private static final int EXCLUSIVE_GROUP_SHIFT = 4;
        private static final int GLOBAL_GROUP_SHIFT = 7;

        private static final int NONSYNC_BIT = 1 << 10;
        private static final int CACHE_QUEUED_BIT = 1 << 11;

        private static final int INCREMENTAL_MASK       = 0x3 << 12;
        private static final int INCREMENTAL            = 0x1 << 12;
        private static final int INCREMENTAL_STARTED    = 0x2 << 12;
        private static final int INCREMENTAL_FINISHING  = 0x3 << 12;

        private static final int SEQUENCED_SHIFT    = 14;
        private static final int SEQUENCED_MASK     = 0x3 << SEQUENCED_SHIFT;
        private static final int SEQUENCED_PRIORITY = 0x1 << SEQUENCED_SHIFT;
        private static final int SEQUENCED_ATOMIC   = 0x2 << SEQUENCED_SHIFT;
        private static final int SEQUENCED_ATOMIC_AND_QUEUED = 0x3 << SEQUENCED_SHIFT;

        // spare two bits

        private static final int HAS_TRANCHE_BIT = 1 << 18;
        private static final int HAS_INHERITED_BIT = 1 << 19;
        private static final int HAS_INHERITED_RANGE_SCAN_BIT = 1 << 20;

        private static final int TRANCHE_SHIFT = 22;
        static final int MAX_TRANCHE = 0x3ff;

        static
        {
            Invariants.require(SEQUENCED_PRIORITY == BY_PRIORITY.ordinal() << SEQUENCED_SHIFT);
            Invariants.require(SEQUENCED_ATOMIC == BY_PRIORITY_ATOMIC.ordinal() << SEQUENCED_SHIFT);
            Invariants.require(ExecutionSequence.values().length <= 3);
        }

        public final WithResources resources;
        Task next;

        long position;
        private int info;

        // TODO (expected): do we need this? we should be able to determine the queue from state() if needed for e.g. cancellation
        private TaskQueue queued;

        public final long createdAt;
        // TODO (expected): expose via executors vtable
        // TODO (expected): use just one long and some flag bits to indicate which point it represents, and report incrementally
        public long loadedAt, runningAt, completeAt;
        private byte runState;

        Task(GlobalGroup group)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            info = init(group, ExclusiveGroup.OTHER);
            createdAt = nanoTime();
        }

        Task(ExclusiveGroup group)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            info = init(GlobalGroup.OTHER, group);
            createdAt = nanoTime();
        }

        Task(GlobalGroup group, long position, int tranche)
        {
            this(group);
            this.position = position;
            setInheritedWithTranche(tranche);
        }

        Task(ExclusiveGroup group, long position, int tranche)
        {
            this(group);
            this.position = position;
            setInheritedWithTranche(tranche);
        }

        protected Task(ExecutionContext context, AtomicLong lastCreatedAt)
        {
            resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
            createdAt = lastCreatedAt.accumulateAndGet(nanoTime(), (prev, next) -> next < prev ? prev + 1 : next);
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
                            // TODO (expected): port to ExecutionKind; also we aren't consistent about using Ballot
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

            this.info = init(GlobalGroup.OTHER, group);
            if (txnId != null)
                this.position = txnId.hlc();
        }

        public final Task unwrap()
        {
            if (this instanceof ExclusiveExecutorTask)
                return ((ExclusiveExecutorTask) this).queue.task;
            return this;
        }

        static Task unwrap(Task task)
        {
            return task == null ? null : task.unwrap();
        }

        public DebuggableTask debuggable() { return null; }

        abstract String toDescription();

        abstract void submitExclusive(AccordExecutor owner);

        /**
         * Prepare to run while holding the state cache lock
         */
        void preRunExclusive() { setStateExclusive(RUNNING); }

        /**
         * Run the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract void run();

        /**
         * Fail the command; the state cache lock may or may not be held depending on the executor implementation
         */
        abstract void reportFailure(Throwable fail);

        final void failExclusive(Throwable fail, State newState)
        {
            try { setStateExclusive(newState); }
            finally { reportFailure(fail); }

        }

        final void failExecution(Throwable fail)
        {
            Invariants.require(is(RUNNING));
            try { setRunState(RunState.FAILED); }
            finally { reportFailure(fail); }

        }

        abstract boolean isNewWork();

        /**
         * Cleanup the command while holding the state cache lock
         */
        void cleanupExclusive(AccordExecutor executor, boolean executed)
        {
            if (executed) setStateExclusive(EXECUTED);
            else Invariants.require(state().isExecuted());
            executor.unregisterExclusive(this);
            completeAt = nanoTime();
            if (runningAt != 0)
            {
                if (loadedAt == 0)
                    loadedAt = runningAt;
                executor.elapsedWaitingToRun.increment(runningAt - loadedAt, runningAt);
                executor.elapsedPreparingToRun.increment(loadedAt - createdAt, runningAt);
                executor.elapsedRunning.increment(completeAt - runningAt, completeAt);
                executor.elapsed.increment(completeAt - createdAt, completeAt);
            }
            if (DEBUG_EXECUTION) DebugTask.get(this).onCompleted(executor.debug);
        }

        void cancelExclusive(AccordExecutor owner) {}

        @Nullable
        final TaskQueue<?> queued()
        {
            return queued;
        }

        final void unqueueIfQueued()
        {
            if (queued != null)
            {
                queued.unqueue(this);
                queued = null;
            }
        }

        final void unqueue(TaskQueue expected)
        {
            Invariants.require(queued == expected, "%s != %s", queued, expected);
            queued.unqueue(this);
            queued = null;
        }

        final void unsetQueue(TaskQueue<?> expected)
        {
            Invariants.require(queued == expected, "%s != %s", queued, expected);
            queued = null;
        }

        final void setQueue(TaskQueue<?> queue)
        {
            Invariants.require(queued == null);
            Invariants.require(isCompatible(queue));
            queued = queue;
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

        final void onLoaded()
        {
            loadedAt = nanoTime();
        }

        final State state()
        {
            return State.forOrdinal(stateOrdinal());
        }

        final RunState runState()
        {
            return RunState.forOrdinal(runState);
        }

        final Enum<?> describeState()
        {
            State state = state();
            if (state == RUNNING || state == EXECUTED)
            {
                RunState runState = runState();
                if (runState == RunState.NONE)
                    return state;
                return runState;
            }
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

        final boolean isState(int stateBitSet)
        {
            return TinyEnumSet.contains(stateBitSet, stateOrdinal());
        }

        final boolean is(GlobalGroup group)
        {
            return globalGroupOrdinal() == group.ordinal();
        }

        final boolean is(ExclusiveGroup group)
        {
            return exclusiveGroupOrdinal() == group.ordinal();
        }

        final void override(GlobalGroup group)
        {
            info = (info & ~(GROUP_MASK << GLOBAL_GROUP_SHIFT)) | (group.ordinal() << GLOBAL_GROUP_SHIFT);
        }

        final int compareTo(State state)
        {
            return stateOrdinal() - state.ordinal();
        }

        final void setStateExclusive(State state)
        {
            Invariants.require(state.isPermittedFrom(stateOrdinal()), "%s forbidden from %s", state, this, Task::reportBadStateTransition);
            setStateUnsafe(state);
        }

        final void setRunState(RunState state)
        {
            Invariants.require(isState(RUNNING_OR_EXECUTED));
            runState = (byte) state.ordinal();
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

        private boolean isCompatible(TaskQueue<?> queue)
        {
            int self = stateOrdinal();
            return TinyEnumSet.contains(queue.states, self);
        }

        final boolean isSync()
        {
            return 0 == (info & NONSYNC_BIT);
        }

        final boolean isNonSync()
        {
            return !isSync();
        }

        final void setNonSyncExclusive()
        {
            info |= NONSYNC_BIT;
        }

        final boolean isIncremental()
        {
            return 0 != (info & INCREMENTAL_MASK);
        }

        final void setIncrementalExclusive()
        {
            info |= INCREMENTAL | NONSYNC_BIT;
        }

        final boolean hasIncrementalStarted()
        {
            return (info & INCREMENTAL_MASK) >= INCREMENTAL_STARTED;
        }

        final void setIncrementalStartedExclusive()
        {
            Invariants.require(isIncremental());
            if (!isIncrementalFinishing())
                info = (info & ~INCREMENTAL_MASK) | INCREMENTAL_STARTED;
        }

        final boolean isIncrementalFinishing()
        {
            return (info & INCREMENTAL_MASK) >= INCREMENTAL_FINISHING;
        }

        final void setIncrementalFinishingExclusive()
        {
            Invariants.require(isIncremental());
            info |= INCREMENTAL_FINISHING;
        }

        final void setSequencedExclusive(ExecutionSequence sequence)
        {
            Invariants.require(isUnsequenced());
            info |= sequence.ordinal() << SEQUENCED_SHIFT;
        }

        final boolean isUnsequenced()
        {
            return (info & SEQUENCED_MASK) == 0;
        }

        final boolean isSequencedByPriority()
        {
            return (info & SEQUENCED_MASK) == SEQUENCED_PRIORITY;
        }

        final boolean isSequencedByPriorityAtomic()
        {
            return (info & SEQUENCED_MASK) >= SEQUENCED_ATOMIC;
        }

        final boolean isCacheQueuedFifo()
        {
            return (info & SEQUENCED_MASK) == SEQUENCED_ATOMIC_AND_QUEUED;
        }

        final boolean isCacheQueued()
        {
            return 0 != (info & CACHE_QUEUED_BIT);
        }

        // supersedes priority, in whichever order they're called
        final void setCacheQueuedFifoExclusive()
        {
            Invariants.require(isSequencedByPriorityAtomic());
            info |= SEQUENCED_ATOMIC_AND_QUEUED | CACHE_QUEUED_BIT;
        }

        final void setCacheQueuedExclusive()
        {
            info |= CACHE_QUEUED_BIT;
        }

        final int tranche()
        {
            Invariants.require((info & HAS_TRANCHE_BIT) != 0);
            return info >>> TRANCHE_SHIFT;
        }

        final void setTranche(int tranche)
        {
            Invariants.require(tranche <= MAX_TRANCHE);
            info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT;
        }

        final void setInheritedWithTranche(int tranche)
        {
            Invariants.require(tranche <= MAX_TRANCHE);
            info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT | HAS_INHERITED_BIT;
        }

        final boolean hasInherited()
        {
            return (info & HAS_INHERITED_BIT) != 0;
        }

        final void setInheritedRangeScan()
        {
            info = info | HAS_INHERITED_RANGE_SCAN_BIT;
        }

        final boolean hasInheritedRangeScan()
        {
            return (info & HAS_INHERITED_RANGE_SCAN_BIT) != 0;
        }

        static int init(GlobalGroup global, ExclusiveGroup exclusive)
        {
            return (global.ordinal() << GLOBAL_GROUP_SHIFT) | (exclusive.ordinal() << EXCLUSIVE_GROUP_SHIFT);
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
            super(COMMAND_STORE);
            this.queue = queue;
        }

        @Override
        String toDescription()
        {
            return queue.task.toDescription();
        }

        @Override void submitExclusive(AccordExecutor owner) { throw new UnsupportedOperationException(); }

        @Override
        void preRunExclusive()
        {
            queue.preRunTask();
        }

        @Override
        void run()
        {
            queue.runTask();
        }

        @Override
        void reportFailure(Throwable t)
        {
            queue.task.reportFailure(t);
        }

        @Override
        boolean isNewWork()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        void cleanupExclusive(AccordExecutor executor, boolean executed)
        {
            queue.cleanupTask(executed);
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
            super(RUNNABLE, commandStoreId < 0 ? GroupKind.NONE : GroupKind.EXCLUSIVE, EXCLUSIVE_QUEUE_LIMITS);
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
                task.reportFailure(new RejectedExecutionException(commandStoreId + " is terminated. Cannot execute " + ((AccordTask<?>) task).executionContext()));
            else
                task.run();

            // NOTE: we can ONLY safely release owner here due to AccordCacheEntry locking, which remains in place until AccordTask.releaseResourcesExclusive
            //       this also relies on AccordSafeCommandStore$ExclusiveCaches.acquireIfLoaded returning false when the entry is locked
            owner = null;
        }

        private boolean reject(Task task)
        {
            if (!(task instanceof AccordTask<?>))
                return true;

            ExecutionContext context = ((AccordTask<?>) task).executionContext();
            return !(terminated ? (context instanceof Unterminatable) : (context instanceof Unstoppable));
        }

        void cleanupTask(boolean executed)
        {
            try
            {
                task.unsetQueue(this);
                task.cleanupExclusive(AccordExecutor.this, executed);
            }
            finally
            {
                active = 0;
                task = super.pollMulti();
                if (DEBUG_EXECUTION) debug.onSetTask(task);
                if (task != null)
                {
                    selfTask.position = task.position;
                    selfTask.setStateUnsafe(WAITING_TO_RUN);
                    runnable.enqueue(selfTask, false);
                }
            }
        }

        void enqueue(Task newTask, boolean incrementArrivals)
        {

            if (task != null)
            {
                if (incrementArrivals)
                    runnable.incrementArrivals(selfTask);
                // TODO (expected): restore some invariant here
//                Invariants.require(selfTask.isInHeap() || selfTask.is(RUNNING));
                super.enqueueMulti(newTask, incrementArrivals);
            }
            else
            {
                Invariants.require(isEmptySingle());
                if (incrementArrivals)
                    incrementArrivals(newTask);
                incrementDispatches(newTask);
                task = newTask;
                task.setQueue(this);
                selfTask.position = newTask.position;
                selfTask.setStateUnsafe(WAITING_TO_RUN);
                runnable.enqueue(selfTask, incrementArrivals);
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
                    runnable.enqueue(selfTask, false);
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
                return Task.unwrap(task);
            return Task.unwrap(AccordTaskRunner.get(thread).accordActiveSelfTask());
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

            if (owner == null && !ownerUpdater.compareAndSet(this, null, thread))
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

        @Override
        public String toString()
        {
            return TinyEnumSet.toString(states, State::forOrdinal);
        }
    }

    static final class StandaloneTaskQueue<T extends Task> extends TaskQueue<T>
    {
        StandaloneTaskQueue(int states)
        {
            super(states);
        }

        StandaloneTaskQueue(State state)
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
        final long limits;

        /** sets overflow bits for a queue that has been stopped */
        long stopped;
        /** sets overflow bits for each counter when it needs its position updated */
        long dirty;
        /** sets overflow bits for each counter when there's associated work */
        long hasWork;
        /** Stores recent dequeue counts for up to 8 sub queues. */
        long dispatches;
        // TODO (required): increment arrivals based on internal queue for ExclusiveExecutors
        //    also: experiment with decaying on arrival schedule rather than poll schedule, since this should respond to work growth more accurately
        /** Stores recent enqueue counts for up to 8 sub queues. */
        long arrivals;
        /** Stores currently-active counts for up to 8 sub queues. We can use this to impose limits on specific queues. */
        long active;
        /** deficit-round-robin credits for the two PRIORITY_FAIR strategies (flow/age). */
        int creditFlow, creditAge;

        int waitingCount;

        MultiTaskQueue(int waitingStates, GroupKind groups, long limits)
        {
            super(waitingStates);
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

        void stop(int group)
        {
            stopped |= overflowBit(group);
        }

        void restart(int group)
        {
            stopped &= ~overflowBit(group);
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
                case PHASE_FAIR: return pollGroupByPhaseFair();
                case BLENDED_PRIORITY_PHASE_FAIR: return pollGroupByBlended();
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

        private int pollGroupByPhaseFair()
        {
            return minCounterIndex(recentFlowImbalances());
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
            return hasWork & unsaturated() & ~stopped;
        }

        private long saturatedOrWithoutWork()
        {
            return (hasWork ^ COUNTER_OVERFLOWS) | saturated() | stopped;
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

            TaskQueue<T> queue = queues[group];
            T head = queue.pollSingle();
            // NOTE: must clear dirty when emptied, symmetrically with unqueue(): the fair selection paths
            // never consume the dirty bit, so a group drained during a fairness episode would otherwise
            // retain a stale dirty bit and NPE in minGroupByPriority.peekSingle() when balance is restored.
            if (queue.isEmptySingle()) { unsetHasWork(group); unsetDirty(group); }
            else setDirty(group);
            return head;
        }

        final void enqueueMulti(T task, boolean incrementArrivals)
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
                if (incrementArrivals)
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

        final void incrementArrivals(Task task)
        {
            int group = group(task);
            if (group >= 0)
                incrementArrivals(group);
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

        final boolean hasWaitingToRun()
        {
            return unsaturatedWithWork() != 0;
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
        static final int RUNNABLE = TinyEnumSet.encode(WAITING_TO_RUN, RUNNING);

        final TaskQueue<T> assigned;

        RunnableTaskQueue()
        {
            super(RUNNABLE, GroupKind.GLOBAL, GLOBAL_QUEUE_LIMITS);
            this.assigned = new TaskQueue<>(0);
        }

        T poll()
        {
            T next = pollMulti();
            if (next == null)
                return null;

            assigned.enqueueSingle(next);
            return next;
        }

        void enqueue(T enqueue, boolean incrementArrivals)
        {
            enqueueMulti(enqueue, incrementArrivals);
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

        void cleanup(T task)
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
            super(GlobalGroup.OTHER);
            this.cancel = cancel;
        }

        @Override void submitExclusive(AccordExecutor owner) { cancel.cancelExclusive(owner); }
        @Override void preRunExclusive() { throw new UnsupportedOperationException(); }
        @Override void run() { throw new UnsupportedOperationException(); }
        @Override void reportFailure(Throwable fail) { throw new UnsupportedOperationException(); }
        @Override boolean isNewWork() { return false; }
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
            super(group, position, tranche);
        }

        Plain(ExclusiveGroup group, long position, int tranche)
        {
            super(group, position, tranche);
        }

        Plain(GlobalGroup group)
        {
            super(group);
        }

        Plain(ExclusiveGroup group)
        {
            super(group);
        }

        abstract ExclusiveExecutor executor();

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
                try { failExclusive(new CancellationException(), CANCELLED); }
                catch (Throwable t) { agent.onException(t); }
                finally { cleanupTaskExclusive(this, false); }
            }
        }

        @Override
        final void submitExclusive(AccordExecutor owner)
        {
            setStateExclusive(WAITING_TO_RUN);
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
        protected void reportFailure(Throwable t)
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
    public abstract class IOTask extends Plain implements Cancellable, DebuggableTask
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
        void cleanupExclusive(AccordExecutor executor, boolean executed)
        {
            super.cleanupExclusive(executor, executed);
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

    <K, V> LoadRunnable<K, V> newLoad(AccordCacheEntry<K, V, ?> entry, boolean isForRange)
    {
        return new LoadRunnable<>(entry, isForRange ? RANGE_LOAD : LOAD);
    }

    public class LoadRunnable<K, V> extends IOTask
    {
        final AccordCacheEntry<K, V, ?> entry;
        Object result = FailureHolder.NOT_STARTED;

        LoadRunnable(AccordCacheEntry<K, V, ?> entry, GlobalGroup group)
        {
            super(group);
            Invariants.require(group == LOAD || group == RANGE_LOAD);
            this.entry = entry;
        }

        void postRunExclusive()
        {
            if (!(result instanceof FailureHolder)) onLoadedExclusive(entry, (V)result, null);
            else onLoadedExclusive(entry, null, ((FailureHolder)result).fail);
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
        void reportFailure(Throwable t)
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
        abstract void runInternal();
        abstract void postRunExclusive();
        abstract void fail(Throwable t);
        abstract String description();
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
        protected void reportFailure(Throwable fail)
        {
            wrapped.fail(fail);
        }
    }

    private static final Throwable NOT_STARTED = new Throwable();
    class SaveRunnable extends IOTask
    {
        final AccordCacheEntry<?, ?, ?> entry;
        final UniqueSave identity;
        final Runnable run;
        Throwable failure = NOT_STARTED;

        SaveRunnable(AccordCacheEntry<?, ?, ?> entry, UniqueSave identity, Runnable run)
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
        protected void reportFailure(Throwable t)
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
        protected void reportFailure(Throwable fail)
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
        return loading.size() + scanningRanges.size();
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
