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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Journal;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
import accord.local.ExecutionContext;
import accord.local.LoadKeys;
import accord.local.SafeCommandStore;
import accord.local.SafeState;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.Invariants.Paranoia;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.RangeIndex;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Loading;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.local.LoadKeys.INCR;
import static accord.local.LoadKeys.NONE;
import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.RECOVERY;
import static accord.local.LoadKeysFor.WRITE;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.Paranoia.SUPERLINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.isParanoid;
import static accord.utils.Invariants.testParanoia;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask.SANITY_CHECK;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.HOLD_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.RELEASE_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_BLOCKING_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NEWLY_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.CACHE_QUEUES_ENABLED;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_BLOCKED_LIMIT;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_ENABLED;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_MIN_BATCH_SIZE;
import static org.apache.cassandra.service.accord.execution.SaferState.global;
import static org.apache.cassandra.service.accord.execution.SaferState.postExecute;
import static org.apache.cassandra.service.accord.execution.SaferState.preExecute;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_LOAD;
import static org.apache.cassandra.service.accord.execution.Task.RunState.NOT_YET_RUN;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_PERSISTING;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED_UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OR_WAITING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARING;
import static org.apache.cassandra.service.accord.execution.Task.State.REGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.RUNNING_WHILE_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.execution.Task.State.UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_KEY;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_TXN;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_OR_RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public final class SafeTask<R> extends Task implements Cancellable, DebuggableTask
{
    private static final Logger logger = LoggerFactory.getLogger(SafeTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    static final int WAITING_FOR_TXN_INCR = 0x40000000;
    static final int WAITING_FOR_KEY_MASK = 0x3fffffff;

    public static SafeTask<Void> create(CommandStore commandStore, ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return new SafeTask<>((AccordCommandStore) commandStore, context, safeStore -> {
            consumer.accept(safeStore);
            return null;
        });
    }

    public static <R> SafeTask<R> create(CommandStore commandStore, ExecutionContext context, Function<? super SafeCommandStore, R> function)
    {
        return new SafeTask<>((AccordCommandStore) commandStore, context, function);
    }

    static class NonSyncState extends ExecutionContext.Wrapped
    {
        RoutingKeys active;
        ObjectHashSet<RoutingKey> blocking, notBlocking;
        int loaded, processed;
        boolean alwaysReady;

        public NonSyncState(ExecutionContext context)
        {
            super(context);
        }

        @Override
        public final Unseekables<?> keys()
        {
            return active;
        }

        final void addLoaded()
        {
            ++loaded;
        }

        final void onNotHead(AccordCacheEntry<?, ?, ?> entry)
        {
            if ((notBlocking == null || !notBlocking.remove((RoutingKey) entry.key())) && blocking != null)
                blocking.remove((RoutingKey) entry.key());
        }

        final void onNewHead(AccordCacheEntry<?, ?, ?> entry)
        {
            ensureNotBlocking().add((RoutingKey) entry.key());
            Invariants.paranoid(blocking == null || !blocking.contains(entry.key()));
        }

        final void onNewBlockingHead(AccordCacheEntry<?, ?, ?> entry)
        {
            ensureBlocking().add((RoutingKey) entry.key());
            Invariants.paranoid(notBlocking == null || !notBlocking.contains(entry.key()));
        }

        final void onStillHeadNewBlocking(AccordCacheEntry<?, ?, ?> entry)
        {
            // we are only told this if we lead the entry, but we may not have recorded it as ready - a task keeps its
            // place in the queues of keys it has not yet processed - so promote only what we consider not blocking
            if (notBlocking != null && notBlocking.remove((RoutingKey) entry.key()))
                ensureBlocking().add((RoutingKey) entry.key());
        }

        private ObjectHashSet<RoutingKey> ensureBlocking()
        {
            if (blocking == null)
                blocking = new ObjectHashSet<>();
            return blocking;
        }

        private ObjectHashSet<RoutingKey> ensureNotBlocking()
        {
            if (notBlocking == null)
                notBlocking = new ObjectHashSet<>();
            return notBlocking;
        }

        private int readyCount()
        {
            return (blocking == null ? 0 : blocking.size()) + (notBlocking == null ? 0 : notBlocking.size());
        }

        final boolean isLoaded(SafeTask<?> owner)
        {
            return loaded >= Math.min(owner.keys, alwaysReady ? 1 : NONSYNC_MIN_BATCH_SIZE);
        }

        final boolean isWaitReady(SafeTask<?> owner)
        {
            if (readyCount() >= Math.min(owner.keys - processed, alwaysReady ? 1 : NONSYNC_MIN_BATCH_SIZE))
                return true;

            return blocking != null && blocking.size() >= NONSYNC_BLOCKED_LIMIT;
        }

        void prepareExclusive(SafeTask<?> owner)
        {
            try (BufferList<RoutingKey> keys = new BufferList<>();
                 BufferList<RoutingKey> locked = new BufferList<>())
            {
                if ((blocking == null || !populate(keys, blocking)) && notBlocking != null)
                    populate(keys, notBlocking);

                // the previous batch's locks must have been released before we take the next
                Invariants.require(active == null);
                // Taking a lock notifies the entry's new head, and that notification can re-enter the queues and revoke
                // a key we captured above but have not locked yet (e.g. a started task hoisted ahead of us to break a
                // lock cycle). The revocation cannot reach us via blocking/notBlocking as populate has drained them, so
                // we re-check each key and leave any we no longer lead for a later batch, keeping our position on it.
                for (RoutingKey key : keys)
                {
                    SafeState<?> safeState = owner.refs.get(key);
                    if (global(safeState).statusIfPresent(owner) == NOT_RUNNABLE)
                        continue;

                    preExecute(safeState, owner, RELEASE_QUEUE);
                    locked.add(key);
                }
                locked.sort(RoutingKey::compareTo);
                active = RoutingKeys.of(locked);
                Invariants.require(active.size() == locked.size());
            }
            processed += active.size();
            if (processed == owner.keys && owner.isIncremental())
                owner.setIncrementalFinishingExclusive();
        }

        private boolean populate(List<RoutingKey> keys, ObjectHashSet<RoutingKey> from)
        {
            if (keys.size() + from.size() <= AccordExecutor.NONSYNC_MAX_BATCH_SIZE)
            {
                keys.addAll(from);
                from.clear();
                return keys.size() == AccordExecutor.NONSYNC_MAX_BATCH_SIZE;
            }

            Iterator<RoutingKey> iterator = from.iterator();
            while (iterator.hasNext())
            {
                if (keys.size() == AccordExecutor.NONSYNC_MAX_BATCH_SIZE)
                    return true;

                RoutingKey key = iterator.next();
                keys.add(key);
                iterator.remove();
            }

            return false;
        }

        void postRunExclusive(SafeTask<?> owner)
        {
            if (active != null)
            {
                for (RoutingKey key : active)
                {
                    SafeState<?> safeState = owner.refs.remove(key);
                    Invariants.require(safeState != null);
                    Invariants.require(!safeState.isReleased());
                    AccordCacheEntry<?, ?, ?> entry = global(safeState);
                    postExecute(safeState, owner);
                    Invariants.require(!owner.refs.containsKey(key));
                    Invariants.require(!entry.isLockedBy(owner));
                }
                active = null;
            }
        }
    }

    final AccordCommandStore commandStore;
    private final ExecutionContext context;
    private final Function<? super SafeCommandStore, R> function;

    // TODO (expected): simple custom map that allows (at least):
    //   - efficient putIfAbsent
    //   - efficient small collections (2-4 entries)
    //   - forEach with parameters to avoid boxing lambdas
    //   - destructive forEach
    //   - forEach over specific SafeState types
    Object2ObjectHashMap<Object, SafeState<?>> refs = new Object2ObjectHashMap<>();

    /**
     * if is(LOADING), this is the number of cache entries we're waiting to complete loading before we can transition to WAITING_ON_CACHE_QUEUES;
     * if is(WAITING_ON_CACHE_QUEUES), it's the number we're waiting to be head of before we can run
     * <p>
     * if isNonSync(), this counts only txnId; otherwise it counts keys and txnId
     */
    int waitingFor;

    /**
     * Only set when isNonSync()
     * <p>
     * if is(LOADING), this is the cache entries that have finished loading
     * otherwise it's the cache entries for which we're at the head of the queue and are ready to run with
     */
    @Nullable NonSyncState nonSync;

    int keys; // TODO (expected): not counting keys we add during execution

    LogLinearDecayingHistograms.Buffer histogramBuffer;

    @Nullable Object ranges;
    long waitingAt;
    long fifoAt; // TODO (expected): should not consume memory for this for all tasks (make NonSyncState OptionalState

    private volatile BiConsumer<? super R, Throwable> callback;
    private static final AtomicReferenceFieldUpdater<SafeTask, BiConsumer> callbackUpdater = AtomicReferenceFieldUpdater.newUpdater(SafeTask.class, BiConsumer.class, "callback");

    public SafeTask(@Nonnull AccordCommandStore commandStore, ExecutionContext context, Function<? super SafeCommandStore, R> function)
    {
        this(commandStore, context, function, commandStore.executor().uniqueCreatedAt);
    }

    /**
     * Takes its createdAt counter directly, for tests that need a task without a store behind it.
     * createdAt must be unique within a store, as compare() relies on it to order two tasks that share an entry.
     */
    @VisibleForTesting
    SafeTask(AccordCommandStore commandStore, ExecutionContext context, Function<? super SafeCommandStore, R> function, AtomicLong uniqueCreatedAt)
    {
        super(context, uniqueCreatedAt);
        this.commandStore = commandStore;
        this.context = context;
        this.function = function;
        if (logger.isTraceEnabled())
            logger.trace("Created {} on {}", this, commandStore);
    }

    String loggingId()
    {
        return executor().executorId + "/" + Long.toHexString(createdAt);
    }

    @Override
    public String toString()
    {
        return "@[" + commandStore.id() + ',' + commandStore.node().id() + "] " + context.describe() + ' ' + toBriefString();
    }

    public String toBriefString()
    {
        return '{' + loggingId() + ',' + currentState() + '}';
    }

    public String summarise()
    {
        return loggingId() + ' ' + context.executionKind()
               + ": primaryTxnId: " + context.primaryTxnId()
               + ", state: " + summarise(refs, SaferState::global);
    }

    private static <V> String summarise(Map<?, V> map, Function<V, Object> transform)
    {
        if (map == null)
            return "null";

        return summarise(map.values(), transform);
    }

    private static <V> String summarise(Collection<V> collection)
    {
        return summarise(collection, Function.identity());
    }

    private static <V> String summarise(Collection<V> collection, Function<? super V, Object> transform)
    {
        if (collection == null)
            return "null";

        StringBuilder out = new StringBuilder("[");
        int count = 0;
        for (V v : collection)
        {
            if (count++ > 0)
            {
                out.append(',');
                if (count >= 10)
                {
                    out.append("...(*").append(collection.size() - 10).append(')');
                    break;
                }
            }
            out.append(transform.apply(v));
        }
        out.append(']');
        return out.toString();
    }

    // TODO (expected): try to execute immediately BUT consider ordering requirements
    //  esp. with deferred actions on e.g. CommandsForKey (not yet supported but also important for performance)
    public AsyncChain<R> chain()
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super R, Throwable> callback)
            {
                if (!preSetup(callback))
                    executor().submitTask(SafeTask.this);
                return SafeTask.this;
            }
        };
    }

    private boolean preSetup(BiConsumer<? super R, Throwable> callback)
    {
        Invariants.require(this.callback == null);
        callbackUpdater.lazySet(this, callback);
        Task inherit = executor().inherit();
        if (inherit == null)
            return false;

        if (!Invariants.expect(inherit.is(PREPARED) || inherit.is(RUNNING_WHILE_FAILED), "%s %s task attempted preSetup of consequence", state(), briefDescription()))
            return false;

        if (!inherit.is(RUNNING))
            return false;

        if (inherit instanceof SafeTask<?>)
        {
            SafeTask<?> inheritRefs = (SafeTask<?>) inherit;
            if (inheritRefs.commandStore == commandStore)
                preSetup(inheritRefs);
        }
        inherit.addConsequence(this);
        return true;
    }

    // to be invoked only by the CommandStore owning thread, to take references to objects already in use by the current execution
    private void preSetup(SafeTask<?> parent)
    {
        setHasPreSetupExclusive();
        // note we use the caches "unsafely" here deliberately, as we only reference commands we already have references to
        // so we do not mutate anything, except the atomic counter of references
        LoadKeys loadKeys = loadKeys(context);
        setSequencedExclusive(context.executionSequence());
        if (loadKeys != NONE)
        {
            Unseekables<?> parentKeysOrRanges = parent.context.keys();
            Unseekables<?> keysOrRanges = context.keys();

            boolean isKeySubset = parent.isIncremental() ? parent.nonSync().active.containsAll(keysOrRanges) : parentKeysOrRanges.containsAll(keysOrRanges);
            if (isKeySubset)
            {
                setInheritedRangeScan();
                boolean needsCfr = keysOrRanges.domain() == Key ? context.loadKeysFor() == RECOVERY : context.loadKeysFor() != WRITE;
                if (needsCfr)
                    ranges = parent.ranges;
            }

            if (loadKeys != SYNC)
            {
                nonSync = new NonSyncState(context);
                setNonSyncExclusive();
                if (loadKeys == INCR)
                {
                    setIncrementalExclusive();
                    requireSequencedIfHoldsLocksBetweenRuns();
                }
            }

            if (isSequencedByPriorityAtomic() && !isKeySubset)
            {
                Invariants.require(keysOrRanges.domain() == Key, "ATOMIC tasks over ranges must declare a subset of their parent's task keys() to avoid a range scan across which we would not impose sequencing");
                // TODO (expected): explain the scenario in which priority inversion deadlocks could occur
                // to avoid priority inversion deadlocks, if we are not a strict subset of the parent task we permit running with a single key ready
                if (isNonSync()) nonSync().alwaysReady = true;
                else
                {
                    // we cannot be alwaysReady for sync tasks, we must reject this submission entirely
                    // this restriction exists because we impose ATOMIC across the complete chain of tasks; if a consequence can have its own separate isolation guarantee we can easily introduce a suitable enum to request it, and permit this execution
                    throw new UnsupportedOperationException("ATOMIC SYNC tasks must declare a subset of their parent's task keys() to avoid a priority inversion deadlock with inconsistent lock acquisition order");
                }
            }

            if (keysOrRanges.equals(parentKeysOrRanges))
            {
                // TODO (desired): custom map we can more cheaply fork/copy
                parent.refs.forEach((key, val) -> {
                    if (val instanceof SaferCommandsForKey)
                        preSetup((RoutingKey) key, parent.refs, commandStore.cachesUnsafe().commandsForKeys());
                });
            }
            else
            {
                switch (keysOrRanges.domain())
                {
                    case Key:
                        for (RoutingKey key : (AbstractUnseekableKeys) keysOrRanges)
                            preSetup(key, parent.refs, commandStore.cachesUnsafe().commandsForKeys());
                        break;

                    case Range:
                        AbstractRanges ranges = (AbstractRanges) keysOrRanges;
                        parent.refs.forEach((key, val) -> {
                            if (val instanceof SaferCommandsForKey && ranges.contains((RoutingKey) key))
                                preSetup((RoutingKey) key, parent.refs, commandStore.cachesUnsafe().commandsForKeys());
                        });
                        break;
                }
            }
        }

        if (isSequencedByPriorityAtomic())
        {
            TxnId primaryTxnId = context.primaryTxnId();
            if (primaryTxnId != null)
            {
                SaferCommand primary = parent.refs == null ? null : (SaferCommand) parent.refs.get(primaryTxnId);
                Invariants.require(primary != null && primary.global().isLockedBy(parent), "ATOMIC tasks must declare a subset of their parent's task txnIds() to avoid a priority inversion deadlock");
                TxnId additionalTxnId = context.additionalTxnId();
                if (additionalTxnId != null)
                {
                    SaferCommand additional = parent.refs == null ? null : (SaferCommand) parent.refs.get(additionalTxnId);
                    Invariants.require(additional != null && additional.global().isLockedBy(parent), "ATOMIC tasks must declare a subset of their parent's task txnIds() to avoid a priority inversion deadlock");
                }
            }

            if (parent.fifoAt > 0) fifoAt = parent.fifoAt;
            else fifoAt = executor().uniqueCreatedAt.incrementAndGet();
            // a fifo position is a cache queue position: with the queues disabled there is nowhere to take one, and
            // no ordering to provide with it
            if (CACHE_QUEUES_ENABLED)
                setCacheQueuedFifoExclusive();
        }

        for (TxnId txnId : context.txnIds())
            preSetup(txnId, parent.refs, commandStore.cachesUnsafe().commands());
    }

    @Override
    void submitExclusiveMayThrow()
    {
        Caches caches = commandStore.cachesExclusive();
        executor().registerExclusive(this);
        setStateExclusive(REGISTERED);
        boolean hasPreSetup = hasPreSetup();

        if (!hasPreSetup)
            setSequencedExclusive(context.executionSequence());
        else
            Invariants.require(isSequencedBy(context.executionSequence()), "%s was presetup with a region other than %s", this, context.executionSequence());

        LoadKeys loadKeys = loadKeys(context);
        if (loadKeys != NONE)
        {
            if (loadKeys != SYNC && !hasPreSetup)
            {
                nonSync = new NonSyncState(context);
                setNonSyncExclusive();
                if (loadKeys == INCR)
                {
                    setIncrementalExclusive();
                    requireSequencedIfHoldsLocksBetweenRuns();
                }
            }

            if (isParanoid() && isCacheQueuedFifo())
            {
                TxnId primaryTxnId = context.primaryTxnId();
                if (primaryTxnId != null)
                {
                    SaferCommand primary = (SaferCommand) refs.get(primaryTxnId);
                    SafeTask<?> lockedBy = primary.global().lockedBy();
                    Invariants.require(lockedBy != null && lockedBy.position == position && (!lockedBy.isCacheQueuedFifo() || lockedBy.fifoAt == fifoAt));
                    TxnId additionalTxnId = context.additionalTxnId();
                    if (additionalTxnId != null)
                    {
                        SaferCommand additional = (SaferCommand) refs.get(additionalTxnId);
                        Invariants.require(additional.global().isLockedBy(lockedBy));
                    }
                }
            }

            Unseekables<?> keysOrRanges = context.keys();
            switch (keysOrRanges.domain())
            {
                case Range:
                    if (!hasInheritedRangeScan()) setupRangeLoadsExclusive(caches);
                    else refs.forEach((k, v) -> {
                        if (v instanceof SaferCommandsForKey)
                            completePresetupExclusive((SaferCommandsForKey)v, isSync() ? 1 : 0);
                    });
                    break;
                case Key:
                    setupKeyLoadsExclusive(hasPreSetup, caches, (AbstractUnseekableKeys) keysOrRanges, hasInheritedRangeScan());
                    break;
            }
        }

        if (is(SCANNING_RANGES))
            return;

        onSetupOrScannedExclusive(caches);
    }

    private void setupKeyLoadsExclusive(boolean hasPreSetup, Caches caches, Iterable<? extends RoutingKey> setupKeys, boolean doNotScanRanges)
    {
        if (context.loadKeys() == NONE)
            return;

        if (!doNotScanRanges && context.loadKeysFor() == RECOVERY)
        {
            Invariants.require(ranges == null);
            RangeTxnScanner scanner = new RangeTxnScanner();
            ranges = scanner;
            scanner.start();
        }

        int waitsForIncrement = isSync() ? 1 : 0;
        for (RoutingKey setupKey : setupKeys)
        {
            if (hasPreSetup && tryCompletePresetupExclusive(setupKey, waitsForIncrement))
                continue;

            setupExclusive(setupKey, caches.commandsForKeys(), waitsForIncrement);
        }
    }

    private void setupRangeLoadsExclusive(Caches caches)
    {
        if (context.loadKeysFor() == WRITE)
            return;

        RangeTxnAndKeyScanner scanner = new RangeTxnAndKeyScanner(caches.commandsForKeys());
        ranges = scanner;
        scanner.start();
    }

    // expects mutual exclusivity only on the command store
    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> void preSetup(K k, Map<Object, SafeState<?>> parentMap, AccordCache.Type<K, V, S>.Instance cache)
    {
        S ref = (S) parentMap.get(k);
        if (ref == null)
            return;

        AccordCacheEntry<K, V, S> node = ref.global();
        int refs = node.increment();
        Invariants.require(refs > 1);
        S safeState = cache.parent().adapter().safeRef(node);
        this.refs.put(k, safeState);
        if (cache.isCommandsForKey())
            keys++;
    }

    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> boolean tryCompletePresetupExclusive(K k, int waitForIncrement)
    {
        S preacquired = (S) refs.get(k);
        if (preacquired != null)
        {
            completePresetupExclusive(preacquired, waitForIncrement);
            return true;
        }
        return false;
    }

    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> void completePresetupExclusive(S preacquired, int waitForIncrement)
    {
        AccordCacheEntry<K, V, S> entry = preacquired.global();
        if (entry.isLoaded()) completeSetupOfLoaded(entry);
        else
        {
            // as setupExclusive: a reference we inherited from the task that submitted us may still be loading, and we
            // must account for waiting on it, or we proceed as though everything we require were loaded
            waitingFor += waitForIncrement;
            completeSetupOfLoading(entry, true);
        }
    }

    // expects to hold lock
    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> void setupExclusive(K k, AccordCache.Type<K, V, S>.Instance cache, int waitForIncrement)
    {
        S safeRef = cache.acquire(k);
        AccordCacheEntry<K, V, ?> entry = safeRef.global();
        boolean submitLoad = false;
        boolean isLoaded;
        try
        {
            Status entryStatus = entry.status();
            switch (entryStatus)
            {
                default:
                    throw new UnhandledEnum(entryStatus);
                case FAILED_TO_LOAD:
                    throw new RuntimeException("Failed to load " + safeRef.global().key());
                case WAITING_TO_LOAD:
                    submitLoad = true;
                case LOADING:
                    isLoaded = false;
                    waitingFor += waitForIncrement;
                    break;
                case WAITING_TO_SAVE:
                case SAVING:
                case LOADED:
                case MODIFIED:
                case FAILED_TO_SAVE:
                    isLoaded = true;
            }
        }
        catch (Throwable t)
        {
            safeRef.setAbandoned();
            safeRef.global().owner.release(safeRef, this);
            throw t;
        }

        Object prev = refs.putIfAbsent(k, safeRef);
        if (prev != null)
        {
            noSpamLogger.warn("ExecutionContext {} contained key {} more than once", refs, k);
            cache.release(safeRef, this);
            if (!isLoaded)
                waitingFor -= waitForIncrement;
        }
        else
        {
            if (entry.isCommandsForKey())
                keys++;

            if (isLoaded) completeSetupOfLoaded(entry);
            else
            {
                if (submitLoad) executor().cacheUnsafe().load(executor(), this, is(ExclusiveGroup.RANGE), entry);
                completeSetupOfLoading(entry, !submitLoad);
            }
        }
    }

    private void completeSetupOfLoaded(AccordCacheEntry<?, ?, ?> entry)
    {
        if (isOptional(entry))
        {
            nonSync().addLoaded();
            if (isCacheQueuedFifo())
                entry.addFifo(this);
        }
        else if (isCacheQueuedFifo())
        {
            entry.addFifo(this);
        }
    }

    private void completeSetupOfLoading(AccordCacheEntry<?, ?, ?> entry, boolean alreadyLoading)
    {
        if (alreadyLoading)
        {
            Loading loading = entry.loading();
            if (loading.loading != null && loading.loading.is(RANGE_LOAD) && loading.loading.is(WAITING_TO_RUN) && !is(ExclusiveGroup.RANGE))
            {
                // requeue anything setup as a range load that's now needed for a key-based operation, so it can use the correct the queue limits
                loading.loading.unqueue(executor().runnable);
                loading.loading.override(LOAD);
                executor().runnable.enqueue(loading.loading, false);
            }
        }

        // both branches place us: a fifo claim takes its position, everyone else is bagged until the drain
        if (isCacheQueuedFifo()) entry.addFifo(this);
        else entry.addWaitingToLoad(this);
        Invariants.paranoid(entry.waitingCount() == entry.references());
    }

    private void onSetupOrScannedExclusive(Caches caches)
    {
        for (TxnId txnId : context.txnIds())
        {
            if (hasPreSetup() && tryCompletePresetupExclusive(txnId, WAITING_FOR_TXN_INCR))
                continue;
            setupExclusive(txnId, caches.commands(), WAITING_FOR_TXN_INCR);
        }

        if (waitingFor != 0)
        {
            setStateExclusive(LOADING_REQUIRED);
            executor().loading.enqueue(this);
        }
        else onLoadedRequiredExclusive();
    }

    private void onLoadedRequiredExclusive()
    {
        if (isSync() || nonSync().isLoaded(this) || isCacheQueuedFifo())
        {
            waitOnTxnsExclusive();
        }
        else
        {
            setStateExclusive(LOADING_OPTIONAL);
            executor().loading.enqueue(this);
        }
    }

    boolean isUnsequenced(AccordCacheEntry<?, ?, ?> entry)
    {
        if (!isUnsequenced())
            return false;

        Invariants.require(entry.isCommandsForKey() || !isIncremental());
        return true;
    }

    /**
     * UNSEQUENCED is not supported for incremental tasks, because to avoid cyclicity in execution when we hold txnId locks
     * across runs, we must upgrade the task to FIFO on first run, which immediately revokes other unsequenced tasks'
     * permission to run.
     */
    private void requireSequencedIfHoldsLocksBetweenRuns()
    {
        Invariants.require(isIncremental());
        Invariants.require(!isUnsequenced() || !holdsLocksBetweenRuns(),
                           "UNSEQUENCED INCR tasks may not declare a txnId; use BY_PRIORITY instead");
        Invariants.require(CACHE_QUEUES_ENABLED);
    }

    boolean isOptional(AccordCacheEntry<?, ?, ?> entry)
    {
        return isNonSync() && entry.isCommandsForKey();
    }

    boolean holdsLocksBetweenRuns()
    { // TODO (desired): encode as a state bit
        return isIncremental() && context.primaryTxnId() != null;
    }

    int waitingForTxnCount()
    {
        return waitingFor >>> 30;
    }

    private void incrementWaitingForTxnCount()
    {
        Invariants.require(waitingForTxnCount() < 3);
        waitingFor += WAITING_FOR_TXN_INCR;
    }

    private int decrementWaitingForTxnCount()
    {
        Invariants.require(waitingForTxnCount() > 0);
        waitingFor -= WAITING_FOR_TXN_INCR;
        return waitingForTxnCount();
    }

    int waitingForKeyCount()
    {
        return waitingFor & WAITING_FOR_KEY_MASK;
    }

    private void incrementWaitingForKeyCount()
    {
        Invariants.require(waitingForKeyCount() < keys);
        ++waitingFor;
    }

    private int decrementWaitingForKeyCount()
    {
        Invariants.require(waitingForKeyCount() > 0);
        return --waitingFor & WAITING_FOR_KEY_MASK;
    }

    private void waitOnTxnsExclusive()
    {
        waitingAt = Math.max(createdAt, nanoTime());
        executor().runnable.incrementArrivals(this);
        commandStore.exclusiveExecutor().incrementArrivals(this);

        if (!CACHE_QUEUES_ENABLED)
        {
            waitToRunExclusive();
            return;
        }

        Invariants.require(waitingFor == 0);
        // register txnId first, to avoid reentry via onBlockingHigherPriorityTask
        {
            TxnId primaryTxnId = context.primaryTxnId();
            if (primaryTxnId != null)
            {
                SaferCommand primary = (SaferCommand) refs.get(primaryTxnId);
                if (NOT_RUNNABLE == ensureCacheQueued(primary.global()))
                    incrementWaitingForTxnCount();

                TxnId additionalTxnId = context.additionalTxnId();
                if (additionalTxnId != null)
                {
                    SaferCommand additional = (SaferCommand) refs.get(additionalTxnId);
                    if (NOT_RUNNABLE == ensureCacheQueued(additional.global()))
                        incrementWaitingForTxnCount();
                }
            }
        }

        queueOnKeysExclusive();

        // TODO (desired): exception-safe rollback for addUnsequenced
        if (waitingForTxnCount() == 0) waitOnKeysExclusive();
        else
        {
            // a contains() per ref, so O(refs x queue)
            if (testParanoia(SUPERLINEAR, Paranoia.NONE, LOW))
            {
                refs.forEach((k, v) -> {
                    if (v instanceof SaferCommandsForKey)
                    {
                        SaferCommandsForKey safeCfk = (SaferCommandsForKey) v;
                        Invariants.require(!safeCfk.global().isLoaded() || safeCfk.global().contains(this));
                    }
                    else
                    {
                        SaferCommand safeCommand = (SaferCommand) v;
                        Invariants.require(safeCommand.global().contains(this));
                    }
                });
            }
            setStateExclusive(WAITING_ON_TXN);
            executor().waiting.enqueue(this);
        }
    }

    private void queueOnKeysExclusive()
    {
        Invariants.require(CACHE_QUEUES_ENABLED);
        Invariants.require(nonSync == null || (nonSync.blocking == null && nonSync.notBlocking == null));

        this.refs.forEach((key, safeState) -> {
            if (safeState instanceof SaferCommandsForKey)
            {
                SaferCommandsForKey safeCfk = (SaferCommandsForKey) safeState;
                if (!safeCfk.isUninitialised())
                    return;

                AccordCacheEntry<?, ?, ?> entry = safeCfk.global();
                boolean optional = isNonSync();
                if (entry.isLoaded())
                {
                    RunnableStatus status = ensureCacheQueued(entry);
                    if (optional) addQueuedOptionalKey(entry, status);
                    else if (status == NOT_RUNNABLE)
                        incrementWaitingForKeyCount();
                }
                else Invariants.require(optional);
            }
        });

        // we do this after since this is validation-only, and it's only valid once they're all inserted
        setCacheQueuedExclusive();
    }

    private void waitOnKeysExclusive()
    {
        Invariants.require(waitingForTxnCount() == 0);
        if (isSync() ? waitingFor == 0 : nonSync().isWaitReady(this)) waitToRunExclusive();
        else
        {
            setStateExclusive(WAITING_ON_KEY);
            executor().waiting.enqueue(this);
        }
    }

    RunnableStatus ensureCacheQueued(AccordCacheEntry<?, ?, ?> loaded)
    {
        if (isCacheQueuedFifo())
            return loaded.statusIfPresent(this);
        else if (isUnsequenced(loaded))
            return loaded.addUnsequenced(this);
        else
            return loaded.addPrioritised(this);
    }

    RunnableStatus addCacheQueued(AccordCacheEntry<?, ?, ?> loaded)
    {
        if (isCacheQueuedFifo())
            return loaded.addFifo(this);
        else if (isUnsequenced(loaded))
            return loaded.addUnsequenced(this);
        else
            return loaded.addPrioritised(this);
    }

    public static final int ADOPT_CACHED_KEY_ADD_TO_QUEUE_STATES = WAITING;

    void adoptCachedKeyExclusive(AccordCacheEntry<?, ?, ?> entry, SafeState<?> safeRef)
    {
        Invariants.require(entry.isLoaded());
        // !isCacheQueuedFifo was originally included to ensure fifo acquisition occurred in one go (to ensure acyclicity),
        //  which it is not needed for: the fifo region is ordered by fifoAt, so a claim taken outside the acquisition pass
        //  is placed by its stamp rather than by arrival, and the ordering argument is unaffected. It is needed for the
        //  ATOMIC atomicity guarantee, which is why it stays: adopting late lets a task with an OLDER stamp insert itself
        //  ahead of a younger unit's consequence, i.e. between that consequence and its submitter. Modelled as
        //  ctl-fifo-adopt in spec/accord-execution, where relaxing it breaks Inv_Isolation and nothing else.
        Invariants.require(!isCacheQueuedFifo());

        refs.put(entry.key(), safeRef);
        ++keys;

        boolean addToQueue = CACHE_QUEUES_ENABLED && isState(ADOPT_CACHED_KEY_ADD_TO_QUEUE_STATES);
        if (addToQueue)
        {
            RunnableStatus status = addCacheQueued(entry);
            if (isOptional(entry))
                addQueuedOptionalKey(entry, status);
            else if (status == NOT_RUNNABLE)
                incrementWaitingKeys();
        }

        if (isOptional(entry))
            addLoadedOptionalKey();

        Invariants.paranoidLinearCost(!isCacheQueued() || entry.contains(this));
    }

    void onLoadOneExclusive(AccordCacheEntry<?, ?, ?> loaded)
    {
        if (isOptional(loaded))
        {
            // if we're incremental/async we don't block on keys loading, so we don't need to decrement anything
            // however, if we're in fifo mode this loaded key might be ready for us to run with
            State state = state();
            switch (state)
            {
                default: throw new UnhandledEnum(state);
                case WAITING_ON_KEY:
                case WAITING_TO_RUN:
                case PREPARED:
                case RUNNING_WHILE_FAILED:
                case WAITING_ON_TXN:
                    Invariants.require(CACHE_QUEUES_ENABLED);
                    RunnableStatus status = ensureCacheQueued(loaded);
                    if (status != NOT_RUNNABLE)
                        addQueuedOptionalKey(loaded, status);
                    // fall-through
                case SCANNING_RANGES:
                case LOADING_REQUIRED:
                    nonSync().addLoaded();
                    break;

                case LOADING_OPTIONAL:
                    addLoadedOptionalKey();
                    break;
            }
        }
        else if (isState(LOADING_OR_WAITING_REQUIRED))
        {
            if (is(WAITING_ON_TXN) && loaded.isCommandsForKey())
                return;

            if (is(WAITING_ON_KEY)) Invariants.require(loaded.isCommandsForKey());

            if (loaded.isCommandsForKey()) decrementWaitingForKeyCount();
            else decrementWaitingForTxnCount();

            if (waitingFor == 0)
            {
                if (is(LOADING_REQUIRED))
                {
                    unqueue(executor().loading);
                    onLoadedRequiredExclusive();
                }
                else Invariants.require(is(SCANNING_RANGES));
            }
        }
    }

    void addLoadedOptionalKey()
    {
        Invariants.require(CACHE_QUEUES_ENABLED);
        nonSync().addLoaded();
        if (is(LOADING_OPTIONAL) && nonSync().isLoaded(this))
        {
            unqueue(executor().loading);
            waitOnTxnsExclusive();
        }
    }

    // TODO (expected): add vs setup vs onChange; some callers don't need to try
    void addQueuedOptionalKey(AccordCacheEntry<?, ?, ?> loaded, RunnableStatus status)
    {
        Invariants.require(CACHE_QUEUES_ENABLED);
        // A key only belongs in the batch sets if we hold a position on it, as NonSyncState.prepareExclusive locks with
        // RELEASE_QUEUE, which requires us to lead the entry. Checked here rather than at the lock to distinguish a key
        // that entered the sets holding nothing from one that held a position and lost it afterwards.
        if (status != NOT_RUNNABLE && testParanoia(Invariants.Paranoia.LINEAR, Invariants.Paranoia.NONE, LOW))
            Invariants.require(loaded.contains(this), "%s reports %s on %s but holds no position there (loaded=%s, state=%s)",
                               description(), status, loaded.key(), loaded.isLoaded(), currentState());

        switch (status)
        {
            default: throw UnhandledEnum.unknown(status);
            case NOT_RUNNABLE: break;
            case STILL_RUNNABLE:
            case NEWLY_RUNNABLE:
                nonSync().onNewHead(loaded);
                break;
            case STILL_RUNNABLE_NEWLY_BLOCKING:
            case NEWLY_BLOCKING_RUNNABLE:
                nonSync().onNewBlockingHead(loaded);
                break;
        }

        if (is(WAITING_ON_KEY) && nonSync().isWaitReady(this))
        {
            unqueue(executor().waiting);
            waitToRunExclusive();
        }
    }

    void onChangeRunnableStatus(AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
    {
        if (entry.isCommandsForKey()) onChangeKeyRunnableStatus(entry, status);
        else onChangeTxnRunnableStatus(status);
    }

    private void incrementWaitingKeys()
    {
        Invariants.require(isState(WAITING));
        if (waitingFor == 0)
        {
            Invariants.require(is(WAITING_TO_RUN));
            unqueue(commandStore.exclusiveExecutor());
            setStateExclusive(WAITING_ON_KEY);
            executor().waiting.enqueue(this);
        }
        // the revocation must have taken us out of the run queue, whatever the count was when it arrived
        incrementWaitingForKeyCount();
        Invariants.require(!is(WAITING_TO_RUN));
    }

    private void incrementWaitingTxns()
    {
        Invariants.require(isState(WAITING));
        if (!is(WAITING_ON_TXN))
        {
            // we take our key positions in waitOnTxnsExclusive and keep them for the rest of our life, so losing a txnId
            // position revokes nothing: both the positions and the waits we count for them survive, and
            // waitOnKeysExclusive has nothing to re-place when the txnId comes back to us
            if (is(WAITING_ON_KEY)) setStateExclusive(WAITING_ON_TXN);
            else
            {
                Invariants.require(is(WAITING_TO_RUN));
                unqueue(commandStore.exclusiveExecutor());
                setStateExclusive(WAITING_ON_TXN);
                executor().waiting.enqueue(this);
            }
        }
        Invariants.require(waitingForTxnCount() < 2);
        incrementWaitingForTxnCount();
        Invariants.require(is(WAITING_ON_TXN));
    }

    void onChangeTxnRunnableStatus(RunnableStatus newStatus)
    {
        Invariants.require(CACHE_QUEUES_ENABLED);
        Invariants.require(compareTo(WAITING_ON_TXN) >= 0, "%s notified %s of a txnId before it waits on txnIds", this, newStatus);
        if (newStatus == NOT_RUNNABLE)
        {
            incrementWaitingTxns();
        }
        else if (newStatus != STILL_RUNNABLE_NEWLY_BLOCKING)
        {
            Invariants.require(is(WAITING_ON_TXN));
            if (decrementWaitingForTxnCount() == 0)
            {
                unqueue(executor().waiting);
                waitOnKeysExclusive();
            }
        }
    }

    void onChangeKeyRunnableStatus(AccordCacheEntry<?, ?, ?> entry, RunnableStatus newStatus)
    {
        if (compareTo(WAITING_ON_TXN) < 0)
        {
            // below WAITING_ON_TXN nothing has placed our key claims yet, so the only task that can be notified here is
            // one that took a fifo position at setup
            Invariants.require(isCacheQueuedFifo());
            return;
        }

        if (isSync())
        {
            if (newStatus == NOT_RUNNABLE)
            {
                incrementWaitingKeys();
            }
            else if (newStatus != STILL_RUNNABLE_NEWLY_BLOCKING)
            {
                if (decrementWaitingForKeyCount() == 0 && is(WAITING_ON_KEY))
                {
                    unqueue(executor().waiting);
                    waitToRunExclusive();
                }
            }
        }
        else
        {
            Invariants.require(isState(WAITING_OR_RUNNING));
            switch (newStatus)
            {
                default: throw UnhandledEnum.unknown(newStatus);
                case STILL_RUNNABLE: throw UnhandledEnum.invalid(STILL_RUNNABLE); // onChange -> changed (but this means no change)
                case NOT_RUNNABLE:
                    nonSync().onNotHead(entry);
                    if (is(WAITING_TO_RUN) && !nonSync().isWaitReady(this))
                    {
                        unqueue(commandStore.exclusiveExecutor());
                        setStateExclusive(WAITING_ON_KEY);
                        executor().waiting.enqueue(this);
                    }
                    return;

                case STILL_RUNNABLE_NEWLY_BLOCKING:
                    nonSync().onStillHeadNewBlocking(entry);
                    break;

                case NEWLY_RUNNABLE:
                    nonSync().onNewHead(entry);
                    break;

                case NEWLY_BLOCKING_RUNNABLE:
                    nonSync().onNewBlockingHead(entry);
                    break;
            }

            if (is(WAITING_ON_KEY) && nonSync().isWaitReady(this))
            {
                unqueue(executor().waiting);
                waitToRunExclusive();
            }
        }
    }

    void waitToRunExclusive()
    {
        // a revocation must have moved us out of WAITING_TO_RUN, so reaching here we must lead every entry we hold.
        // O(refs x queue), and nothing to check with the cache queues off: waitOnTxnsExclusive takes no position.
        if (CACHE_QUEUES_ENABLED && testParanoia(SUPERLINEAR, Paranoia.NONE, LOW))
        {
            refs.forEach((k, v) -> {
                AccordCacheEntry<?, ?, ?> entry = SaferState.global(v);
                Invariants.require(entry.contains(this));
                if (isSync() || !entry.isCommandsForKey())
                {
                    RunnableStatus status = entry.statusIfPresent(this);
                    Invariants.require(status == NEWLY_RUNNABLE || status == NEWLY_BLOCKING_RUNNABLE);
                }
            });
        }
        Invariants.require(waitingFor == 0);
        setStateExclusive(WAITING_TO_RUN);
        commandStore.exclusiveExecutor().enqueue(this, false);
    }

    public ExecutionContext executionContext()
    {
        return context;
    }

    @Override
    protected void prepareExclusiveMayThrow()
    {
        if (keys > 0)
            setStateExclusive(PREPARING);

        try
        {
            if (ranges instanceof SafeTask<?>.RangeTxnScanner)
                ranges = ((SafeTask<?>.RangeTxnScanner)ranges).finish(commandStore.cachesExclusive());

            if (isSync())
            {
                refs.forEach((k, v) -> {
                    if (v instanceof SaferCommandsForKey)
                        ((SaferCommandsForKey) v).preExecute(this, RELEASE_QUEUE);
                });
                // do txns last to avoid reentry during prepare affecting runnability, when we remove ourselves (and promote another)
                prepareTxnsExclusive();
            }
            else
            {
                if (hasIncrementalStarted()) nonSync().prepareExclusive(this);
                else
                {
                    // Upgrade on start: a task that holds txnId locks across runs takes a fifo position to prevent dependency cycles;
                    // a task that is declared ATOMIC does so to provide the requested isolation guarantees
                    if (isIncremental() && (holdsLocksBetweenRuns() || isSequencedByPriorityAtomic()) && !isCacheQueuedFifo())
                    {
                        fifoAt = executor().uniqueCreatedAt.incrementAndGet();
                        setCacheQueuedFifoExclusive();
                        refs.forEach((key, safeState) -> {
                            AccordCacheEntry<?, ?, ?> entry = global(safeState);
                            RunnableStatus status = entry.moveToFifo(this);
                            if (entry.isLoaded() && entry.isCommandsForKey())
                                onKeyMovedToFifo(entry, status);
                        });
                    }

                    nonSync().prepareExclusive(this);
                    prepareTxnsExclusive();

                    if (isIncremental())
                        setIncrementalStartedExclusive();
                }
            }
        }
        catch (Throwable t)
        {
            refs.forEach((k, v) -> { v.setAbandoned(); });
            throw t;
        }
    }

    private void prepareTxnsExclusive()
    {
        TxnId primaryTxnId = context.primaryTxnId();
        if (primaryTxnId != null)
        {
            LockMode lockMode = holdsLocksBetweenRuns() ? HOLD_QUEUE : RELEASE_QUEUE;
            preExecute(refs.get(primaryTxnId), this, lockMode);
            TxnId additionalTxnId = context.additionalTxnId();
            if (additionalTxnId != null)
                preExecute(refs.get(additionalTxnId), this, lockMode);
        }
    }

    private void onKeyMovedToFifo(AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
    {
        switch (status)
        {
            default: throw UnhandledEnum.unknown(status);
            case NOT_RUNNABLE:
            case STILL_RUNNABLE:
            case STILL_RUNNABLE_NEWLY_BLOCKING:
                break;
            case NEWLY_RUNNABLE:
                nonSync().onNewHead(entry);
                break;
            case NEWLY_BLOCKING_RUNNABLE:
                nonSync().onNewBlockingHead(entry);
                break;
        }
    }

    @Override
    public boolean runMayThrow()
    {
        try
        {
            SaferCommandStore safeStore = new SaferCommandStore(this, isSync() ? context : nonSync());
            commandStore.begin(safeStore);
            try
            {
                if (Tracing.isTracing())
                    Tracing.trace(context.describe());

                R result = function.apply(safeStore);

                boolean finished = !isIncremental() || isIncrementalFinishing();
                if (!finished)
                {
                    setRunState(RUN_INCOMPLETE);
                    // TODO (required): consider safety semantics here carefully
                    safeStore.persistFieldUpdatesInternal(null);
                }
                else
                {
                    List<Journal.CommandUpdate> changes = new ArrayList<>();
                    // TODO (expected): save any TxnId we add so that we don't need to iterate all of refs
                    refs.forEach((key, value) -> {
                        if (value instanceof SaferCommand)
                        {
                            SaferCommand safeCommand = (SaferCommand) value;
                            if (safeCommand.txnId().is(EphemeralRead))
                                return;

                            Journal.CommandUpdate diff = safeCommand.update();
                            if (diff != null)
                            {
                                changes.add(diff);
                                maybeSanityCheck(safeCommand);
                            }
                        }
                    });

                    boolean flush = !changes.isEmpty() || safeStore.fieldUpdates() != null;
                    if (flush)
                    {
                        setRunState(RUN_PERSISTING);
                        Runnable onFlush = () -> finish(result);
                        safeStore.persistFieldUpdatesInternal(changes.isEmpty() ? onFlush : null);
                        if (!changes.isEmpty())
                            save(changes, onFlush);
                        finished = false;
                    }
                }

                safeStore.postExecute();
                // TODO (required): do not notify callback until cfk are updated; must mark continuation tasks
                if (finished)
                    finish(result);
                return finished;
            }
            finally
            {
                commandStore.complete(safeStore);
            }
        }
        catch (Throwable t)
        {
            refs.forEach((k, v) -> v.setAbandoned());
            throw t;
        }
    }

    private void save(List<Journal.CommandUpdate> diffs, Runnable onFlush)
    {
        if (SANITY_CHECK && DebugTask.get(this).sanityCheck != null)
        {
            Condition condition = Condition.newOneTimeCondition();
            this.commandStore.appendCommands(diffs, condition::signal);
            condition.awaitUninterruptibly();

            for (Command check : DebugTask.get(this).sanityCheck)
                this.commandStore.sanityCheckCommand(commandStore.unsafeGetRedundantBefore(), check);

            if (onFlush != null) onFlush.run();
        }
        else
        {
            this.commandStore.appendCommands(diffs, onFlush);
        }
    }

    private void maybeSanityCheck(SaferCommand safeCommand)
    {
        if (SANITY_CHECK)
        {
            DebugTask debug = DebugTask.get(this);
            if (debug.sanityCheck == null)
                debug.sanityCheck = new ArrayList<>(2);
            debug.sanityCheck.add(safeCommand.current());
        }
    }

    void reportFailureMayThrow(Throwable failure)
    {
        BiConsumer<? super R, Throwable> callback = callbackUpdater.getAndSet(this, null);
        if (callback == null) executor().agent.onException(failure);
        else
        {
            if (executor().isInLoop()) callback.accept(null, failure);
            else executor().submit(() -> callback.accept(null, failure));
        }
    }

    @Override
    void completeExclusiveMayThrow()
    {
        AccordExecutor executor = executor();
        long now = Math.max(runningAt, nanoTime());
        if (isNonSync() && !isEither(NOT_YET_RUN, RUN_FAILED))
        {
            if (is(PREPARED))
            {
                nonSync().postRunExclusive(this);
                if (isIncremental() && !isIncrementalFinishing())
                {
                    executor.elapsedWaitingToRun.increment(runningAt - waitingAt, runningAt);
                    executor.elapsedRunning.increment(now - runningAt, now);
                    flushHistogramBuffer(runningAt);
                    waitingAt = Math.max(waitingAt, nanoTime());
                    setStateExclusive(INCOMPLETE);
                    waitOnKeysExclusive();
                    return;
                }
            }
            else
            {
                Invariants.expect(isIncremental());
                setRunState(RUN_FAILED);
                if (is(RUNNING_WHILE_FAILED)) refs.forEach((k, v) -> v.setAbandoned());
                else Invariants.require(is(FAILED), "unexpected state %s completing a failed non-sync task", currentState());
            }
        }

        releaseResourcesExclusiveNoExcept(commandStore.cachesExclusive());

        if (completeState())
        {
            executor.elapsedPreparingToRun.increment(waitingAt - createdAt, runningAt);
            executor.elapsedWaitingToRun.increment(runningAt - waitingAt, runningAt);
            executor.elapsedRunning.increment(now - runningAt, now);
            executor.elapsed.increment(now - createdAt, now);
            executor.keys.increment(keys, runningAt);
            flushHistogramBuffer(runningAt);
        }
        else if (histogramBuffer != null)
        {
            // we are done with the store's shared buffer either way, so do not leave it reachable from a dead task
            histogramBuffer.clear();
            histogramBuffer = null;
        }
    }

    private void flushHistogramBuffer(long at)
    {
        if (histogramBuffer == null)
            return;

        histogramBuffer.flush(at);
        histogramBuffer = null;
    }

    @Override
    public void cancel()
    {
        if (!state().hasStarted())
            executor().submit(Task::tryCancelExclusive, CancelTask::new, this);
    }

    void tryCancelExclusive()
    {
        try
        {
            if (is(UNREGISTERED))
            {
                releaseResourcesExclusiveNoExcept();
                failExclusive(new CancellationException(), CANCELLED_UNREGISTERED);
            }
            else if (compareTo(REGISTERED) >= 0 && compareTo(WAITING_TO_RUN) <= 0 && !hasIncrementalStarted())
            {
                cancelExclusive();
            }
        }
        catch (Throwable t)
        {
            unhandledException(t);
        }
    }

    /**
     * A cache entry we hold failed to load. If we have not run we simply fail; if our run is in flight we can only be
     * failed if we will come back for that entry, i.e. if we are incremental (the entry cannot have been processed, as
     * processing it requires loading it). Otherwise we need nothing further from it and just release our reference.
     */
    void onFailedToLoadExclusive(Throwable fail)
    {
        // we may be told more than once, as each of our failing keys drains us, and may already have completed or been
        // failed while running - a run in flight keeps its positions, so it is still drained by a second failing entry
        if (hasAlreadyFailed())
            return;

        if (is(State.UNREGISTERED) || compareTo(WAITING_TO_RUN) <= 0)
            tryFailAndCompleteUnexecutedExclusive(fail, FAILED);
        else if (isIncremental() && !isIncrementalFinishing())
            failWhileRunningExclusive(fail);
        else
        {
            // our run is in flight and will not come back for this entry, so simply release our reference
            Invariants.require(hasStartedRunning());
        }
    }

    void cancelExclusive()
    {
        if (ranges instanceof SafeTask<?>.RangeTxnScanner)
            ((SafeTask<?>.RangeTxnScanner)ranges).cancelled = true; // TODO (expected): should we try to cancel this directly?
        failAndCompleteExclusive(new CancellationException(), CANCELLED);
    }

    void unqueueIfQueued()
    {
        TaskQueue expected;
        switch (queued())
        {
            default: throw UnhandledEnum.unknown(queued());
            case NONE: return;
            case LOADING:
                expected = executor().loading;
                break;
            case WAITING:
                expected = executor().waiting;
                break;
            case RUNNABLE:
                expected = commandStore.exclusiveExecutor();
                break;
        }
        expected.unqueue(this);
    }

    private void finish(R result)
    {
        BiConsumer<? super R, Throwable> callback = callbackUpdater.getAndSet(this, null);
        if (callback != null)
        {
            try { callback.accept(result, null); }
            catch (Throwable t) { commandStore.agent().onException(t); }
        }
    }

    @Override
    void releaseResourcesExclusiveNoExcept()
    {
        releaseResourcesExclusiveNoExcept(commandStore.cachesExclusive());
    }

    void releaseResourcesExclusiveNoExcept(Caches caches)
    {
        if (refs == null)
            return;

        try
        {
            if (ranges instanceof SafeTask<?>.RangeTxnScanner)
            {
                ((SafeTask<?>.RangeTxnScanner)ranges).cleanup(caches);
                if (DEBUG_EXECUTION) DebugTask.get(this).onReleasedRangeScanner();
            }
            ranges = null;

            refs.forEach((key, safeState) -> {
                SaferState.postExecute(safeState, this);
            });
            if (DEBUG_EXECUTION) DebugTask.get(this).onReleasedState();
        }
        catch (Throwable t)
        {
            releaseResourcesSlowExclusiveNoExcept(t);
            unhandledException(t);
        }
        finally
        {
            refs = null;
        }
    }

    private void releaseResourcesSlowExclusiveNoExcept(Throwable suppressedBy)
    {
        if (refs == null)
            return;

        try
        {
            refs.forEach((k, safeState) -> {
                if (!safeState.isReleased())
                {
                    try { SaferState.postExecute(safeState, this); }
                    catch (Throwable t) { suppressedBy.addSuppressed(t); }
                }
            });
        }
        catch (Throwable t)
        {
            unhandledException(t);
        }
        refs = null;
    }

    public class RangeTxnAndKeyScanner extends RangeTxnScanner
    {
        class KeyWatcher implements AccordCache.Listener<RoutingKey, CommandsForKey>
        {
            @Override
            public void onUpdate(AccordCacheEntry<RoutingKey, CommandsForKey, ?> state)
            {
                if (ranges.contains(state.key()))
                    reference((AccordCacheEntry<RoutingKey, CommandsForKey, SaferCommandsForKey>) state);
            }
        }

        final Set<TokenKey> intersectingKeys = new ObjectHashSet<>();
        final Ranges ranges = ((AbstractRanges) context.keys()).toRanges();
        final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeyCache;
        KeyWatcher keyWatcher = new KeyWatcher();

        public RangeTxnAndKeyScanner(AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeyCache)
        {
            this.commandsForKeyCache = commandsForKeyCache;
        }

        protected void runInternal()
        {
            for (Range range : ranges)
            {
                // the on-disk half of the key scan, via the loader so that an index that knows its own keys can answer
                // it without consulting the commands_for_key table
                loader.findKeysBetween((TokenKey) range.start(), range.startInclusive(),
                                       (TokenKey) range.end(), range.endInclusive(),
                                       key -> {
                                           if (cancelled)
                                               throw new CancellationException();
                                           intersectingKeys.add(key);
                                       });
            }
            super.runInternal();
        }

        private void reference(AccordCacheEntry<RoutingKey, CommandsForKey, SaferCommandsForKey> entry)
        {
            switch (entry.status())
            {
                default: throw new AssertionError("Unhandled Status: " + entry.status());
                case WAITING_TO_LOAD:
                case LOADING:
                    return;

                case MODIFIED:
                case WAITING_TO_SAVE:
                case SAVING:
                case LOADED:
                case FAILED_TO_SAVE:
                    if (refs.containsKey(entry.key()))
                        return;

                    Object v = entry.getOrShrunkExclusive();
                    if (v == null) return;
                    else if (v instanceof CommandsForKey)
                    {
                        if (!loader.isRelevant((CommandsForKey) v))
                            return;
                    }
                    else
                    {
                        TxnId last = CommandSerializers.txnId.deserialize((ByteBuffer) v);
                        int position = (int)CommandSerializers.txnId.serializedSize(last);
                        TxnId minUndecided = CommandSerializers.txnId.deserialize((ByteBuffer) v, position);
                        if (!loader.isRelevant(entry.key(), last, minUndecided))
                            return;
                    }

                    SafeTask.this.adoptCachedKeyExclusive(entry, commandsForKeyCache.acquire(entry));
            }
        }

        void startInternal(Caches caches)
        {
            for (Range range : ranges)
            {
                for (RoutingKey key : caches.commandsForKeys().keysBetween(range.start(), range.startInclusive(), range.end(), range.endInclusive()))
                    intersectingKeys.add((TokenKey) key);
            }
            caches.commandsForKeys().register(keyWatcher);
            super.startInternal(caches);
        }

        void scannedInternal()
        {
            intersectingKeys.removeAll(refs.keySet());
            setupKeyLoadsExclusive(false, commandStore.cachesExclusive(), intersectingKeys, true);
            super.scannedInternal();
        }

        void cleanup(Caches caches)
        {
            if (keyWatcher != null)
                caches.commandsForKeys().tryUnregister(keyWatcher);
            super.cleanup(caches);
        }

        CommandSummaries finish(Caches caches)
        {
            caches.commandsForKeys().unregister(keyWatcher);
            keyWatcher = null;
            return super.finish(caches);
        }
    }

    public class RangeTxnScanner extends IOTaskWrapper.WrappableIOTask
    {
        final Map<Timestamp, Summary> summaries = new HashMap<>();
        final Map<Timestamp, Summary> guardedSummaries = Collections.synchronizedMap(summaries);

        RangeIndex.Loader loader;
        boolean scanned;
        Throwable failure;

        volatile boolean cancelled;

        protected void runInternal()
        {
            loader.load(guardedSummaries, () -> cancelled);
        }

        ExecutionContext preLoadContext()
        {
            return context;
        }

        @Override
        protected void postRunExclusive()
        {
            executor().onScannedRangesExclusive(SafeTask.this, failure);
        }

        @Override
        protected void fail(Throwable t)
        {
            this.failure = t;
        }

        public void start()
        {
            Caches caches = commandStore.cachesExclusive();
            SafeTask.this.setStateExclusive(SCANNING_RANGES);
            executor().loading.enqueue(SafeTask.this);
            startInternal(caches);
            executor().submitExclusive(SafeTask.this, GlobalGroup.RANGE_SCAN, this);
        }

        void startInternal(Caches caches)
        {
            loader = commandStore.rangeIndex().loader(context.primaryTxnId(), context.executeAt(), context.loadKeysFor(), context.keys());
            loader.loadExclusive(guardedSummaries, caches);
        }

        public void scannedExclusive()
        {
            Invariants.require(is(SCANNING_RANGES), "Expected SCANNING_RANGES; found %s", SafeTask.this, SafeTask::description);
            scanned = true;
            scannedInternal();
            unqueue(executor().loading); // likely to be requeued to same queue, but simpler invariants if we remove here
            onSetupOrScannedExclusive(commandStore.cachesExclusive());
        }

        void scannedInternal()
        {
        }

        void cleanup(Caches caches)
        {
            if (loader != null)
                loader.cleanupExclusive(caches);
        }

        CommandSummaries finish(Caches caches)
        {
            loader.finish(summaries);
            loader.cleanupExclusive(caches);
            loader = null;
            TreeMap<Timestamp, Summary> byId = new TreeMap<>(summaries);
            return (CommandSummaries.ByTxnIdSnapshot) () -> byId;
        }

        @Override
        public String description()
        {
            return "Scanning range intersections for " + context.reason() + ' ' + toBriefString();
        }

        @Override
        public String toString()
        {
            return description();
        }
    }

    @Override
    public String description()
    {
        return context.describe();
    }

    @Override
    public String briefDescription()
    {
        return context.reason();
    }

    final AccordExecutor executor()
    {
        return commandStore.executor();
    }

    @Override
    protected boolean isNewWork()
    {
        return is(UNREGISTERED);
    }

    public @Nullable SafeTask<?>.RangeTxnScanner rangeScanner()
    {
        if (ranges instanceof SafeTask<?>.RangeTxnScanner)
            return ((SafeTask<?>.RangeTxnScanner) ranges);
        return null;
    }

    public @Nullable CommandSummaries commandsForRanges()
    {
        if (ranges instanceof CommandSummaries)
            return (CommandSummaries) ranges;
        return null;
    }

    NonSyncState nonSync()
    {
        return nonSync;
    }

    /**
     * The keys we will really load, which is not necessarily what the context asked for:
     * with {@code queue_nonsync_enabled=false} nothing is batched, everything becomes SYNC
     */
    private static LoadKeys loadKeys(ExecutionContext context)
    {
        LoadKeys loadKeys = context.loadKeys();
        if (loadKeys == NONE)
            return NONE;
        return NONSYNC_ENABLED ? loadKeys : SYNC;
    }
}
