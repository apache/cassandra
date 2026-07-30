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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.ArrayBuffers.BufferList;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeyAccessor;
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
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask.SANITY_CHECK;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.HOLD_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.RELEASE_QUEUE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.NOT_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_BLOCKED_LIMIT;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_ENABLED;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.NONSYNC_MIN_BATCH_SIZE;
import static org.apache.cassandra.service.accord.execution.SaferState.global;
import static org.apache.cassandra.service.accord.execution.SaferState.postExecute;
import static org.apache.cassandra.service.accord.execution.SaferState.preExecute;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.LOAD;
import static org.apache.cassandra.service.accord.execution.Task.GlobalGroup.RANGE_LOAD;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.RunState.NOT_YET_RUN;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_PERSISTING;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED_UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.REGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_OR_PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public final class SafeTask<R> extends Task implements Cancellable, DebuggableTask
{
    private static final Logger logger = LoggerFactory.getLogger(SafeTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

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

    static class NonSyncState extends ExecutionContext.Wrapped implements ExecutionContext
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
        }

        final void onNewBlockingHead(AccordCacheEntry<?, ?, ?> entry)
        {
            ensureBlocking().add((RoutingKey) entry.key());
        }

        final void onStillHeadNewBlocking(AccordCacheEntry<?, ?, ?> entry)
        {
            notBlocking.remove((RoutingKey) entry.key());
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
            try (BufferList<RoutingKey> keys = new BufferList<>())
            {
                if ((blocking == null || !populate(keys, blocking)) && notBlocking != null)
                    populate(keys, notBlocking);

                keys.forEach(key -> preExecute(owner.refs.get(key), owner, RELEASE_QUEUE));
                keys.sort(RoutingKey::compareTo);
                active = RoutingKeys.of(keys);
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
                    postExecute(owner.refs.remove(key), owner);
                active = null;
            }
        }
    }

    static class IncrementalState extends NonSyncState
    {
        long waiting, running;

        public IncrementalState(ExecutionContext context)
        {
            super(context);
        }

        void updateMetrics(long waiting, long running)
        {
            this.running += running;
            this.waiting += waiting;
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
    int waitingForState;

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
    long loadedAt;

    private BiConsumer<? super R, Throwable> callback;

    public SafeTask(@Nonnull AccordCommandStore commandStore, ExecutionContext context, Function<? super SafeCommandStore, R> function)
    {
        super(context, commandStore.executor().uniqueCreatedAt);
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
        this.callback = callback;
        Task inherit = executor().inherit();
        if (inherit == null)
            return false;

        Invariants.require(inherit.is(PREPARED));
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
        // note we use the caches "unsafely" here deliberately, as we only reference commands we already have references to
        // so we do not mutate anything, except the atomic counter of references
        LoadKeys loadKeys = loadKeys(context);
        if (loadKeys != NONE)
        {
            Unseekables<?> parentKeysOrRanges = parent.context.keys();
            Unseekables<?> keysOrRanges = context.keys();

            boolean isKeySubset = parent.isIncremental() ? parent.nonSync.active.containsAll(keysOrRanges) : parentKeysOrRanges.containsAll(keysOrRanges);
            if (isKeySubset)
                setInheritedRangeScan();

            setSequencedExclusive(context.executionSequence());
            if (loadKeys != SYNC)
            {
                setNonSyncExclusive();
                if (loadKeys == INCR)
                {
                    nonSync = new IncrementalState(context);
                    setIncrementalExclusive();
                    // forbid BY_PRIORITY sequencing to avoid priority inversion deadlocks on INCR tasks that lock a TxnId but await some key that has a higher priority task (that is waiting on our locked TxnId) - solvable in future if necessary
                    Invariants.require(isSequencedByPriorityAtomic() || isUnsequenced(), "INCR tasks may currently only be ATOMIC or UNSEQUENCED");
                }
                else
                {
                    nonSync = new NonSyncState(context);
                }
            }

            if (isSequencedByPriorityAtomic())
            {
                boolean isTxnIdSubset = context.isTxnIdSubsetOf(parent.context);
                if (!isKeySubset)
                {
                    Invariants.require(keysOrRanges.domain() == Key, "ATOMIC tasks over ranges must declare a subset of their parent's task keys() to avoid a range scan across which we would not impose sequencing");
                    // to avoid priority inversion deadlocks, if we are not a strict subset of the parent task we permit running with a single key ready
                    nonSync.alwaysReady = true;
                }
                Invariants.require(isTxnIdSubset, "ATOMIC tasks must declare a subset of their parent's task txnIds() to avoid a priority inversion deadlock");
                // TODO (required): we're appending to the fifo queue - does this maintain correct order?
                setCacheQueuedFifoExclusive();
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

        for (TxnId txnId : context.txnIds())
            preSetup(txnId, parent.refs, commandStore.cachesUnsafe().commands());
    }

    @Override
    void submitExclusiveMayThrow()
    {
        Caches caches = commandStore.cachesExclusive();
        executor().registerExclusive(this);
        setStateExclusive(REGISTERED);
        boolean hasPreSetup = hasInherited();
        LoadKeys loadKeys = loadKeys(context);
        if (loadKeys != NONE)
        {
            if (loadKeys != SYNC && !hasPreSetup)
            {
                setNonSyncExclusive();
                if (loadKeys != INCR) nonSync = new NonSyncState(context);
                else
                {
                    nonSync = new IncrementalState(context);
                    setIncrementalExclusive();
                }
            }

            Unseekables<?> keysOrRanges = context.keys();
            switch (keysOrRanges.domain())
            {
                case Range:
                    if (!hasInheritedRangeScan()) setupRangeLoadsExclusive(caches);
                    else refs.forEach((k, v) -> {
                        if (v instanceof SaferCommandsForKey)
                            completePresetupExclusive((SaferCommandsForKey)v);
                    });
                    break;
                case Key:
                    setupKeyLoadsExclusive(hasPreSetup, caches, (AbstractUnseekableKeys) keysOrRanges, hasInheritedRangeScan());
                    break;
            }
        }

        for (TxnId txnId : context.txnIds())
        {
            if (hasPreSetup && completePresetupExclusive(txnId))
                continue;
            setupExclusive(txnId, caches.commands(), 1);
        }

        if (is(SCANNING_RANGES))
            return;

        onSetupOrScannedExclusive();
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
            if (hasPreSetup && completePresetupExclusive(setupKey))
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

    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> boolean completePresetupExclusive(K k)
    {
        S preacquired = (S) refs.get(k);
        if (preacquired != null)
        {
            completePresetupExclusive(preacquired);
            return true;
        }
        return false;
    }

    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> void completePresetupExclusive(S preacquired)
    {
        AccordCacheEntry<K, V, S> entry = preacquired.global();
        if (entry.isLoaded()) completeSetupOfLoaded(entry);
        else completeSetupOfLoading(entry, true);
    }

    // expects to hold lock
    private <K, V, S extends SafeState<V> & SaferState<K, V, S>> void setupExclusive(K k, AccordCache.Type<K, V, S>.Instance cache, int waitForIncrement)
    {
        S safeRef = cache.acquire(k);
        AccordCacheEntry<K, V, ?> entry = safeRef.global();
        Status entryStatus = entry.status();
        boolean submitLoad = false;
        boolean isLoaded;
        switch (entryStatus)
        {
            default: throw new UnhandledEnum(entryStatus);
            case WAITING_TO_LOAD:
                submitLoad = true;
            case LOADING:
                isLoaded = false;
                waitingForState += waitForIncrement;
                break;
            case WAITING_TO_SAVE:
            case SAVING:
            case LOADED:
            case MODIFIED:
            case FAILED_TO_SAVE:
                isLoaded = true;
        }

        Object prev = refs.putIfAbsent(k, safeRef);
        if (prev != null)
        {
            noSpamLogger.warn("ExecutionContext {} contained key {} more than once", refs, k);
            cache.release(safeRef, this);
            waitingForState -= waitForIncrement;
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
            nonSync.addLoaded();
            if (isCacheQueuedFifo())
                addQueuedOptionalKey(entry, entry.addFifo(this));
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

        if (isCacheQueuedFifo()) entry.addFifo(this);
        else entry.addWaitingToLoad(this);
        Invariants.paranoid(entry.waitingCount() == entry.references());
    }

    private void onSetupOrScannedExclusive()
    {
        if (waitingForState > 0)
        {
            setStateExclusive(LOADING_REQUIRED);
            executor().loading.enqueue(this);
        }
        else onLoadedRequiredExclusive();
    }

    private void onLoadedRequiredExclusive()
    {
        if (isSync() || nonSync.isLoaded(this) || isCacheQueuedFifo())
        {
            waitOnCacheQueuesExclusive();
        }
        else
        {
            setStateExclusive(LOADING_OPTIONAL);
            executor().loading.enqueue(this);
        }
    }

    boolean isUnsequenced(AccordCacheEntry<?, ?, ?> entry)
    {
        return isUnsequenced() && (entry.isCommandsForKey() || !isIncremental());
    }

    boolean isOptional(AccordCacheEntry<?, ?, ?> entry)
    {
        return isNonSync() && entry.isCommandsForKey();
    }

    boolean holdsLocksBetweenRuns()
    { // TODO (desired): encode as a state bit
        return isIncremental() && context.primaryTxnId() != null;
    }

    private void waitOnCacheQueuesExclusive()
    {
        Invariants.require(waitingForState == 0);
        loadedAt = Math.max(createdAt, nanoTime());
        executor().runnable.incrementArrivals(this);
        commandStore.exclusiveExecutor().incrementArrivals(this);

        this.refs.forEach((key, safeState) -> {
            AccordCacheEntry<?, ?, ?> entry = global(safeState);
            boolean optional = isOptional(entry);
            if (entry.isLoaded())
            {
                RunnableStatus status = addToCacheQueue(entry, false);
                if (optional) addQueuedOptionalKey(entry, status);
                else if (status == NOT_RUNNABLE)
                    ++waitingForState;
            }
            else Invariants.require(optional);
        });

        // TODO (desired): exception-safe rollback for addUnsequenced
        setCacheQueuedExclusive();
        if (waitingForState == 0) waitOnOptionalCacheQueuesExclusive();
        else
        {
            setStateExclusive(WAITING_ON_REQUIRED);
            executor().waiting.enqueue(this);
        }
    }

    private void waitOnOptionalCacheQueuesExclusive()
    {
        if (isSync() || nonSync.isWaitReady(this)) waitToRunExclusive();
        else
        {
            setStateExclusive(WAITING_ON_OPTIONAL);
            executor().waiting.enqueue(this);
        }
    }

    RunnableStatus addToCacheQueue(AccordCacheEntry<?, ?, ?> loaded, boolean addIfFifo)
    {
        if (isCacheQueuedFifo()) return addIfFifo ? loaded.addFifo(this) : loaded.headStatus(this);
        else if (isUnsequenced(loaded)) return loaded.addUnsequenced(this);
        else return loaded.addPrioritised(this);
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
                case WAITING_ON_REQUIRED:
                case WAITING_ON_OPTIONAL:
                case WAITING_TO_RUN:
                case PREPARED:
                    RunnableStatus status = addToCacheQueue(loaded, false);
                    if (status != NOT_RUNNABLE)
                        addQueuedOptionalKey(loaded, status);
                    // fall-through
                case LOADING_REQUIRED:
                    nonSync.addLoaded();
                    break;

                case LOADING_OPTIONAL:
                    addLoadedOptionalKey();
                    break;
            }
        }
        else
        {
            if (--waitingForState == 0)
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
        nonSync.addLoaded();
        if (is(LOADING_OPTIONAL) && nonSync.isLoaded(this))
        {
            unqueue(executor().loading);
            waitOnCacheQueuesExclusive();
        }
    }

    // TODO (expected): add vs setup vs onChange; some callers don't need to try
    void addQueuedOptionalKey(AccordCacheEntry<?, ?, ?> loaded, RunnableStatus status)
    {
        switch (status)
        {
            default: throw UnhandledEnum.unknown(status);
            case NOT_RUNNABLE: break;
            case STILL_RUNNABLE:
            case NEWLY_RUNNABLE:
                nonSync.onNewHead(loaded);
                break;
            case STILL_RUNNABLE_NEWLY_BLOCKING:
            case NEWLY_BLOCKING_RUNNABLE:
                nonSync.onNewBlockingHead(loaded);
                break;
        }

        if (is(WAITING_ON_OPTIONAL) && nonSync.isWaitReady(this))
        {
            unqueue(executor().waiting);
            waitToRunExclusive();
        }
    }

    void onChangeHeadStatus(AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
    {
        if (isSync() || !entry.isCommandsForKey()) onChangeRequiredHeadStatus(status);
        if (isNonSync() && entry.isCommandsForKey()) onChangeOptionalHeadStatus(entry, status);
    }

    private void incrementWaitingWhileAlreadyWaiting()
    {
        Invariants.require(isState(WAITING));
        if (waitingForState == 0)
        {
            if (is(WAITING_ON_OPTIONAL)) setStateExclusive(WAITING_ON_REQUIRED);
            else
            {
                // TODO (expected): this is potentially costly; maybe we don't want to swap these in and out (but harder to maintain invariants)
                unqueue(commandStore.exclusiveExecutor());
                setStateExclusive(WAITING_ON_REQUIRED);
                executor().waiting.enqueue(this);
            }
        }
        Invariants.require(waitingForState < refs.size());
        ++waitingForState;
    }

    void onChangeRequiredHeadStatus(RunnableStatus newStatus)
    {
        if (newStatus == NOT_RUNNABLE)
        {
            incrementWaitingWhileAlreadyWaiting();
        }
        else if (newStatus != STILL_RUNNABLE_NEWLY_BLOCKING)
        {
            Invariants.require(is(WAITING_ON_REQUIRED));
            if (--waitingForState == 0)
            {
                unqueue(executor().waiting);
                waitOnOptionalCacheQueuesExclusive();
            }
        }
    }

    void onChangeOptionalHeadStatus(AccordCacheEntry<?, ?, ?> entry, RunnableStatus status)
    {
        Invariants.require(isState(WAITING_OR_PREPARED));
        switch (status)
        {
            default: throw UnhandledEnum.unknown(status);
            case STILL_RUNNABLE: throw UnhandledEnum.invalid(STILL_RUNNABLE); // onChange -> changed (but this means no change)
            case NOT_RUNNABLE:
                nonSync.onNotHead(entry);
                if (is(WAITING_TO_RUN) && !nonSync.isWaitReady(this))
                {
                    unqueue(commandStore.exclusiveExecutor());
                    setStateExclusive(WAITING_ON_OPTIONAL);
                    executor().waiting.enqueue(this);
                }
                return;

            case STILL_RUNNABLE_NEWLY_BLOCKING:
                nonSync.onStillHeadNewBlocking(entry);
                break;

            case NEWLY_RUNNABLE:
                nonSync.onNewHead(entry);
                break;

            case NEWLY_BLOCKING_RUNNABLE:
                nonSync.onNewBlockingHead(entry);
                break;
        }

        if (is(WAITING_ON_OPTIONAL) && nonSync.isWaitReady(this))
        {
            unqueue(executor().waiting);
            waitToRunExclusive();
        }
    }

    void waitToRunExclusive()
    {
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
        try
        {
            if (ranges instanceof SafeTask<?>.RangeTxnScanner)
                ranges = ((SafeTask<?>.RangeTxnScanner)ranges).finish(commandStore.cachesExclusive());

            if (isSync())
            {
                refs.forEach((k, v) -> {
                    preExecute(v, this, RELEASE_QUEUE);
                });
            }
            else
            {
                if (!hasIncrementalStarted())
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

                    if (isIncremental() && isSequencedByPriorityAtomic() && !isCacheQueuedFifo())
                    {
                        setCacheQueuedFifoExclusive();
                        refs.forEach((key, safeState) -> {
                            AccordCacheEntry<?, ?, ?> entry = global(safeState);
                            RunnableStatus status = entry.moveToFifo(this);
                            if (entry.isLoaded())
                            {
                                switch (status)
                                {
                                    default: throw UnhandledEnum.unknown(status);
                                    case NOT_RUNNABLE:
                                    case STILL_RUNNABLE:
                                    case STILL_RUNNABLE_NEWLY_BLOCKING:
                                        break;
                                    case NEWLY_RUNNABLE:
                                        nonSync.onNewHead(entry);
                                        break;
                                    case NEWLY_BLOCKING_RUNNABLE:
                                        nonSync.onNewBlockingHead(entry);
                                        break;
                                }
                            }
                        });
                    }

                    if (isIncremental())
                        setIncrementalStartedExclusive();
                }
                nonSync.prepareExclusive(this);
            }
        }
        catch (Throwable t)
        {
            refs.forEach((k, v) -> { v.setAbandoned(); });
            throw t;
        }
    }

    @Override
    public boolean runMayThrow()
    {
        try
        {
            SaferCommandStore safeStore = new SaferCommandStore(this, isSync() ? context : nonSync);
            commandStore.begin(safeStore);
            try
            {
                if (Tracing.isTracing())
                    Tracing.trace(context.describe());

                R result = function.apply(safeStore);
                boolean finished = !isIncremental() || isIncrementalFinishing();
                if (!finished) setRunState(RUN_INCOMPLETE);
                else
                {
                    List<Journal.CommandUpdate> changes = new ArrayList<>();
                    // TODO (expected): save any TxnId we add so that we don't need to iterate all of refs
                    refs.forEach((key, value) -> {
                        if (value instanceof SaferCommand)
                        {
                            SaferCommand safeCommand = (SaferCommand) value;
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
        if (callback == null) executor().agent.onException(failure);
        else
        {
            BiConsumer<?, Throwable> reportTo = callback;
            callback = null;

            if (executor().isInLoop()) reportTo.accept(null, failure);
            else executor().submit(() -> reportTo.accept(null, failure));
        }
    }

    @Override
    void maybeCompleteExclusiveMayThrow()
    {
        if (isNonSync() && !isEither(NOT_YET_RUN, RUN_FAILED))
        {
            if (is(PREPARED))
            {
                nonSync.postRunExclusive(this);
                if (isIncremental())
                {
                    long now = nanoTime();
                    ((IncrementalState)nonSync).updateMetrics(now - runningAt, runningAt - loadedAt);
                    if (!isIncrementalFinishing())
                    {
                        setStateExclusive(INCOMPLETE);
                        waitOnOptionalCacheQueuesExclusive();
                        return;
                    }
                }
            }
            else
            {
                Invariants.expect(is(FAILED));
                Invariants.expect(isIncremental());
                setRunState(RUN_FAILED);
                refs.forEach((k, v) -> v.setAbandoned());
            }
        }

        releaseResourcesExclusive(commandStore.cachesExclusive());

        AccordExecutor executor = executor();
        if (completeState())
        {
            long completeAt = nanoTime();
            executor.elapsedPreparingToRun.increment(loadedAt - createdAt, runningAt);
            if (isIncremental())
            {
                IncrementalState incrementalState = (IncrementalState) nonSync;
                executor.elapsedWaitingToRun.increment(incrementalState.waiting, runningAt);
                executor.elapsedRunning.increment(incrementalState.running, completeAt);
            }
            else
            {
                executor.elapsedWaitingToRun.increment(runningAt - loadedAt, runningAt);
                executor.elapsedRunning.increment(completeAt - runningAt, completeAt);
            }
            executor.elapsed.increment(completeAt - createdAt, completeAt);
            executor.keys.increment(keys, runningAt);
            if (histogramBuffer != null)
            {
                histogramBuffer.flush(runningAt);
                histogramBuffer = null;
            }
        }
        else if (histogramBuffer != null)
        {
            histogramBuffer.clear();
        }
    }

    @Override
    public void cancel()
    {
        if (!state().hasStarted())
            executor().submit(Task::tryCancelExclusive, CancelTask::new, this);
    }

    void tryCancelExclusive()
    {
        State state = state();
        switch (state)
        {
            default: throw new UnhandledEnum(state);
            case UNREGISTERED:
                failExclusive(new CancellationException(), CANCELLED_UNREGISTERED);
                break;

            case REGISTERED:
            case SCANNING_RANGES:
            case LOADING_REQUIRED:
            case LOADING_OPTIONAL:
            case WAITING_ON_REQUIRED:
            case WAITING_ON_OPTIONAL:
            case WAITING_TO_RUN:
                if (!hasIncrementalStarted())
                    cancelExclusive();
                break;

            case CANCELLED_UNREGISTERED:
            case INCOMPLETE:
            case PREPARED:
            case EXECUTED:
            case CANCELLED:
            case FAILED:
                // cannot safely cancel
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
        if (callback != null)
        {
            try { callback.accept(result, null); }
            catch (Throwable t) { commandStore.agent().onException(t); }
        }
    }

    void releaseResourcesExclusive(Caches caches)
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
            releaseResourcesSlowExclusive(t);
            commandStore.agent().onException(t);
        }
        finally
        {
            refs = null;
        }
    }

    private void releaseResourcesSlowExclusive(Throwable suppressedBy)
    {
        if (refs == null)
            return;

        refs.forEach((k, safeState) -> {
            if (!safeState.isReleased())
            {
                try { SaferState.postExecute(safeState, this); }
                catch (Throwable t) { suppressedBy.addSuppressed(t); }
            }
        });
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
                CommandsForKeyAccessor.findAllKeysBetween(commandStore.id(), commandStore.tableId(), getPartitioner(),
                                                          (TokenKey) range.start(), range.startInclusive(),
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

                    refs.put(entry.key(), commandsForKeyCache.acquire(entry));
                    ++keys;

                    Invariants.require(!isCacheQueuedFifo(), "Unsafe to addFifo in listener as no ordering guarantees");
                    if (isOptional(entry))
                        addLoadedOptionalKey();

                    if (isCacheQueued())
                    {
                        Invariants.require(isState(WAITING));
                        RunnableStatus status = addToCacheQueue(entry, false);
                        if (isOptional(entry)) addQueuedOptionalKey(entry, status);
                        else if (status == NOT_RUNNABLE) incrementWaitingWhileAlreadyWaiting();
                    }
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
            onSetupOrScannedExclusive();
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
        return true;
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

    private static LoadKeys loadKeys(ExecutionContext context)
    {
        LoadKeys loadKeys = context.loadKeys();
        if (NONSYNC_ENABLED)
            return loadKeys;
        return loadKeys == NONE ? NONE : SYNC;
    }
}
