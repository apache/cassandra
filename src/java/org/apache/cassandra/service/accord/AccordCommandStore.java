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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.TimestampsForKey;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.DurableBefore;
import accord.local.KeyHistory;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Deps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.checkState;

public class AccordCommandStore extends CommandStore implements CacheSize
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);
    private static final boolean CHECK_THREADS = CassandraRelevantProperties.TEST_ACCORD_STORE_THREAD_CHECKS_ENABLED.getBoolean();

    private static long getThreadId(ExecutorService executor)
    {
        if (!CHECK_THREADS)
            return 0;
        try
        {
            return executor.submit(() -> Thread.currentThread().getId()).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final long threadId;
    public final String loggingId;
    private final IJournal journal;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache;
    private final AccordStateCache.Instance<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKeyCache;
    private final AccordStateCache.Instance<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
    private AsyncOperation<?> currentOperation = null;
    private AccordSafeCommandStore current = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private final CommandsForRangesLoader commandsForRangesLoader;

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              AccordStateCacheMetrics cacheMetrics,
                              ExecutorService executor)
    {
        this(id,
             time,
             agent,
             dataStore,
             progressLogFactory,
             listenerFactory,
             epochUpdateHolder,
             journal,
             executor,
             Stage.READ.executor(),
             Stage.MUTATION.executor(),
             cacheMetrics);
    }

    private static <K, V> void registerJfrListener(int id, AccordStateCache.Instance<K, V, ?> instance, String name)
    {
        if (!DatabaseDescriptor.getAccordStateCacheListenerJFREnabled())
            return;
        instance.register(new AccordStateCache.Listener<K, V>() {
            private final IdentityHashMap<AccordCachingState<?, ?>, CacheEvents.Evict> pendingEvicts = new IdentityHashMap<>();

            @Override
            public void onAdd(AccordCachingState<K, V> state)
            {
                CacheEvents.Add add = new CacheEvents.Add();
                CacheEvents.Evict evict = new CacheEvents.Evict();
                if (!add.isEnabled())
                    return;
                add.begin();
                evict.begin();
                add.store = evict.store = id;
                add.instance = evict.instance = name;
                add.key = evict.key = state.key().toString();
                updateMutable(instance, state, add);
                add.commit();
                pendingEvicts.put(state, evict);
            }

            @Override
            public void onRelease(AccordCachingState<K, V> state)
            {

            }

            @Override
            public void onEvict(AccordCachingState<K, V> state)
            {
                CacheEvents.Evict event = pendingEvicts.remove(state);
                if (event == null) return;
                updateMutable(instance, state, event);
                event.commit();
            }
        });
    }

    private static void updateMutable(AccordStateCache.Instance<?, ?, ?> instance, AccordCachingState<?, ?> state, CacheEvents event)
    {
        event.status = state.state().status().name();

        event.lastQueriedEstimatedSizeOnHeap = state.lastQueriedEstimatedSizeOnHeap();

        event.instanceAllocated = instance.weightedSize();
        AccordStateCache.Stats stats = instance.stats();
        event.instanceStatsQueries = stats.queries;
        event.instanceStatsHits = stats.hits;
        event.instanceStatsMisses = stats.misses;

        event.globalSize = instance.size();
        event.globalReferenced = instance.globalReferencedEntries();
        event.globalUnreferenced = instance.globalUnreferencedEntries();
        event.globalCapacity = instance.capacity();
        event.globalAllocated = instance.globalAllocated();

        stats = instance.globalStats();
        event.globalStatsQueries = stats.queries;
        event.globalStatsHits = stats.hits;
        event.globalStatsMisses = stats.misses;

        event.update();
    }

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              ExecutorService commandStoreExecutor,
                              ExecutorPlus loadExecutor,
                              ExecutorPlus saveExecutor,
                              AccordStateCacheMetrics cacheMetrics)
    {
        super(id, time, agent, dataStore, progressLogFactory, listenerFactory, epochUpdateHolder);
        this.journal = journal;
        loggingId = String.format("[%s]", id);
        executor = commandStoreExecutor;
        threadId = getThreadId(executor);
        stateCache = new AccordStateCache(loadExecutor, saveExecutor, 8 << 20, cacheMetrics);
        commandCache =
            stateCache.instance(TxnId.class,
                                AccordSafeCommand.class,
                                AccordSafeCommand.safeRefFactory(),
                                this::loadCommand,
                                this::appendToKeyspace,
                                this::validateCommand,
                                AccordObjectSizes::command);
        registerJfrListener(id, commandCache, "Command");
        timestampsForKeyCache =
            stateCache.instance(RoutingKey.class,
                                AccordSafeTimestampsForKey.class,
                                AccordSafeTimestampsForKey::new,
                                this::loadTimestampsForKey,
                                this::saveTimestampsForKey,
                                this::validateTimestampsForKey,
                                AccordObjectSizes::timestampsForKey);
        registerJfrListener(id, timestampsForKeyCache, "TimestampsForKey");
        commandsForKeyCache =
            stateCache.instance(RoutingKey.class,
                                AccordSafeCommandsForKey.class,
                                AccordSafeCommandsForKey::new,
                                this::loadCommandsForKey,
                                this::saveCommandsForKey,
                                this::validateCommandsForKey,
                                AccordObjectSizes::commandsForKey,
                                AccordCachingState::new);
        registerJfrListener(id, commandsForKeyCache, "CommandsForKey");

        this.commandsForRangesLoader = new CommandsForRangesLoader(this);

        loadRedundantBefore(journal.loadRedundantBefore(id()));
        loadDurableBefore(journal.loadDurableBefore(id()));
        loadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
        loadSafeToRead(journal.loadSafeToRead(id()));
        loadRangesForEpoch(journal.loadRangesForEpoch(id()));
        loadHistoricalTransactions(journal.loadHistoricalTransactions(id()));

        executor.execute(() -> CommandStore.register(this));
    }

    static Factory factory(AccordJournal journal, AccordStateCacheMetrics cacheMetrics, IntFunction<ExecutorService> executorFactory)
    {
        return (id, time, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch) ->
               new AccordCommandStore(id, time, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, cacheMetrics, executorFactory.apply(id));
    }

    public CommandsForRangesLoader diskCommandsForRanges()
    {
        return commandsForRangesLoader;
    }

    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        store.snapshot(ranges, globalSyncId);
        super.markShardDurable(safeStore, globalSyncId, ranges);
    }

    @Override
    public boolean inStore()
    {
        if (!CHECK_THREADS)
            return true;
        return Thread.currentThread().getId() == threadId;
    }

    @Override
    public void setCapacity(long bytes)
    {
        checkInStoreThread();
        stateCache.setCapacity(bytes);
    }

    @Override
    public long capacity()
    {
        return stateCache.capacity();
    }

    @Override
    public int size()
    {
        return stateCache.size();
    }

    @Override
    public long weightedSize()
    {
        return stateCache.weightedSize();
    }

    public void checkInStoreThread()
    {
        checkState(inStore());
    }

    public void checkNotInStoreThread()
    {
        if (!CHECK_THREADS)
            return;
        checkState(!inStore());
    }

    public ExecutorService executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKeyCache()
    {
        return timestampsForKeyCache;
    }

    public AccordStateCache.Instance<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    @Nullable
    @VisibleForTesting
    public Runnable appendToKeyspace(Command before, Command after)
    {
        if (after.txnId().is(Routable.Domain.Key))
            return null;

        Mutation mutation = AccordKeyspace.getCommandMutation(this.id, before, after, nextSystemTimestampMicros());

        // TODO (required): make sure we test recovering when this has failed to be persisted
        if (null != mutation)
            return mutation::applyUnsafe;

        return null;
    }

    public void persistFieldUpdates(AccordSafeCommandStore.FieldUpdates fieldUpdates, Runnable onFlush)
    {
        journal.persistStoreState(id, fieldUpdates, onFlush);
    }


    @Nullable
    @VisibleForTesting
    public void appendToLog(Command before, Command after, Runnable onFlush)
    {
        journal.appendCommand(id, SavedCommand.diff(before, after), onFlush);
    }

    boolean validateCommand(TxnId txnId, Command evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        Command reloaded = loadCommand(txnId);
        return Objects.equals(evicting, reloaded);
    }

    @VisibleForTesting
    public void sanityCheckCommand(Command command)
    {
        ((AccordJournal) journal).sanityCheck(id, command);
    }

    boolean validateTimestampsForKey(RoutableKey key, TimestampsForKey evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        TimestampsForKey reloaded = AccordKeyspace.unsafeLoadTimestampsForKey(this, (TokenKey) key);
        return Objects.equals(evicting, reloaded);
    }

    TimestampsForKey loadTimestampsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadTimestampsForKey(this, (TokenKey) key);
    }

    CommandsForKey loadCommandsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadCommandsForKey(this, (TokenKey) key);
    }

    boolean validateCommandsForKey(RoutableKey key, CommandsForKey evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        CommandsForKey reloaded = AccordKeyspace.loadCommandsForKey(this, (TokenKey) key);
        return Objects.equals(evicting, reloaded);
    }

    @Nullable
    private Runnable saveTimestampsForKey(TimestampsForKey before, TimestampsForKey after)
    {
        Mutation mutation = AccordKeyspace.getTimestampsForKeyMutation(id, before, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
    }

    @Nullable
    private Runnable saveCommandsForKey(CommandsForKey before, CommandsForKey after)
    {
        Mutation mutation = AccordKeyspace.getCommandsForKeyMutation(id, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
    }

    @VisibleForTesting
    public AccordStateCache cache()
    {
        return stateCache;
    }

    @VisibleForTesting
    public void unsafeClearCache()
    {
        stateCache.unsafeClear();
    }

    public void setCurrentOperation(AsyncOperation<?> operation)
    {
        checkState(currentOperation == null);
        currentOperation = operation;
    }

    public AsyncOperation<?> getContext()
    {
        checkState(currentOperation != null);
        return currentOperation;
    }

    public void unsetCurrentOperation(AsyncOperation<?> operation)
    {
        checkState(currentOperation == operation);
        currentOperation = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AsyncOperation.create(this, loadCtx, function);
    }

    @Override
    public <T> AsyncChain<T> submit(Callable<T> task)
    {
        return AsyncChains.ofCallable(executor, task);
    }

    public DataStore dataStore()
    {
        return store;
    }

    NodeTimeService time()
    {
        return time;
    }

    ProgressLog progressLog()
    {
        return progressLog;
    }

    @Override
    public AsyncChain<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AsyncOperation.create(this, preLoadContext, consumer);
    }

    public void executeBlocking(Runnable runnable)
    {
        try
        {
            executor.submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AccordSafeCommandStore beginOperation(PreLoadContext preLoadContext,
                                                 Map<TxnId, AccordSafeCommand> commands,
                                                 NavigableMap<RoutingKey, AccordSafeTimestampsForKey> timestampsForKeys,
                                                 NavigableMap<RoutingKey, AccordSafeCommandsForKey> commandsForKeys,
                                                 @Nullable AccordSafeCommandsForRanges commandsForRanges)
    {
        checkState(current == null);
        commands.values().forEach(AccordSafeState::preExecute);
        commandsForKeys.values().forEach(AccordSafeState::preExecute);
        timestampsForKeys.values().forEach(AccordSafeState::preExecute);
        if (commandsForRanges != null)
            commandsForRanges.preExecute();

        current = AccordSafeCommandStore.create(preLoadContext, commands, timestampsForKeys, commandsForKeys, commandsForRanges, this);
        return current;
    }

    public boolean hasSafeStore()
    {
        return current != null;
    }

    public void completeOperation(AccordSafeCommandStore store)
    {
        checkState(current == store);
        try
        {
            current.postExecute();
        }
        finally
        {
            current = null;
        }
    }

    public void abortCurrentOperation()
    {
        current = null;
    }

    @Override
    public void shutdown()
    {
    }

    public void registerHistoricalTransactions(Deps deps, SafeCommandStore safeStore)
    {
        if (deps.isEmpty()) return;

        CommandStores.RangesForEpoch ranges = safeStore.ranges();
        // used in places such as accord.local.CommandStore.fetchMajorityDeps
        // We find a set of dependencies for a range then update CommandsFor to know about them
        Ranges allRanges = safeStore.ranges().all();
        deps.keyDeps.keys().forEach(allRanges, key -> {
            // TODO (now): batch register to minimise GC
            deps.keyDeps.forEach(key, (txnId, txnIdx) -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).contains(key))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).contains(key))
                    return;

                safeStore.get(key).registerHistorical(safeStore, txnId);
            });
        });
        for (int i = 0; i < deps.rangeDeps.rangeCount(); i++)
        {
            var range = deps.rangeDeps.range(i);
            if (!allRanges.intersects(range))
                continue;
            deps.rangeDeps.forEach(range, txnId -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).intersects(range))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).intersects(range))
                    return;

                diskCommandsForRanges().mergeHistoricalTransaction(txnId, Ranges.single(range).slice(allRanges), Ranges::with);
            });
        }
    }

    public NavigableMap<Timestamp, Ranges> safeToRead() { return super.safeToRead(); }

    public void appendCommands(List<SavedCommand.DiffWriter> diffs, Runnable onFlush)
    {
        for (int i = 0; i < diffs.size(); i++)
        {
            boolean isLast = i == diffs.size() - 1;
            SavedCommand.DiffWriter writer = diffs.get(i);
            journal.appendCommand(id, writer, isLast  ? onFlush : null);
        }
    }

    @VisibleForTesting
    public Command loadCommand(TxnId txnId)
    {
        return journal.loadCommand(id, txnId);
    }

    public interface Loader
    {
        Promise<?> load(Command next);
        Promise<?> apply(Command next);
    }

    public Loader loader()
    {
        return new Loader()
        {
            private PreLoadContext context(Command command, KeyHistory keyHistory)
            {
                TxnId txnId = command.txnId();
                Participants<?> keys = null;
                List<TxnId> deps = null;
                if (CommandsForKey.manages(txnId))
                    keys = command.hasBeen(Committed) ? command.participants().hasTouched() : command.participants().touches();
                else if (!CommandsForKey.managesExecution(txnId) && command.hasBeen(Status.Stable) && !command.hasBeen(Status.Truncated))
                    keys = command.asCommitted().waitingOn.keys;

                if (command.partialDeps() != null)
                    deps = command.partialDeps().txnIds();

                if (keys != null)
                {
                    if (deps != null)
                        return PreLoadContext.contextFor(txnId, deps, keys, keyHistory);

                    return PreLoadContext.contextFor(txnId, keys, keyHistory);
                }

                return PreLoadContext.contextFor(txnId);
            }

            public Promise<?> load(Command command)
            {
                TxnId txnId = command.txnId();

                AsyncPromise<?> future = new AsyncPromise<>();
                execute(context(command, KeyHistory.COMMANDS),
                        safeStore -> {
                            Command local = command;
                            if (local.status() != Truncated && local.status() != Invalidated)
                            {
                                Cleanup cleanup = Cleanup.shouldCleanup(AccordCommandStore.this, local, local.participants());
                                switch (cleanup)
                                {
                                    case NO:
                                        break;
                                    case INVALIDATE:
                                    case TRUNCATE_WITH_OUTCOME:
                                    case TRUNCATE:
                                    case ERASE:
                                        local = Commands.purge(local, local.participants(), cleanup);
                                }
                            }

                            local = safeStore.unsafeGet(txnId).update(safeStore, local);
                            if (local.status() == Truncated)
                                safeStore.progressLog().clear(local.txnId());
                        })
                .begin((unused, throwable) -> {
                    if (throwable != null)
                        future.setFailure(throwable);
                    else
                        future.setSuccess(null);
                });
                return future;
            }

            public Promise<?> apply(Command command)
            {
                TxnId txnId = command.txnId();

                AsyncPromise<?> future = new AsyncPromise<>();
                PreLoadContext context = context(command, KeyHistory.TIMESTAMPS);
                execute(context,
                         safeStore -> {
                             SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                             Command local = safeCommand.current();
                             if (local.is(Stable) || local.is(PreApplied))
                                 Commands.maybeExecute(safeStore, safeCommand, local, true, true);
                             else if (local.saveStatus().compareTo(Applying) >= 0 && !local.hasBeen(Truncated))
                                 Commands.applyWrites(safeStore, context, local).begin(agent);
                         })
                .begin((unused, throwable) -> {
                    if (throwable != null)
                        future.setFailure(throwable);
                    else
                        future.setSuccess(null);
                });
                return future;
            }
        };
    }

    /**
     * Replay/state reloading
     */

    void loadRedundantBefore(RedundantBefore redundantBefore)
    {
        if (redundantBefore != null)
            unsafeSetRedundantBefore(redundantBefore);
    }

    void loadDurableBefore(DurableBefore durableBefore)
    {
        if (durableBefore != null)
            unsafeSetDurableBefore(durableBefore);
    }

    void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> bootstrapBeganAt)
    {
        if (bootstrapBeganAt != null)
            unsafeSetBootstrapBeganAt(bootstrapBeganAt);
    }

    void loadSafeToRead(NavigableMap<Timestamp, Ranges> safeToRead)
    {
        if (safeToRead != null)
            unsafeSetSafeToRead(safeToRead);
    }

    void loadRangesForEpoch(CommandStores.RangesForEpoch.Snapshot rangesForEpoch)
    {
        if (rangesForEpoch != null)
            unsafeSetRangesForEpoch(new CommandStores.RangesForEpoch(rangesForEpoch.epochs, rangesForEpoch.ranges, this));
    }

    void loadHistoricalTransactions(List<Deps> deps)
    {
        if (deps != null)
        {
            execute(PreLoadContext.empty(),
                    safeStore -> {
                        for (Deps dep : deps)
                            registerHistoricalTransactions(dep, safeStore);
                    });
        }
    }
}
