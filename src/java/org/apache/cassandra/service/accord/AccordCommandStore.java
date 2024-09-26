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

import java.util.ArrayList;
import java.util.Collections;
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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.impl.TimestampsForKey;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStore.FlushReason;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.local.SaveStatus.Applying;
import static accord.local.Status.Applied;
import static accord.local.Status.Invalidated;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;
import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

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
    private final AccordStateCache.Instance<Key, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKeyCache;
    private final AccordStateCache.Instance<Key, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
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
                              AccordStateCacheMetrics cacheMetrics)
    {
        this(id,
             time,
             agent,
             dataStore,
             progressLogFactory,
             listenerFactory,
             epochUpdateHolder,
             journal,
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

    @VisibleForTesting
    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              ExecutorPlus loadExecutor,
                              ExecutorPlus saveExecutor,
                              AccordStateCacheMetrics cacheMetrics)
    {
        super(id, time, agent, dataStore, progressLogFactory, listenerFactory, epochUpdateHolder);
        this.journal = journal;
        loggingId = String.format("[%s]", id);
        executor = executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']');
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
            stateCache.instance(Key.class,
                                AccordSafeTimestampsForKey.class,
                                AccordSafeTimestampsForKey::new,
                                this::loadTimestampsForKey,
                                this::saveTimestampsForKey,
                                this::validateTimestampsForKey,
                                AccordObjectSizes::timestampsForKey);
        registerJfrListener(id, timestampsForKeyCache, "TimestampsForKey");
        commandsForKeyCache =
            stateCache.instance(Key.class,
                                AccordSafeCommandsForKey.class,
                                AccordSafeCommandsForKey::new,
                                this::loadCommandsForKey,
                                this::saveCommandsForKey,
                                this::validateCommandsForKey,
                                AccordObjectSizes::commandsForKey,
                                AccordCachingState::new);
        registerJfrListener(id, commandsForKeyCache, "CommandsForKey");

        this.commandsForRangesLoader = new CommandsForRangesLoader(this);

//        AccordKeyspace.loadCommandStoreMetadata(id, ((rejectBefore, durableBefore, redundantBefore, bootstrapBeganAt, safeToRead) -> {
//            executor.submit(() -> {
//                if (rejectBefore != null)
//                    super.setRejectBefore(rejectBefore);
//                if (durableBefore != null)
//                    super.setDurableBefore(DurableBefore.merge(durableBefore, durableBefore()));
//                if (redundantBefore != null)
//                    super.setRedundantBefore(RedundantBefore.merge(redundantBefore, redundantBefore));
//                if (bootstrapBeganAt != null)
//                    super.setBootstrapBeganAt(bootstrapBeganAt);
//                if (safeToRead != null)
//                    super.setSafeToRead(safeToRead);
//            });
//        }));

        executor.execute(() -> CommandStore.register(this));
    }

    static Factory factory(AccordJournal journal, AccordStateCacheMetrics cacheMetrics)
    {
        return (id, time, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch) ->
               new AccordCommandStore(id, time, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, cacheMetrics);
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

    public AccordStateCache.Instance<Key, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKeyCache()
    {
        return timestampsForKeyCache;
    }

    public AccordStateCache.Instance<Key, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    @Nullable
    @VisibleForTesting
    public Runnable appendToKeyspace(Command before, Command after)
    {
        if (after.keysOrRanges() != null && after.keysOrRanges() instanceof Keys)
            return null;

        Mutation mutation = AccordKeyspace.getCommandMutation(this.id, before, after, nextSystemTimestampMicros());

        // TODO (required): make sure we test recovering when this has failed to be persisted
        if (null != mutation)
            return mutation::applyUnsafe;

        return null;
    }

    public void persistStoreState()
    {
        journal.appendRedundantBefore(id, redundantBefore());
    }
    @Nullable
    @VisibleForTesting
    public void appendToLog(Command before, Command after, Runnable onFlush)
    {
        JournalKey key = new JournalKey(after.txnId(), JournalKey.Type.COMMAND_DIFF, id);
        journal.append(key, Collections.singletonList(SavedCommand.diff(before, after)), onFlush);
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

        TimestampsForKey reloaded = AccordKeyspace.unsafeLoadTimestampsForKey(this, (PartitionKey) key);
        return Objects.equals(evicting, reloaded);
    }

    TimestampsForKey loadTimestampsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadTimestampsForKey(this, (PartitionKey) key);
    }

    CommandsForKey loadCommandsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadCommandsForKey(this, (PartitionKey) key);
    }

    boolean validateCommandsForKey(RoutableKey key, CommandsForKey evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        CommandsForKey reloaded = AccordKeyspace.loadCommandsForKey(this, (PartitionKey) key);
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
                                                 NavigableMap<Key, AccordSafeTimestampsForKey> timestampsForKeys,
                                                 NavigableMap<Key, AccordSafeCommandsForKey> commandsForKeys,
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
        executor.shutdown();
        try
        {
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }
        catch (InterruptedException t)
        {
            throw new RuntimeException("Could not shut down command store " + this);
        }
    }

    public void registerHistoricalTransactions(Deps deps, SafeCommandStore safeStore)
    {
        // TODO:
        // journal.registerHistoricalTransactions(id(), deps);

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

    public void appendCommands(List<SavedCommand.DiffWriter> commands, Runnable onFlush)
    {
        for (int i = 0; i < commands.size(); i++)
        {
            JournalKey key = new JournalKey(commands.get(i).after().txnId(), JournalKey.Type.COMMAND_DIFF, id);
            boolean isLast = i == commands.size() - 1;
            journal.append(key, commands.get(i), isLast  ? onFlush : null);
        }
    }

    @VisibleForTesting
    public Command loadCommand(TxnId txnId)
    {
        return journal.loadCommand(id, txnId);
    }

    public Future<?> prepareForGC(@Nonnull Timestamp gcBefore, @Nonnull Ranges ranges)
    {
        Invariants.nonNull(gcBefore, "gcBefore should not be null");
        Invariants.nonNull(ranges, "ranges should not be null");
        ListMultimap<TableId, org.apache.cassandra.dht.Range<Token>> toPrepare = ArrayListMultimap.create();
        for (Range r : ranges)
        {
            TokenRange tr = (TokenRange)r;
            TableId tableId = ((TokenRange) r).table();
            toPrepare.put(tableId, tr.toKeyspaceRange());
        }

        List<Future<CommitLogPosition>> flushes = new ArrayList<>(toPrepare.keySet().size());
        for (TableId tableId : toPrepare.keySet())
        {
            List<org.apache.cassandra.dht.Range<Token>> tableRanges = toPrepare.get(tableId);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
            if (cfs == null)
            {
                // TODO (review): Error handling?
                logger.warn("Cannot prepare CFS with tableId {} for Accord GC because it does not exist", tableId);
                continue;
            }

            Memtable currentMemtable = cfs.getCurrentMemtable();
            if (currentMemtable.getMinTimestamp() > gcBefore.hlc())
                continue;

            boolean intersects = false;
            // TrieMemtable doesn't support reverse iteration so can't find the last token
            if (currentMemtable instanceof TrieMemtable)
                intersects = true;
            else
            {
                Token firstToken = null;
                try (UnfilteredPartitionIterator iterator = currentMemtable.partitionIterator(ColumnFilter.all(cfs.metadata()), DataRange.allData(cfs.getPartitioner()), SSTableReadsListener.NOOP_LISTENER))
                {
                    if (iterator.hasNext())
                        firstToken = iterator.next().partitionKey().getToken();
                }
                Token lastToken = currentMemtable.lastToken();

                if (firstToken != null)
                {
                    checkState(lastToken != null);
                    if (firstToken.equals(lastToken))
                    {
                        for (org.apache.cassandra.dht.Range<Token> tableRange : tableRanges)
                        {
                            if (tableRange.contains(firstToken))
                            {
                                intersects = true;
                                break;
                            }
                        }
                    }
                    else
                    {
                        checkState(firstToken.compareTo(lastToken) < 0);
                        org.apache.cassandra.dht.Range<Token> memtableRange = new org.apache.cassandra.dht.Range<>(firstToken, lastToken);
                        for (org.apache.cassandra.dht.Range<Token> tableRange : tableRanges)
                        {
                            if (tableRange.intersects(memtableRange))
                            {
                                intersects = true;
                                break;
                            }
                        }
                    }
                }
            }

            if (intersects)
                flushes.add(cfs.forceFlush(FlushReason.ACCORD_TXN_GC));
        }

        if (flushes.isEmpty())
            return ImmediateFuture.success(null);
        else
            return FutureCombiner.allOf(flushes);
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
                Keys keys = null;
                List<TxnId> deps = null;
                if (CommandsForKey.manages(txnId))
                    keys = (Keys) command.keysOrRanges();
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
                                Cleanup cleanup = Cleanup.shouldCleanup(AccordCommandStore.this, local, null, local.route(), false);
                                switch (cleanup)
                                {
                                    case NO:
                                        break;
                                    case INVALIDATE:
                                    case TRUNCATE_WITH_OUTCOME:
                                    case TRUNCATE:
                                    case ERASE:
                                        local = Commands.purge(local, local.route(), cleanup);
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
                             if (local.is(Stable) && !local.hasBeen(Applied))
                                 Commands.maybeExecute(safeStore, safeCommand, local, true, true);
                             else if (local.saveStatus().compareTo(Applying) >= 0 && !local.is(Invalidated) && !local.is(Truncated))
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
}
