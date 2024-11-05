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
import java.util.concurrent.Future;
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
import accord.local.KeyHistory;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
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
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.SavedCommand.MinimalCommand;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.local.KeyHistory.COMMANDS;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.service.accord.SavedCommand.Load.MINIMAL;

public class AccordCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);
    private static final boolean CHECK_THREADS = CassandraRelevantProperties.TEST_ACCORD_STORE_THREAD_CHECKS_ENABLED.getBoolean();

    public final String loggingId;
    private final IJournal journal;

    private final CommandStoreExecutor executor;
    private final AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache;
    private final AccordStateCache.Instance<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey> timestampsForKeyCache;
    private final AccordStateCache.Instance<RoutingKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
    private AsyncOperation<?> currentOperation = null;
    private AccordSafeCommandStore current = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private final CommandsForRangesLoader commandsForRangesLoader;

    private static <K, V> void registerJfrListener(int id, AccordStateCache.Instance<K, V, ?> instance, String name)
    {
        if (!DatabaseDescriptor.getAccordStateCacheListenerJFREnabled())
            return;
        instance.register(new AccordStateCache.Listener<>() {
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
                              NodeCommandStoreService node,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              CommandStoreExecutor commandStoreExecutor)
    {
        super(id, node, agent, dataStore, progressLogFactory, listenerFactory, epochUpdateHolder);
        this.journal = journal;
        loggingId = String.format("[%s]", id);
        executor = commandStoreExecutor;
        AccordStateCache stateCache = executor.stateCache;
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
        loadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
        loadSafeToRead(journal.loadSafeToRead(id()));
        loadRangesForEpoch(journal.loadRangesForEpoch(id()));

        executor.execute(() -> CommandStore.register(this));
    }

    static Factory factory(AccordJournal journal, IntFunction<CommandStoreExecutor> executorFactory)
    {
        return (id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch) ->
               new AccordCommandStore(id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, executorFactory.apply(id));
    }

    public CommandsForRangesLoader diskCommandsForRanges()
    {
        return commandsForRangesLoader;
    }

    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        store.snapshot(ranges, globalSyncId);
        super.markShardDurable(safeStore, globalSyncId, ranges);
        commandsForRangesLoader.gcBefore(globalSyncId, ranges);
    }

    @Override
    public boolean inStore()
    {
        return executor.isInThread();
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
        return executor.delegate();
    }

    /**
     * Note that this cache is shared with other commandStores!
     */
    public AccordStateCache cache()
    {
        return executor.cache();
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

    @VisibleForTesting
    @Override
    protected void unsafeSetRangesForEpoch(CommandStores.RangesForEpoch newRangesForEpoch)
    {
        super.unsafeSetRangesForEpoch(newRangesForEpoch);
    }

    @Nullable
    @VisibleForTesting
    public Runnable appendToKeyspace(Command after)
    {
        if (after.txnId().is(Routable.Domain.Key))
            return null;

        Mutation mutation = AccordKeyspace.getCommandMutation(this.id, after, nextSystemTimestampMicros());

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
    private Runnable saveTimestampsForKey(TimestampsForKey after)
    {
        Mutation mutation = AccordKeyspace.getTimestampsForKeyMutation(id, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
    }

    @Nullable
    private Runnable saveCommandsForKey(CommandsForKey after)
    {
        Mutation mutation = AccordKeyspace.getCommandsForKeyMutation(id, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
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
        return AsyncChains.ofCallable(executor.delegate(), task);
    }

    public DataStore dataStore()
    {
        return store;
    }

    NodeCommandStoreService node()
    {
        return node;
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
                                                 Map<RoutingKey, AccordSafeTimestampsForKey> timestampsForKeys,
                                                 Map<RoutingKey, AccordSafeCommandsForKey> commandsForKeys,
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

    public void registerTransitive(SafeCommandStore safeStore, RangeDeps rangeDeps)
    {
        if (rangeDeps.isEmpty())
            return;

        RedundantBefore redundantBefore = unsafeGetRedundantBefore();
        CommandStores.RangesForEpoch ranges = safeStore.ranges();
        // used in places such as accord.local.CommandStore.fetchMajorityDeps
        // We find a set of dependencies for a range then update CommandsFor to know about them
        Ranges allRanges = safeStore.ranges().all();
        Ranges coordinateRanges = Ranges.EMPTY;
        long coordinateEpoch = -1;
        for (int i = 0; i < rangeDeps.txnIdCount(); i++)
        {
            TxnId txnId = rangeDeps.txnId(i);
            AccordCachingState<TxnId, Command> state = commandCache.getUnsafe(txnId);
            if (state != null && state.isLoaded() && state.get() != null && state.get().known().isDefinitionKnown())
                continue;

            Ranges addRanges = rangeDeps.ranges(i).slice(allRanges);
            if (addRanges.isEmpty()) continue;

            if (coordinateEpoch != txnId.epoch())
            {
                coordinateEpoch = txnId.epoch();
                coordinateRanges = ranges.allAt(txnId.epoch());
            }
            if (addRanges.intersects(coordinateRanges)) continue;
            addRanges = redundantBefore.removeShardRedundant(txnId, txnId, addRanges);
            if (addRanges.isEmpty()) continue;
            diskCommandsForRanges().mergeTransitive(txnId, addRanges, Ranges::with);
        }
    }

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
        return journal.loadCommand(id, txnId, unsafeGetRedundantBefore(), durableBefore());
    }

    public MinimalCommand loadMinimal(TxnId txnId)
    {
        return journal.loadMinimal(id, txnId, MINIMAL, unsafeGetRedundantBefore(), durableBefore());
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
                execute(context(command, COMMANDS),
                        safeStore -> {
                            Command local = command;
                            if (local.status() != Truncated && local.status() != Invalidated)
                            {
                                Cleanup cleanup = Cleanup.shouldCleanup(local, unsafeGetRedundantBefore(), durableBefore());
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
                             if (local.hasBeen(Truncated))
                                 return;

                             if (local.saveStatus().compareTo(Applying) >= 0) Commands.applyWrites(safeStore, context, local).begin(agent);
                             else Commands.maybeExecute(safeStore, safeCommand, local, true, true);
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

    public static class CommandStoreExecutor implements CacheSize
    {
        final AccordStateCache stateCache;
        final SequentialExecutorPlus delegate;
        final long threadId;

        CommandStoreExecutor(AccordStateCache stateCache, SequentialExecutorPlus delegate, long threadId)
        {
            this.stateCache = stateCache;
            this.delegate = delegate;
            this.threadId = threadId;
        }

        public boolean hasTasks()
        {
            return delegate.getPendingTaskCount() > 0 || delegate.getActiveTaskCount() > 0;
        }

        CommandStoreExecutor(AccordStateCache stateCache, SequentialExecutorPlus delegate)
        {
            this.stateCache = stateCache;
            this.delegate = delegate;
            this.threadId = getThreadId();
        }

        public boolean isInThread()
        {
            if (!CHECK_THREADS)
                return true;

            return threadId == Thread.currentThread().getId();
        }

        public void shutdown()
        {
            delegate.shutdown();
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return delegate.awaitTermination(timeout, unit);
        }

        public Future<?> submit(Runnable task)
        {
            return delegate.submit(task);
        }

        public ExecutorService delegate()
        {
            return delegate;
        }

        public void execute(Runnable command)
        {
            delegate.submit(command);
        }

        private long getThreadId()
        {
            try
            {
                return delegate.submit(() -> Thread.currentThread().getId()).get();
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

        @Override
        public void setCapacity(long bytes)
        {
            Invariants.checkState(isInThread());
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
    }
}
