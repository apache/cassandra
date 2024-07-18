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
import javax.annotation.Nullable;

import accord.api.Key;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.cfk.CommandsForKey;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.service.accord.events.CacheEvents;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

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
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              AccordStateCacheMetrics cacheMetrics)
    {
        this(id, time, agent, dataStore, progressLogFactory, epochUpdateHolder, journal, Stage.READ.executor(), Stage.MUTATION.executor(), cacheMetrics);
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
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              ExecutorPlus loadExecutor,
                              ExecutorPlus saveExecutor,
                              AccordStateCacheMetrics cacheMetrics)
    {
        super(id, time, agent, dataStore, progressLogFactory, epochUpdateHolder);
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

        AccordKeyspace.loadCommandStoreMetadata(id, ((rejectBefore, durableBefore, redundantBefore, bootstrapBeganAt, safeToRead) -> {
            executor.submit(() -> {
                if (rejectBefore != null)
                    super.setRejectBefore(rejectBefore);
                if (durableBefore != null)
                    super.setDurableBefore(durableBefore);
                if (redundantBefore != null)
                    super.setRedundantBefore(redundantBefore);
                if (bootstrapBeganAt != null)
                    super.setBootstrapBeganAt(bootstrapBeganAt);
                if (safeToRead != null)
                    super.setSafeToRead(safeToRead);
            });
        }));

        executor.execute(() -> CommandStore.register(this));
    }

    static Factory factory(AccordJournal journal, AccordStateCacheMetrics cacheMetrics)
    {
        return (id, time, agent, dataStore, progressLogFactory, rangesForEpoch) ->
               new AccordCommandStore(id, time, agent, dataStore, progressLogFactory, rangesForEpoch, journal, cacheMetrics);
    }

    public CommandsForRangesLoader diskCommandsForRanges()
    {
        return commandsForRangesLoader;
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
        Invariants.checkState(inStore());
    }

    public void checkNotInStoreThread()
    {
        if (!CHECK_THREADS)
            return;
        Invariants.checkState(!inStore());
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

    @Nullable
    @VisibleForTesting
    public void appendToLog(Command before, Command after, Runnable runnable)
    {
        journal.appendCommand(id, Collections.singletonList(SavedCommand.SavedDiff.diff(before, after)), null, runnable);
    }

    boolean validateCommand(TxnId txnId, Command evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        Command reloaded = loadCommand(txnId);
        return Objects.equals(evicting, reloaded);
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
        Invariants.checkState(currentOperation == null);
        currentOperation = operation;
    }

    public AsyncOperation<?> getContext()
    {
        Invariants.checkState(currentOperation != null);
        return currentOperation;
    }

    public void unsetCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == operation);
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
        Invariants.checkState(current == null);
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
        Invariants.checkState(current == store);
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
    }

    protected void setRejectBefore(ReducingRangeMap<Timestamp> newRejectBefore)
    {
        super.setRejectBefore(newRejectBefore);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateRejectBefore(this, newRejectBefore);
    }

    protected void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        super.setBootstrapBeganAt(newBootstrapBeganAt);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateBootstrapBeganAt(this, newBootstrapBeganAt);
    }

    protected void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        super.setSafeToRead(newSafeToRead);
        // TODO (required, correctness): rework to persist via journal once available, this can lose updates in some edge cases
        AccordKeyspace.updateSafeToRead(this, newSafeToRead);
    }

    @Override
    public void setDurableBefore(DurableBefore newDurableBefore)
    {
        super.setDurableBefore(newDurableBefore);
        AccordKeyspace.updateDurableBefore(this, newDurableBefore);
    }

    @Override
    protected void setRedundantBefore(RedundantBefore newRedundantBefore)
    {
        super.setRedundantBefore(newRedundantBefore);
        // TODO (required): this needs to be synchronous, or at least needs to take effect before we rely upon it
        AccordKeyspace.updateRedundantBefore(this, newRedundantBefore);
    }

    public NavigableMap<TxnId, Ranges> bootstrapBeganAt() { return super.bootstrapBeganAt(); }
    public NavigableMap<Timestamp, Ranges> safeToRead() { return super.safeToRead(); }

    public void appendCommands(List<SavedCommand.SavedDiff> commands, List<Command> sanityCheck, Runnable onFlush)
    {
        journal.appendCommand(id, commands, sanityCheck, onFlush);
    }

    @VisibleForTesting
    public Command loadCommand(TxnId txnId)
    {
        return journal.loadCommand(id, txnId);
    }
}
