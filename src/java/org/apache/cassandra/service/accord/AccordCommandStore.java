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

import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
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
import accord.utils.async.AsyncResults;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.SavedCommand.MinimalCommand;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Promise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.SaveStatus.Applying;
import static accord.local.KeyHistory.SYNC;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Truncated;
import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.service.accord.SavedCommand.Load.MINIMAL;

public class AccordCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);
    private static final boolean CHECK_THREADS = CassandraRelevantProperties.TEST_ACCORD_STORE_THREAD_CHECKS_ENABLED.getBoolean();

    // TODO (required): track this via a PhantomReference, so that if we remove a CommandStore without clearing the caches we can be sure to release them
    public static class Caches
    {
        private final AccordStateCache global;
        private final AccordStateCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        private final AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys;
        private final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys;

        Caches(AccordStateCache global, AccordStateCache.Type<TxnId, Command, AccordSafeCommand>.Instance commandCache, AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeyCache, AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache)
        {
            this.global = global;
            this.commands = commandCache;
            this.timestampsForKeys = timestampsForKeyCache;
            this.commandsForKeys = commandsForKeyCache;
        }

        public final AccordStateCache global()
        {
            return global;
        }

        public final AccordStateCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands()
        {
            return commands;
        }

        public final AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys()
        {
            return timestampsForKeys;
        }

        public final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys()
        {
            return commandsForKeys;
        }
    }

    public static final class ExclusiveCaches extends Caches implements AutoCloseable
    {
        private final Lock lock;

        public ExclusiveCaches(Lock lock, AccordStateCache global, AccordStateCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands, AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys, AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys)
        {
            super(global, commands, timestampsForKeys, commandsForKeys);
            this.lock = lock;
        }

        @Override
        public void close()
        {
            lock.unlock();
        }
    }

    public final String loggingId;
    private final IJournal journal;
    private final AccordExecutor executor;
    private final Executor taskExecutor;
    private final ExclusiveCaches guardedCaches;
    private final Caches unguardedCaches;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private final CommandsForRangesLoader commandsForRangesLoader;

    private AccordSafeCommandStore current;
    private Thread currentThread;

    public AccordCommandStore(int id,
                              NodeCommandStoreService node,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              IJournal journal,
                              AccordExecutor executor)
    {
        super(id, node, agent, dataStore, progressLogFactory, listenerFactory, epochUpdateHolder);
        loggingId = String.format("[%s]", id);
        this.journal = journal;
        this.executor = executor;

        final AccordStateCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        final AccordStateCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKey;
        final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKey;
        try (AccordExecutor.ExclusiveGlobalCaches exclusive = executor.lockCaches())
        {
            commands = exclusive.commands.newInstance(this);
            timestampsForKey = exclusive.timestampsForKey.newInstance(this);
            commandsForKey = exclusive.commandsForKey.newInstance(this);
            this.guardedCaches = new ExclusiveCaches(executor.lock, exclusive.global, commands, timestampsForKey, commandsForKey);
            this.unguardedCaches = new ExclusiveCaches(null, exclusive.global, commands, timestampsForKey, commandsForKey);
        }

        this.taskExecutor = executor.executor(this);
        this.commandsForRangesLoader = new CommandsForRangesLoader(this);
        loadRedundantBefore(journal.loadRedundantBefore(id()));
        loadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
        loadSafeToRead(journal.loadSafeToRead(id()));
        loadRangesForEpoch(journal.loadRangesForEpoch(id()));
    }

    static Factory factory(AccordJournal journal, IntFunction<AccordExecutor> executorFactory)
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
        return currentThread == Thread.currentThread();
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

    public AccordExecutor executor()
    {
        return executor;
    }

    // TODO (desired): we use this for executing callbacks with mutual exclusivity,
    //  but we don't need to block the actual CommandStore - could quite easily
    //  inflate a separate queue dynamically in AccordExecutor
    public Executor taskExecutor()
    {
        return taskExecutor;
    }

    public ExclusiveCaches lockCaches()
    {
        //noinspection LockAcquiredButNotSafelyReleased
        guardedCaches.lock.lock();
        return guardedCaches;
    }

    public ExclusiveCaches tryLockCaches()
    {
        if (guardedCaches.lock.tryLock())
            return guardedCaches;
        return null;
    }

    public Caches cachesExclusive()
    {
        Invariants.checkState(executor.isInThread());
        return unguardedCaches;
    }

    public Caches cachesUnsafe()
    {
        return unguardedCaches;
    }

    @VisibleForTesting
    @Override
    public void unsafeSetRangesForEpoch(CommandStores.RangesForEpoch newRangesForEpoch)
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

        TimestampsForKey reloaded = AccordKeyspace.unsafeLoadTimestampsForKey(id, (TokenKey) key);
        return Objects.equals(evicting, reloaded);
    }

    TimestampsForKey loadTimestampsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadTimestampsForKey(id, (TokenKey) key);
    }

    CommandsForKey loadCommandsForKey(RoutableKey key)
    {
        return AccordKeyspace.loadCommandsForKey(id, (TokenKey) key);
    }

    boolean validateCommandsForKey(RoutableKey key, CommandsForKey evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        CommandsForKey reloaded = AccordKeyspace.loadCommandsForKey(id, (TokenKey) key);
        return Objects.equals(evicting, reloaded);
    }

    @Nullable
    Runnable saveTimestampsForKey(TimestampsForKey after)
    {
        Mutation mutation = AccordKeyspace.getTimestampsForKeyMutation(id, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
    }

    @Nullable
    Runnable saveCommandsForKey(CommandsForKey after)
    {
        Mutation mutation = AccordKeyspace.getCommandsForKeyMutation(id, after, nextSystemTimestampMicros());
        return null != mutation ? mutation::applyUnsafe : null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AccordTask.create(this, loadCtx, function).chain();
    }

    @Override
    public <T> AsyncChain<T> submit(Callable<T> task)
    {
        return AsyncChains.ofCallable(taskExecutor(), task);
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
        return AccordTask.create(this, preLoadContext, consumer).chain();
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

    public AccordSafeCommandStore begin(AccordTask<?> operation,
                                        @Nullable CommandsForRanges commandsForRanges)
    {
        checkState(current == null);
        current = AccordSafeCommandStore.create(operation, commandsForRanges, this);
        return current;
    }

    void setOwner(Thread thread, Thread self)
    {
        Invariants.checkState(thread == null ? currentThread == self : currentThread == null);
        currentThread = thread;
        if (thread != null) CommandStore.register(this);

    }

    public boolean hasSafeStore()
    {
        return current != null;
    }

    public void complete(AccordSafeCommandStore store)
    {
        checkState(current == store);
        current.postExecute();
        current = null;
    }

    public void abort(AccordSafeCommandStore store)
    {
        checkInStore();
        Invariants.checkState(store == current);
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
        try (ExclusiveCaches caches = lockCaches())
        {
            for (int i = 0; i < rangeDeps.txnIdCount(); i++)
            {
                TxnId txnId = rangeDeps.txnId(i);
                AccordCachingState<TxnId, Command> state = caches.commands().getUnsafe(txnId);
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
                if (CommandsForKey.manages(txnId))
                    keys = command.hasBeen(Committed) ? command.participants().hasTouched() : command.participants().touches();
                else if (!CommandsForKey.managesExecution(txnId) && command.hasBeen(Status.Stable) && !command.hasBeen(Status.Truncated))
                    keys = command.asCommitted().waitingOn.keys;

                if (keys != null)
                    return PreLoadContext.contextFor(txnId, keys, keyHistory);

                return PreLoadContext.contextFor(txnId);
            }

            public Promise<?> load(Command command)
            {
                TxnId txnId = command.txnId();

                AsyncPromise<?> future = new AsyncPromise<>();
                execute(context(command, SYNC),
                        safeStore -> {
                            Command local = command;
                            if (local.status() != Truncated && local.status() != Invalidated)
                            {
                                Cleanup cleanup = Cleanup.shouldCleanup(agent, local, unsafeGetRedundantBefore(), durableBefore());
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
}
