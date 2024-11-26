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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractLoader;
import accord.impl.AbstractSafeCommandStore.CommandStoreCaches;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialTxn;
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
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.SavedCommand.MinimalCommand;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.api.Journal.CommandUpdate;
import static accord.api.Journal.FieldUpdates;
import static accord.api.Journal.Loader;
import static accord.api.Journal.OnDone;
import static accord.local.KeyHistory.SYNC;
import static accord.primitives.Status.Committed;
import static accord.utils.Invariants.checkState;
import static org.apache.cassandra.service.accord.SavedCommand.Load.MINIMAL;

public class AccordCommandStore extends CommandStore
{
    // TODO (required): track this via a PhantomReference, so that if we remove a CommandStore without clearing the caches we can be sure to release them
    public static class Caches
    {
        private final AccordCache global;
        private final AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        private final AccordCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys;
        private final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys;

        Caches(AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commandCache, AccordCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeyCache, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache)
        {
            this.global = global;
            this.commands = commandCache;
            this.timestampsForKeys = timestampsForKeyCache;
            this.commandsForKeys = commandsForKeyCache;
        }

        public final AccordCache global()
        {
            return global;
        }

        public final AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands()
        {
            return commands;
        }

        public final AccordCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys()
        {
            return timestampsForKeys;
        }

        public final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys()
        {
            return commandsForKeys;
        }
    }

    public static final class ExclusiveCaches extends Caches implements CommandStoreCaches<AccordSafeCommand, AccordSafeTimestampsForKey, AccordSafeCommandsForKey>
    {
        private final Lock lock;

        public ExclusiveCaches(Lock lock, AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands, AccordCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKeys, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys)
        {
            super(global, commands, timestampsForKeys, commandsForKeys);
            this.lock = lock;
        }


        @Override
        public AccordSafeCommand acquireIfLoaded(TxnId txnId)
        {
            return commands().acquireIfLoaded(txnId);
        }

        @Override
        public AccordSafeCommandsForKey acquireIfLoaded(RoutingKey key)
        {
            return commandsForKeys().acquireIfLoaded(key);
        }

        @Override
        public AccordSafeTimestampsForKey acquireTfkIfLoaded(RoutingKey key)
        {
            return timestampsForKeys().acquireIfLoaded(key);
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
    private final ExclusiveCaches caches;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private final CommandsForRanges.Manager commandsForRanges;

    private AccordSafeCommandStore current;
    private Thread currentThread;

    private final CommandStoreLoader loader;

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

        final AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        final AccordCache.Type<RoutingKey, TimestampsForKey, AccordSafeTimestampsForKey>.Instance timestampsForKey;
        final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKey;
        try (AccordExecutor.ExclusiveGlobalCaches exclusive = executor.lockCaches())
        {
            commands = exclusive.commands.newInstance(this);
            timestampsForKey = exclusive.timestampsForKey.newInstance(this);
            commandsForKey = exclusive.commandsForKey.newInstance(this);
            this.caches = new ExclusiveCaches(executor.lock, exclusive.global, commands, timestampsForKey, commandsForKey);
        }

        this.taskExecutor = executor.executor(this);
        this.commandsForRanges = new CommandsForRanges.Manager(this);
        this.loader = new CommandStoreLoader(this);

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

    public CommandsForRanges.Manager diskCommandsForRanges()
    {
        return commandsForRanges;
    }

    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        store.snapshot(ranges, globalSyncId);
        super.markShardDurable(safeStore, globalSyncId, ranges);
        commandsForRanges.gcBefore(globalSyncId, ranges);
    }

    @Override
    public boolean inStore()
    {
        return currentThread == Thread.currentThread();
    }

    void tryPreSetup(AccordTask<?> task)
    {
        if (inStore() && current != null)
            task.presetup(current.task);
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
        caches.lock.lock();
        return caches;
    }

    public ExclusiveCaches tryLockCaches()
    {
        if (caches.lock.tryLock())
            return caches;
        return null;
    }

    public Caches cachesExclusive()
    {
        Invariants.checkState(executor.isOwningThread());
        return caches;
    }

    public Caches cachesUnsafe()
    {
        return caches;
    }

    @VisibleForTesting
    @Override
    public void unsafeSetRangesForEpoch(CommandStores.RangesForEpoch newRangesForEpoch)
    {
        super.unsafeSetRangesForEpoch(newRangesForEpoch);
    }

    @Nullable
    @VisibleForTesting
    public Runnable appendToKeyspace(TxnId txnId, Command after)
    {
        if (txnId.is(Routable.Domain.Key))
            return null;

        Mutation mutation = AccordKeyspace.getCommandMutation(this.id, after, nextSystemTimestampMicros());

        // TODO (required): make sure we test recovering when this has failed to be persisted
        if (null != mutation)
            return mutation::applyUnsafe;

        return null;
    }

    public void persistFieldUpdates(FieldUpdates fieldUpdates, Runnable onFlush)
    {
        journal.saveStoreState(id, fieldUpdates, onFlush);
    }

    @Nullable
    @VisibleForTesting
    public void appendToLog(Command before, Command after, Runnable onFlush)
    {
        journal.saveCommand(id, new CommandUpdate(before, after), onFlush);
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
    Runnable saveTimestampsForKey(RoutingKey key, TimestampsForKey after, Object serialized)
    {
        return AccordKeyspace.getTimestampsForKeyUpdater(this, after, nextSystemTimestampMicros());
    }

    @Nullable
    Runnable saveCommandsForKey(RoutingKey key, CommandsForKey after, Object serialized)
    {
        return AccordKeyspace.getCommandsForKeyUpdater(id, (TokenKey) key, after, serialized, nextSystemTimestampMicros());
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
                AccordCacheEntry<TxnId, Command> state = caches.commands().getUnsafe(txnId);
                if (state != null && state.isLoaded() && state.getExclusive() != null && state.getExclusive().known().isDefinitionKnown())
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

    public void appendCommands(List<CommandUpdate> diffs, Runnable onFlush)
    {
        for (int i = 0; i < diffs.size(); i++)
        {
            boolean isLast = i == diffs.size() - 1;
            CommandUpdate change = diffs.get(i);
            journal.saveCommand(id, change, isLast ? onFlush : null);
        }
    }

    @VisibleForTesting
    public Command loadCommand(TxnId txnId)
    {
        return journal.loadCommand(id, txnId, unsafeGetRedundantBefore(), durableBefore());
    }

    public static Command prepareToCache(Command command)
    {
        // TODO (required): validate we don't have duplicate objects
        if (command != null)
        {
            PartialTxn txn = command.partialTxn();
            if (txn != null)
            {
                TxnRead read = (TxnRead) txn.read();
                read.unmemoize();
            }
        }
        return command;
    }

    public MinimalCommand loadMinimal(TxnId txnId)
    {
        return journal.loadMinimal(id, txnId, MINIMAL, unsafeGetRedundantBefore(), durableBefore());
    }

    public Loader loader()
    {
        return loader;
    }

    private static class CommandStoreLoader extends AbstractLoader
    {
        private final AccordCommandStore store;

        private CommandStoreLoader(AccordCommandStore store)
        {
            this.store = store;
        }

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

        @Override
        public void load(Command command, OnDone onDone)
        {
            store.execute(context(command, SYNC),
                          safeStore -> loadInternal(command, safeStore))
                 .begin((unused, throwable) -> {
                     if (throwable != null)
                         onDone.failure(throwable);
                     else
                         onDone.success();
                 });
        }

        @Override
        public void apply(Command command, OnDone onDone)
        {
            PreLoadContext context = context(command, KeyHistory.TIMESTAMPS);
            store.execute(context,
                          safeStore -> {
                              applyWrites(command, safeStore, (safeCommand, cmd) -> {
                                  Commands.applyWrites(safeStore, context, cmd).begin(store.agent);
                              });
                          })
                 .begin((unused, throwable) -> {
                     if (throwable != null)
                         onDone.failure(throwable);
                     else
                         onDone.success();
                 });
        }
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

    void loadRangesForEpoch(CommandStores.RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
            unsafeSetRangesForEpoch(rangesForEpoch);
    }
}
