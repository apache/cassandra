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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
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
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractReplayer;
import accord.impl.AbstractSafeCommandStore.CommandStoreCaches;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults.CountingResult;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeyAccessor;
import org.apache.cassandra.service.accord.IAccordService.AccordCompactionInfo;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.Clock;

import static accord.api.Journal.CommandUpdate;
import static accord.api.Journal.FieldUpdates;
import static accord.utils.Invariants.require;
import static org.apache.cassandra.journal.Params.ReplayMode.ONLY_NON_DURABLE;

public class AccordCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);

    // TODO (required): track this via a PhantomReference, so that if we remove a CommandStore without clearing the caches we can be sure to release them
    public static class Caches
    {
        private final AccordCache global;
        private final AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        private final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys;

        Caches(AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commandCache, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache)
        {
            this.global = global;
            this.commands = commandCache;
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

        public final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys()
        {
            return commandsForKeys;
        }
    }

    public static final class ExclusiveCaches extends Caches implements CommandStoreCaches<AccordSafeCommand, AccordSafeCommandsForKey>
    {
        private final Lock lock;

        public ExclusiveCaches(Lock lock, AccordCache global, AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands, AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeys)
        {
            super(global, commands, commandsForKeys);
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
        public void close()
        {
            lock.unlock();
        }
    }

    static final AtomicReferenceFieldUpdater<AccordCommandStore, SafeRedundantBefore> safeRedundantBeforeUpdater
        = AtomicReferenceFieldUpdater.newUpdater(AccordCommandStore.class, SafeRedundantBefore.class, "safeRedundantBefore");
    static final AtomicLong nextSafeRedundantBeforeTicket = new AtomicLong();

    public final String loggingId;
    public final Journal journal;
    private final RangeSearcher rangeSearcher;
    private final AccordExecutor sharedExecutor;
    final AccordExecutor.SequentialExecutor exclusiveExecutor;
    private final ExclusiveCaches caches;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;
    private final CommandsForRanges.Manager commandsForRanges;
    private final TableId tableId;
    private TableMetadataRef metadata;
    volatile SafeRedundantBefore safeRedundantBefore;

    private AccordSafeCommandStore current;

    public AccordCommandStore(int id,
                              NodeCommandStoreService node,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              EpochUpdateHolder epochUpdateHolder,
                              Journal journal,
                              AccordExecutor sharedExecutor)
    {
        super(id, node, agent, dataStore, progressLogFactory, listenerFactory, epochUpdateHolder);
        this.loggingId = String.format("[%s]", id);
        this.journal = journal;
        this.rangeSearcher = RangeSearcher.extractRangeSearcher(journal);
        this.sharedExecutor = sharedExecutor;
        if (this.progressLog instanceof DefaultProgressLog)
            ((DefaultProgressLog)this.progressLog).unsafeSetConfig(DatabaseDescriptor.getAccordProgressLogConfig());

        final AccordCache.Type<TxnId, Command, AccordSafeCommand>.Instance commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKey;
        try (AccordExecutor.ExclusiveGlobalCaches exclusive = sharedExecutor.lockCaches())
        {
            commands = exclusive.commands.newInstance(this);
            commandsForKey = exclusive.commandsForKey.newInstance(this);
            this.caches = new ExclusiveCaches(sharedExecutor.lock, exclusive.global, commands, commandsForKey);
        }

        this.exclusiveExecutor = sharedExecutor.executor();
        this.commandsForRanges = new CommandsForRanges.Manager(this);

        maybeLoadRedundantBefore(journal.loadRedundantBefore(id()));
        maybeLoadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
        maybeLoadSafeToRead(journal.loadSafeToRead(id()));
        maybeLoadRangesForEpoch(journal.loadRangesForEpoch(id()));

        CommandStores.RangesForEpoch ranges = this.rangesForEpoch;
        if (ranges == null || ranges.all().isEmpty())
        {
            EpochUpdate update = epochUpdateHolder.get();
            if (update != null)
                ranges = update.newRangesForEpoch;
            Invariants.require(ranges != null, "CommandStore %d created with no ranges", id);
        }
        tableId = (TableId)ranges.all().stream().map(r -> r.start().prefix()).reduce((a, b) -> {
            Invariants.require(a.equals(b), "CommandStore created with multiple distinct TableId (%s and %s)", a, b);
            return a;
        }).orElseThrow(() -> Invariants.illegalState("CommandStore %d created with no ranges", id));
    }

    static Factory factory(IntFunction<AccordExecutor> executorFactory)
    {
        return (id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal) ->
               new AccordCommandStore(id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, executorFactory.apply(id));
    }

    public CommandsForRanges.Manager commandsForRanges()
    {
        return commandsForRanges;
    }

    @Override
    public boolean inStore()
    {
        return exclusiveExecutor.inExecutor();
    }

    void tryPreSetup(AccordTask<?> task)
    {
        if (inStore() && current != null)
            task.presetup(current.task);
    }

    public final TableId tableId()
    {
        return tableId;
    }

    public AccordExecutor executor()
    {
        return sharedExecutor;
    }

    // TODO (desired): we use this for executing callbacks with mutual exclusivity,
    //  but we don't need to block the actual CommandStore - could quite easily
    //  inflate a separate queue dynamically in AccordExecutor
    public Executor taskExecutor()
    {
        return exclusiveExecutor;
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
        Invariants.require(sharedExecutor.isOwningThread());
        return caches;
    }

    public Caches cachesUnsafe()
    {
        return caches;
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
    public void sanityCheckCommand(RedundantBefore redundantBefore, Command command)
    {
        ((AccordJournal) journal).sanityCheck(id, redundantBefore, command);
    }

    CommandsForKey loadCommandsForKey(RoutableKey key)
    {
        CommandsForKey cfk = CommandsForKeyAccessor.load(id, (TokenKey) key);
        if (cfk == null)
            return null;
        RedundantBefore.QuickBounds bounds = unsafeGetRedundantBefore().get(key);
        if (bounds == null)
            return cfk; // TODO (required): I don't think this should be possible? but we hit it on some test
        return cfk.withRedundantBeforeAtLeast(bounds.gcBefore, false);
    }

    boolean validateCommandsForKey(RoutableKey key, CommandsForKey evicting)
    {
        if (!Invariants.isParanoid())
            return true;

        CommandsForKey reloaded = CommandsForKeyAccessor.load(id, (TokenKey) key);
        return Objects.equals(evicting, reloaded);
    }

    @Nullable
    Runnable saveCommandsForKey(RoutingKey key, CommandsForKey after, Object serialized)
    {
        return CommandsForKeyAccessor.systemTableUpdater(id, (TokenKey) key, after, serialized, nextSystemTimestampMicros());
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> build(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AccordTask.create(this, loadCtx, function).chain();
    }

    @Override
    public <T> AsyncChain<T> build(Callable<T> task)
    {
        return AsyncChains.ofCallable(taskExecutor(), task);
    }

    @Override
    public AsyncChain<Void> build(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AccordTask.create(this, preLoadContext, consumer).chain();
    }

    public AccordSafeCommandStore begin(AccordTask<?> operation,
                                        @Nullable CommandsForRanges commandsForRanges)
    {
        require(current == null);
        current = AccordSafeCommandStore.create(operation, commandsForRanges, this);
        return current;
    }

    public boolean hasSafeStore()
    {
        return current != null;
    }

    DataStore dataStore()
    {
        return dataStore;
    }

    ProgressLog progressLog()
    {
        return progressLog;
    }

    public void complete(AccordSafeCommandStore store)
    {
        require(current == store);
        current.postExecute();
        current = null;
    }

    public void abort(AccordSafeCommandStore store)
    {
        Invariants.require(store == current);
        current = null;
    }

    @Override
    public void shutdown()
    {
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

    @VisibleForTesting
    public List<AccordJournal.DebugEntry> debugCommand(TxnId txnId)
    {
        return (List<AccordJournal.DebugEntry>) journal.debugCommand(id, txnId);
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

    public Command.Minimal loadMinimal(TxnId txnId)
    {
        return journal.loadMinimal(id, txnId, unsafeGetRedundantBefore(), durableBefore());
    }

    public Command.MinimalWithDeps loadMinimalWithDeps(TxnId txnId)
    {
        return journal.loadMinimalWithDeps(id, txnId, unsafeGetRedundantBefore(), durableBefore());
    }

    public AccordCompactionInfo getCompactionInfo()
    {
        SafeRedundantBefore safeRedundantBefore = this.safeRedundantBefore;
        RedundantBefore redundantBefore;
        if (safeRedundantBefore == null) redundantBefore = RedundantBefore.EMPTY;
        else redundantBefore = safeRedundantBefore.redundantBefore;
        CommandStores.RangesForEpoch ranges = this.rangesForEpoch;
        if (ranges == null) ranges = CommandStores.RangesForEpoch.EMPTY;
        return new AccordCompactionInfo(id, redundantBefore, ranges, tableId);
    }

    public RangeSearcher rangeSearcher()
    {
        return rangeSearcher;
    }

    public AccordCommandStoreReplayer replayer()
    {
        boolean replayOnlyDurable = true;
        if (journal instanceof AccordJournal)
            replayOnlyDurable = ((AccordJournal)journal).configuration().replayMode() == ONLY_NON_DURABLE;
        return new AccordCommandStoreReplayer(this, replayOnlyDurable);
    }

    static final AtomicLong nextDurabilityLoggingId = new AtomicLong();
    @Override
    protected void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable)
    {
        if (!CommandsForKey.reportLinearizabilityViolations())
            return;

        long reportId = nextDurabilityLoggingId.incrementAndGet();
        logger.debug("{} awaiting local metadata durability for {} ({})", this, ranges, reportId);
        executor().afterSubmittedAndConsequences(() -> {
            logger.debug("{}: saving intersecting keys ({})", this, reportId);
            class Ready extends CountingResult implements Runnable
            {
                public Ready() { super(1); }
                @Override public void run() { decrement(); }
            }

            Ready ready = new Ready();
            try (ExclusiveCaches caches = lockCaches())
            {
                for (AccordCacheEntry<RoutingKey, CommandsForKey> e : caches.commandsForKeys())
                {
                    if (ranges.contains(e.key()) && e.isModified())
                    {
                        ready.increment();
                        caches.global().saveWhenReadyExclusive(e, ready);
                    }
                }
            }

            ready.begin((success, fail) -> {
                if (fail != null)
                {
                    logger.error("{}: failed to ensure durability of {} ({})", this, ranges, reportId, fail);
                }
                else
                {
                    logger.debug("{}: waiting for CommandsForKey to flush ({})", this, reportId);
                    ColumnFamilyStore cfs = AccordKeyspace.AccordColumnFamilyStores.commandsForKey;

                    AccordDurableOnFlush onFlush = null;
                    while (onFlush == null)
                        onFlush = cfs.getCurrentMemtable().ensureFlushListener(AccordDataStore.FlushListenerKey.KEY, AccordDurableOnFlush::new);

                    if (!onFlush.add(id, onCommandStoreDurable))
                        AccordDurableOnFlush.notify(cfs.metadata(), this, onCommandStoreDurable);
                }
            });
            ready.decrement();
        });
    }

    @VisibleForTesting
    public void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        super.unsafeUpsertRedundantBefore(addRedundantBefore);
    }

    public static class AccordCommandStoreReplayer extends AbstractReplayer
    {
        private final AccordCommandStore store;
        private final boolean onlyNonDurable;

        private AccordCommandStoreReplayer(AccordCommandStore store, boolean onlyNonDurable)
        {
            super(store.unsafeGetRedundantBefore());
            this.store = store;
            this.onlyNonDurable = onlyNonDurable;
        }

        @Override
        public AsyncChain<Route> replay(TxnId txnId)
        {
            if (onlyNonDurable && !maybeShouldReplay(txnId))
                return AsyncChains.success(null);

            return store.submit(PreLoadContext.contextFor(txnId, "Replay"), safeStore -> {
                if (onlyNonDurable && !shouldReplay(txnId, safeStore.unsafeGet(txnId).current().participants()))
                    return null;

                initialiseState(safeStore, txnId);
                return safeStore.unsafeGet(txnId).current().route();
            });
        }
    }

    /**
     * Replay/state reloading
     */

    void maybeLoadRedundantBefore(RedundantBefore redundantBefore)
    {
        if (redundantBefore != null)
        {
            loadRedundantBefore(redundantBefore);
            Invariants.require(safeRedundantBefore == null);
            safeRedundantBefore = new SafeRedundantBefore(0, redundantBefore);
        }
    }

    void maybeLoadBootstrapBeganAt(NavigableMap<TxnId, Ranges> bootstrapBeganAt)
    {
        if (bootstrapBeganAt != null)
            loadBootstrapBeganAt(bootstrapBeganAt);
    }

    void maybeLoadSafeToRead(NavigableMap<Timestamp, Ranges> safeToRead)
    {
        if (safeToRead != null)
            loadSafeToRead(safeToRead);
    }

    void maybeLoadRangesForEpoch(CommandStores.RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
            loadRangesForEpoch(rangesForEpoch);
    }

    @Override
    public void updateRangesForEpoch(SafeCommandStore safeStore)
    {
        super.updateRangesForEpoch(safeStore);
        updateMinHlc(PaxosState.ballotTracker().getLowBound().unixMicros() + 1);
    }

    // TODO (expected): handle journal failures, and consider how we handle partial failures.
    //  Very likely we will not be able to safely or cleanly handle partial failures of this logic, but decide and document.
    // TODO (desired): consider merging with PersistentField? This version is cheaper to manage which may be preferable at the CommandStore level.
    static class SafeRedundantBefore
    {
        final long ticket;
        final RedundantBefore redundantBefore;

        SafeRedundantBefore(long ticket, RedundantBefore redundantBefore)
        {
            this.ticket = ticket;
            this.redundantBefore = redundantBefore;
        }

        static SafeRedundantBefore max(SafeRedundantBefore a, SafeRedundantBefore b)
        {
            return a.ticket >= b.ticket ? a : b;
        }
    }

    private @Nullable TableMetadata tableMetadata()
    {
        TableMetadataRef metadataRef = this.metadata;
        if (metadataRef != null)
            return metadataRef.get();

        TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
        if (metadata == null)
            return null;
        this.metadata = metadata.ref;
        return metadata;
    }

    @Override
    public String toString()
    {
        TableMetadata metadata = tableMetadata();
        StringBuilder sb = new StringBuilder("[");
        if (metadata != null)
            sb.append(metadata).append('|');
        sb.append(tableId);
        sb.append('|').append(id).append(',').append(node.id().id).append(']');
        return sb.toString();
    }
}
