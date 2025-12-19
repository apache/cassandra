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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import accord.primitives.SaveStatus;
import accord.primitives.Status;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.AccordSpec.JournalSpec.ReplayMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractReplayer;
import accord.impl.AbstractReplayer.Mode;
import accord.impl.AbstractSafeCommandStore.CommandStoreCaches;
import accord.impl.DefaultLocalListeners;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommandSummaries;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.CountingResult;

import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeyAccessor;
import org.apache.cassandra.service.accord.IAccordService.AccordCompactionInfo;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.api.Journal.CommandUpdate;
import static accord.api.Journal.FieldUpdates;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.utils.Invariants.require;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccord;
import static org.apache.cassandra.io.util.CompressedFrameDataInputPlus.readList;
import static org.apache.cassandra.io.util.CompressedFrameDataInputPlus.readOne;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.writeList;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.writeOne;
import static org.apache.cassandra.service.accord.AccordJournal.saveDirectory;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.maxConflicts;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.maxDecidedRX;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.progressLogState;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.rejectBefore;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.txnListener;

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
            global().tryShrinkOrEvict(lock);
            lock.unlock();
        }
    }

    static final AtomicReferenceFieldUpdater<AccordCommandStore, SafeRedundantBefore> safeRedundantBeforeUpdater
        = AtomicReferenceFieldUpdater.newUpdater(AccordCommandStore.class, SafeRedundantBefore.class, "safeRedundantBefore");
    static final AtomicReferenceFieldUpdater<AccordCommandStore, Condition> terminatedUpdater
        = AtomicReferenceFieldUpdater.newUpdater(AccordCommandStore.class, Condition.class, "terminated");
    static final AtomicLong nextSafeRedundantBeforeTicket = new AtomicLong();

    private static final AtomicLong lastSystemTimestampMicros = new AtomicLong();

    public final String loggingId;
    public final Journal journal;
    private final AccordExecutor sharedExecutor;
    final AccordExecutor.SequentialExecutor exclusiveExecutor;
    private final ExclusiveCaches caches;
    private final RangeIndex rangeIndex;
    private final TableId tableId;
    private TableMetadataRef metadata;

    volatile SafeRedundantBefore safeRedundantBefore;
    volatile Condition terminated;

    private AccordSafeCommandStore current;
    LogLinearDecayingHistograms.Buffer metricsBuffer;

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

        this.exclusiveExecutor = sharedExecutor.executor(id);
        {
            AccordSpec.RangeIndexMode mode = getAccord().range_index_mode;
            switch (mode)
            {
                default: throw new UnhandledEnum(mode);
                case journal_sai: rangeIndex = new JournalRangeIndex(this); break;
                case in_memory: rangeIndex = new InMemoryRangeIndex(this); break;
            }
        }

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

        if (AccordService.isStarted())
            progressLog.unsafeStart();
    }

    static Factory factory(IntFunction<AccordExecutor> executorFactory)
    {
        return (id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal) ->
               new AccordCommandStore(id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, executorFactory.apply(id));
    }

    public RangeIndex rangeIndex()
    {
        return rangeIndex;
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
    public AsyncExecutor taskExecutor()
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
        RedundantBefore.QuickBounds bounds = safeGetRedundantBefore().get(key);
        if (!Invariants.expect(bounds != null, "No RedundantBefore information found when loading key %s", key))
            return cfk;
        return cfk.withGcBeforeAtLeast(bounds.gcBefore, false);
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
        return lastSystemTimestampMicros.accumulateAndGet(node.now(), (a, b) -> Math.max(a + 1, b));
    }
    @Override
    public <T> AsyncChain<T> chain(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AccordTask.create(this, loadCtx, function).chain();
    }

    @Override
    public AsyncChain<Void> chain(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AccordTask.create(this, preLoadContext, consumer).chain();
    }

    @Override
    public <T> AsyncChain<T> chain(Callable<T> call)
    {
        return taskExecutor().chain(call);
    }

    @Override
    public void execute(Runnable run)
    {
        taskExecutor().execute(run);
    }

    public AccordSafeCommandStore begin(AccordTask<?> operation, @Nullable CommandSummaries commandsForRanges)
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
        shutdownAsync();
    }

    public AsyncResult<Void> shutdownAsync()
    {
        terminatedUpdater.compareAndSet(this, null, Condition.newOneTimeCondition());
        progressLog.stop();
        return execute((PreLoadContext.Empty)() -> "Shutdown", safeStore -> {
            exclusiveExecutor.stop();
            logger.info("{} stopping. Durably applied: {}, waiting: {}", this,
                        DurablyAppliedTo.summarise(safeStore.redundantBefore(), DurablyAppliedTo::isDone),
                        DurablyAppliedTo.summarise(safeStore.redundantBefore(), DurablyAppliedTo::isNotDone));
            maybeTerminated();
        });
    }

    @Override
    protected void upsertedRedundantBefore(SafeCommandStore safeStore, RedundantBefore added)
    {
        super.upsertedRedundantBefore(safeStore, added);
        maybeTerminated();
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges durableRanges, Status.Durability.HasOutcome durability)
    {
        super.markShardDurable(safeStore, globalSyncId, durableRanges, durability);
        if (durability == Universal)
            rangeIndex.prune(globalSyncId, durableRanges, safeStore.redundantBefore());
    }

    @Override
    protected void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId syncId, Ranges ranges, SaveStatus prevStatus)
    {
        super.markExclusiveSyncPointLocallyApplied(safeStore, syncId, ranges, prevStatus);
        rangeIndex.prune(syncId, ranges, safeStore.redundantBefore());
    }

    private void maybeTerminated()
    {
        if (terminated != null)
        {
            boolean durable = unsafeGetRedundantBefore().foldl((b, v, p2, p3) -> {
                return v && (b == null || b.maxBound(LOCALLY_APPLIED).compareTo(b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE)) <= 0);
            }, true, null, null, ignore -> false);
            boolean stopped = exclusiveExecutor.stopped();
            if (durable && stopped)
            {
                logger.debug("{} Signalling termination", this);
                terminated.signalAll();
            }
            else
            {
                logger.debug("{} Not signalling termination with waiting: {} ({}), stopped: {}", this, DurablyAppliedTo.summarise(unsafeGetRedundantBefore(), DurablyAppliedTo::isNotDone), durable, stopped);
            }
        }
    }

    public boolean awaitTerminationUntil(long deadlineNanos)
    {
        if (terminated == null)
            throw new IllegalStateException("Not shutdown");
        return terminated.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos);
    }

    public boolean isTerminated()
    {
        return terminated != null && terminated.isSignalled();
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
        return journal.loadCommand(id, txnId, safeGetRedundantBefore(), durableBefore());
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
        return journal.loadMinimal(id, txnId, safeGetRedundantBefore(), durableBefore());
    }

    public Command.MinimalWithDeps loadMinimalWithDeps(TxnId txnId)
    {
        return journal.loadMinimalWithDeps(id, txnId, safeGetRedundantBefore(), durableBefore());
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

    public final RedundantBefore safeGetRedundantBefore()
    {
        return safeRedundantBefore.redundantBefore;
    }

    public AccordCommandStoreReplayer replayer()
    {
        Mode mode;
        if (journal instanceof AccordJournal)
        {
            ReplayMode replayMode = getAccord().journal.replayMode;
            switch (replayMode)
            {
                default: throw new UnhandledEnum(replayMode);
                case NON_DURABLE:
                    mode = Mode.NON_DURABLE;
                    throw new UnsupportedOperationException("Not yet safe to use NON_DURABLE ReplayMode");
                case PART_NON_DURABLE:
                    mode = Mode.PART_NON_DURABLE;
                    break;
                case ALL:
                case RESET:
                    mode = Mode.ALL;
            }
        }
        else
        {
            mode = Mode.ALL;
        }
        return new AccordCommandStoreReplayer(this, mode);
    }

    static final AtomicLong nextDurabilityLoggingId = new AtomicLong();
    @Override
    protected void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable)
    {
        if (node().isReplaying())
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
                for (Range range : ranges)
                {
                    for (RoutingKey k : caches.commandsForKeys().keysBetween(range.start(), range.startInclusive(), range.end(), range.endInclusive()))
                    {
                        AccordCacheEntry<RoutingKey, CommandsForKey> e = caches.commandsForKeys().getUnsafe(k);
                        if (e.isModified())
                        {
                            ready.increment();
                            caches.global().saveWhenReadyExclusive(e, ready);
                        }
                    }
                }
            }

            ready.invoke((success, fail) -> {
                if (fail != null)
                {
                    logger.error("{}: failed to ensure durability of {} ({})", this, ranges, reportId, fail);
                }
                else
                {
                    logger.debug("{}: waiting for CommandsForKey to flush ({})", this, reportId);
                    ColumnFamilyStore cfs = AccordKeyspace.AccordColumnFamilyStores.commandsForKey;

                    AccordDurableOnFlush.notifyOnDurable(cfs, this, onCommandStoreDurable);
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

    @VisibleForTesting
    public void unsafeUpdateRangesForEpoch()
    {
        super.unsafeUpdateRangesForEpoch();
        safeRedundantBefore = new SafeRedundantBefore(0, unsafeGetRedundantBefore());
    }

    public static class AccordCommandStoreReplayer extends AbstractReplayer
    {
        private final AccordCommandStore commandStore;

        private AccordCommandStoreReplayer(AccordCommandStore commandStore, Mode mode)
        {
            super(commandStore, mode, null);
            this.commandStore = commandStore;
        }

        @Override
        public AsyncChain<Route> replay(TxnId txnId)
        {
            if (!maybeShouldReplay(txnId))
                return AsyncChains.success(null);

            return commandStore.chain(PreLoadContext.contextFor(txnId, "Replay"), safeStore -> {
                Replay replay = shouldReplay(txnId, safeStore.unsafeGet(txnId).current().participants());
                if (replay == Replay.NONE)
                    return null;

                replay(safeStore, txnId, replay);
                return safeStore.unsafeGet(txnId).current().route();
            });
        }
    }

    /**
     * Replay/state reloading
     */

    void maybeLoadRedundantBefore(RedundantBefore redundantBefore)
    {
        Invariants.require(safeRedundantBefore == null);
        if (redundantBefore != null)
        {
            loadRedundantBefore(redundantBefore);
            safeRedundantBefore = new SafeRedundantBefore(0, redundantBefore);
        }
        else
        {
            safeRedundantBefore = new SafeRedundantBefore(0, this.unsafeGetRedundantBefore());
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

    AsyncChain<Boolean> saveState(Descriptor descriptor)
    {
        return chain((AccordExecutor.Unstoppable)() -> "Save State", safeStore -> {
            File storeDir = storeSaveDir();

            {
                File[] tmpDirs = listTmpSaveDirs(storeDir);
                if (tmpDirs != null)
                {
                    logger.info("Cleaning up incomplete save points: {}", Arrays.toString(tmpDirs));
                    for (File dir : tmpDirs)
                        dir.tryDeleteRecursive();
                }
            }

            File[] sortedSaveDirs = listSortedSaveDirs(storeDir);
            File tmpSaveDir = new File(storeDir, "tmp" + descriptor.timestamp);
            File saveDir = new File(storeDir, "" + descriptor.timestamp);
            if (sortedSaveDirs != null && Long.parseLong(sortedSaveDirs[sortedSaveDirs.length - 1].name()) >= descriptor.timestamp)
            {
                logger.error("There already exists a save point {} >= {}; aborting.", sortedSaveDirs[sortedSaveDirs.length - 1].name(), descriptor.timestamp);
                return false;
            }

            try
            {
                logger.info("{}: Saving state to {}", this, saveDir);
                tmpSaveDir.createDirectoriesIfNotExists();
                writeOne(new File(tmpSaveDir, "max_decidedrx"), unsafeGetMaxDecidedRX(), maxDecidedRX);
                writeOne(new File(tmpSaveDir, "max_conflicts"), unsafeGetMaxConflicts(), maxConflicts);
                writeOne(new File(tmpSaveDir, "reject_before"), unsafeGetRejectBefore(), rejectBefore);
                writeList(new File(tmpSaveDir, "listeners"), ((DefaultLocalListeners)listeners).snapshot(), txnListener);
                writeList(new File(tmpSaveDir, "progress_log"), ((DefaultProgressLog)progressLog).snapshot(), progressLogState);
                rangeIndex.save(new File(tmpSaveDir, "range_index"));
                tmpSaveDir.move(saveDir);
            }
            catch (IOException e)
            {
                logger.error("{}: Failed to save replay state {}", this, saveDir);
                tmpSaveDir.tryDeleteRecursive();
                saveDir.tryDeleteRecursive();
                return false;
            }

            if (sortedSaveDirs != null)
            {
                int delete = (sortedSaveDirs.length + 1) - getAccord().journal.save_points;
                if (delete > 0)
                {
                    sortedSaveDirs = Arrays.copyOf(sortedSaveDirs, delete);
                    logger.debug("Deleting old save points: {}", Arrays.toString(sortedSaveDirs));
                    for (File dir : sortedSaveDirs)
                        dir.tryDeleteRecursive();
                }
            }

            return true;
        });
    }

    private File storeSaveDir()
    {
        return new File(saveDirectory(), String.format("%s_%d", tableId().toShortString(""), id()));
    }

    private static File[] listSortedSaveDirs(File storeDir)
    {
        File[] savePoints = storeDir.tryList(f -> f.isDirectory() && f.name().matches("[0-9]+"));
        if (savePoints == null || savePoints.length == 0)
            return null;

        Arrays.sort(savePoints, Comparator.comparingLong(f -> Long.parseLong(f.name())));
        return savePoints;
    }

    private static File[] listTmpSaveDirs(File storeDir)
    {
        File[] tmpDirs = storeDir.tryList(f -> f.isDirectory() && f.name().matches("tmp[0-9]+"));
        if (tmpDirs == null || tmpDirs.length == 0)
            return null;
        return tmpDirs;
    }

    AsyncChain<Map.Entry<Integer, Long>> restoreState()
    {
        return chain((AccordExecutor.Unstoppable)() -> "Restore State", safeStore -> {
            File storeDir = storeSaveDir();
            File[] savePoints = listSortedSaveDirs(storeDir);
            if (savePoints == null)
                return null;

            File savePoint = savePoints[savePoints.length - 1];
            try
            {
                long segment = Long.parseLong(savePoint.name());
                logger.info("{}: Restoring state from {}", this, savePoint);
                unsafeSetMaxDecidedRX(readOne(new File(savePoint, "max_decidedrx"), maxDecidedRX));
                unsafeSetMaxConflicts(readOne(new File(savePoint, "max_conflicts"), maxConflicts));
                unsafeSetRejectBefore(readOne(new File(savePoint, "reject_before"), rejectBefore));
                ((DefaultLocalListeners) listeners).restore(readList(new File(savePoint, "listeners"), txnListener));
                ((DefaultProgressLog) progressLog).restore(safeStore, readList(new File(savePoint, "progress_log"), progressLogState));
                rangeIndex.restore(new File(savePoint, "range_index"));
                return Map.entry(id, segment + 1);
            }
            catch (IOException e)
            {
                logger.warn("{}: Could not replay save point {}", this, savePoint);
                return null;
            }
        });
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
        sb.append('|').append(id).append(',').append(executor().executorId).append(']');
        return sb.toString();
    }

    public static class DurablyAppliedTo
    {
        final TxnId journal, commandStore, dataStore;

        public DurablyAppliedTo(RedundantBefore.Bounds bounds)
        {
            this(bounds.maxBound(LOCALLY_APPLIED), bounds.maxBound(LOCALLY_DURABLE_TO_COMMAND_STORE), bounds.maxBound(LOCALLY_DURABLE_TO_DATA_STORE));
        }

        public DurablyAppliedTo(TxnId journal, TxnId commandStore, TxnId dataStore)
        {
            this.journal = journal;
            this.commandStore = commandStore;
            this.dataStore = dataStore;
        }

        @Override
        public String toString()
        {
            return "journal:" + journal + ", commandStore:" + commandStore + ", dataStore:" + dataStore;
        }

        public boolean isDone()
        {
            return journal.compareTo(TxnId.min(commandStore, dataStore)) <= 0;
        }

        public boolean isNotDone()
        {
            return !isDone();
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof DurablyAppliedTo && equals((DurablyAppliedTo) that);
        }

        public boolean equals(DurablyAppliedTo that)
        {
            return this.journal.equals(that.journal)
                   && this.commandStore.equals(that.commandStore)
                   && this.dataStore.equals(that.dataStore);
        }

        public static ReducingRangeMap<DurablyAppliedTo> summarise(RedundantBefore redundantBefore)
        {
            return redundantBefore.map(b -> b == null ? null : new DurablyAppliedTo(b), DurablyAppliedTo[]::new);
        }

        public static ReducingRangeMap<DurablyAppliedTo> summarise(RedundantBefore redundantBefore, Predicate<DurablyAppliedTo> include)
        {
            return redundantBefore.map(b -> {
                if (b == null)
                    return null;
                DurablyAppliedTo result = new DurablyAppliedTo(b);
                if (!include.test(result))
                    return null;
                return result;
            }, DurablyAppliedTo[]::new);
        }
    }
}
