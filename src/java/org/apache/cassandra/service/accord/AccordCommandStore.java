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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.LocalListeners.TxnListener;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractReplayer;
import accord.impl.AbstractReplayer.Mode;
import accord.impl.AbstractSafeCommandStore.CommandStoreCaches;
import accord.impl.DefaultLocalListeners;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.TxnState;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.Empty;
import accord.local.MaxConflicts;
import accord.local.MaxDecidedRX;
import accord.local.MinimalCommand;
import accord.local.MinimalCommand.MinimalWithDeps;
import accord.local.NodeCommandStoreService;
import accord.local.RedundantBefore;
import accord.local.RedundantBefore.Bounds;
import accord.local.RedundantStatus.Property;
import accord.local.RedundantStatus.SomeStatus;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.CountingResult;
import accord.utils.async.Cancellable;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.AccordConfig.JournalConfig.ReplayMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.accord.AccordDurableOnFlush.ReportDurable;
import org.apache.cassandra.service.accord.IAccordService.AccordCompactionInfo;
import org.apache.cassandra.service.accord.execution.AccordCache;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.execution.ExclusiveExecutor;
import org.apache.cassandra.service.accord.execution.InconsistentEntryException;
import org.apache.cassandra.service.accord.execution.SafeTask;
import org.apache.cassandra.service.accord.execution.SaferCommand;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;
import org.apache.cassandra.service.accord.execution.SaferCommandsForKey;
import org.apache.cassandra.service.accord.execution.TaskRunner;
import org.apache.cassandra.service.accord.execution.Unterminatable;
import org.apache.cassandra.service.accord.journal.AccordJournal;
import org.apache.cassandra.service.accord.journal.JournalRangeIndex;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;

import static accord.api.Journal.CommandUpdate;
import static accord.api.Journal.FieldUpdates;
import static accord.impl.progresslog.DefaultProgressLog.ModeFlag.CATCH_UP;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_DATA_STORE_ONLY;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.utils.Invariants.require;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccord;
import static org.apache.cassandra.io.util.CompressedFrameDataInputPlus.readList;
import static org.apache.cassandra.io.util.CompressedFrameDataInputPlus.readOne;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.writeList;
import static org.apache.cassandra.io.util.CompressedFrameDataOutputPlus.writeOne;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.saveDirectory;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.maxConflicts;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.maxDecidedRX;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.progressLogState;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.redundantBefore;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.rejectBefore;
import static org.apache.cassandra.service.accord.serializers.CommandStoreSerializers.txnListener;

public class AccordCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStore.class);
    /** a stalled durability report repeats on every attempt, so it must be loud but not unbounded */
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    // TODO (required): track this via a PhantomReference, so that if we remove a CommandStore without clearing the caches we can be sure to release them
    public static class Caches
    {
        private final AccordCache global;
        private final AccordCache.Type<TxnId, Command, SaferCommand>.Instance commands;
        private final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeys;

        Caches(AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand>.Instance commandCache, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeyCache)
        {
            this.global = global;
            this.commands = commandCache;
            this.commandsForKeys = commandsForKeyCache;
        }

        public final AccordCache global()
        {
            return global;
        }

        public final AccordCache.Type<TxnId, Command, SaferCommand>.Instance commands()
        {
            return commands;
        }

        public final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeys()
        {
            return commandsForKeys;
        }
    }

    public static final class ExclusiveCaches extends Caches implements CommandStoreCaches<SaferCommand, SaferCommandsForKey>
    {
        private final AccordExecutor owner;

        public ExclusiveCaches(AccordExecutor owner, AccordCache global, AccordCache.Type<TxnId, Command, SaferCommand>.Instance commands, AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKeys)
        {
            super(global, commands, commandsForKeys);
            this.owner = owner;
        }

        @Override
        public SaferCommand acquireIfLoaded(TxnId txnId)
        {
            // note: we must return false if the entry is locked to enforce ordering.
            // note importantly that this is also coupled to the safety of synchronously releasing ExclusiveExecutor.owner,
            // rather than waiting until the (potentially asynchronous) cleanup of the task completes
            return commands().acquireIfLoadedAndPermitted(txnId);
        }

        @Override
        public SaferCommandsForKey acquireIfLoaded(RoutingKey key)
        {
            return commandsForKeys().acquireIfLoadedAndPermitted(key);
        }

        @Override
        public void close()
        {
            try { global().tryShrinkOrEvict(owner.unsafeLock()); }
            finally { owner.unlock(TaskRunner.get()); }
        }
    }

    static class Termination extends Condition.Sync
    {
        private boolean commandStoreFlushed;
        private boolean dataStoreFlushed;
        private boolean isReadyToTerminate()
        {
            return commandStoreFlushed && dataStoreFlushed;
        }
    }

    static final AtomicReferenceFieldUpdater<AccordCommandStore, SafeRedundantBefore> safeRedundantBeforeUpdater
        = AtomicReferenceFieldUpdater.newUpdater(AccordCommandStore.class, SafeRedundantBefore.class, "safeRedundantBefore");
    static final AtomicReferenceFieldUpdater<AccordCommandStore, Termination> terminatedUpdater
        = AtomicReferenceFieldUpdater.newUpdater(AccordCommandStore.class, Termination.class, "terminated");
    static final AtomicLong nextSafeRedundantBeforeTicket = new AtomicLong();

    public final String loggingId;
    public final Journal journal;
    private final AccordExecutor sharedExecutor;
    private final ExclusiveExecutor exclusiveExecutor;
    private final ExclusiveCaches caches;
    private final RangeIndex rangeIndex;
    private final TableId tableId;
    private TableMetadataRef metadata;

    volatile SafeRedundantBefore safeRedundantBefore;
    volatile Termination terminated;

    private SaferCommandStore current;
    public LogLinearDecayingHistograms.Buffer metricsBuffer;

    public AccordCommandStore(int id,
                              NodeCommandStoreService node,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              LocalListeners.Factory listenerFactory,
                              RangesForEpoch rangesForEpoch,
                              Journal journal,
                              AccordExecutor sharedExecutor)
    {
        super(id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch);
        this.loggingId = String.format("[%s]", id);
        this.journal = journal;
        this.sharedExecutor = sharedExecutor;
        if (this.progressLog instanceof DefaultProgressLog)
            ((DefaultProgressLog)this.progressLog).unsafeSetConfig(DatabaseDescriptor.getAccordProgressLogConfig());

        maybeLoadRangesForEpoch(journal.loadRangesForEpoch(id()));
        maybeLoadRedundantBefore(journal.loadRedundantBefore(id()));
        maybeLoadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
        maybeLoadSafeToRead(journal.loadSafeToRead(id()));
        maybeLoadRangesForEpoch(journal.loadRangesForEpoch(id()));
        RangesForEpoch ranges = this.rangesForEpoch;
        Invariants.require(ranges != null && !ranges.all().isEmpty(), "CommandStore %d created with no ranges", id);
        tableId = (TableId)ranges.all().stream().map(r -> r.start().prefix()).reduce((a, b) -> {
            Invariants.require(a.equals(b), "CommandStore created with multiple distinct TableId (%s and %s)", a, b);
            return a;
        }).orElseThrow(() -> Invariants.illegalState("CommandStore %d created with no ranges", id));

        final AccordCache.Type<TxnId, Command, SaferCommand>.Instance commands;
        final AccordCache.Type<RoutingKey, CommandsForKey, SaferCommandsForKey>.Instance commandsForKey;
        try (AccordExecutor.ExclusiveGlobalCaches exclusive = sharedExecutor.lockCaches())
        {
            commands = exclusive.commands.newInstance(this);
            commandsForKey = exclusive.commandsForKey.newInstance(this);
            this.caches = new ExclusiveCaches(sharedExecutor, exclusive.global, commands, commandsForKey);
        }
        this.exclusiveExecutor = sharedExecutor.newExclusiveExecutor(id);

        {
            // a test may supply its own index, so that a range scan can be driven without one populated behind it
            java.util.function.Function<AccordCommandStore, RangeIndex> factory = unsafeRangeIndexFactory;
            if (factory != null)
            {
                rangeIndex = factory.apply(this);
            }
            else
            {
                AccordConfig.RangeIndexMode mode = getAccord().range_index_mode;
                switch (mode)
                {
                    default: throw new UnhandledEnum(mode);
                    case journal_sai: rangeIndex = new JournalRangeIndex(this); break;
                    case in_memory: rangeIndex = new InMemoryRangeIndex(this); break;
                }
            }
        }

        if (AccordService.isStarted())
            progressLog.unsafeStart();
    }

    static Factory factory(IntFunction<AccordExecutor> executorFactory)
    {
        return (id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal) ->
               new AccordCommandStore(id, node, agent, dataStore, progressLogFactory, listenerFactory, rangesForEpoch, journal, executorFactory.apply(id));
    }

    /**
     * Test-only override for {@code range_index_mode}. Supplies a whole {@link RangeIndex} rather than intercepting a
     * scan, so that {@code SafeTask.RangeTxnScanner} needs no test-only branches.
     */
    @VisibleForTesting
    public static volatile java.util.function.Function<AccordCommandStore, RangeIndex> unsafeRangeIndexFactory = null;

    public RangeIndex rangeIndex()
    {
        return rangeIndex;
    }

    @Override
    public boolean inStore()
    {
        return exclusiveExecutor.inExecutor();
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
    public ExclusiveExecutor exclusiveExecutor()
    {
        return exclusiveExecutor;
    }

    public ExclusiveCaches lockCaches()
    {
        //noinspection LockAcquiredButNotSafelyReleased
        caches.owner.lock(TaskRunner.get());
        return caches;
    }

    public ExclusiveCaches tryLockCaches()
    {
        if (caches.owner.tryLock(TaskRunner.get()))
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

    @VisibleForTesting
    public void sanityCheckCommand(RedundantBefore redundantBefore, Command command)
    {
        ((AccordJournal) journal).sanityCheck(id, redundantBefore, command);
    }

    @Override
    public <T> AsyncChain<T> chain(ExecutionContext context, Function<? super SafeCommandStore, T> function)
    {
        return SafeTask.create(this, context, function).chain();
    }

    @Override
    public AsyncChain<Void> chain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return SafeTask.create(this, context, consumer).chain();
    }

    @Override
    public <T> AsyncChain<T> continuationChain(ExecutionContext context, Function<? super SafeCommandStore, T> function)
    {
        return SafeTask.createContinuation(this, context, function).chain();
    }

    @Override
    public <T> Cancellable executeContinuation(ExecutionContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
    {
        return SafeTask.createContinuation(this, context, function).submit(callback);
    }

    @Override
    public AsyncChain<Void> continuationChain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return SafeTask.createContinuation(this, context, consumer).chain();
    }

    @Override
    public Cancellable executeContinuation(ExecutionContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        return SafeTask.createContinuation(this, context, consumer).submit(callback);
    }

    @Override
    public <T> AsyncChain<T> chain(Callable<T> call)
    {
        return exclusiveExecutor().chain(call);
    }

    @Override
    public AsyncChain<Void> continuationChain(Runnable run)
    {
        return exclusiveExecutor().continuationChain(run);
    }

    @Override
    public void execute(Runnable run)
    {
        exclusiveExecutor().execute(run);
    }

    @Override
    public Cancellable execute(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return exclusiveExecutor().execute(run, callback);
    }

    @Override
    public <V> Cancellable execute(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return exclusiveExecutor().execute(call, callback);
    }

    @Override
    public <V> Cancellable flatExecute(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return exclusiveExecutor().flatExecute(call, callback);
    }

    @Override
    public Cancellable executeContinuation(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return exclusiveExecutor().executeContinuation(run, callback);
    }

    @Override
    public <V> Cancellable executeContinuation(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return exclusiveExecutor().executeContinuation(call, callback);
    }

    @Override
    public <V> Cancellable flatExecuteContinuation(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return exclusiveExecutor().flatExecuteContinuation(call, callback);
    }

    @Override
    public boolean tryExecuteImmediately(Runnable run)
    {
        return exclusiveExecutor().tryExecuteImmediately(run);
    }

    public SaferCommandStore begin(SaferCommandStore safeStore)
    {
        require(current == null);
        current = safeStore;
        return current;
    }

    public void complete(SaferCommandStore store)
    {
        require(current == store);
        current = null;
    }

    public boolean hasSafeStore()
    {
        return current != null;
    }

    public DataStore dataStore()
    {
        return dataStore;
    }

    public ProgressLog progressLog()
    {
        return progressLog;
    }

    @Override
    public void shutdown()
    {
        shutdownAsync();
    }

    public AsyncResult<Void> shutdownAsync()
    {
        terminatedUpdater.compareAndSet(this, null, new Termination());
        progressLog.stop();
        return execute((Empty)() -> "Shutdown", safeStore -> {
            exclusiveExecutor.stop();
            logger.info("{} stopping. Durably applied: {}, waiting: {}", this,
                        DurablyAppliedTo.summarise(safeStore.redundantBefore(), DurablyAppliedTo::isDone),
                        DurablyAppliedTo.summarise(safeStore.redundantBefore(), DurablyAppliedTo::isNotDone));
            this.ensureDurable(null, ReportDurable.commandStoreFlush());
            dataStore.ensureDurable(this, RedundantBefore.EMPTY, ReportDurable.DATA_STORE_FLUSH);
        });
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

    void maybeTerminated(boolean setCommandStoreDurable, boolean setDataStoreDurable)
    {
        if (terminated != null)
        {
            if (setCommandStoreDurable) terminated.commandStoreFlushed = true;
            if (setDataStoreDurable) terminated.dataStoreFlushed = true;
            if (terminated.isReadyToTerminate())
            {
                Invariants.require(exclusiveExecutor.stopped());
                boolean syncPointsDurable = unsafeGetRedundantBefore().foldl((b, v, p2, p3) -> {
                    return v && (b == null || b.maxBound(LOCALLY_APPLIED).compareTo(b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE)) <= 0);
                }, true, null, null, ignore -> false);

                if (!syncPointsDurable)
                    logger.error("{} has flushed command and data stores, but sync points recorded in RedundantBefore are not durable: {}", this, DurablyAppliedTo.summarise(unsafeGetRedundantBefore()));

                exclusiveExecutor.fullStop();
                terminated.signalAll();
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

    public MinimalCommand loadMinimal(TxnId txnId)
    {
        return journal.loadMinimal(id, txnId, safeGetRedundantBefore(), durableBefore());
    }

    public MinimalWithDeps loadMinimalWithDeps(TxnId txnId)
    {
        return journal.loadMinimalWithDeps(id, txnId, safeGetRedundantBefore(), durableBefore());
    }

    public AccordCompactionInfo getCompactionInfo()
    {
        SafeRedundantBefore safeRedundantBefore = this.safeRedundantBefore;
        RedundantBefore redundantBefore;
        if (safeRedundantBefore == null) redundantBefore = RedundantBefore.EMPTY;
        else redundantBefore = safeRedundantBefore.redundantBefore;
        RangesForEpoch ranges = this.rangesForEpoch;
        if (ranges == null) ranges = RangesForEpoch.EMPTY;
        return new AccordCompactionInfo(id, redundantBefore, ranges, tableId);
    }

    public final RedundantBefore safeGetRedundantBefore()
    {
        return safeRedundantBefore.redundantBefore;
    }

    @Override
    public AccordCommandStoreReplayer replayer(Mode mode)
    {
        ReplayMode replayMode = getAccord().journal.replay;
        return new AccordCommandStoreReplayer(this, mode);
    }
    
    static final AtomicLong nextDurabilityLoggingId = new AtomicLong();

    @Override
    protected void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable)
    {
        ensureDurable(ranges, ReportDurable.of(onCommandStoreDurable));
    }

    protected void ensureDurable()
    {
        RedundantBefore forCommandStore = nonDurable(safeGetRedundantBefore(), LOCALLY_DURABLE_TO_COMMAND_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY);
        RedundantBefore forDataStore = nonDurable(safeGetRedundantBefore(), LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_DATA_STORE_ONLY);
        this.ensureDurable(forCommandStore.ranges(Objects::nonNull), forCommandStore);
        dataStore.ensureDurable(this, forDataStore, 0);
    }

    private RedundantBefore nonDurable(RedundantBefore redundantBefore, Property durableProperty, SomeStatus durableStatus)
    {
        return redundantBefore.map(b -> {
            if (b == null)
                return null;

            TxnId applied = b.maxBound(LOCALLY_APPLIED);
            TxnId durable = b.maxBound(durableProperty);
            if (applied.compareTo(durable) <= 0)
                return null;
            return Bounds.create(b.range, b.maxBound(LOCALLY_APPLIED), durableStatus, null);
        });
    }

    protected void ensureDurable(@Nullable Ranges ranges, ReportDurable onCommandStoreDurable)
    {
        if (node().isReplaying() && onCommandStoreDurable.flags == 0 && safeGetRedundantBefore().isAtLeast(onCommandStoreDurable.redundantBefore))
            return;

        long reportId = nextDurabilityLoggingId.incrementAndGet();
        logger.debug("{} durability: ensuring for {} ({})", this, onCommandStoreDurable, reportId);
        executor().afterSubmittedAndConsequences(() -> {
            logger.debug("{} durability: saving intersecting keys ({})", this, reportId);
            class Ready extends CountingResult implements BiConsumer<Void, Throwable>
            {
                public Ready() { super(1); }

                @Override
                public void accept(Void success, Throwable failure)
                {
                    if (failure == null) decrement();
                    else tryFailure(failure);
                }

                void maybeFlush(ExclusiveCaches caches, AccordCacheEntry<RoutingKey, CommandsForKey, ?> e)
                {
                    if (e.isInconsistent())
                    {
                        noSpamLogger.warn("{} durability: refusing to report {} ({}): a failed update blocks progress for {}",
                                          AccordCommandStore.this, onCommandStoreDurable, reportId, e.key());
                        throw new InconsistentEntryException(e.key());
                    }

                    if (e.isModified())
                    {
                        increment();
                        caches.global().saveWhenReadyExclusive(e, this);
                    }
                }
            }

            Ready ready = new Ready();
            try (ExclusiveCaches caches = lockCaches())
            {
                int count = 0;
                if (ranges == null)
                {
                    try
                    {
                        for (AccordCacheEntry<RoutingKey, CommandsForKey, ?> e : caches.commandsForKeys())
                        {
                            ++count;
                            ready.maybeFlush(caches, e);
                        }
                    }
                    catch (Throwable t)
                    {
                        for (AccordCacheEntry<RoutingKey, CommandsForKey, ?> e : caches.commandsForKeys())
                        {
                            if (--count < 0)
                                break;
                            try { caches.global().unregisterSaveCallback(e, ready); }
                            catch (Throwable t2) { t.addSuppressed(t2); }
                        }
                        throw t;
                    }
                }
                else
                {
                    try
                    {
                        for (Range range : ranges)
                        {
                            for (RoutingKey k : caches.commandsForKeys().keysBetween(range.start(), range.startInclusive(), range.end(), range.endInclusive()))
                            {
                                ++count;
                                ready.maybeFlush(caches, caches.commandsForKeys().getUnsafe(k));
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        outer: for (Range range : ranges)
                        {
                            for (RoutingKey k : caches.commandsForKeys().keysBetween(range.start(), range.startInclusive(), range.end(), range.endInclusive()))
                            {
                                if (--count < 0)
                                    break outer;
                                try { caches.global().unregisterSaveCallback(caches.commandsForKeys().getUnsafe(k), ready); }
                                catch (Throwable t2) { t.addSuppressed(t2); }
                            }
                        }
                        throw t;
                    }
                }
            }

            ready.invoke((success, fail) -> {
                if (fail != null)
                {
                    logger.error("{} failed to ensure durability of {} ({})", this, ranges, reportId, fail);
                }
                else
                {
                    logger.debug("{} waiting for CommandsForKey to flush ({})", this, reportId);
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

            return commandStore.chain(ExecutionContext.unsequenced(txnId, "Replay"), safeStore -> {
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

    protected void loadRedundantBefore(RedundantBefore redundantBefore)
    {
        super.loadRedundantBefore(redundantBefore);
        safeRedundantBefore = new SafeRedundantBefore(0, redundantBefore);
    }

    protected void maybeLoadRedundantBefore(RedundantBefore redundantBefore)
    {
        if (redundantBefore != null && !redundantBefore.isEmpty())
            loadRedundantBefore(redundantBefore);
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

    void maybeLoadRangesForEpoch(RangesForEpoch rangesForEpoch)
    {
        if (rangesForEpoch != null)
            loadRangesForEpoch(rangesForEpoch);
    }

    AsyncChain<Boolean> saveState(Descriptor descriptor)
    {
        return chain((Unterminatable)() -> "Save State", safeStore -> {
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

            RedundantBefore validateRedundantBefore = journal.loadRedundantBefore(id);
            Invariants.expect(validateRedundantBefore.equals(unsafeGetRedundantBefore()), "Journal RedundantBefore does not match in memory: %s != %s", validateRedundantBefore, unsafeGetRedundantBefore());

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
                logger.info("{} saving state to {}", this, saveDir);
                tmpSaveDir.createDirectoriesIfNotExists();
                writeOne(new File(tmpSaveDir, "max_decidedrx"), unsafeGetMaxDecidedRX(), maxDecidedRX);
                writeOne(new File(tmpSaveDir, "max_conflicts"), unsafeGetMaxConflicts(), maxConflicts);
                writeList(new File(tmpSaveDir, "listeners"), ((DefaultLocalListeners)listeners).snapshot(), txnListener);
                writeList(new File(tmpSaveDir, "progress_log"), ((DefaultProgressLog)progressLog).snapshot(), progressLogState);
                rangeIndex.save(new File(tmpSaveDir, "range_index"));
                writeOne(new File(tmpSaveDir, "redundant_before"), unsafeGetRedundantBefore(), redundantBefore);
                tmpSaveDir.move(saveDir);
            }
            catch (Throwable t)
            {
                logger.error("{} failed to save replay state {}", this, saveDir, t);
                tmpSaveDir.tryDeleteRecursive();
                saveDir.tryDeleteRecursive();
                return false;
            }

            if (sortedSaveDirs != null)
            {
                int delete = (sortedSaveDirs.length + 1) - getAccord().journal.retainSavePoints;
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
        return chain((Empty)() -> "Restore State", safeStore -> {
            File storeDir = storeSaveDir();
            File[] savePoints = listSortedSaveDirs(storeDir);
            if (savePoints == null)
            {
                logger.info("{} no save points found at {}", this, storeDir);
                return null;
            }

            File savePoint = savePoints[savePoints.length - 1];
            long segment = Long.parseLong(savePoint.name());
            MaxDecidedRX mxd; MaxConflicts mxc;
            List<TxnListener> dll; List<TxnState> dpl; Object rgi;
            RedundantBefore rdb;
            try
            {
                logger.info("{} loading state from {}", this, savePoint);
                mxd = readOne(new File(savePoint, "max_decidedrx"), maxDecidedRX);
                mxc = readOne(new File(savePoint, "max_conflicts"), maxConflicts);
                {
                    File rjbf = new File(savePoint, "reject_before");
                    if (rjbf.exists())
                        mxc = mxc.update(readOne(rjbf, rejectBefore));
                }
                dll = readList(new File(savePoint, "listeners"), txnListener);
                dpl = readList(new File(savePoint, "progress_log"), progressLogState);
                rgi = rangeIndex.load(new File(savePoint, "range_index"));
                rdb = readOne(new File(savePoint, "redundant_before"), redundantBefore);
            }
            catch (Throwable t)
            {
                logger.warn("{} could not replay save point {}", this, savePoint, t);
                return null;
            }

            if (journal instanceof AccordJournal && ((AccordJournal)journal).maxDescriptor() <= segment)
                Invariants.expect(rdb.equals(unsafeGetRedundantBefore()));

            rangeIndex.restore(rgi);
            unsafeSetMaxDecidedRX(mxd);
            unsafeSetMaxConflicts(mxc);
            ((DefaultLocalListeners) listeners).restore(dll);
            boolean unsetCatchup = ((DefaultProgressLog) progressLog).setModeExclusive(safeStore, CATCH_UP);
            ((DefaultProgressLog) progressLog).restore(safeStore, dpl);
            if (unsetCatchup)
                ((DefaultProgressLog) progressLog).unsetModeExclusive(CATCH_UP);
            return Map.entry(id, segment + 1);
        });
    }

    // TODO (expected): handle journal failures, and consider how we handle partial failures.
    //  Very likely we will not be able to safely or cleanly handle partial failures of this logic, but decide and document.
    // TODO (desired): consider merging with PersistentField? This version is cheaper to manage which may be preferable at the CommandStore level.
    public static class SafeRedundantBefore
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

        public static Runnable updater(AccordCommandStore commandStore, RedundantBefore newRedundantBefore)
        {
            long ticket = nextSafeRedundantBeforeTicket.incrementAndGet();
            SafeRedundantBefore update = new SafeRedundantBefore(ticket, newRedundantBefore);
            return () -> {
                safeRedundantBeforeUpdater.accumulateAndGet(commandStore, update, SafeRedundantBefore::max);
            };
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
        sb.append('|')
          .append(id).append(',')
          .append(executor().executorId).append(',')
          .append(node.id().id)
          .append(']');
        return sb.toString();
    }

    public static class DurablyAppliedTo
    {
        final TxnId journal, commandStore, dataStore;

        public DurablyAppliedTo(Bounds bounds)
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
