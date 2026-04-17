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

package accord.local;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.topology.EpochReady;
import accord.api.DataStore;
import accord.api.DataStore.FetchKind;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.impl.AbstractAsyncExecutor;
import accord.coordinate.CoordinateMaxConflict;
import accord.local.CommandStores.BootstrapRangeAction;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Commands.NotifyWaitingOnPlus;
import accord.local.PreLoadContext.Empty;
import accord.local.RedundantBefore.Bounds;
import accord.local.RedundantStatus.SomeStatus;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableByCallback;
import accord.utils.async.AsyncResults.SettableWithDescription;
import accord.utils.async.Cancellable;
import accord.utils.async.AsyncResults.SettableResult;
import org.agrona.collections.LongHashSet;

import static accord.topology.EpochReady.DONE;
import static accord.topology.EpochReady.done;
import static accord.api.DataStore.FetchKind.Image;
import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_DATA_STORE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_WITNESSED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOG_UNAVAILABLE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.QUORUM_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.UNREADY_ONLY;
import static accord.local.RedundantStatus.SomeStatus.SHARD_APPLIED_ONLY;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.utils.Invariants.nonNull;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AbstractAsyncExecutor, SequentialAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    public static class EpochUpdate
    {
        public final RangesForEpoch newRangesForEpoch;
        public final RedundantBefore addRedundantBefore;

        EpochUpdate(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            this.newRangesForEpoch = newRangesForEpoch;
            this.addRedundantBefore = addRedundantBefore;
        }
    }

    // TODO (required): we only REMOVE ranges now, so it should be possible to simplify this
    public static class EpochUpdateHolder extends AtomicReference<EpochUpdate>
    {
        // TODO (desired): can better encapsulate by accepting only the newRangesForEpoch and deriving the add/remove ranges
        public void add(long epoch, RangesForEpoch newRangesForEpoch, Ranges addRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(addRanges, epoch, Long.MAX_VALUE, TxnId.minForEpoch(epoch), UNREADY_ONLY);
            update(newRangesForEpoch, addRedundantBefore);
        }

        public void remove(long epoch, RangesForEpoch newRangesForEpoch, Ranges removeRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(removeRanges, Long.MIN_VALUE, epoch, TxnId.NONE, SomeStatus.NONE);
            update(newRangesForEpoch, addRedundantBefore);
        }

        private void update(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            EpochUpdate baseUpdate = new EpochUpdate(newRangesForEpoch, addRedundantBefore);
            EpochUpdate cur = get();
            if (cur == null || !compareAndSet(cur, new EpochUpdate(newRangesForEpoch, RedundantBefore.merge(cur.addRedundantBefore, addRedundantBefore))))
                set(baseUpdate);
        }
    }

    public interface Factory
    {
        CommandStore create(int id,
                            NodeCommandStoreService node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            LocalListeners.Factory listenersFactory,
                            EpochUpdateHolder rangesForEpoch,
                            Journal journal);
    }

    protected final int id;
    protected final NodeCommandStoreService node;
    protected final Agent agent;
    protected final DataStore dataStore;
    protected final ProgressLog progressLog;
    protected final LocalListeners listeners;
    protected final EpochUpdateHolder epochUpdateHolder;

    // Used in markShardStale to make sure the staleness includes in progress bootstraps
    // TODO (desired): migrate to BTree
    private transient NavigableMap<TxnId, Ranges> bootstrapBeganAt = emptyBootstrapBeganAt(); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    protected boolean hasResumedBootstraps;
    private RedundantBefore redundantBefore = RedundantBefore.EMPTY;
    private MaxConflicts maxConflicts = MaxConflicts.EMPTY;
    private MaxDecidedRX maxDecidedRX = MaxDecidedRX.EMPTY;
    private int maxConflictsUpdates = 0;
    protected RangesForEpoch rangesForEpoch;
    protected @Nullable Ranges refuses;
    List<SyncPointListener> syncPointListeners;

    /**
     * safeToRead is related to RedundantBefore, but a distinct concept.
     * While readyAt defines the txnId bounds we expect to maintain data for locally,
     * safeToRead defines executeAt bounds we can safely participate in transaction execution for.
     * safeToRead is defined by the no-op transaction we execute after a bootstrap is initiated,
     * and creates a global bound before which we know we have complete data from our bootstrap.
     *
     * There's a smearing period during bootstrap where some keys may be ahead of others, for instance,
     * since we do not create a precise instant in the transaction log for bootstrap to avoid impeding execution.
     *
     * We also update safeToRead when we go stale, to remove ranges we may have bootstrapped but that are now known to
     * be incomplete. In this case we permit transactions to execute in any order for the unsafe key ranges.
     * But they may still be ordered for other key ranges they participate in.
     */
    private NavigableMap<Timestamp, Ranges> safeToRead = emptySafeToRead();
    private Ranges permanentlyUnsafeToRead = Ranges.EMPTY;
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    @Nullable private RejectBefore rejectBefore;

    static class WaitingOnVisibility
    {
        final SettableResult<Void> whenDone;
        final Ranges allRanges;
        Ranges waitingOn, waitingOnDurable;

        WaitingOnVisibility(SettableResult<Void> whenDone, Ranges ranges)
        {
            this.whenDone = whenDone;
            this.allRanges = this.waitingOn = this.waitingOnDurable = ranges;
        }
    }
    private final TreeMap<Long, WaitingOnVisibility> waitingOnVisibility = new TreeMap<>();

    protected CommandStore(int id,
                           NodeCommandStoreService node,
                           Agent agent,
                           DataStore dataStore,
                           ProgressLog.Factory progressLogFactory,
                           LocalListeners.Factory listenersFactory,
                           EpochUpdateHolder epochUpdateHolder)
    {
        this.id = id;
        this.node = node;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.listeners = listenersFactory.create(this);
        this.epochUpdateHolder = epochUpdateHolder;
    }

    public final int id()
    {
        return id;
    }

    public void restore() {};

    public abstract Journal.Replayer replayer();
    // expected to invoke safeStore.upsertRedundantBefore at some future point, when the commandStore state is durably persisted
    protected abstract void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable);

    public Agent agent()
    {
        return agent;
    }

    public void unsafeClearForTesting()
    {
        progressLog.clear();
        bootstraps.clear();
        rangesForEpoch = null;
        bootstrapBeganAt = emptyBootstrapBeganAt();
        redundantBefore = RedundantBefore.EMPTY;
        maxConflicts = MaxConflicts.EMPTY;
        maxDecidedRX = MaxDecidedRX.EMPTY;
        safeToRead = emptySafeToRead();
        listeners.clear();
        waitingOnVisibility.clear();
    }

    public void updateRangesForEpoch(SafeCommandStore safeStore)
    {
        EpochUpdate update = epochUpdateHolder.get();
        if (update == null)
            return;

        update = epochUpdateHolder.getAndSet(null);
        if (update.addRedundantBefore.size() > 0)
            safeStore.upsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            safeStore.setRangesForEpoch(update.newRangesForEpoch);

        safeStore.persistFieldUpdates();
    }

    @VisibleForTesting
    public void unsafeUpdateRangesForEpoch()
    {
        EpochUpdate update = epochUpdateHolder.getAndSet(null);
        if (update == null)
            return;

        if (update.addRedundantBefore.size() > 0)
            unsafeUpsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            unsafeSetRangesForEpoch(update.newRangesForEpoch);
    }

    public RangesForEpoch unsafeGetRangesForEpoch()
    {
        return rangesForEpoch;
    }

    public MaxDecidedRX unsafeGetMaxDecidedRX()
    {
        return maxDecidedRX;
    }

    @VisibleForTesting
    public final void unsafeSetRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        rangesForEpoch = nonNull(newRangesForEpoch);
    }

    protected final void unsafeClearRangesForEpoch()
    {
        rangesForEpoch = null;
    }

    protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        Invariants.require(this.rangesForEpoch == null);
        unsafeSetRangesForEpoch(newRangesForEpoch);
    }

    public abstract boolean inStore();

    public boolean tryExecuteImmediately(Runnable run)
    {
        if (!inStore())
            return false;

        try { run.run(); }
        catch (Throwable t) { agent.onException(t); }
        return true;
    }

    public abstract AsyncChain<Void> chain(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    public abstract <T> AsyncChain<T> chain(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    public Cancellable execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        return chain(context, consumer).begin(callback);
    }

    public AsyncResult<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return chain(context, consumer).beginAsResult();
    }

    public <T> Cancellable execute(PreLoadContext context, Function<? super SafeCommandStore, T> apply, BiConsumer<? super T, Throwable> callback)
    {
        return chain(context, apply).begin(callback);
    }

    public <T> AsyncResult<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply)
    {
        return chain(context, apply).beginAsResult();
    }

    public abstract void shutdown();

    protected void unsafeSetMaxDecidedRX(MaxDecidedRX newMaxDecidedRX)
    {
        this.maxDecidedRX = newMaxDecidedRX;
    }

    protected void unsafeSetRejectBefore(RejectBefore newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
    }

    final void unsafeSetRedundantBefore(RedundantBefore newRedundantBefore)
    {
        redundantBefore = newRedundantBefore;
    }

    protected void unsafeClearRedundantBefore()
    {
        unsafeSetRedundantBefore(null);
    }

    protected void loadRedundantBefore(RedundantBefore newRedundantBefore)
    {
        Invariants.require(redundantBefore == null || redundantBefore.equals(RedundantBefore.EMPTY));
        Invariants.require(newRedundantBefore != null);
        unsafeSetRedundantBefore(newRedundantBefore);
    }

    protected void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        unsafeSetRedundantBefore(RedundantBefore.merge(redundantBefore, addRedundantBefore));
    }

    @VisibleForTesting
    public boolean unsafeIsRefusingAny()
    {
        return refuses != null;
    }

    protected void unsafeRefuseRequests(Ranges refuse)
    {
        Invariants.require(refuses == null || !refuses.intersects(refuse));
        if (refuses == null) refuses = refuse;
        else refuses = refuses.with(refuse);
    }

    protected void unsafeAcceptRequests(Ranges accept)
    {
        Invariants.require(refuses != null && refuses.containsAll(accept));
        refuses = refuses.without(accept);
        if (refuses.isEmpty())
            refuses = null;
    }

    /**
     * This method may be invoked on a non-CommandStore thread
     */
    final void unsafeSetSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        if (newSafeToRead != null)
        {
            for (Map.Entry<Timestamp, Ranges> entry : newSafeToRead.entrySet())
            {
                Ranges rangeExcluded = entry.getValue().without(this.permanentlyUnsafeToRead);
                logger.info("{} is excluded from newSafeToRead because it is in the regained ranges", rangeExcluded);
            }
        }

        node.updateStamp();
        this.safeToRead = newSafeToRead;
    }

    final void unsafeSetPermanentlyUnsafeToRead(Ranges newPermanentlyUnsafeToRead)
    {
        this.permanentlyUnsafeToRead = newPermanentlyUnsafeToRead;
    }

    protected final void unsafeClearSafeToRead()
    {
        unsafeSetSafeToRead(null);
    }

    protected void loadSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        Invariants.require(safeToRead == null || safeToRead.equals(emptySafeToRead()));
        Invariants.require(newSafeToRead != null);
        unsafeSetSafeToRead(newSafeToRead);
        updateMaxConflicts(newSafeToRead);
    }

    final void unsafeSetBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        this.bootstrapBeganAt = newBootstrapBeganAt;
    }

    protected final void unsafeClearBootstrapBeganAt()
    {
        unsafeSetBootstrapBeganAt(null);
    }

    protected synchronized void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        Invariants.require(bootstrapBeganAt == null || bootstrapBeganAt.equals(emptyBootstrapBeganAt()));
        Invariants.require(newBootstrapBeganAt != null);
        unsafeSetBootstrapBeganAt(newBootstrapBeganAt);
        updateMaxConflicts(newBootstrapBeganAt);
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setMaxConflicts(MaxConflicts maxConflicts)
    {
        this.maxConflicts = maxConflicts;
    }

    protected int dumpCounter = 0;

    protected void updateMaxConflicts(Command prev, Command updated, boolean force)
    {
        Timestamp executeAt = updated.executeAt();
        if (executeAt == null) return;
        if (prev != null && prev.executeAt() != null && prev.executeAt().compareToStrict(executeAt) >= 0 && !force) return;
        executeAt = executeAt.flattenUniqueHlc(); // this is what guarantees a bootstrap recipient can compute uniqueHlc safely
        MaxConflicts updatedMaxConflicts = maxConflicts.update(updated.participants().hasTouched(), executeAt);
        updateMaxConflicts(executeAt, updatedMaxConflicts);
    }

    protected void updateMaxConflicts(Ranges ranges, Timestamp executeAt)
    {
        updateMaxConflicts(executeAt, maxConflicts.update(ranges, executeAt));
    }

    protected void updateMaxConflicts(NavigableMap<? extends Timestamp, Ranges> map)
    {
        Timestamp max = Timestamp.NONE;
        MaxConflicts updated = maxConflicts;
        for (Map.Entry<? extends Timestamp, Ranges> e : map.entrySet())
        {
            Timestamp at = e.getKey();
            if (at.compareTo(Timestamp.NONE) > 0)
            {
                updated = updated.update(e.getValue(), at);
                max = Timestamp.max(max, at);
            }
        }
        if (updated != maxConflicts)
            updateMaxConflicts(max, updated);
    }

    protected void updateMaxConflicts(Timestamp executeAt, MaxConflicts updatedMaxConflicts)
    {
        if (++maxConflictsUpdates >= agent.maxConflictsPruneInterval())
        {
            int initialSize = updatedMaxConflicts.size();
            MaxConflicts initialConflicts = updatedMaxConflicts;
            long pruneHlc = executeAt.hlc() - agent.maxConflictsHlcPruneDelta();
            Timestamp pruneBefore = pruneHlc > 0 ? Timestamp.fromValues(executeAt.epoch(), pruneHlc, executeAt.node) : null;
            Ranges ranges = rangesForEpoch.all();
            if (pruneBefore != null)
                updatedMaxConflicts = updatedMaxConflicts.update(ranges, pruneBefore);

            int prunedSize = updatedMaxConflicts.size();
            if (initialSize > 100 && prunedSize == initialSize)
            {
                logger.debug("Ineffective prune for {}. Initial size: {}, pruned size: {}, executeAt: {}, pruneBefore: {}", ranges, initialSize, prunedSize, executeAt, pruneBefore);
                if (dumpCounter == 0)
                {
                    logger.trace("initial MaxConflicts dump: {}", initialConflicts);
                    logger.trace("pruned MaxConflicts dump: {}", updatedMaxConflicts);
                }
                dumpCounter++;
                dumpCounter %= 100;
            }
            else if (prunedSize != initialSize)
            {
                logger.trace("Successfully pruned {} to {}", initialSize, prunedSize);
            }


            maxConflictsUpdates = 0;
        }
        setMaxConflicts(updatedMaxConflicts);
    }

    final void markExclusiveSyncPoint(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.isSyncPoint());
        RejectBefore newRejectBefore = rejectBefore != null ? rejectBefore : new RejectBefore();
        newRejectBefore = RejectBefore.add(newRejectBefore, ranges, txnId);
        unsafeSetRejectBefore(newRejectBefore);
    }

    final void markExclusiveSyncPointDecided(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        unsafeSetMaxDecidedRX(maxDecidedRX.update(ranges, txnId));
    }

    final void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.isSyncPoint());
        RedundantBefore addNow = RedundantBefore.create(ranges, txnId, LOCALLY_APPLIED_ONLY);
        safeStore.upsertRedundantBefore(addNow);
        RedundantBefore addOnDataStoreDurable = RedundantBefore.create(ranges, txnId, LOCALLY_DURABLE_TO_DATA_STORE_ONLY);
        RedundantBefore addOnCommandStoreDurable = RedundantBefore.create(ranges, txnId, LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY);
        dataStore.ensureDurable(this, ranges, addOnDataStoreDurable);
        ensureDurable(ranges, addOnCommandStoreDurable);
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    final Timestamp preaccept(TxnId txnId, Routables<?> keys, SafeCommandStore safeStore, boolean permitFastPath)
    {
        NodeCommandStoreService node = safeStore.node();

        boolean isExpired = safeStore.agent().rejectPreAccept(safeStore.node(), txnId) && !txnId.isSyncPoint();
        if (rejectBefore != null && !isExpired)
            isExpired = rejectBefore.rejects(txnId, keys);

        if (isExpired)
            return node.uniqueTimestamp(txnId).asRejected();

        Timestamp min = TxnId.mergeMax(txnId, maxConflicts.get(keys));
        if (permitFastPath && txnId == min && txnId.epoch() >= node.epoch())
            return txnId;

        return node.uniqueTimestamp(min);
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    public final Timestamp maxConflict(Routables<?> keys)
    {
        return maxConflicts.get(keys);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{id=" + id + ", node=" + node.id().id + '}';
    }

    public final AsyncResult<Void> cancelBootstraps()
    {
        return submit((Empty)() -> "Cancel Bootstraps", safeStore -> {
            cancelBootstraps(safeStore, safeStore.ranges().all());
            return null;
        });
    }

    public final void cancelBootstraps(SafeCommandStore safeStore, Ranges ranges)
    {
        Invariants.require(safeStore.commandStore() == this && inStore());
        bootstraps.forEach(b -> b.invalidate(ranges));
    }

    public final AsyncResult<EpochReady> resumeBootstrap(Node node)
    {
        synchronized (this)
        {
            Invariants.require(!hasResumedBootstraps);
            hasResumedBootstraps = true;
        }

        return submit((Empty)() -> "Resume Bootstrap", safeStore -> {
            Ranges unfinished = rangesForEpoch.all();
            unfinished = unfinished.without(safeToRead.lastEntry().getValue());
            unfinished = redundantBefore.removeLostOrStale(unfinished);
            for (Bootstrap bootstrap : bootstraps)
                unfinished = unfinished.without(bootstrap.all);

            long epoch = rangesForEpoch.epochAtIndex(0);
            if (unfinished.isEmpty())
                return done(epoch);

            logger.info("{}: Resuming bootstrap of {}", this, unfinished);
            return epochReadyAfterBootstrap(unfinished, epoch, startSafeBootstrapInternal(node, safeStore, unfinished, epoch));
        });
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    final Supplier<EpochReady> bootstrapper(Node node, Ranges newRanges, long epoch, BootstrapRangeAction action)
    {
        switch (action)
        {
            default: throw new UnhandledEnum(action);
            case BOOTSTRAP_NOT_NEEDED:
                return () -> {
                    AsyncResult<Void> done = execute((Empty) () -> "Initialise New Epoch", (safeStore) -> {
                        logger.info("{}: Initialising {} for epoch {}", this, newRanges, epoch);
                        // Merge in a base for any ranges that needs to be covered
                        Ranges newBootstrapRanges = newRanges;
                        for (Ranges existing : bootstrapBeganAt.values())
                            newBootstrapRanges = newBootstrapRanges.without(existing);
                        if (!newBootstrapRanges.isEmpty())
                            safeStore.setBootstrapBeganAt(bootstrap(TxnId.NONE, newBootstrapRanges, bootstrapBeganAt));
                        safeStore.setSafeToRead(purgeAndInsert(safeToRead, TxnId.NONE, newRanges));
                        markExclusiveSyncPointDecided(safeStore, TxnId.NONE, newRanges);
                    });

                    return EpochReady.all(epoch, done);
                };
            case SAFE_BOOTSTRAP:
                return () -> epochReadyAfterBootstrap(newRanges, epoch, startSafeBootstrap(node, newRanges, epoch));

            case UNSAFE_BOOTSTRAP:
                return () -> epochReadyAfterBootstrap(newRanges, epoch, startUnsafeBootstrap(node, newRanges, epoch, Image));
        }
    }

    private EpochReady epochReadyAfterBootstrap(Ranges newRanges, long epoch, AsyncResult<EpochReady> bootstrap)
    {
        return epochReadyAfterBootstrap(newRanges, epoch, EpochReady.wrap(epoch, bootstrap));
    }

    private EpochReady epochReadyAfterBootstrap(Ranges newRanges, long epoch, EpochReady bootstrap)
    {
        AsyncResult<Void> readyToCoordinate = readyToCoordinate(newRanges, epoch);
        return new EpochReady(epoch,
                              bootstrap.active,
                              readyToCoordinate,
                              bootstrap.data,
                              bootstrap.reads);
    }

    private AsyncResult<EpochReady> startSafeBootstrap(Node node, Ranges newRanges, long epoch)
    {
        return submit((Empty) () -> "New Epoch", safeStore -> {
            return startSafeBootstrapInternal(node, safeStore, newRanges, epoch);
        });
    }

    private static final AsyncResult<Void> MUST_OVERWRITE = AsyncResults.failure(new IllegalStateException());
    private EpochReady startSafeBootstrapInternal(Node node, SafeCommandStore safeStore, Ranges newRanges, long epoch)
    {
        logger.info("{}: Starting Safe Bootstrap for {} for epoch {}", this, newRanges, epoch);
        Bootstrap bootstrap = new Bootstrap(node, this, epoch, newRanges);
        bootstraps.add(bootstrap);
        bootstrap.start(safeStore);
        return new EpochReady(epoch,
                              MUST_OVERWRITE,
                              MUST_OVERWRITE,
                              bootstrap.data,
                              bootstrap.reads);
    }

    /**
     * Rebootstraps some of the ranges for the command store. It follows steps similar to what
     * bootstrap would go through, with two differences:
     *
     *   * Marks pre-rebootstrap transactions with LOCALLY_LOST status, which means the node can not
     *     safely participate in pre-rebootstrap transactions, _even_ if they're coming after the node is
     *     done bootstrapping.
     *   * Marks the store as rebootstrapping, which will preclude rebootstrapping node from responding
     *     to PreAccept, Accept, and BeginRecovery and computing dependencies while node is being rebootstrapped,
     *     and ranges aren't ready to coordinate.
     */
    protected AsyncResult<EpochReady> startUnsafeBootstrap(Node node, Ranges ranges, long epoch, FetchKind fetch)
    {
        return submit((Empty) () -> "Refuse Requests for " + fetch + " Bootstrap", safeStore -> {
            unsafeRefuseRequests(ranges);
            safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            // TODO (expected): rationalise with startSafeBootstrap
            String description = "Bootstrap " + ranges + " for epoch " + epoch + " in " + this;
            return new EpochReady(epoch, MUST_OVERWRITE, readyToCoordinate(ranges, epoch), new SettableWithDescription<>(description), new SettableWithDescription<>(description));
        }).invoke((success, fail) -> {
            if (fail != null) logger.error("Fatal error initiating {} bootstrap for {}", this, fetch, fail);
            else rebootstrap(node, ranges, epoch, 1, success, fetch);
        });
    }

    private void rebootstrap(Node node, Ranges ranges, long epoch, int attempt, EpochReady ready, FetchKind fetch)
    {
        CoordinateMaxConflict
        .maxConflict(node, ranges)
        .recover(failure -> {
            Runnable retry = () -> rebootstrap(node, ranges, epoch, attempt + 1, ready, fetch);
            Runnable fail = () -> {
                ((SettableByCallback<Void>)ready.data).tryFailure(failure);
                ((SettableByCallback<Void>)ready.reads).tryFailure(failure);
            };
            agent.ownershipEvents().onFailedBootstrap(attempt, "Fetch Max Conflict (to mark log safe at)", ranges, retry, fail, failure);
            return AsyncChains.failure(failure);
        }).flatMap(success -> chain((Empty) () -> "Initiate Unsafe " + fetch + " Bootstrap", safeStore -> {
            node.uniqueNow(success.hlc()); // ensure we pick a higher timestamp than the maximum conflict we found globally
            // Mark unsafe to read first

            Ranges remaining = ranges.slice(rangesForEpoch.currentRanges(), Minimal);
            if (remaining.isEmpty())
            {
                logger.info("Terminating unsafe {} bootstrap process for {} as no active ranges", fetch, this);
                return AsyncChains.success(null);
            }

            Bootstrap bootstrap = new Bootstrap(node, this, epoch, remaining, fetch);
            bootstraps.add(bootstrap);
            // If rebootstrap can grab a later timestamp for subsequent attempts, but this timestamp is enough for us
            // to establish which transactions, for which ranges the node can safely participate in).
            TxnId unreadyBefore = bootstrap.start(safeStore);
            safeStore.unsafeUpsertRedundantBefore(RedundantBefore.create(ranges, unreadyBefore, LOG_UNAVAILABLE_ONLY));
            updateMaxConflicts(ranges, unreadyBefore);
            // TODO (desired): we could start accepting non-dep requests here
            bootstrap.data.invoke((SettableByCallback<Void>)ready.data);
            bootstrap.reads.invoke((SettableByCallback<Void>)ready.reads);
            ready.coordinate.invokeIfSuccess(() -> {
                execute((Empty)() -> "Accept Dependency Requests", safeStore0 -> {
                    unsafeAcceptRequests(remaining);
                });
            });
            return null;
        })).begin(agent);
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    protected Supplier<EpochReady> refreshReadyToCoordinate(Node node, Ranges ranges, long epoch)
    {
        return () -> {
            AsyncResult<Void> readyToCoordinate = readyToCoordinate(ranges, epoch);
            return new EpochReady(epoch, DONE, readyToCoordinate, DONE, DONE);
        };
    }

    // may be invoked by any thread without holding the command store lock
    private AsyncResult<Void> readyToCoordinate(Ranges ranges, long epoch)
    {
        if (redundantBefore.min(ranges, Bounds::locallyWitnessedBefore).epoch() >= epoch)
            return DONE;

        SettableResult<Void> whenDone = new SettableWithDescription<>(this + " is ready to coordinate " + ranges + " on epoch " + epoch);
        TxnId minForEpoch = TxnId.minForEpoch(epoch);
        Ranges remaining = redundantBefore.removeWitnessed(minForEpoch, ranges);
        WaitingOnVisibility sync = new WaitingOnVisibility(whenDone, remaining);
        synchronized (waitingOnVisibility)
        {
            WaitingOnVisibility prev = waitingOnVisibility.putIfAbsent(epoch, sync);
            Invariants.require(prev == null);
        }
        ensureReadyToCoordinate(epoch, ranges);
        return whenDone;
    }

    private void ensureReadyToCoordinate(long epoch, Ranges ranges)
    {
        TxnId minForEpoch = TxnId.minForEpoch(epoch);
        node.durability().close("[" + this + " Epoch " + epoch + ']', VisibilitySyncPoint, minForEpoch, ranges, 1, TimeUnit.HOURS)
            .invoke((success, fail) -> {
                if (fail != null)
                {
                    Ranges notRetired = redundantBefore.removeRetired(ranges);
                    Ranges retired = ranges.without(notRetired);
                    Ranges remaining = redundantBefore.removeWitnessed(minForEpoch, notRetired);

                    if (!retired.isEmpty())
                    {
                        logger.info("Failed to close epoch {} for ranges {} on store {}, but some are retired; marking these as synced.", epoch, ranges, id, fail);
                        execute((Empty)() -> "Mark Retired Ranges Synced", safeStore -> {
                            markVisibleInternal(safeStore, epoch, retired, "(Retired)");
                        });
                    }
                    else if (remaining.isEmpty())
                    {
                        logger.info("Failed to close epoch {} for ranges {} on store {}, but none remaining. Aborting.", epoch, ranges, id, fail);
                    }
                    if (!remaining.isEmpty())
                    {
                        logger.error("Failed to close epoch {} for ranges {} on store {}. Retrying.", epoch, remaining, id, fail);
                        node.someExecutor().execute(() -> ensureReadyToCoordinate(epoch, remaining));
                    }
                }
            });
    }

    Supplier<EpochReady> unbootstrap(long epoch, Ranges removedRanges)
    {
        return () -> {
            AsyncResult<Void> done = submit((Empty) () -> "Unbootstrap", safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.slice(removedRanges, Minimal);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                return null;
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    final void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    final void markBootstrapping(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        safeStore.setBootstrapBeganAt(bootstrap(globalSyncId, ranges, bootstrapBeganAt));
        safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
        updateMaxConflicts(ranges, globalSyncId);
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, globalSyncId, UNREADY_ONLY);
        safeStore.upsertRedundantBefore(addRedundantBefore);
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges durableRanges, HasOutcome durability)
    {
        if (!durability.isDurable())
            return;

        SomeStatus status = durability.isUniversal() ? SHARD_APPLIED_ONLY : QUORUM_APPLIED_ONLY;
        final Ranges slicedRanges = durableRanges.slice(safeStore.ranges().allUntil(globalSyncId.epoch()), Minimal);
        TxnId locallyRedundantBefore = safeStore.redundantBefore().min(slicedRanges, Bounds::maxLocallyAppliedBefore);
        RedundantBefore addNow = RedundantBefore.create(slicedRanges, globalSyncId, status);
        safeStore.upsertRedundantBefore(addNow);

        if (status != SHARD_APPLIED_ONLY)
            return;

        if (locallyRedundantBefore.compareTo(globalSyncId) < 0)
        {
            // TODO (expected): if bootstrapping only part of the range, mark the rest for GC; or relax this as can safely GC behind bootstrap
            TxnId maxBootstrap = safeStore.redundantBefore().max(slicedRanges, Bounds::maxReadyAt);
            if (maxBootstrap.compareTo(globalSyncId) >= 0)
                logger.info("Ignoring markShardDurable for a point we are bootstrapping. Bootstrapping: {}, Global: {}, Ranges: {}", maxBootstrap, globalSyncId, slicedRanges);
            else
                logger.warn("Trying to markShardDurable a point we have not yet caught-up to locally. Local: {}, Global: {}, Ranges: {}", locallyRedundantBefore, globalSyncId, slicedRanges);
            return;
        }

        // TODO (desired): not all systems care about HLC_BOUND for GC, make configurable
        if (globalSyncId.is(HLC_BOUND) || !requiresUniqueHlcs())
        {
            RedundantBefore addOnDataStoreDurable = RedundantBefore.create(slicedRanges, globalSyncId, GC_BEFORE_AND_LOCALLY_DURABLE);
            dataStore.ensureDurable(this, slicedRanges, addOnDataStoreDurable);
        }
    }

    protected void updatedRedundantBefore(SafeCommandStore safeStore, RedundantBefore added)
    {
        TxnId clearWaitingBefore = redundantBefore.minShardAndLocallyAppliedBefore();
        TxnId clearAllBefore = TxnId.min(clearWaitingBefore, durableBefore().min.quorumBefore);
        progressLog.clearBefore(safeStore, clearWaitingBefore, clearAllBefore);
        listeners.clearBefore(clearWaitingBefore);
    }

    @VisibleForTesting
    public AsyncResult<Void> awaitVisibility(long epoch, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return AsyncResults.success(null);

            List<AsyncResult<Void>> awaiting = new ArrayList<>();
            for (Map.Entry<Long, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey() > epoch)
                    break;

                Ranges remaining = e.getValue().waitingOn;
                Ranges intersecting = remaining.slice(ranges, Minimal);
                if (!intersecting.isEmpty())
                {
                    awaiting.add(e.getValue().whenDone);
                    ranges = ranges.without(intersecting);
                }
            }

            if (awaiting.isEmpty())
                return AsyncResults.success(null);
            return AsyncResults.debuggableReduce(awaiting, Reduce.toNull());
        }
    }

    protected final Ranges isWaitingOnVisibility(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return Ranges.EMPTY;

            Ranges waitingOn = Ranges.EMPTY;
            for (Map.Entry<Long, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey() > syncId.epoch())
                    break;

                Ranges remaining = e.getValue().waitingOn;
                Ranges intersecting = remaining.slice(ranges, Minimal);
                if (!intersecting.isEmpty())
                {
                    ranges = ranges.without(intersecting);
                    waitingOn = waitingOn.with(intersecting);
                }
            }

            return waitingOn;
        }
    }

    protected final void markingVisible(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            for (Map.Entry<Long, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey() > syncId.epoch())
                    break;

                Ranges remaining = e.getValue().waitingOn.without(ranges);
                if (e.getValue().waitingOn != remaining)
                    e.getValue().waitingOn = remaining;
            }
        }
    }

    protected final void cancelMarkingVisible(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            for (Map.Entry<Long, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey() > syncId.epoch())
                    break;

                Ranges unmark = e.getValue().waitingOnDurable.slice(ranges, Minimal);
                if (!unmark.isEmpty())
                    e.getValue().waitingOn = e.getValue().waitingOn.with(unmark);
            }
        }
    }

    protected final void markVisible(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        Invariants.require(syncId.is(VisibilitySyncPoint));
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, syncId, LOCALLY_WITNESSED_ONLY);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        markVisibleInternal(safeStore, syncId.epoch(), ranges, syncId);
    }

    private void markVisibleInternal(SafeCommandStore safeStore, long epoch, Ranges ranges, Object describe)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            LongHashSet remove = null;
            for (Map.Entry<Long, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey() > epoch)
                    break;

                Ranges waitingOn = e.getValue().waitingOn;
                Ranges waitingOnDurable = e.getValue().waitingOnDurable;
                Ranges synced = waitingOnDurable.slice(ranges, Minimal);
                boolean intersects = waitingOnDurable.intersects(ranges);
                if (intersects)
                {
                    e.getValue().waitingOn = waitingOn = waitingOn.without(ranges);
                    e.getValue().waitingOnDurable = waitingOnDurable = waitingOnDurable.without(ranges);
                    if (waitingOnDurable.isEmpty())
                    {
                        SettableResult<Void> done = e.getValue().whenDone;
                        logger.debug("{} completed full visibility sync for {} on epoch {} using {}", this, e.getValue().allRanges, e.getKey(), describe);
                        done.trySuccess(null);
                        if (remove == null)
                            remove = new LongHashSet();
                        remove.add(e.getKey());
                    }
                    else
                    {
                        logger.debug("{} completed partial visibility sync for {} on epoch {} using {}; {} still to sync and {} to sync durably", this, synced, e.getKey(), describe, waitingOn, waitingOnDurable);
                    }
                }
            }
            if (remove != null)
                remove.forEach(waitingOnVisibility::remove);
        }
    }

    public void markShardStale(SafeCommandStore safeStore, Timestamp staleSince, Ranges ranges, boolean isSincePrecise)
    {
        Timestamp staleUntilAtLeast = staleSince;
        if (isSincePrecise)
        {
            ranges = ranges.slice(safeStore.ranges().allAt(staleSince.epoch()), Minimal);
        }
        else
        {
            ranges = ranges.slice(safeStore.ranges().allSince(staleSince.epoch()), Minimal);
            // make sure no in-progress bootstrap attempts will override the stale since for commands whose staleness bounds are unknown
            staleUntilAtLeast = Timestamp.max(bootstrapBeganAt.lastKey(), staleUntilAtLeast);
        }

        if (ranges.isEmpty())
            return;

        agent.ownershipEvents().onStale(staleSince, ranges);

        RedundantBefore addRedundantBefore = RedundantBefore.createStale(ranges, staleUntilAtLeast);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        // find which ranges need to bootstrap, subtracting those already in progress that cover the id

        markUnsafeToRead(ranges);
    }

    /**
     * This is a heavy-handed operator action to unstick waiting transactions whose transitive dependencies
     * may already be applied.
     */
    public final AsyncResult<Void> operatorTryToExecuteListeningTxns()
    {
        SettableResult<Void> done = new SettableResult<>();
        execute((Empty)() -> "Try Execute Listening", safeStore -> {
            tryExecuteListening(safeStore, listeners.txnsWaitingOn(SaveStatus.Applied).iterator(), done);
        });
        return done;
    }

    private void tryExecuteListening(SafeCommandStore safeStore, Iterator<TxnId> iterator, SettableResult<Void> done)
    {
        if (!iterator.hasNext())
        {
            done.trySuccess(null);
            return;
        }

        try
        {
            TxnId waitingOn = iterator.next();
            PreLoadContext context = PreLoadContext.contextFor(waitingOn, "Try Execute Listening");
            if (!safeStore.canExecuteWith(context) || !safeStore.tryRecurse())
            {
                //noinspection DataFlowIssue
                safeStore = safeStore;
                execute(context, safeStore0 -> tryExecuteListening(safeStore0, waitingOn, iterator, done));
            }
            else
            {
                try { tryExecuteListening(safeStore, waitingOn, iterator, done); }
                finally { safeStore.unrecurse(); }
            }
        }
        catch (Throwable t)
        {
            done.tryFailure(t);
        }
    }

    private void tryExecuteListening(SafeCommandStore safeStore, TxnId waitingOn, Iterator<TxnId> iterator, SettableResult<Void> done)
    {
        try
        {
            SafeCommand safeCommand = safeStore.unsafeGet(waitingOn);
            //noinspection DataFlowIssue
            safeStore = safeStore;
            //noinspection DataFlowIssue
            safeCommand = safeCommand;
            boolean wasApplied = safeCommand.current().hasBeen(Status.Applied);
            Consumer<SafeCommandStore> continuation = safeStore0 -> {
                if (!wasApplied)
                {
                    SafeCommand safeCommand0 = safeStore0.ifLoadedAndInitialised(waitingOn);
                    if (safeCommand0 != null && safeCommand0.current().saveStatus().hasBeen(Status.Applied))
                        logger.warn("{} was successfully applied by tryToExecuteListening", waitingOn);
                }
                tryExecuteListening(safeStore0, iterator, done);
            };

            Commands.maybeExecute(safeStore, safeCommand, safeCommand.current(), true, true, NotifyWaitingOnPlus.adapter(continuation, true, true));
        }
        catch (Throwable t)
        {
            done.tryFailure(t);
        }
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?> participants)
    {
        if (rejectBefore == null)
            return false;

        return rejectBefore.rejects(txnId, participants);
    }

    public final MaxConflicts unsafeGetMaxConflicts()
    {
        return maxConflicts;
    }

    public final RedundantBefore unsafeGetRedundantBefore()
    {
        return redundantBefore;
    }

    public final LocalListeners unsafeGetListeners()
    {
        return listeners;
    }

    @Nullable
    public final RejectBefore unsafeGetRejectBefore()
    {
        return rejectBefore;
    }

    public final DurableBefore durableBefore()
    {
        return node.durableBefore();
    }

    public final ProgressLog unsafeProgressLog()
    {
        return progressLog;
    }

    @VisibleForTesting
    public final NavigableMap<TxnId, Ranges> unsafeGetBootstrapBeganAt() { return bootstrapBeganAt; }

    @VisibleForTesting
    public NavigableMap<Timestamp, Ranges> unsafeGetSafeToRead() { return safeToRead; }

    final void markUnsafeToRead(Ranges ranges)
    {
        if (safeToRead.values().stream().anyMatch(r -> r.intersects(ranges)))
        {
            execute((Empty) () -> "Mark Unsafe To Read", safeStore -> {
                safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            }, agent);
        }
    }

    final AsyncChain<Void> markPermanentlyUnsafeToRead(Ranges ranges)
    {
        return chain((Empty) () -> "Mark Range As Regained", safeStore -> {
            safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            safeStore.setPermanentlyUnsafeToRead(permanentlyUnsafeToRead.union(MERGE_ADJACENT, ranges));
        });
    }

    public final DataStore unsafeGetDataStore()
    {
        return dataStore;
    }

    final synchronized AsyncResult<Void> markSafeToRead(Timestamp forBootstrapAt, Timestamp at, Ranges ranges)
    {
        return execute((Empty) () -> "Mark Safe To Read", safeStore -> {
            // TODO (required): handle weird edge cases like newer at having a lower HLC than prior existing at, but higher epoch
            Ranges validatedSafeToRead = redundantBefore.validateSafeToRead(forBootstrapAt, ranges);
            safeStore.setSafeToRead(purgeAndInsert(safeToRead, at, validatedSafeToRead));
            updateMaxConflicts(ranges, at);
        });
    }

    public static ImmutableSortedMap<TxnId, Ranges> bootstrap(TxnId at, Ranges ranges, NavigableMap<TxnId, Ranges> readyAt)
    {
        Invariants.requireArgument(!ranges.isEmpty());
        if (at == TxnId.NONE)
        {
            for (Ranges rs : readyAt.values())
                Invariants.require(!ranges.intersects(rs));
        }
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        return purgeAndInsert(readyAt, at, ranges);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> purgeAndInsert(NavigableMap<T, Ranges> in, T insertAt, Ranges insert)
    {
        TreeMap<T, Ranges> build = new TreeMap<>(in);
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().without(insert)));
        build.tailMap(insertAt, true).entrySet().forEach(e -> e.setValue(e.getValue().union(MERGE_ADJACENT, insert)));
        build.entrySet().removeIf(e -> e.getKey().compareTo(Timestamp.NONE) > 0 && e.getValue().isEmpty());
        Map.Entry<T, Ranges> prev = build.floorEntry(insertAt);
        build.putIfAbsent(insertAt, prev.getValue().with(insert));
        return ImmutableSortedMap.copyOf(build);
    }

    private static ImmutableSortedMap<Timestamp, Ranges> purgeHistory(NavigableMap<Timestamp, Ranges> in, Ranges remove)
    {
        return ImmutableSortedMap.copyOf(purgeHistoryIterator(in, remove));
    }

    private static <T extends Timestamp> Iterable<Map.Entry<T, Ranges>> purgeHistoryIterator(NavigableMap<T, Ranges> in, Ranges removeRanges)
    {
        return () -> in.entrySet().stream()
                       .map(e -> without(e, removeRanges))
                       .filter(e -> !e.getValue().isEmpty() || e.getKey().equals(TxnId.NONE))
                       .iterator();
    }

    private static <T extends Timestamp> Map.Entry<T, Ranges> without(Map.Entry<T, Ranges> in, Ranges remove)
    {
        Ranges without = in.getValue().without(remove);
        if (without == in.getValue())
            return in;
        return new SimpleImmutableEntry<>(in.getKey(), without);
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    public boolean isBootstrapping()
    {
        return !bootstraps.isEmpty();
    }

    public void updateMinHlc(long minHlc)
    {
        Timestamp timestamp = Timestamp.fromValues(rangesForEpoch.epochs[rangesForEpoch.epochs.length - 1], minHlc, 0, node.id());
        MaxConflicts updated = maxConflicts.update(rangesForEpoch.all(), timestamp);
        setMaxConflicts(updated);
    }

    public static NavigableMap<TxnId, Ranges> emptyBootstrapBeganAt()
    {
        return ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
    }

    public static NavigableMap<Timestamp, Ranges> emptySafeToRead()
    {
        return ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    }

    public NodeCommandStoreService node()
    {
        return node;
    }

    void unsafeRegister(SyncPointListener listener)
    {
        Invariants.require(inStore());
        List<SyncPointListener> newListeners = new ArrayList<>();
        if (syncPointListeners != null)
            newListeners.addAll(syncPointListeners);
        newListeners.add(listener);
        syncPointListeners = newListeners;
    }

    void unsafeUnregister(SyncPointListener listener)
    {
        Invariants.require(inStore());
        if (syncPointListeners != null)
        {
            List<SyncPointListener> newListeners = new ArrayList<>(syncPointListeners);
            newListeners.remove(listener);
            if (newListeners.isEmpty())
                newListeners = null;
            syncPointListeners = newListeners;
        }
    }
}
