/*
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

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandListener;
import accord.local.Listeners;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.local.Status.Known;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.DeterministicIdentitySet;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.PreApplied;
import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applyMapChanges;
import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applySetChanges;

public class AccordCommand extends Command implements AccordState<TxnId>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommand.class);

    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommand(null));

    public static class WriteOnly extends AccordCommand implements AccordState.WriteOnly<TxnId, AccordCommand>
    {
        private Future<?> future = null;

        public WriteOnly(TxnId txnId)
        {
            super(txnId);
        }

        @Override
        public void future(Future<?> future)
        {
            Preconditions.checkArgument(this.future == null);
            this.future = future;
        }

        @Override
        public Future<?> future()
        {
            return future;
        }

        @Override
        public void applyChanges(AccordCommand instance)
        {
            applySetChanges(this, instance, cmd -> cmd.waitingOnCommit);
            applyMapChanges(this, instance, cmd -> cmd.waitingOnApply);
            applySetChanges(this, instance, cmd -> cmd.blockingCommitOn);
            applySetChanges(this, instance, cmd -> cmd.blockingApplyOn);
        }
    }

    private final TxnId txnId;
    private final int instanceCount = INSTANCE_COUNTER.getAndIncrement();
    public final StoredValue<Route<?>> route;
    public final StoredValue<RoutingKey> homeKey;
    public final StoredValue<RoutingKey> progressKey;
    public final StoredValue<PartialTxn> partialTxn;
    public final StoredValue<Txn.Kind> kind; // TODO: store this in TxnId
    public final StoredValue<Ballot> promised;
    public final StoredValue<Ballot> accepted;
    public final StoredValue<Timestamp> executeAt;
    public final StoredValue<PartialDeps> partialDeps;
    public final StoredValue<Writes> writes;
    public final StoredValue<Result> result;

    public final StoredValue.HistoryPreserving<SaveStatus> status;
    public final StoredValue<Durability> durability;

    public final StoredSet.Navigable<TxnId> waitingOnCommit;
    public final StoredNavigableMap<Timestamp, TxnId> waitingOnApply;
    public final StoredSet.Navigable<TxnId> blockingCommitOn;
    public final StoredSet.Navigable<TxnId> blockingApplyOn;

    public final StoredSet.DeterministicIdentity<ListenerProxy> storedListeners;
    private final Listeners transientListeners;

    public AccordCommand(TxnId txnId)
    {
        logger.trace("Instantiating new command {} @ {}", txnId, instanceHash());
        this.txnId = txnId;
        homeKey = new StoredValue<>(rw());
        progressKey = new StoredValue<>(rw());
        route = new StoredValue<>(rw());
        partialTxn = new StoredValue<>(rw());
        kind = new StoredValue<>(rw());
        promised = new StoredValue<>(rw());
        accepted = new StoredValue<>(rw());
        executeAt = new StoredValue<>(rw());
        partialDeps = new StoredValue<>(rw());
        writes = new StoredValue<>(rw());
        result = new StoredValue<>(rw());
        status = new StoredValue.HistoryPreserving<>(rw());
        durability = new StoredValue<>(rw());
        waitingOnCommit = new StoredSet.Navigable<>(rw());
        waitingOnApply = new StoredNavigableMap<>(rw());
        storedListeners = new StoredSet.DeterministicIdentity<>(rw());
        transientListeners = new Listeners();
        blockingCommitOn = new StoredSet.Navigable<>(rw());
        blockingApplyOn = new StoredSet.Navigable<>(rw());
    }

    @Override
    public String toString()
    {
        return "AccordCommand{" +
               "txnId=" + txnId +
               ", instanceHash=" + instanceHash() +
               ", status=" + status +
               ", executeAt=" + executeAt +
               ", promised=" + promised +
               ", accepted=" + accepted +
//               ", deps=" + deps +
//               ", homeKey=" + homeKey +
//               ", progressKey=" + progressKey +
//               ", txn=" + txn +
//               ", writes=" + writes +
//               ", result=" + result +
               // TODO: Should we have to check for isLoaded() here?
               ", txn is null?=" + (!partialTxn.isLoaded() || partialTxn.get() == null) +
               ", durability=" + durability +
               ", waitingOnCommit=" + waitingOnCommit +
               ", waitingOnApply=" + waitingOnApply +
               ", storedListeners=" + storedListeners +
               ", transientListeners=" + transientListeners +
               ", blockingCommitOn=" + blockingCommitOn +
               ", blockingApplyOn=" + blockingApplyOn +
               '}';
    }

    @Override
    public boolean isEmpty()
    {
        return homeKey.isEmpty()
               || progressKey.isEmpty()
               || route.isEmpty()
               || partialTxn.isEmpty()
               || promised.isEmpty()
               || accepted.isEmpty()
               || executeAt.isEmpty()
               || partialDeps.isEmpty()
               || writes.isEmpty()
               || result.isEmpty()
               || status.isEmpty()
               || durability.isEmpty()
               || waitingOnCommit.isEmpty()
               || blockingCommitOn.isEmpty()
               || waitingOnApply.isEmpty()
               || blockingApplyOn.isEmpty()
               || storedListeners.isEmpty();
    }

    public void setEmpty()
    {
        homeKey.setEmpty();
        progressKey.setEmpty();
        route.setEmpty();
        partialTxn.setEmpty();
        promised.setEmpty();
        accepted.setEmpty();
        executeAt.setEmpty();
        partialDeps.setEmpty();
        writes.setEmpty();
        result.setEmpty();
        status.setEmpty();
        durability.setEmpty();
        waitingOnCommit.setEmpty();
        blockingCommitOn.setEmpty();
        waitingOnApply.setEmpty();
        blockingApplyOn.setEmpty();
        storedListeners.setEmpty();;
    }

    public AccordCommand initialize()
    {
        logger.trace("Initializing command {} @ {}", txnId, instanceHash());
        status.set(SaveStatus.NotWitnessed);
        homeKey.set(null);
        progressKey.set(null);
        route.set(null);
        partialTxn.set(null);
        kind.set(null);
        executeAt.load(null);
        promised.set(Ballot.ZERO);
        accepted.set(Ballot.ZERO);
        partialDeps.set(PartialDeps.NONE);
        writes.load(null);
        result.load(null);
        durability.set(Durability.NotDurable);
        waitingOnCommit.load(new TreeSet<>());
        waitingOnApply.load(new TreeMap<>());
        blockingCommitOn.load(new TreeSet<>());
        blockingApplyOn.load(new TreeSet<>());
        storedListeners.load(new DeterministicIdentitySet<>());
        return this;
    }

    @Override
    public boolean isLoaded()
    {
        return homeKey.isLoaded()
               && progressKey.isLoaded()
               && route.isLoaded()
               && partialTxn.isLoaded()
               && promised.isLoaded()
               && accepted.isLoaded()
               && executeAt.isLoaded()
               && partialDeps.isLoaded()
               && writes.isLoaded()
               && result.isLoaded()
               && status.isLoaded()
               && durability.isLoaded()
               && waitingOnCommit.isLoaded()
               && blockingCommitOn.isLoaded()
               && waitingOnApply.isLoaded()
               && blockingApplyOn.isLoaded()
               && storedListeners.isLoaded();
    }

    public boolean isPartiallyLoaded()
    {
        return homeKey.isLoaded()
               || progressKey.isLoaded()
               || route.isLoaded()
               || partialTxn.isLoaded()
               || promised.isLoaded()
               || accepted.isLoaded()
               || executeAt.isLoaded()
               || partialDeps.isLoaded()
               || writes.isLoaded()
               || result.isLoaded()
               || status.isLoaded()
               || durability.isLoaded()
               || waitingOnCommit.isLoaded()
               || blockingCommitOn.isLoaded()
               || waitingOnApply.isLoaded()
               || blockingApplyOn.isLoaded()
               || storedListeners.isLoaded();
    }

    @Override
    public boolean hasModifications()
    {
        return homeKey.hasModifications()
               || progressKey.hasModifications()
               || route.hasModifications()
               || partialTxn.hasModifications()
               || promised.hasModifications()
               || accepted.hasModifications()
               || executeAt.hasModifications()
               || partialDeps.hasModifications()
               || writes.hasModifications()
               || result.hasModifications()
               || status.hasModifications()
               || durability.hasModifications()
               || waitingOnCommit.hasModifications()
               || blockingCommitOn.hasModifications()
               || waitingOnApply.hasModifications()
               || blockingApplyOn.hasModifications()
               || storedListeners.hasModifications();
    }

    @Override
    public void clearModifiedFlag()
    {
        logger.trace("Clearing modified flag on command {} @ {}", txnId, instanceHash());
        homeKey.clearModifiedFlag();
        progressKey.clearModifiedFlag();
        route.clearModifiedFlag();
        partialTxn.clearModifiedFlag();
        promised.clearModifiedFlag();
        accepted.clearModifiedFlag();
        executeAt.clearModifiedFlag();
        partialDeps.clearModifiedFlag();
        writes.clearModifiedFlag();
        result.clearModifiedFlag();
        status.clearModifiedFlag();
        durability.clearModifiedFlag();
        waitingOnCommit.clearModifiedFlag();
        blockingCommitOn.clearModifiedFlag();
        waitingOnApply.clearModifiedFlag();
        blockingApplyOn.clearModifiedFlag();
        storedListeners.clearModifiedFlag();;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordCommand command = (AccordCommand) o;
        return    homeKey.equals(command.homeKey)
               && progressKey.equals(command.progressKey)
               && route.equals(command.route)
               && txnId.equals(command.txnId)
               && partialTxn.equals(command.partialTxn)
               && promised.equals(command.promised)
               && accepted.equals(command.accepted)
               && executeAt.equals(command.executeAt)
               && partialDeps.equals(command.partialDeps)
               && writes.equals(command.writes)
               && result.equals(command.result)
               && status.equals(command.status)
               && durability.equals(command.durability)
               && waitingOnCommit.equals(command.waitingOnCommit)
               && blockingCommitOn.equals(command.blockingCommitOn)
               && waitingOnApply.equals(command.waitingOnApply)
               && blockingApplyOn.equals(command.blockingApplyOn)
               && storedListeners.equals(command.storedListeners)
               && transientListeners.equals(command.transientListeners);
    }

    boolean isReadOnly()
    {
        return false;
    }

    private int instanceHash()
    {
//        return System.identityHashCode(this);
        return instanceCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(txnId,
                            homeKey,
                            progressKey,
                            route,
                            partialTxn,
                            promised,
                            accepted,
                            executeAt,
                            partialDeps,
                            writes,
                            result,
                            status,
                            durability,
                            waitingOnCommit,
                            blockingCommitOn,
                            waitingOnApply,
                            blockingApplyOn,
                            storedListeners,
                            transientListeners);
    }

    @Override
    public TxnId key()
    {
        return txnId;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += AccordObjectSizes.timestamp(txnId);
        size += homeKey.estimatedSizeOnHeap(AccordObjectSizes::key);
        size += progressKey.estimatedSizeOnHeap(AccordObjectSizes::key);
        size += route.estimatedSizeOnHeap(AccordObjectSizes::route);
        size += partialTxn.estimatedSizeOnHeap(AccordObjectSizes::txn);
        size += promised.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += accepted.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += executeAt.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += partialDeps.estimatedSizeOnHeap(AccordObjectSizes::dependencies);
        size += writes.estimatedSizeOnHeap(AccordObjectSizes::writes);
        size += result.estimatedSizeOnHeap(r -> ((TxnData) r).estimatedSizeOnHeap());
        size += status.estimatedSizeOnHeap(s -> 0);
        size += durability.estimatedSizeOnHeap(s -> 0);
        size += waitingOnCommit.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += blockingCommitOn.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += waitingOnApply.estimatedSizeOnHeap(AccordObjectSizes::timestamp, AccordObjectSizes::timestamp);
        size += blockingApplyOn.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += storedListeners.estimatedSizeOnHeap(ListenerProxy::estimatedSizeOnHeap);
        return size;
    }

    public boolean shouldUpdateDenormalizedWaitingOn()
    {
        if (blockingCommitOn.getView().isEmpty() && blockingApplyOn.getView().isEmpty())
            return false;
        return AccordPartialCommand.serializer.needsUpdate(this);
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey.get();
    }

    @Override
    protected void setHomeKey(RoutingKey key)
    {
        homeKey.set(key);
    }

    @Override
    public RoutingKey progressKey()
    {
        return progressKey.get();
    }

    @Override
    protected void setProgressKey(RoutingKey key)
    {
        progressKey.set(key);
    }

    @Override
    public Route<?> route()
    {
        return route.get();
    }

    @Override
    protected void setRoute(Route<?> newRoute)
    {
        route.set(newRoute);
    }

    @Override
    public PartialTxn partialTxn()
    {
        return partialTxn.get();
    }

    @Override
    public void setPartialTxn(PartialTxn txn)
    {
        this.partialTxn.set(txn);
    }

    @Override
    public Ballot promised()
    {
        return promised.get();
    }

    @Override
    public void setPromised(Ballot ballot)
    {
        this.promised.set(ballot);
    }

    @Override
    public Ballot accepted()
    {
        return accepted.get();
    }

    @Override
    public void setAccepted(Ballot ballot)
    {
        this.accepted.set(ballot);
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt.get();
    }

    @Override
    public Txn.Kind kind()
    {
        return kind.get();
    }

    @Override
    public void setKind(Txn.Kind kind)
    {
        this.kind.set(kind);
    }

    @Override
    public void setExecuteAt(Timestamp timestamp)
    {
        Preconditions.checkState(!status().hasBeen(Status.Committed) || executeAt().equals(timestamp));
        this.executeAt.set(timestamp);
    }

    @Override
    public PartialDeps partialDeps()
    {
        return partialDeps.get();
    }

    @Override
    public void setPartialDeps(PartialDeps deps)
    {
        this.partialDeps.set(deps);
    }

    @Override
    public Writes writes()
    {
        return writes.get();
    }

    @Override
    public void setWrites(Writes writes)
    {
        this.writes.set(writes);
    }

    @Override
    public Result result()
    {
        return result.get();
    }

    @Override
    public void setResult(Result result)
    {
        this.result.set(result);
    }

    @Override
    public SaveStatus saveStatus()
    {
        return status.get();
    }

    @Override
    public void setSaveStatus(SaveStatus status)
    {
        this.status.set(status);
    }

    @Override
    public void setStatus(Status status)
    {
        super.setStatus(status);
    }

    @Override
    public Known known()
    {
        return this.status.get().known;
    }

    @Override
    public Durability durability()
    {
        Durability durability = this.durability.get();
        if (status().hasBeen(PreApplied) && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }

    @Override
    public void setDurability(Durability v)
    {
        durability.set(v);
    }

    @Override
    protected void postApply(SafeCommandStore safeStore)
    {
        AccordStateCache.Instance<TxnId, AccordCommand> cache = ((AccordCommandStore) safeStore).commandCache();
        cache.cleanupWriteFuture(txnId);
        super.postApply(safeStore);
    }

    private boolean canApplyWithCurrentScope(SafeCommandStore safeStore)
    {
        Ranges ranges = safeStore.ranges().at(executeAt().epoch);
        Seekables<?, ?> keys = partialTxn().keys();
        for (int i=0,mi=keys.size(); i<mi; i++)
        {
            PartitionKey key = (PartitionKey) keys.get(i);
            if (((AccordCommandStore)safeStore).isCommandsForKeyInContext(key))
                continue;

            if (!safeStore.commandStore().hashIntersects(key))
                continue;

            if (!ranges.contains(key))
                continue;

            return false;
        }
        return true;
    }

    private Future<Void> applyWithCorrectScope(CommandStore unsafeStore)
    {
        TxnId txnId = txnId();
        AsyncPromise<Void> promise = new AsyncPromise<>();
        unsafeStore.execute(this, safeStore -> {
            AccordCommand command = (AccordCommand) safeStore.command(txnId);
            command.apply(safeStore, false).addCallback((v, throwable) -> {
                if (throwable != null)
                    promise.tryFailure(throwable);
                else
                    promise.trySuccess(null);
            });
        });
        return promise;
    }

    private Future<Void> apply(SafeCommandStore safeStore, boolean canReschedule)
    {
        AccordStateCache.Instance<TxnId, AccordCommand> cache = ((AccordCommandStore) safeStore).commandCache();
        Future<Void> future = cache.getWriteFuture(txnId);
        if (future != null)
            return future;

        // this can be called via a listener callback, in which case we won't
        // have the appropriate commandsForKey in scope, so start a new operation
        // with the correct scope and notify the caller when that completes
        if (!canApplyWithCurrentScope(safeStore))
        {
            Preconditions.checkArgument(canReschedule);
            return applyWithCorrectScope(safeStore.commandStore());
        }

        future = super.apply(safeStore);
        cache.setWriteFuture(txnId, future);
        return future;
    }

    @Override
    public Future<Void> apply(SafeCommandStore safeStore)
    {
        return apply(safeStore, true);
    }

    @Override
    public Future<Data> read(SafeCommandStore safeStore)
    {
        AccordStateCache.Instance<TxnId, AccordCommand> cache = ((AccordCommandStore) safeStore).commandCache();
        Future<Data> future = cache.getReadFuture(txnId);
        if (future != null)
            return future;
        future = super.read(safeStore);
        cache.setReadFuture(txnId, future);
        return future;
    }

    private CommandListener maybeWrapListener(CommandListener listener)
    {
        if (listener.isTransient())
            return listener;

        if (listener instanceof AccordCommand)
            return new ListenerProxy.CommandListenerProxy(((AccordCommand) listener).txnId());

        if (listener instanceof AccordCommandsForKey)
            return new ListenerProxy.CommandsForKeyListenerProxy(((AccordCommandsForKey) listener).key());

        throw new RuntimeException("Unhandled non-transient listener: " + listener);
    }

    @Override
    public Command addListener(CommandListener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindAdd((ListenerProxy) listener);
        else
            transientListeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(CommandListener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindRemove((ListenerProxy) listener);
        else
            transientListeners.remove(listener);
    }

    public boolean hasListenerFor(TxnId txnId)
    {
        return storedListeners.getView().contains(new ListenerProxy.CommandListenerProxy(txnId));
    }

    @Override
    public void notifyListeners(SafeCommandStore safeStore)
    {
        // TODO: efficiency (introduce BiConsumer method)
        storedListeners.getView().forEach(l -> l.onChange(safeStore, this));
        transientListeners.forEach(listener -> {
            PreLoadContext ctx = listener.listenerPreLoadContext(txnId());
            AsyncContext context = ((AccordCommandStore)safeStore).getContext();
            if (context.containsScopedItems(ctx))
            {
                logger.trace("{}: synchronously updating listener {}", txnId(), listener);
                listener.onChange(safeStore, this);
            }
            else
            {
                logger.trace("{}: asynchronously updating listener {}", txnId(), listener);
                safeStore.execute(ctx, reSafeStore -> {
                    listener.onChange(reSafeStore, reSafeStore.command(txnId()));
                });
            }
        });
    }

    @Override
    public void addWaitingOnCommit(TxnId txnId)
    {
        waitingOnCommit.blindAdd(txnId);
    }

    public boolean isWaitingOnCommit()
    {
        return !waitingOnCommit.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(TxnId txnId)
    {
        waitingOnCommit.blindRemove(txnId);
    }

    @Override
    public TxnId firstWaitingOnCommit()
    {
        if (!isWaitingOnCommit())
            return null;
        return waitingOnCommit.getView().first();
    }

    @Override
    public void addWaitingOnApplyIfAbsent(TxnId txnId, Timestamp executeAt)
    {
        waitingOnApply.blindPut(executeAt, txnId);
    }

    public boolean isWaitingOnApply()
    {
        return !waitingOnApply.getView().isEmpty();
    }

    @Override
    public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
    {
        waitingOnCommit.blindRemove(txnId);
        waitingOnApply.blindRemove(executeAt, txnId);
    }

    @Override
    public boolean isWaitingOnDependency()
    {
        return isWaitingOnCommit() || isWaitingOnApply();
    }

    @Override
    public TxnId firstWaitingOnApply(@Nullable TxnId ifExecutesBefore)
    {
        if (!isWaitingOnApply())
            return null;

        Map.Entry<Timestamp, TxnId> first = waitingOnApply.getView().firstEntry();
        if (ifExecutesBefore == null || first.getKey().compareTo(ifExecutesBefore) < 0)
            return first.getValue();

        return null;
    }
}
