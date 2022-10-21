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

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Key;
import accord.api.Result;
import accord.local.Command;
import accord.local.Listener;
import accord.local.Listeners;
import accord.local.PartialCommand;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Txn;
import accord.txn.Writes;
import accord.utils.DeterministicIdentitySet;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.service.accord.store.StoredBoolean;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applyMapChanges;
import static org.apache.cassandra.service.accord.AccordState.WriteOnly.applySetChanges;

public class AccordCommand extends Command implements AccordState<TxnId>
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommand.class);

    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommand(null, null));

    public static class WriteOnly extends AccordCommand implements AccordState.WriteOnly<TxnId, AccordCommand>
    {
        private Future<?> future = null;

        public WriteOnly(AccordCommandStore commandStore, TxnId txnId)
        {
            super(commandStore, txnId);
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
            applyMapChanges(this, instance, cmd -> cmd.waitingOnCommit);
            applyMapChanges(this, instance, cmd -> cmd.waitingOnApply);
            applySetChanges(this, instance, cmd -> cmd.blockingCommitOn);
            applySetChanges(this, instance, cmd -> cmd.blockingApplyOn);
        }
    }

    public static class ReadOnly extends AccordCommand implements AccordState.ReadOnly<TxnId, AccordCommand>
    {
        public ReadOnly(AccordCommandStore commandStore, TxnId txnId)
        {
            super(commandStore, txnId);
        }

        @Override
        boolean isReadOnly()
        {
            return true;
        }
    }

    private final AccordCommandStore commandStore;
    private final TxnId txnId;
    private final int instanceCount = INSTANCE_COUNTER.getAndIncrement();
    public final StoredValue<Key> homeKey;
    public final StoredValue<Key> progressKey;
    public final StoredValue<Txn> txn;
    public final StoredValue<Ballot> promised;
    public final StoredValue<Ballot> accepted;
    public final StoredValue<Timestamp> executeAt;
    public final StoredValue<Deps> deps;
    public final StoredValue<Writes> writes;
    public final StoredValue<Result> result;

    public final StoredValue.HistoryPreserving<Status> status;
    public final StoredBoolean isGloballyPersistent;

    public final StoredNavigableMap<TxnId, ByteBuffer> waitingOnCommit;
    public final StoredNavigableMap<TxnId, ByteBuffer> waitingOnApply;

    public final StoredSet.DeterministicIdentity<ListenerProxy> storedListeners;
    private final Listeners transientListeners;

    public final StoredSet.Navigable<TxnId> blockingCommitOn;
    public final StoredSet.Navigable<TxnId> blockingApplyOn;

    public AccordCommand(AccordCommandStore commandStore, TxnId txnId)
    {
        logger.trace("Instantiating new command {} @ {}", txnId, instanceHash());
        this.commandStore = commandStore;
        this.txnId = txnId;
        homeKey = new StoredValue<>(kind());
        progressKey = new StoredValue<>(kind());
        txn = new StoredValue<>(kind());
        promised = new StoredValue<>(kind());
        accepted = new StoredValue<>(kind());
        executeAt = new StoredValue<>(kind());
        deps = new StoredValue<>(kind());
        writes = new StoredValue<>(kind());
        result = new StoredValue<>(kind());
        status = new StoredValue.HistoryPreserving<>(kind());
        isGloballyPersistent = new StoredBoolean(kind());
        waitingOnCommit = new StoredNavigableMap<>(kind());
        waitingOnApply = new StoredNavigableMap<>(kind());
        storedListeners = new StoredSet.DeterministicIdentity<>(kind());
        transientListeners = new Listeners();
        blockingCommitOn = new StoredSet.Navigable<>(kind());
        blockingApplyOn = new StoredSet.Navigable<>(kind());
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
               ", txn is null?=" + (txn.get() == null) +
               ", isGloballyPersistent=" + isGloballyPersistent +
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
               || txn.isEmpty()
               || promised.isEmpty()
               || accepted.isEmpty()
               || executeAt.isEmpty()
               || deps.isEmpty()
               || writes.isEmpty()
               || result.isEmpty()
               || status.isEmpty()
               || isGloballyPersistent.isEmpty()
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
        txn.setEmpty();
        promised.setEmpty();
        accepted.setEmpty();
        executeAt.setEmpty();
        deps.setEmpty();
        writes.setEmpty();
        result.setEmpty();
        status.setEmpty();
        isGloballyPersistent.setEmpty();
        waitingOnCommit.setEmpty();
        blockingCommitOn.setEmpty();
        waitingOnApply.setEmpty();
        blockingApplyOn.setEmpty();
        storedListeners.setEmpty();;
    }

    public AccordCommand initialize()
    {
        logger.trace("Initializing command {} @ {}", txnId, instanceHash());
        status.set(Status.NotWitnessed);
        homeKey.set(null);
        progressKey.set(null);
        txn.set(null);
        executeAt.load(null);
        promised.set(Ballot.ZERO);
        accepted.set(Ballot.ZERO);
        deps.set(Deps.NONE);
        writes.load(null);
        result.load(null);
        isGloballyPersistent.set(false);
        waitingOnCommit.load(new TreeMap<>());
        blockingCommitOn.load(new TreeSet<>());
        waitingOnApply.load(new TreeMap<>());
        blockingApplyOn.load(new TreeSet<>());
        storedListeners.load(new DeterministicIdentitySet<>());
        return this;
    }

    @Override
    public boolean isLoaded()
    {
        return homeKey.isLoaded()
               && progressKey.isLoaded()
               && txn.isLoaded()
               && promised.isLoaded()
               && accepted.isLoaded()
               && executeAt.isLoaded()
               && deps.isLoaded()
               && writes.isLoaded()
               && result.isLoaded()
               && status.isLoaded()
               && isGloballyPersistent.isLoaded()
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
               || txn.isLoaded()
               || promised.isLoaded()
               || accepted.isLoaded()
               || executeAt.isLoaded()
               || deps.isLoaded()
               || writes.isLoaded()
               || result.isLoaded()
               || status.isLoaded()
               || isGloballyPersistent.isLoaded()
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
               || txn.hasModifications()
               || promised.hasModifications()
               || accepted.hasModifications()
               || executeAt.hasModifications()
               || deps.hasModifications()
               || writes.hasModifications()
               || result.hasModifications()
               || status.hasModifications()
               || isGloballyPersistent.hasModifications()
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
        txn.clearModifiedFlag();
        promised.clearModifiedFlag();
        accepted.clearModifiedFlag();
        executeAt.clearModifiedFlag();
        deps.clearModifiedFlag();
        writes.clearModifiedFlag();
        result.clearModifiedFlag();
        status.clearModifiedFlag();
        isGloballyPersistent.clearModifiedFlag();
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
        return commandStore == command.commandStore
               && homeKey.equals(command.homeKey)
               && progressKey.equals(command.progressKey)
               && txnId.equals(command.txnId)
               && txn.equals(command.txn)
               && promised.equals(command.promised)
               && accepted.equals(command.accepted)
               && executeAt.equals(command.executeAt)
               && deps.equals(command.deps)
               && writes.equals(command.writes)
               && result.equals(command.result)
               && status.equals(command.status)
               && isGloballyPersistent.equals(command.isGloballyPersistent)
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
        return Objects.hash(commandStore,
                            txnId,
                            homeKey,
                            progressKey,
                            txn,
                            promised,
                            accepted,
                            executeAt,
                            deps,
                            writes,
                            result,
                            status,
                            isGloballyPersistent,
                            waitingOnCommit,
                            blockingCommitOn,
                            waitingOnApply,
                            blockingApplyOn,
                            storedListeners,
                            transientListeners);
    }

    private AccordStateCache.Instance<TxnId, AccordCommand> cache()
    {
        return commandStore.commandCache();
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
        size += txn.estimatedSizeOnHeap(AccordObjectSizes::txn);
        size += promised.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += accepted.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += executeAt.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += deps.estimatedSizeOnHeap(AccordObjectSizes::dependencies);
        size += writes.estimatedSizeOnHeap(AccordObjectSizes::writes);
        size += result.estimatedSizeOnHeap(r -> ((AccordData) r).estimatedSizeOnHeap());
        size += status.estimatedSizeOnHeap(s -> 0);
        size += isGloballyPersistent.estimatedSizeOnHeap();
        size += waitingOnCommit.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
        size += blockingCommitOn.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += waitingOnApply.estimatedSizeOnHeap(AccordObjectSizes::timestamp, ByteBufferUtil::estimatedSizeOnHeap);
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
    public AccordCommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public Key homeKey()
    {
        return homeKey.get();
    }

    @Override
    protected void setHomeKey(Key key)
    {
        homeKey.set(key);
    }

    @Override
    public Key progressKey()
    {
        return progressKey.get();
    }

    @Override
    protected void setProgressKey(Key key)
    {
        progressKey.set(key);
    }

    @Override
    public Txn txn()
    {
        return txn.get();
    }

    @Override
    protected void setTxn(Txn txn)
    {
        this.txn.set(txn);
    }

    @Override
    public Ballot promised()
    {
        return promised.get();
    }

    @Override
    public void promised(Ballot ballot)
    {
        this.promised.set(ballot);
    }

    @Override
    public Ballot accepted()
    {
        return accepted.get();
    }

    @Override
    public void accepted(Ballot ballot)
    {
        this.accepted.set(ballot);
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt.get();
    }

    @Override
    public void executeAt(Timestamp timestamp)
    {
        Preconditions.checkState(!status().hasBeen(Status.Committed) || executeAt().equals(timestamp));
        this.executeAt.set(timestamp);
    }

    @Override
    public Deps savedDeps()
    {
        return deps.get();
    }

    @Override
    public void savedDeps(Deps deps)
    {
        this.deps.set(deps);
    }

    @Override
    public Writes writes()
    {
        return writes.get();
    }

    @Override
    public void writes(Writes writes)
    {
        this.writes.set(writes);
    }

    @Override
    public Result result()
    {
        return result.get();
    }

    @Override
    public void result(Result result)
    {
        this.result.set(result);
    }

    @Override
    public Status status()
    {
        return status.get();
    }

    @Override
    public void status(Status status)
    {
        this.status.set(status);
    }

    @Override
    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent.get();
    }

    @Override
    public void isGloballyPersistent(boolean v)
    {
        isGloballyPersistent.set(v);
    }

    @Override
    protected void postApply()
    {
        cache().cleanupWriteFuture(txnId);
        super.postApply();
    }

    private boolean canApplyWithCurrentScope()
    {
        KeyRanges ranges = commandStore.ranges().at(executeAt().epoch);
        Keys keys = txn().keys();
        for (int i=0,mi=keys.size(); i<mi; i++)
        {
            Key key = keys.get(i);
            if (commandStore.isCommandsForKeyInContext((AccordKey.PartitionKey) key))
                continue;

            if (!commandStore.hashIntersects(key))
                continue;
            if (!ranges.contains(key))
                continue;

            return false;
        }
        return true;
    }

    private Future<Void> applyWithCorrectScope()
    {
        TxnId txnId = txnId();
        AsyncPromise<Void> promise = new AsyncPromise<>();
        commandStore().process(this, commandStore -> {
            AccordCommand command = (AccordCommand) commandStore.command(txnId);
            command.apply(false).addCallback((v, throwable) -> {
                if (throwable != null)
                    promise.tryFailure(throwable);
                else
                    promise.trySuccess(null);
            });
        });
        return promise;
    }

    private Future<Void> apply(boolean canReschedule)
    {
        Future<Void> future = cache().getWriteFuture(txnId);
        if (future != null)
            return future;

        // this can be called via a listener callback, in which case we won't
        // have the appropriate commandsForKey in scope, so start a new operation
        // with the correct scope and notify the caller when that completes
        if (!canApplyWithCurrentScope())
        {
            Preconditions.checkArgument(canReschedule);
            return applyWithCorrectScope();
        }

        future = super.apply();
        cache().setWriteFuture(txnId, future);
        return future;
    }

    @Override
    public Future<Void> apply()
    {
        return apply(true);
    }

    @Override
    public Future<Data> read(Keys scope)
    {
        ReadFuture future = cache().getReadFuture(txnId);
        if (future != null)
            return future.scope.equals(scope) ? future : super.read(scope);
        future = new ReadFuture(scope, super.read(scope));
        cache().setReadFuture(txnId, future);
        return future;
    }

    private Listener maybeWrapListener(Listener listener)
    {
        if (listener.isTransient())
            return listener;

        if (listener instanceof AccordCommand)
            return new ListenerProxy.CommandListenerProxy(commandStore, ((AccordCommand) listener).txnId());

        if (listener instanceof AccordCommandsForKey)
            return new ListenerProxy.CommandsForKeyListenerProxy(commandStore, ((AccordCommandsForKey) listener).key());

        throw new RuntimeException("Unhandled non-transient listener: " + listener);
    }

    @Override
    public Command addListener(Listener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindAdd((ListenerProxy) listener);
        else
            transientListeners.add(listener);
        return this;
    }

    @Override
    public void removeListener(Listener listener)
    {
        listener = maybeWrapListener(listener);
        if (listener instanceof ListenerProxy)
            storedListeners.blindRemove((ListenerProxy) listener);
        else
            transientListeners.remove(listener);
    }

    public boolean hasListenerFor(TxnId txnId)
    {
        return storedListeners.getView().contains(new ListenerProxy.CommandListenerProxy(commandStore, txnId));
    }

    @Override
    public void notifyListeners()
    {
        storedListeners.getView().forEach(this);
        transientListeners.forEach(listener -> {
            PreLoadContext ctx = listener.listenerPreLoadContext(txnId());
            AsyncContext context = commandStore().getContext();
            if (context.containsScopedItems(ctx))
            {
                logger.trace("{}: synchronously updating listener {}", txnId(), listener);
                listener.onChange(this);
            }
            else
            {
                logger.trace("{}: asynchronously updating listener {}", txnId(), listener);
                commandStore().process(ctx, instance -> {
                    listener.onChange(instance.command(txnId()));
                });
            }
        });
    }

    @Override
    public void addWaitingOnCommit(Command command)
    {
        waitingOnCommit.blindPut(command.txnId(), AccordPartialCommand.serializer.serialize(command));
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return !waitingOnCommit.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(PartialCommand command)
    {
        waitingOnCommit.blindRemove(command.txnId());
    }

    @Override
    public PartialCommand firstWaitingOnCommit()
    {
        if (!isWaitingOnCommit())
            return null;
        ByteBuffer bytes = waitingOnCommit.getView().firstEntry().getValue();
        return AccordPartialCommand.serializer.deserialize(commandStore, bytes);
    }

    @Override
    public void addWaitingOnApplyIfAbsent(PartialCommand command)
    {
        waitingOnApply.blindPut(command.txnId(), AccordPartialCommand.serializer.serialize(command));
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return !waitingOnApply.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnApply(PartialCommand command)
    {
        waitingOnApply.blindRemove(command.txnId());
    }

    @Override
    public PartialCommand firstWaitingOnApply()
    {
        if (!isWaitingOnApply())
            return null;
        ByteBuffer bytes = waitingOnApply.getView().firstEntry().getValue();
        return AccordPartialCommand.serializer.deserialize(commandStore, bytes);
    }
}
