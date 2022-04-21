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

import com.google.common.base.Preconditions;

import accord.api.Read;
import accord.api.Result;
import accord.local.Command;
import accord.local.Listener;
import accord.local.Listeners;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.service.accord.serializers.CommandSummaries;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.service.accord.store.StoredValue;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;

public class AccordCommand extends Command implements AccordStateCache.AccordState<TxnId, AccordCommand>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommand(null, null));

    public static class WriteOnly extends AccordCommand implements AccordStateCache.WriteOnly<TxnId, AccordCommand>
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
    }

    private final AccordCommandStore commandStore;
    private final TxnId txnId;
    public final StoredValue<Txn> txn = new StoredValue<>();
    public final StoredValue<Ballot> promised = new StoredValue<>();
    public final StoredValue<Ballot> accepted = new StoredValue<>();
    public final StoredValue<Timestamp> executeAt = new StoredValue<>();
    public final StoredValue<Dependencies> deps = new StoredValue<>();
    public final StoredValue<Writes> writes = new StoredValue<>();
    public final StoredValue<Result> result = new StoredValue<>();

    public final StoredValue.HistoryPreserving<Status> status = new StoredValue.HistoryPreserving<>();

    public final StoredNavigableMap<TxnId, ByteBuffer> waitingOnCommit = new StoredNavigableMap<>();
    public final StoredNavigableMap<TxnId, ByteBuffer> waitingOnApply = new StoredNavigableMap<>();

    public final StoredSet.DeterministicIdentity<ListenerProxy> storedListeners = new StoredSet.DeterministicIdentity<>();
    private final Listeners transientListeners = new Listeners();

    public final StoredSet.Navigable<TxnId> blockingCommitOn = new StoredSet.Navigable<>();
    public final StoredSet.Navigable<TxnId> blockingApplyOn = new StoredSet.Navigable<>();

    public AccordCommand(AccordCommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    public AccordCommand initialize()
    {
        status.set(Status.NotWitnessed);
        txn.set(null);
        executeAt.load(null);
        promised.set(Ballot.ZERO);
        accepted.set(Ballot.ZERO);
        deps.set(new Dependencies());
        writes.load(null);
        result.load(null);
        waitingOnCommit.load(new TreeMap<>());
        blockingCommitOn.load(new TreeSet<>());
        waitingOnApply.load(new TreeMap<>());
        blockingApplyOn.load(new TreeSet<>());
        storedListeners.load(new TreeSet<>());
        return this;
    }

    // FIXME: should be able to get rid of this with segregated command classes
    public boolean isLoaded()
    {
        return txn.isLoaded()
               && promised.isLoaded()
               && accepted.isLoaded()
               && executeAt.isLoaded()
               && deps.isLoaded()
               && writes.isLoaded()
               && result.isLoaded()
               && status.isLoaded()
               && waitingOnCommit.isLoaded()
               && blockingCommitOn.isLoaded()
               && waitingOnApply.isLoaded()
               && blockingApplyOn.isLoaded()
               && storedListeners.isLoaded();
    }

    @Override
    public boolean hasModifications()
    {
        return txn.hasModifications()
               || promised.hasModifications()
               || accepted.hasModifications()
               || executeAt.hasModifications()
               || deps.hasModifications()
               || writes.hasModifications()
               || result.hasModifications()
               || status.hasModifications()
               || waitingOnCommit.hasModifications()
               || blockingCommitOn.hasModifications()
               || waitingOnApply.hasModifications()
               || blockingApplyOn.hasModifications()
               || storedListeners.hasModifications();
    }

    @Override
    public void clearModifiedFlag()
    {
        txn.clearModifiedFlag();
        promised.clearModifiedFlag();
        accepted.clearModifiedFlag();
        executeAt.clearModifiedFlag();
        deps.clearModifiedFlag();
        writes.clearModifiedFlag();
        result.clearModifiedFlag();
        status.clearModifiedFlag();
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
               && txnId.equals(command.txnId)
               && txn.equals(command.txn)
               && promised.equals(command.promised)
               && accepted.equals(command.accepted)
               && executeAt.equals(command.executeAt)
               && deps.equals(command.deps)
               && writes.equals(command.writes)
               && result.equals(command.result)
               && status.equals(command.status)
               && waitingOnCommit.equals(command.waitingOnCommit)
               && blockingCommitOn.equals(command.blockingCommitOn)
               && waitingOnApply.equals(command.waitingOnApply)
               && blockingApplyOn.equals(command.blockingApplyOn)
               && storedListeners.equals(command.storedListeners)
               && transientListeners.equals(command.transientListeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore,
                            txnId,
                            txn,
                            promised,
                            accepted,
                            executeAt,
                            deps,
                            writes,
                            result,
                            status,
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
    public AccordStateCache.Node<TxnId, AccordCommand> createNode()
    {
        return new AccordStateCache.Node<>(this)
        {
            @Override
            long sizeInBytes(AccordCommand value)
            {
                return value.estimatedSizeOnHeap();
            }
        };
    }

    @Override
    public TxnId key()
    {
        return txnId;
    }

    private long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += AccordObjectSizes.timestamp(txnId);
        size += txn.estimatedSizeOnHeap(AccordObjectSizes::txn);
        size += promised.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += accepted.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += executeAt.estimatedSizeOnHeap(AccordObjectSizes::timestamp);
        size += deps.estimatedSizeOnHeap(AccordObjectSizes::dependencies);
        size += writes.estimatedSizeOnHeap(AccordObjectSizes::writes);
        size += result.estimatedSizeOnHeap(r -> ((AccordData) r).estimatedSizeOnHeap());
        size += status.estimatedSizeOnHeap(s -> 0);
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
        return CommandSummaries.waitingOn.needsUpdate(this);
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
    public Txn txn()
    {
        return txn.get();
    }

    @Override
    public void txn(Txn txn)
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
        Preconditions.checkState(!status().hasBeen(Status.Committed));
        this.executeAt.set(timestamp);
    }

    @Override
    public Dependencies savedDeps()
    {
        return deps.get();
    }

    @Override
    public void savedDeps(Dependencies deps)
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
    protected void postApply()
    {
        super.postApply();
        cache().clearWriteFuture(txnId);
    }

    @Override
    public Future<?> apply(Txn txn, Dependencies deps, Timestamp executeAt, Writes writes, Result result)
    {
        Future<?> future = cache().getWriteFuture(txnId);
        if (future != null)
            return future;
        future = super.apply(txn, deps, executeAt, writes, result);
        cache().setWriteFuture(txnId, future);
        return future;
    }

    @Override
    public Read.ReadFuture read(Keys keyscope)
    {
        Read.ReadFuture future = cache().getReadFuture(txnId);
        if (future != null)
            return future.keyScope.equals(keyscope) ? future : super.read(keyscope);
        future = super.read(keyscope);
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

    @Override
    public void notifyListeners()
    {
        storedListeners.getView().forEach(this);
        transientListeners.forEach(this);
    }

    @Override
    public void addWaitingOnCommit(Command command)
    {
        waitingOnCommit.blindPut(command.txnId(), CommandSummaries.waitingOn.serialize((AccordCommand) command));
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return !waitingOnCommit.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnCommit(Command command)
    {
        waitingOnCommit.blindRemove(command.txnId());
    }

    @Override
    public Command firstWaitingOnCommit()
    {
        if (!isWaitingOnCommit())
            return null;
        ByteBuffer bytes = waitingOnCommit.getView().firstEntry().getValue();
        return CommandSummaries.waitingOn.deserialize(commandStore, bytes);
    }

    @Override
    public void addWaitingOnApplyIfAbsent(Command command)
    {
        waitingOnApply.blindPut(command.txnId(), CommandSummaries.waitingOn.serialize((AccordCommand) command));
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return !waitingOnApply.getView().isEmpty();
    }

    @Override
    public void removeWaitingOnApply(Command command)
    {
        waitingOnApply.blindRemove(command.txnId());
    }

    @Override
    public Command firstWaitingOnApply()
    {
        if (!isWaitingOnApply())
            return null;
        ByteBuffer bytes = waitingOnApply.getView().firstEntry().getValue();
        return CommandSummaries.waitingOn.deserialize(commandStore, bytes);
    }
}
