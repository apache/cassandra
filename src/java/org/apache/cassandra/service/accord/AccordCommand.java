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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Listener;
import accord.local.Listeners;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.local.Status.NotWitnessed;

public class AccordCommand extends Command implements AccordStateCache.AccordState<TxnId, AccordCommand>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new AccordCommand(null, null));

    private final CommandStore commandStore;
    private final TxnId txnId;
    private Txn txn;
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt;
    // TODO: optionally load some fields, and throw exceptions if reads are attempted on un-loaded fields
    private Dependencies deps = new Dependencies();
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;

    // TODO: compact binary format for below collections, with a step to materialize when needed
    private NavigableMap<TxnId, TxnId> waitingOnCommit;
    private NavigableMap<Timestamp, TxnId> waitingOnApply;
    private Listeners listeners;

    boolean isDirty = false;

    public AccordCommand(CommandStore commandStore, TxnId txnId)
    {
        this.commandStore = commandStore;
        this.txnId = txnId;
    }

    void save() throws IOException
    {
        if (!isDirty)
            return;
        // TODO: accumulate changes and flush to partition update at end
        AccordKeyspace.saveCommand(this);
        isDirty = false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordCommand command = (AccordCommand) o;
        return commandStore == command.commandStore
               && txnId.equals(command.txnId)
               && Objects.equals(txn, command.txn)
               && promised.equals(command.promised)
               && accepted.equals(command.accepted)
               && Objects.equals(executeAt, command.executeAt)
               && deps.equals(command.deps)
               && Objects.equals(writes, command.writes)
               && Objects.equals(result, command.result)
               && status == command.status
               && Objects.equals(waitingOnCommit, command.waitingOnCommit)
               && Objects.equals(waitingOnApply, command.waitingOnApply)
               && Objects.equals(listeners, command.listeners);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(commandStore, txnId, txn, promised, accepted, executeAt, deps, writes, result, status, waitingOnCommit, waitingOnApply, listeners);
    }

    @Override
    public AccordStateCache.Node<TxnId, AccordCommand> createNode()
    {
        return new AccordStateCache.Node<>(this)
        {
            @Override
            long sizeInBytes(AccordCommand value)
            {
                return value.unsharedSizeOnHeap();
            }
        };
    }

    @Override
    public TxnId key()
    {
        return txnId;
    }

    private long unsharedSizeOnHeap()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TxnId txnId()
    {
        return txnId;
    }

    @Override
    public CommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public Txn txn()
    {
        return txn;
    }

    @Override
    public void txn(Txn txn)
    {
        isDirty = true;
        this.txn = txn;
    }

    @Override
    public Ballot promised()
    {
        return promised;
    }

    @Override
    public void promised(Ballot ballot)
    {
        isDirty = true;
        this.promised = ballot;
    }

    @Override
    public Ballot accepted()
    {
        return accepted;
    }

    @Override
    public void accepted(Ballot ballot)
    {
        isDirty = true;
        this.accepted = ballot;
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    @Override
    public void executeAt(Timestamp timestamp)
    {
        isDirty = true;
        this.executeAt = timestamp;
    }

    @Override
    public Dependencies savedDeps()
    {
        return deps;
    }

    @Override
    public void savedDeps(Dependencies deps)
    {
        isDirty = true;
        this.deps = deps;
    }

    @Override
    public Writes writes()
    {
        return writes;
    }

    @Override
    public void writes(Writes writes)
    {
        isDirty = true;
        this.writes = writes;
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public void result(Result result)
    {
        isDirty = true;
        this.result = result;
    }

    @Override
    public Status status()
    {
        return status;
    }

    @Override
    public void status(Status status)
    {
        isDirty = true;
        this.status = status;
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
        isDirty = true;
        if (listeners == null)
            listeners = new Listeners();
        listeners.add(maybeWrapListener(listener));
        return this;
    }

    @Override
    public void removeListener(Listener listener)
    {
        isDirty = true;
        if (listeners != null)
            listeners.remove(maybeWrapListener(listener));
    }

    @Override
    public void notifyListeners()
    {
        // TODO: defer to executor
        if (listeners != null)
            listeners.forEach(this);
    }

    @Override
    public void clearWaitingOnCommit()
    {
        waitingOnCommit = null;
    }

    @Override
    public void addWaitingOnCommit(TxnId txnId, Command command)
    {
        isDirty = true;
        if (waitingOnCommit == null)
            waitingOnCommit = new TreeMap<>();

        waitingOnCommit.put(txnId, command.txnId());
    }

    @Override
    public boolean isWaitingOnCommit()
    {
        return waitingOnCommit != null && !waitingOnCommit.isEmpty();
    }

    @Override
    public boolean removeWaitingOnCommit(TxnId txnId)
    {
        isDirty = true;
        if (waitingOnCommit == null)
            return false;
        return waitingOnCommit.remove(txnId) != null;
    }

    @Override
    public Command firstWaitingOnCommit()
    {
        return isWaitingOnCommit() ? commandStore.command(waitingOnCommit.firstEntry().getValue()) : null;
    }

    @Override
    public void clearWaitingOnApply()
    {
        waitingOnApply = null;
    }

    @Override
    public void addWaitingOnApplyIfAbsent(Timestamp timestamp, Command command)
    {
        isDirty = true;
        if (waitingOnApply == null)
            waitingOnApply = new TreeMap<>();

        waitingOnApply.putIfAbsent(timestamp, command.txnId());
    }

    @Override
    public boolean isWaitingOnApply()
    {
        return waitingOnApply != null && !waitingOnApply.isEmpty();
    }

    @Override
    public boolean removeWaitingOnApply(Timestamp timestamp)
    {
        isDirty = true;
        if (waitingOnApply == null)
            return false;
        return waitingOnApply.remove(timestamp) != null;
    }

    @Override
    public Command firstWaitingOnApply()
    {
        return isWaitingOnApply() ? commandStore.command(waitingOnApply.firstEntry().getValue()) : null;
    }

    public void setWaitingOnCommit(NavigableMap<TxnId, TxnId> waitingOnCommit)
    {
        this.waitingOnCommit = waitingOnCommit;
    }

    public NavigableMap<TxnId, TxnId> getWaitingOnCommit()
    {
        return waitingOnCommit;
    }

    public void setWaitingOnApply(NavigableMap<Timestamp, TxnId> waitingOnApply)
    {
        this.waitingOnApply = waitingOnApply;
    }

    public NavigableMap<Timestamp, TxnId> getWaitingOnApply()
    {
        return waitingOnApply;
    }

    public Listeners getListeners()
    {
        return listeners;
    }

    public void setListeners(Listeners listeners)
    {
        this.listeners = listeners;
    }
}
