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

package org.apache.cassandra.service.accord.interop;

import java.util.BitSet;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.Command;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.MessageType;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.service.accord.AccordMessageSink.AccordMessageType;
import org.apache.cassandra.service.accord.serializers.ApplySerializers.ApplySerializer;
import org.apache.cassandra.service.accord.txn.AccordUpdate;

import static accord.utils.Invariants.checkState;
import static accord.utils.MapReduceConsume.forEach;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Apply that waits until the transaction is actually applied before sending a response
 * // TODO (desired): At this point there are a plethora of do X to Command, then wait until state Y before maybe doing Z and returning a response, potentially returning insufficient along the way
 * // and these all are a bit copy pasta in terms of managing things like waiting on, obsoletion, cancellation/listeners, insufficient etc. and it would be less fragile
 * // in the long run to not duplicate these kind of difficult to get right mechanism and have a single pluggable framework to request each specific behavior
 */
public class AccordInteropApply extends Apply implements Command.TransientListener
{
    public static final Apply.Factory FACTORY = new Apply.Factory()
    {
        @Override
        public Apply create(Kind kind, Id to, Topologies participates, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
        {
            checkArgument(kind != Kind.Maximal, "Shouldn't need to send a maximal commit with interop support");
            ConsistencyLevel commitCL = txn.update() instanceof AccordUpdate ? ((AccordUpdate) txn.update()).cassandraCommitCL() : null;
            // Any asynchronous apply option should use the regular Apply that doesn't wait for writes to complete
            if (commitCL == null || commitCL == ConsistencyLevel.ANY)
                return Apply.FACTORY.create(kind, to, participates, txnId, route, txn, executeAt, deps, writes, result);
            return new AccordInteropApply(kind, to, participates, txnId, route, txn, executeAt, deps, writes, result);
        }
    };

    public static final IVersionedSerializer<AccordInteropApply> serializer = new ApplySerializer<AccordInteropApply>()
    {
        @Override
        protected AccordInteropApply deserializeApply(TxnId txnId, PartialRoute<?> scope, long waitForEpoch, Apply.Kind kind, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
        {
            return new AccordInteropApply(kind, txnId, scope, waitForEpoch, keys, executeAt, deps, txn, fullRoute, writes, result);
        }
    };

    transient BitSet waitingOn;
    transient int waitingOnCount;

    private AccordInteropApply(Kind kind, TxnId txnId, PartialRoute<?> route, long waitForEpoch, Seekables<?, ?> keys, Timestamp executeAt, PartialDeps deps, @Nullable PartialTxn txn, @Nullable FullRoute<?> fullRoute, Writes writes, Result result)
    {
        super(kind, txnId, route, waitForEpoch, keys, executeAt, deps, txn, fullRoute, writes, result);
    }

    private AccordInteropApply(Kind kind, Id to, Topologies participates, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        super(kind, to, participates, txnId, route, txn, executeAt, deps, writes, result);
    }

    @Override
    public void process()
    {
        waitingOn = new BitSet();
        super.process();
    }


    @Override
    public ApplyReply apply(SafeCommandStore safeStore)
    {
        ApplyReply reply = super.apply(safeStore);
        checkState(reply == ApplyReply.Redundant || reply == ApplyReply.Applied || reply == ApplyReply.Insufficient, "Unexpected ApplyReply");

        // Hasn't necessarily finished applying yet so need to check and maybe add a listener
        // Redundant means we are competing with a recovery coordinator which is fine
        // we don't need to return an error we can wait for the Apply
        // Insufficient means it is safe to install the listener and wait for Apply to happen
        // once the coordinator sends a maximal commit
        // Applied doesn't actually mean the command is in the Applied state so we still need to check and maybe install
        // the listener
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, scope);
        Command current = safeCommand.current();
        // Don't actually think it is possible for this to reach applied while we are stll running, but just to be safe
        // check anyways
        Status status = current.status();
        switch (status)
        {
            default: throw new AssertionError();
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
            case PreApplied:
                synchronized (this)
                {
                    waitingOn.set(safeStore.commandStore().id());
                    ++waitingOnCount;
                }
                safeCommand.addListener(this);
                break;

            case Applied:
            case Invalidated:
            case Truncated:
        }

        return reply;
    }

    private synchronized void ack()
    {
        // wait for -1 to ensure the setup phase has also completed. Setup calls ack in its callback
        // and prevents races where we respond before dispatching all the required reads (if the reads are
        // completing faster than the reads can be setup on all required shards)
        if (-1 == --waitingOnCount)
        {
            node.reply(replyTo, replyContext, ApplyReply.Applied, null);
        }
    }

    @Override
    public ApplyReply reduce(ApplyReply r1, ApplyReply r2)
    {
        return r1 == null || r2 == null
               ? r1 == null ? r2 : r1
               : r1.compareTo(r2) >= 0 ? r1 : r2;
    }

    @Override
    public void accept(ApplyReply reply, Throwable failure)
    {
        if (reply == ApplyReply.Insufficient)
        {
            // Respond with insufficient which should make the coordinator send us the commit
            // we need to respond
            node.reply(replyTo, replyContext, reply, failure);
        }
        else if (failure != null)
        {
            node.reply(replyTo, replyContext, null, failure);
            node.agent().onUncaughtException(failure);
            cancel();
        }

        // Unless failed always ack to indicate setup has completed otherwise the counter never gets to -1
        if (failure == null)
            ack();
    }

    private void cancel()
    {
        node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(safeStore -> {
            SafeCommand safeCommand = safeStore.ifInitialised(txnId);
            if (safeCommand != null)
                safeCommand.removeListener(this);
        }, node.agent()));
    }
    
    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        if (txn == null) return Keys.EMPTY;
        return txn.keys();
    }

    @Override
    public MessageType type()
    {
        switch (kind)
        {
            case Minimal: return AccordMessageType.INTEROP_APPLY_MINIMAL_REQ;
            case Maximal: return AccordMessageType.INTEROP_APPLY_MAXIMAL_REQ;
            default: throw new IllegalStateException();
        }
    }

    @Override
    public String toString()
    {
        return "AccordInteropApply{" +
               "txnId:" + txnId +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }

    @Override
    public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();

        switch (command.status())
        {
            default: throw new AssertionError();
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
            case PreApplied:
                return;

            case Applied:
            case Invalidated:
            case Truncated:
        }

        if (safeCommand.removeListener(this))
            ack();
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(txnId);
    }
}
