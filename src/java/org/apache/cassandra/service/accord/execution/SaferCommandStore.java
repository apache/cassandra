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

package org.apache.cassandra.service.accord.execution;

import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal.FieldUpdates;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractSafeCommandStore;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommandSummaries;
import accord.local.ExecutionContext;
import accord.local.NodeCommandStoreService;
import accord.local.RedundantBefore;
import accord.local.SafeState;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.metrics.LogLinearDecayingHistograms;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;
import org.apache.cassandra.service.accord.AccordCommandStore.SafeRedundantBefore;
import org.apache.cassandra.service.accord.AccordDurableOnFlush.ReportDurable;
import org.apache.cassandra.service.paxos.PaxosState;

import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.UNQUEUED;

public final class SaferCommandStore extends AbstractSafeCommandStore<SaferCommand, SaferCommandsForKey, ExclusiveCaches>
{
    final SafeTask<?> task;

    SaferCommandStore(SafeTask<?> task, ExecutionContext context)
    {
        super(context);
        this.task = task;
    }

    @VisibleForTesting
    public Iterable<SafeCommandsForKey> safeCommandsForKeys()
    {
        return task.refs.values().stream()
                        .filter(v -> v instanceof SafeCommandsForKey)
                        .map(v -> (SafeCommandsForKey)v)::iterator;
    }

    @Override
    protected SaferCommand getInternal(TxnId txnId)
    {
        return (SaferCommand) task.refs.get(txnId);
    }

    @Override
    protected ExclusiveCaches tryGetCaches()
    {
        if (task.isIncremental())
        {
            // We could relax this, but requires careful consideration of semantics when:
            //  - touching keys we have scheduled to process later
            //  - touching txnIds we may have to logically lock until the whole processing completes to
            //    ensure persistent state machine updates are consistent with incremental updates to cache
            //  - priority inversion deadlock - likely conflicts with the above, since lock acquisition order becomes
            //    non-deterministic so we can introduce cycles, so at least TxnId may need to be forbidden, and keys
            //    must release their locks immediately after the run that processes them
            return null;
        }
        return commandStore().tryLockCaches();
    }

    protected SaferCommand add(SaferCommand safeCommand, ExclusiveCaches caches)
    {
        Object check = task.refs.putIfAbsent(safeCommand.txnId(), safeCommand);
        if (check == null)
        {
            Invariants.require(!task.isIncremental()); // if isIncremental we'll have to supply holdQueue==true, but for now we forbid it
            safeCommand.preExecute(task, UNQUEUED);
            return safeCommand;
        }
        else
        {
            caches.commands().release(safeCommand, task);
            throw illegalState("Attempted to take a duplicate reference to %s", safeCommand.txnId());
        }
    }

    @Override
    protected void persistFieldUpdates()
    {
        // Field persistence is handled by SafeTask
    }

    void persistFieldUpdatesInternal(Runnable onDone)
    {
        FieldUpdates updates = fieldUpdates();
        if (updates == null)
            return;

        if (updates.newRedundantBefore != null)
        {
            Runnable reportRedundantBefore = SafeRedundantBefore.updater(commandStore(), updates.newRedundantBefore);
            Runnable prevOnDone = onDone;
            onDone = prevOnDone == null ? reportRedundantBefore : () -> {
                try { reportRedundantBefore.run(); }
                finally { prevOnDone.run(); }
            };
        }
        commandStore().persistFieldUpdates(updates, onDone);
    }

    protected SaferCommandsForKey add(SaferCommandsForKey safeCfk, ExclusiveCaches caches)
    {
        Object check = task.refs.putIfAbsent(safeCfk.key(), safeCfk);
        if (check == null)
        {
            safeCfk.preExecute(task, UNQUEUED);
            return safeCfk;
        }
        else
        {
            caches.commandsForKeys().release(safeCfk, task);
            throw illegalState("Attempted to take a duplicate reference to CFK for %s", safeCfk.key());
        }
    }

    @Override
    protected SaferCommandsForKey getInternal(RoutingKey key)
    {
        return (SaferCommandsForKey) task.refs.get(key);
    }

    @Override
    public void setRangesForEpoch(CommandStores.RangesForEpoch rangesForEpoch)
    {
        super.setRangesForEpoch(rangesForEpoch);
        commandStore().updateMinHlc(PaxosState.ballotTracker().getLowBound().unixMicros() + 1);
    }

    @Override
    public void updateCommandsForRanges(Command prev, Command updated, boolean force)
    {
        commandStore().rangeIndex().update(prev, updated, force);
    }

    @Override
    public void reportDurable(RedundantBefore addRedundantBefore, int flags)
    {
        upsertRedundantBefore(addRedundantBefore);
        ReportDurable.reportMaybeTerminate(commandStore(), flags);
    }

    @Override
    public AccordCommandStore commandStore()
    {
        return task.commandStore;
    }

    @Override
    public DataStore dataStore()
    {
        return commandStore().dataStore();
    }

    @Override
    public Agent agent()
    {
        return commandStore().agent();
    }

    @Override
    public ProgressLog progressLog()
    {
        return commandStore().progressLog();
    }

    @Override
    public NodeCommandStoreService node()
    {
        return commandStore().node();
    }

    public LogLinearDecayingHistograms.Buffer histogramBuffer()
    {
        if (task.histogramBuffer == null)
        {
            task.histogramBuffer = commandStore().metricsBuffer;
            if (task.histogramBuffer == null)
                task.histogramBuffer = commandStore().metricsBuffer = new LogLinearDecayingHistograms.Buffer(commandStore().executor().histograms);
            if (!Invariants.expect(task.histogramBuffer.isEmpty()))
                task.histogramBuffer.clear();
        }
        return task.histogramBuffer;
    }

    private boolean visitForKey(Unseekables<?> keysOrRanges, Predicate<CommandsForKey> forEach)
    {
        Unseekables<?> unseekables = context.keys();
        switch (unseekables.domain())
        {
            default: throw new UnhandledEnum(unseekables.domain());
            case Key:
                AbstractUnseekableKeys keys = (AbstractUnseekableKeys) context.keys();
                return Routables.foldl(keys, keysOrRanges, (self, f, key, v, index) -> {
                    SafeCommandsForKey safeCfk = (SafeCommandsForKey) self.task.refs.get(key);
                    if (safeCfk == null || safeCfk.isUninitialised())
                        return v;
                    return f.test(safeCfk.current());
                }, this, forEach, Boolean.TRUE, cont -> !cont);

            case Range:
                Unseekables<?> skip = context.keys().without(keysOrRanges);
                for (SafeState<?> safeState : task.refs.values())
                {
                    if (!(safeState instanceof SaferCommandsForKey))
                        continue;

                    SafeCommandsForKey safeCfk = (SafeCommandsForKey) safeState;
                    if (safeCfk.isUninitialised() || skip.contains(safeCfk.key()))
                        continue;

                    if (!forEach.test(safeCfk.current()))
                        return false;
                }
                return true;
        }
    }

    @Override
    public <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visitor, P1 p1, P2 p2)
    {
        visitForKey(keysOrRanges, cfk -> { cfk.visit(startedBefore, testKind, visitor, p1, p2); return true; });
        CommandSummaries commandsForRanges = task.commandsForRanges();
        if (commandsForRanges != null)
            commandsForRanges.visit(keysOrRanges, startedBefore, testKind, visitor, p1, p2);
    }

    @Override
    public boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, SupersedingCommandVisitor visit)
    {
        if (!visitForKey(keysOrRanges, cfk -> cfk.visit(testTxnId, testKind, visit)))
            return false;

        CommandSummaries commandsForRanges = task.commandsForRanges();
        return commandsForRanges == null || commandsForRanges.visit(keysOrRanges, testTxnId, testKind, visit);
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandStore(id=" + commandStore().id() + ')';
    }
}