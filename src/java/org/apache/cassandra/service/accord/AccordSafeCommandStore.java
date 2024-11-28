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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractSafeCommandStore;
import accord.local.CommandStores;
import accord.local.NodeCommandStoreService;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;

import static accord.utils.Invariants.illegalState;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeCommandsForKey, AccordCommandStore.ExclusiveCaches>
{
    final AccordTask<?> task;
    private final @Nullable CommandsForRanges commandsForRanges;
    private final AccordCommandStore commandStore;

    private AccordSafeCommandStore(AccordTask<?> task,
                                   @Nullable CommandsForRanges commandsForRanges,
                                   AccordCommandStore commandStore)
    {
        super(task.preLoadContext(), commandStore);
        this.task = task;
        this.commandsForRanges = commandsForRanges;
        this.commandStore = commandStore;
        commandStore.updateRangesForEpoch(this);
    }

    @Override
    public CommandStores.RangesForEpoch ranges()
    {
        CommandStores.RangesForEpoch ranges = super.ranges();
        if (ranges != null)
            return ranges;

        return commandStore.unsafeGetRangesForEpoch();
    }

    public static AccordSafeCommandStore create(AccordTask<?> operation,
                                                @Nullable CommandsForRanges commandsForRanges,
                                                AccordCommandStore commandStore)
    {
        return new AccordSafeCommandStore(operation, commandsForRanges, commandStore);
    }

    @VisibleForTesting
    public Set<RoutingKey> commandsForKeysKeys()
    {
        if (task.commandsForKey() == null)
            return Collections.emptySet();
        return task.commandsForKey().keySet();
    }

    @Override
    protected AccordSafeCommand getInternal(TxnId txnId)
    {
        Map<TxnId, AccordSafeCommand> commands = task.commands();
        if (commands == null)
            return null;
        return commands.get(txnId);
    }

    @Override
    protected ExclusiveCaches tryGetCaches()
    {
        return commandStore.tryLockCaches();
    }

    protected AccordSafeCommand add(AccordSafeCommand safeCommand, ExclusiveCaches caches)
    {
        Object check = task.ensureCommands().putIfAbsent(safeCommand.txnId(), safeCommand);
        if (check == null)
        {
            safeCommand.preExecute();
            return safeCommand;
        }
        else
        {
            caches.commands().release(safeCommand, task);
            throw illegalState("Attempted to take a duplicate reference to %s", safeCommand.txnId());
        }
    }

    protected AccordSafeCommandsForKey add(AccordSafeCommandsForKey safeCfk, ExclusiveCaches caches)
    {
        Object check = task.ensureCommandsForKey().putIfAbsent(safeCfk.key(), safeCfk);
        if (check == null)
        {
            safeCfk.preExecute();
            return safeCfk;
        }
        else
        {
            caches.commandsForKeys().release(safeCfk, task);
            throw illegalState("Attempted to take a duplicate reference to CFK for %s", safeCfk.key());
        }
    }

    @Override
    protected AccordSafeCommandsForKey getInternal(RoutingKey key)
    {
        Map<RoutingKey, AccordSafeCommandsForKey> commandsForKey = task.commandsForKey();
        if (commandsForKey == null)
            return null;
        return commandsForKey.get(key);
    }

    @Override
    public AccordCommandStore commandStore()
    {
        return commandStore;
    }

    @Override
    public DataStore dataStore()
    {
        return commandStore().dataStore();
    }

    @Override
    public Agent agent()
    {
        return commandStore.agent();
    }

    @Override
    public ProgressLog progressLog()
    {
        return commandStore().progressLog();
    }

    @Override
    public NodeCommandStoreService node()
    {
        // TODO: safe command store should not have arbitrary time
        return commandStore.node();
    }

    private boolean visitForKey(Unseekables<?> keysOrRanges, Predicate<CommandsForKey> forEach)
    {
        Map<RoutingKey, AccordSafeCommandsForKey> commandsForKey = task.commandsForKey;
        if (commandsForKey == null)
            return true;

        Unseekables<?> skip = context.keys().without(keysOrRanges);
        for (SafeCommandsForKey safeCfk : commandsForKey.values())
        {
            if (skip.contains(safeCfk.key()))
                continue;

            if (!forEach.test(safeCfk.current()))
                return false;
        }
        return true;
    }

    @Override
    public <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Txn.Kind.Kinds testKind, ActiveCommandVisitor<P1, P2> visitor, P1 p1, P2 p2)
    {
        visitForKey(keysOrRanges, cfk -> { cfk.visit(startedBefore, testKind, visitor, p1, p2); return true; });
        if (commandsForRanges != null)
            commandsForRanges.visit(keysOrRanges, startedBefore, testKind, visitor, p1, p2);
    }

    // TODO (expected): instead of accepting a slice, accept the min/max epoch and let implementation handle it
    @Override
    public boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, Timestamp testStartedAtTimestamp, ComputeIsDep computeIsDep, AllCommandVisitor visit)
    {
        return visitForKey(keysOrRanges, cfk -> cfk.visit(testTxnId, testKind, testStartedAt, testStartedAtTimestamp, computeIsDep, null, visit))
               && (commandsForRanges == null || commandsForRanges.visit(keysOrRanges, testTxnId, testKind, testStartedAt, testStartedAtTimestamp, computeIsDep, visit));
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandStore(id=" + commandStore().id() + ")";
    }
}