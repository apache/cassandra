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
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.AbstractSafeCommandStore;
import accord.impl.CommandsSummary;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.NodeCommandStoreService;
import accord.local.RedundantBefore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractKeys;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;

import static accord.utils.Invariants.illegalState;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeTimestampsForKey, AccordSafeCommandsForKey, AccordCommandStore.ExclusiveCaches>
{
    final AccordTask<?> task;
    private final @Nullable CommandsForRanges commandsForRanges;
    private final AccordCommandStore commandStore;
    private RangesForEpoch ranges;
    private FieldUpdates fieldUpdates;

    private AccordSafeCommandStore(AccordTask<?> task,
                                   @Nullable CommandsForRanges commandsForRanges,
                                   AccordCommandStore commandStore)
    {
        super(task.preLoadContext());
        this.task = task;
        this.commandsForRanges = commandsForRanges;
        this.commandStore = commandStore;
        commandStore.updateRangesForEpoch(this);
        if (this.ranges == null)
            this.ranges = Invariants.nonNull(commandStore.unsafeRangesForEpoch());
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

    protected AccordSafeTimestampsForKey add(AccordSafeTimestampsForKey safeTfk, ExclusiveCaches caches)
    {
        Object check = task.ensureTimestampsForKey().putIfAbsent(safeTfk.key(), safeTfk);
        if (check == null)
        {
            safeTfk.preExecute();
            return safeTfk;
        }
        else
        {
            caches.timestampsForKeys().release(safeTfk, task);
            throw illegalState("Attempted to take a duplicate reference to CFK for %s", safeTfk.key());
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

    protected AccordSafeTimestampsForKey timestampsForKeyInternal(RoutingKey key)
    {
        Map<RoutingKey, AccordSafeTimestampsForKey> timestampsForKey = task.timestampsForKey();
        if (timestampsForKey == null)
            return null;

        return timestampsForKey.get(key);
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

    @Override
    public RangesForEpoch ranges()
    {
        return ranges;
    }

    private <O> O mapReduce(Routables<?> keysOrRanges, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        Invariants.checkState(context.keys().containsAll(keysOrRanges), "Attempted to access keysOrRanges outside of what was asked for; asked for %s, accessed %s", context.keys(), keysOrRanges);
        accumulate = mapReduceForRange(keysOrRanges, map, accumulate);
        return mapReduceForKey(keysOrRanges, map, accumulate);
    }

    private <O> O mapReduceForRange(Routables<?> keysOrRanges, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        if (commandsForRanges == null)
            return accumulate;

        return map.apply(commandsForRanges, accumulate);
    }

    private <O> O mapReduceForKey(Routables<?> keysOrRanges, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        switch (keysOrRanges.domain())
        {
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
            case Key:
            {
                // TODO: efficiency
                AbstractKeys<RoutingKey> keys = (AbstractKeys<RoutingKey>) keysOrRanges;
                for (RoutingKey key : keys)
                {
                    CommandsForKey commands = get(key).current();
                    accumulate = map.apply(commands, accumulate);
                }
            }
            break;
            case Range:
            {
                // Assuming the range provided is in the PreLoadContext, then AsyncLoader has populated commandsForKeys with keys that
                // are contained within the ranges... so walk all keys found in commandsForKeys
                if (!context.keys().containsAll(keysOrRanges))
                    throw new AssertionError("Range(s) detected not present in the PreLoadContext: expected " + context.keys() + " but given " + keysOrRanges);

                Map<RoutingKey, AccordSafeCommandsForKey> commandsForKey = task.commandsForKey();
                if (commandsForKey == null)
                    break;

                for (RoutingKey key : commandsForKey.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    CommandsForKey commands = get(key).current();
                    accumulate = map.apply(commands, accumulate);
                }
            }
            break;
        }
        return accumulate;
    }

    @Override
    public <P1, T> T mapReduceActive(Unseekables<?> keysOrRanges, @Nullable Timestamp withLowerTxnId, Txn.Kind.Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, (summary, in) -> {
            return summary.mapReduceActive(keysOrRanges, withLowerTxnId, testKind, map, p1, in);
        }, accumulate);
    }

    @Override
    public <P1, T> T mapReduceFull(Unseekables<?> keysOrRanges, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, (summary, in) -> {
            return summary.mapReduceFull(keysOrRanges, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, in);
        }, accumulate);
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandStore(id=" + commandStore().id() + ")";
    }

    @Override
    public void upsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        // TODO (required): this is a temporary measure, see comment on AccordJournalValueSerializers; upsert instead
        //  when modifying, only modify together with AccordJournalValueSerializers
        ensureFieldUpdates().newRedundantBefore = ensureFieldUpdates().addRedundantBefore = RedundantBefore.merge(redundantBefore(), addRedundantBefore);
    }

    @Override
    public void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        ensureFieldUpdates().newBootstrapBeganAt = newBootstrapBeganAt;
    }

    @Override
    public void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        ensureFieldUpdates().newSafeToRead = newSafeToRead;
    }

    @Override
    public void setRangesForEpoch(CommandStores.RangesForEpoch rangesForEpoch)
    {
        ensureFieldUpdates().newRangesForEpoch = rangesForEpoch.snapshot();
        ranges = rangesForEpoch;
    }

    @Override
    public NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        if (fieldUpdates != null && fieldUpdates.newBootstrapBeganAt != null)
            return fieldUpdates.newBootstrapBeganAt;

        return super.bootstrapBeganAt();
    }

    @Override
    public NavigableMap<Timestamp, Ranges> safeToReadAt()
    {
        if (fieldUpdates != null && fieldUpdates.newSafeToRead != null)
            return fieldUpdates.newSafeToRead;

        return super.safeToReadAt();
    }

    @Override
    public RedundantBefore redundantBefore()
    {
        if (fieldUpdates != null && fieldUpdates.newRedundantBefore != null)
            return fieldUpdates.newRedundantBefore;

        return super.redundantBefore();
    }

    private FieldUpdates ensureFieldUpdates()
    {
        if (fieldUpdates == null) fieldUpdates = new FieldUpdates();
        return fieldUpdates;
    }

    public FieldUpdates fieldUpdates()
    {
        return fieldUpdates;
    }

    public void postExecute()
    {
        if (fieldUpdates == null)
            return;

        if (fieldUpdates.newRedundantBefore != null)
            super.unsafeSetRedundantBefore(fieldUpdates.newRedundantBefore);

        if (fieldUpdates.newBootstrapBeganAt != null)
            super.setBootstrapBeganAt(fieldUpdates.newBootstrapBeganAt);

        if (fieldUpdates.newSafeToRead != null)
            super.setSafeToRead(fieldUpdates.newSafeToRead);

        if (fieldUpdates.newRangesForEpoch != null)
            super.setRangesForEpoch(ranges);
    }

    public static class FieldUpdates
    {
        public RedundantBefore addRedundantBefore, newRedundantBefore;
        public NavigableMap<TxnId, Ranges> newBootstrapBeganAt;
        public NavigableMap<Timestamp, Ranges> newSafeToRead;
        public RangesForEpoch.Snapshot newRangesForEpoch;
    }
}