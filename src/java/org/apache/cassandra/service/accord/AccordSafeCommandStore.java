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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import accord.impl.CommandsSummary;
import accord.impl.SafeTimestampsForKey;
import accord.local.CommandStores;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.KeyHistory;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;

import static accord.local.KeyHistory.TIMESTAMPS;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;

public class AccordSafeCommandStore extends SafeCommandStore
{
    final AccordTask<?> task;
    final PreLoadContext context;
    private final @Nullable CommandsForRanges commandsForRanges;
    private final AccordCommandStore commandStore;
    private RangesForEpoch ranges;
    private FieldUpdates fieldUpdates;

    private AccordSafeCommandStore(AccordTask<?> task,
                                   @Nullable CommandsForRanges commandsForRanges,
                                   AccordCommandStore commandStore)
    {
        this.context = task.preLoadContext();
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

    @Override
    public PreLoadContext canExecute(PreLoadContext context)
    {
        if (context.isEmpty()) return context;
        if (context.keys().domain() == Routable.Domain.Range)
            return context.isSubsetOf(this.context) ? context : null;

        try (ExclusiveCaches caches = commandStore.tryLockCaches())
        {
            if (caches == null)
                return context.isSubsetOf(this.context) ? context : null;

            for (TxnId txnId : context.txnIds())
            {
                if (null != getInternal(txnId))
                    continue;

                AccordSafeCommand safeCommand = caches.commands().acquireIfLoaded(txnId);
                if (safeCommand == null)
                    return null;

                add(safeCommand, caches);
            }

            KeyHistory keyHistory = context.keyHistory();
            if (keyHistory == KeyHistory.NONE)
                return context;

            List<RoutingKey> unavailable = null;
            Unseekables<?> keys = context.keys();
            if (keys.size() == 0)
                return context;

            for (int i = 0 ; i < keys.size() ; ++i)
            {
                RoutingKey key = (RoutingKey) keys.get(i);
                if (keyHistory == TIMESTAMPS)
                {
                    if (null != timestampsForKeyInternal(key))
                        continue; // already in working set

                    AccordSafeTimestampsForKey safeTfk = caches.timestampsForKeys().acquireIfLoaded(key);
                    if (safeTfk != null)
                    {
                        add(safeTfk, caches);
                        continue;
                    }
                }
                else
                {
                    if (null != getInternal(key))
                        continue; // already in working set

                    AccordSafeCommandsForKey safeCfk = caches.commandsForKeys().acquireIfLoaded(key);
                    if (safeCfk != null)
                    {
                        add(safeCfk, caches);
                        continue;
                    }
                }
                if (unavailable == null)
                    unavailable = new ArrayList<>();
                unavailable.add(key);
            }

            if (unavailable == null)
                return context;

            if (unavailable.size() == keys.size())
                return null;

            return PreLoadContext.contextFor(context.primaryTxnId(), context.additionalTxnId(), keys.without(RoutingKeys.ofSortedUnique(unavailable)), keyHistory);
        }
    }

    @Override
    public PreLoadContext context()
    {
        return context;
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
    protected AccordSafeCommand ifLoadedAndInitialisedAndNotErasedInternal(TxnId txnId)
    {
        try (ExclusiveCaches caches = commandStore.tryLockCaches())
        {
            if (caches == null)
                return null;

            AccordSafeCommand command = caches.commands().acquireIfLoaded(txnId);
            if (command == null)
                return null;

            return add(command, caches);
        }
    }

    private AccordSafeCommand add(AccordSafeCommand safeCommand, ExclusiveCaches caches)
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

    private AccordSafeCommandsForKey add(AccordSafeCommandsForKey safeCfk, ExclusiveCaches caches)
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

    private AccordSafeTimestampsForKey add(AccordSafeTimestampsForKey safeTfk, ExclusiveCaches caches)
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

    @Override
    protected AccordSafeCommandsForKey ifLoadedInternal(RoutingKey key)
    {
        try (ExclusiveCaches caches = commandStore.tryLockCaches())
        {
            if (caches == null)
                return null;

            AccordSafeCommandsForKey safeCfk = caches.commandsForKeys().acquireIfLoaded(key);
            if (safeCfk == null)
                return null;

            Object check = task.ensureCommandsForKey().putIfAbsent(safeCfk.key(), safeCfk);
            if (check == null)
            {
                safeCfk.preExecute();
                return safeCfk;
            }
            else
            {
                caches.commandsForKeys().release(safeCfk, task);
                throw illegalState("Attempted to take a duplicate reference to CFK for %s", key);
            }
        }
    }

    @Override
    public SafeTimestampsForKey timestampsForKey(RoutingKey key)
    {
        AccordSafeTimestampsForKey safeTfk = timestampsForKeyInternal(key);
        if (safeTfk == null)
            throw illegalArgument("%s not referenced in %s", key, context);
        return safeTfk;
    }

    private AccordSafeTimestampsForKey timestampsForKeyInternal(RoutingKey key)
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
        accumulate = mapReduceForRange(keysOrRanges, map, accumulate);
        return mapReduceForKey(keysOrRanges, map, accumulate);
    }

    private <O> O mapReduceForRange(Routables<?> keysOrRanges, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        if (commandsForRanges == null)
            return accumulate;

        switch (keysOrRanges.domain())
        {
            case Key:
            {
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                if (!commandsForRanges.ranges.intersects(keys))
                    return accumulate;
            }
            break;
            case Range:
            {
                AbstractRanges ranges = (AbstractRanges) keysOrRanges;
                if (!commandsForRanges.ranges.intersects(ranges))
                    return accumulate;
            }
            break;
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
        }
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
            return summary.mapReduceActive(withLowerTxnId, testKind, map, p1, in);
        }, accumulate);
    }

    @Override
    public <P1, T> T mapReduceFull(Unseekables<?> keysOrRanges, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, (summary, in) -> {
            return summary.mapReduceFull(testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, in);
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