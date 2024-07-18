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
import java.util.NavigableMap;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.AbstractSafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.impl.CommandsSummary;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeTimestampsForKey, AccordSafeCommandsForKey>
{
    private final Map<TxnId, AccordSafeCommand> commands;
    private final NavigableMap<Key, AccordSafeCommandsForKey> commandsForKeys;
    private final NavigableMap<Key, AccordSafeTimestampsForKey> timestampsForKeys;
    private final @Nullable AccordSafeCommandsForRanges commandsForRanges;
    private final AccordCommandStore commandStore;
    private final RangesForEpoch ranges;

    private AccordSafeCommandStore(PreLoadContext context,
                                   Map<TxnId, AccordSafeCommand> commands,
                                   NavigableMap<Key, AccordSafeTimestampsForKey> timestampsForKey,
                                   NavigableMap<Key, AccordSafeCommandsForKey> commandsForKey,
                                   @Nullable AccordSafeCommandsForRanges commandsForRanges,
                                   AccordCommandStore commandStore)
    {
        super(context);
        this.commands = commands;
        this.timestampsForKeys = timestampsForKey;
        this.commandsForKeys = commandsForKey;
        this.commandsForRanges = commandsForRanges;
        this.commandStore = commandStore;
        this.ranges = commandStore.updateRangesForEpoch();
    }

    public static AccordSafeCommandStore create(PreLoadContext preLoadContext,
                                                Map<TxnId, AccordSafeCommand> commands,
                                                NavigableMap<Key, AccordSafeTimestampsForKey> timestampsForKey,
                                                NavigableMap<Key, AccordSafeCommandsForKey> commandsForKey,
                                                @Nullable AccordSafeCommandsForRanges commandsForRanges,
                                                AccordCommandStore commandStore)
    {
        return new AccordSafeCommandStore(preLoadContext, commands, timestampsForKey, commandsForKey, commandsForRanges, commandStore);
    }

    @Override
    protected AccordSafeCommand getCommandInternal(TxnId txnId)
    {
        return commands.get(txnId);
    }

    @Override
    protected void addCommandInternal(AccordSafeCommand command)
    {
        commands.put(command.txnId(), command);
    }

    @Override
    protected AccordSafeCommand getIfLoaded(TxnId txnId)
    {
        AccordSafeCommand command = commandStore.commandCache().acquireIfLoaded(txnId);
        if (command != null) command.preExecute();
        return command;
    }

    @Override
    protected AccordSafeCommandsForKey getCommandsForKeyInternal(Key key)
    {
        return commandsForKeys.get(key);
    }

    @Override
    protected void addCommandsForKeyInternal(AccordSafeCommandsForKey cfk)
    {
        commandsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeCommandsForKey getCommandsForKeyIfLoaded(Key key)
    {
        AccordSafeCommandsForKey cfk = commandStore.commandsForKeyCache().acquireIfLoaded(key);
        if (cfk != null) cfk.preExecute();
        return cfk;
    }

    @Override
    protected AccordSafeTimestampsForKey getTimestampsForKeyInternal(Key key)
    {
        return timestampsForKeys.get(key);
    }

    @Override
    protected void addTimestampsForKeyInternal(AccordSafeTimestampsForKey cfk)
    {
        timestampsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeTimestampsForKey getTimestampsForKeyIfLoaded(Key key)
    {
        AccordSafeTimestampsForKey cfk = commandStore.timestampsForKeyCache().acquireIfLoaded(key);
        if (cfk != null) cfk.preExecute();
        return cfk;
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
    public NodeTimeService time()
    {
        // TODO: safe command store should not have arbitrary time
        return commandStore.time();
    }

    @Override
    public RangesForEpoch ranges()
    {
        return commandStore().unsafeRangesForEpoch();
    }

    @Override
    public void registerHistoricalTransactions(Deps deps)
    {
        // used in places such as accord.local.CommandStore.fetchMajorityDeps
        // We find a set of dependencies for a range then update CommandsFor to know about them
        Ranges allRanges = ranges.all();
        deps.keyDeps.keys().forEach(allRanges, key -> {
            // TODO (now): batch register to minimise GC
            deps.keyDeps.forEach(key, (txnId, txnIdx) -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).contains(key))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).contains(key))
                    return;

                get(key).registerHistorical(this, txnId);
            });
        });
        for (int i = 0; i < deps.rangeDeps.rangeCount(); i++)
        {
            var range = deps.rangeDeps.range(i);
            if (!allRanges.intersects(range))
                continue;
            deps.rangeDeps.forEach(range, txnId -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).intersects(range))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).intersects(range))
                    return;

                commandStore.diskCommandsForRanges().mergeHistoricalTransaction(txnId, Ranges.single(range).slice(allRanges), Ranges::with);
            });
        }
    }

    private <O> O mapReduce(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        accumulate = mapReduceForRange(keysOrRanges, slice, map, accumulate);
        return mapReduceForKey(keysOrRanges, slice, map, accumulate);
    }

    private <O> O mapReduceForRange(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        if (commandsForRanges == null)
            return accumulate;
        switch (keysOrRanges.domain())
        {
            case Key:
            {
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges.slice(slice, Routables.Slice.Minimal);
                if (!commandsForRanges.ranges().intersects(keys))
                    return accumulate;
                accumulate = map.apply(commandsForRanges.current(), accumulate);
            }
            break;
            case Range:
            {
                AbstractRanges ranges = (AbstractRanges) keysOrRanges.slice(slice, Routables.Slice.Minimal);
                if (!commandsForRanges.ranges().intersects(ranges))
                    return accumulate;
                accumulate = map.apply(commandsForRanges.current(), accumulate);
            }
            break;
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
        }
        return accumulate;
    }

    private <O> O mapReduceForKey(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        switch (keysOrRanges.domain())
        {
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
            case Key:
            {
                // TODO: efficiency
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    CommandsForKey commands = get(key).current();
                    accumulate = map.apply(commands, accumulate);
                }
            }
            break;
            case Range:
            {
                // Assuming the range provided is in the PreLoadContext, then AsyncLoader has populated commandsForKeys with keys that
                // are contained within the ranges... so walk all keys found in commandsForKeys
                Routables<?> sliced = keysOrRanges.slice(slice, Routables.Slice.Minimal);
                if (!context.keys().slice(slice, Routables.Slice.Minimal).containsAll(sliced))
                    throw new AssertionError("Range(s) detected not present in the PreLoadContext: expected " + context.keys() + " but given " + keysOrRanges);
                for (Key key : commandsForKeys.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    if (!sliced.contains(key)) continue;
                    CommandsForKey commands = get(key).current();
                    accumulate = map.apply(commands, accumulate);
                }
            }
            break;
        }
        return accumulate;
    }

    @Override
    public <P1, T> T mapReduceActive(Seekables<?, ?> keysOrRanges, Ranges slice, @Nullable Timestamp withLowerTxnId, Txn.Kind.Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, slice, (summary, in) -> {
            return summary.mapReduceActive(withLowerTxnId, testKind, map, p1, in);
        }, accumulate);
    }

    @Override
    public <P1, T> T mapReduceFull(Seekables<?, ?> keysOrRanges, Ranges slice, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, slice, (summary, in) -> {
            return summary.mapReduceFull(testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, in);
        }, accumulate);
    }

    @Override
    public String toString()
    {
        return "AccordSafeCommandStore(id=" + commandStore().id() + ")";
    }
}
