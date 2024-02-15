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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.AbstractSafeCommandStore;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeys;
import accord.impl.CommandsSummary;
import accord.local.Command;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.primitives.AbstractKeys;
import accord.primitives.Deps;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import static accord.primitives.Routable.Domain.Range;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeTimestampsForKey, AccordSafeCommandsForKey>
{
    private final Map<TxnId, AccordSafeCommand> commands;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeys;
    private final NavigableMap<RoutableKey, AccordSafeTimestampsForKey> timestampsForKeys;
    private final AccordCommandStore commandStore;
    private final RangesForEpoch ranges;
    CommandsForRanges.Updater rangeUpdates = null;

    public AccordSafeCommandStore(PreLoadContext context,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  NavigableMap<RoutableKey, AccordSafeTimestampsForKey> timestampsForKey,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKey,
                                  AccordCommandStore commandStore)
    {
        super(context);
        this.commands = commands;
        this.timestampsForKeys = timestampsForKey;
        this.commandsForKeys = commandsForKey;
        this.commandStore = commandStore;
        this.ranges = commandStore.updateRangesForEpoch();
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
    protected AccordSafeCommandsForKey getCommandsForKeyInternal(RoutableKey key)
    {
        return commandsForKeys.get(key);
    }

    @Override
    protected void addCommandsForKeyInternal(AccordSafeCommandsForKey cfk)
    {
        commandsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeCommandsForKey getCommandsForKeyIfLoaded(RoutableKey key)
    {
        AccordSafeCommandsForKey cfk = commandStore.commandsForKeyCache().acquireIfLoaded(key);
        if (cfk != null) cfk.preExecute();
        return cfk;
    }

    @Override
    protected AccordSafeTimestampsForKey getTimestampsForKeyInternal(RoutableKey key)
    {
        return timestampsForKeys.get(key);
    }

    @Override
    protected void addTimestampsForKeyInternal(AccordSafeTimestampsForKey cfk)
    {
        timestampsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeTimestampsForKey getTimestampsForKeyIfLoaded(RoutableKey key)
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
            deps.keyDeps.forEach(key, txnId -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).contains(key))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).contains(key))
                    return;

                CommandsForKeys.registerNotWitnessed(this, key, txnId);
            });
        });
        CommandsForRanges commandsForRanges = commandStore.commandsForRanges();
        deps.rangeDeps.forEachUniqueTxnId(allRanges, txnId -> {
            if (commandsForRanges.containsLocally(txnId))
                return;

            Ranges ranges = deps.rangeDeps.ranges(txnId);
            if (this.ranges.coordinates(txnId).intersects(ranges))
                return; // already coordinates, no need to replicate
            if (!this.ranges.allBefore(txnId.epoch()).intersects(ranges))
                return;

            updateRanges().mergeRemote(txnId, ranges.slice(allRanges), Ranges::with);
        });
    }

    private <O> O mapReduce(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandsSummary, O, O> map, O accumulate)
    {
        accumulate = commandStore.mapReduceForRange(keysOrRanges, slice, map, accumulate);
        return mapReduceForKey(keysOrRanges, slice, map, accumulate);
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
                    CommandsForKey commands = commandsForKey(key).current();
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
                for (RoutableKey key : timestampsForKeys.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    if (!sliced.contains(key)) continue;
                    CommandsForKey commands = commandsForKey(key).current();
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
    protected void update(Command prev, Command updated, @Nullable Seekables<?, ?> keysOrRanges)
    {
        super.update(prev, updated, keysOrRanges);

        if (updated.txnId().domain() == Range && CommandsForKey.needsUpdate(prev, updated))
        {
            if (keysOrRanges == null)
            {
                if (updated.known().isDefinitionKnown()) keysOrRanges = updated.partialTxn().keys();
                else if (prev.known().isDefinitionKnown()) keysOrRanges = prev.partialTxn().keys();
                else return;
            }
            List<TxnId> waitingOn;

            if (updated.partialDeps() == null) waitingOn = Collections.emptyList();
            // TODO (required): this is faulty: we cannot simply save the raw transaction ids, as they may be for other ranges
            else waitingOn = updated.partialDeps().txnIds();
            updateRanges().put(updated.txnId(), (Ranges)keysOrRanges, updated.saveStatus(), updated.executeAt(), waitingOn);
        }
    }

    protected CommandsForRanges.Updater updateRanges()
    {
        if (rangeUpdates == null)
            rangeUpdates = commandStore.updateRanges();
        return rangeUpdates;
    }

    @Override
    protected void invalidateSafeState()
    {
        commands.values().forEach(AccordSafeCommand::invalidate);
        timestampsForKeys.values().forEach(AccordSafeTimestampsForKey::invalidate);
        commandsForKeys.values().forEach(AccordSafeCommandsForKey::invalidate);
    }

    public void postExecute(Map<TxnId, AccordSafeCommand> commands,
                            Map<RoutableKey, AccordSafeTimestampsForKey> timestampsForKey,
                            Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys
                            )
    {
        postExecute();
        commands.values().forEach(AccordSafeState::postExecute);
        timestampsForKey.values().forEach(AccordSafeState::postExecute);
        commandsForKeys.values().forEach(AccordSafeState::postExecute);
        if (rangeUpdates != null)
            rangeUpdates.apply();
    }
}
