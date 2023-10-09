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
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.AbstractSafeCommandStore;
import accord.impl.CommandTimeseries;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.impl.CommandTimeseriesHolder;
import accord.impl.CommandsForKey;
import accord.impl.SafeCommandsForKey;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommonAttributes;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.Deps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeCommandsForKey>
{
    private final Map<TxnId, AccordSafeCommand> commands;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeys;
    private final AccordCommandStore commandStore;
    private final RangesForEpoch ranges;
    CommandsForRanges.Updater rangeUpdates = null;

    public AccordSafeCommandStore(PreLoadContext context,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKey,
                                  AccordCommandStore commandStore)
    {
        super(context);
        this.commands = commands;
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
    protected AccordSafeCommand getIfLoaded(TxnId txnId)
    {
        AccordSafeCommand command = commandStore.commandCache().acquireIfLoaded(txnId);
        if (command != null) command.preExecute();
        return command;
    }

    @Override
    protected AccordSafeCommandsForKey getIfLoaded(RoutableKey key)
    {
        AccordSafeCommandsForKey cfk = commandStore.commandsForKeyCache().acquireIfLoaded(key);
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
    public long latestEpoch()
    {
        return commandStore().time().epoch();
    }

    @Override
    public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        Timestamp maxConflict = mapReduce(keysOrRanges, slice, (ts, accum) -> Timestamp.max(ts.max(), accum), Timestamp.NONE, Predicates.isNull());
        return Timestamp.nonNullOrMax(maxConflict, commandStore.commandsForRanges().maxRedundant());
    }

    @Override
    public void registerHistoricalTransactions(Deps deps)
    {
        // used in places such as accord.local.CommandStore.fetchMajorityDeps
        // We find a set of dependencies for a range then update CommandsFor to know about them
        Ranges allRanges = ranges.all();
        deps.keyDeps.keys().forEach(allRanges, key -> {
            SafeCommandsForKey cfk = commandsForKey(key);
            deps.keyDeps.forEach(key, txnId -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (ranges.coordinates(txnId).contains(key))
                    return; // already coordinates, no need to replicate
                if (!ranges.allBefore(txnId.epoch()).contains(key))
                    return;

                cfk.registerNotWitnessed(txnId);
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

    @Override
    public void erase(SafeCommand safeCommand)
    {
    }

    private <O> O mapReduce(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, Predicate<? super O> terminate)
    {
        accumulate = commandStore.mapReduceForRange(keysOrRanges, slice, map, accumulate, terminate);
        if (terminate.test(accumulate))
            return accumulate;
        return mapReduceForKey(keysOrRanges, slice, map, accumulate, terminate);
    }

    private <O> O mapReduceForKey(Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, Predicate<? super O> terminate)
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
                    SafeCommandsForKey forKey = commandsForKey(key);
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (terminate.test((accumulate)))
                        return accumulate;
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
                for (RoutableKey key : commandsForKeys.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    if (!sliced.contains(key)) continue;
                    SafeCommandsForKey forKey = commandsForKey(key);
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (terminate.test(accumulate))
                        return accumulate;
                }
            }
            break;
        }
        return accumulate;
    }

    @Override
    public <P1, T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, Txn.Kind.Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate, Predicate<? super T> terminate)   {
        accumulate = mapReduce(keysOrRanges, slice, (forKey, prev) -> {
            CommandTimeseries<?> timeseries;
            switch (testTimestamp)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                case STARTED_BEFORE:
                    timeseries = forKey.byId();
                    break;
                case EXECUTES_AFTER:
                case MAY_EXECUTE_BEFORE:
                    timeseries = forKey.byExecuteAt();
            }
            CommandTimeseries.TestTimestamp remapTestTimestamp;
            switch (testTimestamp)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                case EXECUTES_AFTER:
                    remapTestTimestamp = CommandTimeseries.TestTimestamp.AFTER;
                    break;
                case STARTED_BEFORE:
                case MAY_EXECUTE_BEFORE:
                    remapTestTimestamp = CommandTimeseries.TestTimestamp.BEFORE;
            }
            return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, p1, prev, terminate);
        }, accumulate, terminate);

        return accumulate;
    }

    @Override
    public CommonAttributes completeRegistration(Seekables<?, ?> seekables, Ranges ranges, AccordSafeCommand liveCommand, CommonAttributes attrs)
    {
        for (Seekable seekable : seekables)
            attrs = completeRegistration(seekable, ranges, liveCommand, attrs);
        return attrs;
    }

    @Override
    public CommonAttributes completeRegistration(Seekable seekable, Ranges ranges, AccordSafeCommand liveCommand, CommonAttributes attrs)
    {
        switch (seekable.domain())
        {
            case Key:
            {
                Key key = seekable.asKey();
                if (ranges.contains(key))
                {
                    AccordSafeCommandsForKey cfk = commandsForKey(key);
                    cfk.register(liveCommand.current());
                    attrs = attrs.mutable().addListener(new CommandsForKey.Listener(key));
                }
            }
            break;
            case Range:
                Range range = seekable.asRange();
                if (!ranges.intersects(range))
                    return attrs;
                // TODO (api) : cleaner way to deal with this?  This is tracked at the Ranges level and not Range level
                // but we register at the Range level...
                if (!attrs.durableListeners().stream().anyMatch(l -> l instanceof CommandsForRanges.Listener))
                {
                    CommandsForRanges.Listener listener = new CommandsForRanges.Listener(liveCommand.txnId());
                    attrs = attrs.mutable().addListener(listener);
                    // trigger to allow it to run right away
                    listener.onChange(this, liveCommand);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown domain: " + seekable.domain());
        }
        return attrs;
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
        commandsForKeys.values().forEach(AccordSafeCommandsForKey::invalidate);
    }

    @Override
    public CommandLoader<?> cfkLoader(RoutableKey key)
    {
        return CommandsForKeySerializer.loader;
    }

    public void postExecute(Map<TxnId, AccordSafeCommand> commands,
                            Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        postExecute();
        commands.values().forEach(AccordSafeState::postExecute);
        commandsForKeys.values().forEach(AccordSafeState::postExecute);
        if (rangeUpdates != null)
            rangeUpdates.apply();
    }
}
