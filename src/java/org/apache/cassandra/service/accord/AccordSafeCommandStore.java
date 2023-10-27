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
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeys;
import accord.impl.DomainCommands;
import accord.impl.DomainTimestamps;
import accord.impl.SafeTimestampsForKey;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommonAttributes;
import accord.local.KeyHistory;
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
import accord.utils.TriFunction;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeTimestampsForKey, AccordSafeCommandsForKey, AccordSafeCommandsForKeyUpdate>
{
    private final Map<TxnId, AccordSafeCommand> commands;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKey> depsCommandsForKeys;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKey> allCommandsForKeys;
    private final NavigableMap<RoutableKey, AccordSafeTimestampsForKey> timestampsForKeys;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKeyUpdate> updatesForKeys;
    private final AccordCommandStore commandStore;
    private final RangesForEpoch ranges;
    CommandsForRanges.Updater rangeUpdates = null;

    public AccordSafeCommandStore(PreLoadContext context,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  NavigableMap<RoutableKey, AccordSafeTimestampsForKey> timestampsForKey,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKey> depsCommandsForKey,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKey> allCommandsForKeys,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKeyUpdate> updatesForKeys,
                                  AccordCommandStore commandStore)
    {
        super(context);
        this.commands = commands;
        this.timestampsForKeys = timestampsForKey;
        this.depsCommandsForKeys = depsCommandsForKey;
        this.allCommandsForKeys = allCommandsForKeys;
        this.updatesForKeys = updatesForKeys;
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

    private NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeyMap(KeyHistory history)
    {
        switch (history)
        {
            case DEPS:
                return depsCommandsForKeys;
            case ALL:
                return allCommandsForKeys;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    protected AccordSafeCommandsForKey getDepsCommandsForKeyInternal(RoutableKey key)
    {
        return depsCommandsForKeys.get(key);
    }

    @Override
    protected void addDepsCommandsForKeyInternal(AccordSafeCommandsForKey cfk)
    {
        depsCommandsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeCommandsForKey getDepsCommandsForKeyIfLoaded(RoutableKey key)
    {
        AccordSafeCommandsForKey cfk = commandStore.depsCommandsForKeyCache().acquireIfLoaded(key);
        if (cfk != null) cfk.preExecute();
        return cfk;
    }

    @Override
    protected AccordSafeCommandsForKey getAllCommandsForKeyInternal(RoutableKey key)
    {
        return allCommandsForKeys.get(key);
    }

    @Override
    protected void addAllCommandsForKeyInternal(AccordSafeCommandsForKey cfk)
    {
        allCommandsForKeys.put(cfk.key(), cfk);
    }

    @Override
    protected AccordSafeCommandsForKey getAllCommandsForKeyIfLoaded(RoutableKey key)
    {
        AccordSafeCommandsForKey cfk = commandStore.allCommandsForKeyCache().acquireIfLoaded(key);
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

    protected AccordSafeCommandsForKeyUpdate getCommandsForKeyUpdateInternal(RoutableKey key)
    {
        return updatesForKeys.get(key);
    }

    protected AccordSafeCommandsForKeyUpdate createCommandsForKeyUpdateInternal(RoutableKey key)
    {
        throw new IllegalStateException("CFK updates should be initialized for operation");
    }

    protected void addCommandsForKeyUpdateInternal(AccordSafeCommandsForKeyUpdate update)
    {
        updatesForKeys.put(update.key(), update);
    }

    protected void applyCommandForKeyUpdates()
    {
        // TODO (now): should this happen as part of invalidate? Less obvious it's happening, but eliminates possibility of post update changes
        updatesForKeys.values().forEach(AccordSafeCommandsForKeyUpdate::setUpdates);
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
        Timestamp maxConflict = mapReduce(keysOrRanges, slice, KeyHistory.NONE, (ts, commands, accum) -> Timestamp.max(ts.max(), accum), Timestamp.NONE, Predicates.isNull());
        return Timestamp.nonNullOrMax(maxConflict, commandStore.commandsForRanges().maxRedundant());
    }

    @Override
    public void registerHistoricalTransactions(Deps deps)
    {
        // used in places such as accord.local.CommandStore.fetchMajorityDeps
        // We find a set of dependencies for a range then update CommandsFor to know about them
        Ranges allRanges = ranges.all();
        deps.keyDeps.keys().forEach(allRanges, key -> {
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

    @Override
    public void erase(SafeCommand safeCommand)
    {
    }

    private <O> O mapReduce(Routables<?> keysOrRanges, Ranges slice, KeyHistory keyHistory, TriFunction<DomainTimestamps, DomainCommands, O, O> map, O accumulate, Predicate<? super O> terminate)
    {
        accumulate = commandStore.mapReduceForRange(keysOrRanges, slice, map, accumulate, terminate);
        if (terminate.test(accumulate))
            return accumulate;
        return mapReduceForKey(keysOrRanges, slice, keyHistory, map, accumulate, terminate);
    }

    private <O> O mapReduceForKey(Routables<?> keysOrRanges, Ranges slice, KeyHistory keyHistory, TriFunction<DomainTimestamps, DomainCommands, O, O> map, O accumulate, Predicate<? super O> terminate)
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
                    SafeTimestampsForKey timestamps = timestampsForKey(key);
                    CommandsForKey commands = !keyHistory.isNone() ? commandsForKey(key, keyHistory).current() : null;
                    accumulate = map.apply(timestamps.current(), commands, accumulate);
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
                for (RoutableKey key : timestampsForKeys.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    if (!sliced.contains(key)) continue;
                    SafeTimestampsForKey timestamps = timestampsForKey(key);
                    CommandsForKey commands = !keyHistory.isNone() ? commandsForKey(key, keyHistory).current() : null;
                    accumulate = map.apply(timestamps.current(), commands, accumulate);
                    if (terminate.test(accumulate))
                        return accumulate;
                }
            }
            break;
        }
        return accumulate;
    }

    @Override
//<<<<<<< HEAD
//    public <P1, T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, Txn.Kind.Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate, Predicate<? super T> terminate)   {
//        accumulate = mapReduce(keysOrRanges, slice, (forKey, prev) -> {
//            CommandTimeseries<?> timeseries;
//=======
    public <P1, T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, KeyHistory keyHistory, Txn.Kind.Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate, Predicate<? super T> terminate)
    {
        accumulate = mapReduce(keysOrRanges, slice, keyHistory, (timestamps, commands, prev) -> {
            CommandTimeseries.TimestampType timestampType;
            switch (testTimestamp)
            {
                default: throw new AssertionError();
                case STARTED_AFTER:
                case STARTED_BEFORE:
                    timestampType = CommandTimeseries.TimestampType.TXN_ID;
                    break;
                case EXECUTES_AFTER:
                case MAY_EXECUTE_BEFORE:
                    timestampType = CommandTimeseries.TimestampType.EXECUTE_AT;
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
            return commands.commands().mapReduce(testKind, timestampType, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, p1, prev, terminate);
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
                    CommandsForKeys.registerCommand(this, key, liveCommand.current());
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
        timestampsForKeys.values().forEach(AccordSafeTimestampsForKey::invalidate);
        depsCommandsForKeys.values().forEach(AccordSafeCommandsForKey::invalidate);
        allCommandsForKeys.values().forEach(AccordSafeCommandsForKey::invalidate);
        updatesForKeys.values().forEach(AccordSafeCommandsForKeyUpdate::invalidate);
    }

    @Override
    public CommandLoader<?> cfkLoader(RoutableKey key)
    {
        return CommandsForKeySerializer.loader;
    }

    public void postExecute(Map<TxnId, AccordSafeCommand> commands,
                            Map<RoutableKey, AccordSafeTimestampsForKey> timestampsForKey,
                            Map<RoutableKey, AccordSafeCommandsForKey> depsCommandsForKeys,
                            Map<RoutableKey, AccordSafeCommandsForKey> allCommandsForKeys,
                            Map<RoutableKey, AccordSafeCommandsForKeyUpdate> updatesForKeys
                            )
    {
        postExecute();
        commands.values().forEach(AccordSafeState::postExecute);
        timestampsForKey.values().forEach(AccordSafeState::postExecute);
        depsCommandsForKeys.values().forEach(AccordSafeState::postExecute);
        allCommandsForKeys.values().forEach(AccordSafeState::postExecute);
        updatesForKeys.values().forEach(AccordSafeState::postExecute);
        if (rangeUpdates != null)
            rangeUpdates.apply();
    }
}
