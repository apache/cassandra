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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

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
import accord.local.Command;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommonAttributes;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.AbstractKeys;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class AccordSafeCommandStore extends AbstractSafeCommandStore<AccordSafeCommand, AccordSafeCommandsForKey>
{
    private final Map<TxnId, AccordSafeCommand> commands;
    private final NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKeys;
    private final AccordCommandStore commandStore;
    private IntervalTree.Builder<RoutableKey, AccordCommandStore.RangeCommandSummary, Interval<RoutableKey, AccordCommandStore.RangeCommandSummary>> builder = null;

    public AccordSafeCommandStore(PreLoadContext context,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  NavigableMap<RoutableKey, AccordSafeCommandsForKey> commandsForKey,
                                  AccordCommandStore commandStore)
    {
        super(context);
        this.commands = commands;
        this.commandsForKeys = commandsForKey;
        this.commandStore = commandStore;
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
        return commandStore.commandCache().referenceAndGetIfLoaded(txnId);
    }

    @Override
    protected AccordSafeCommandsForKey getIfLoaded(RoutableKey key)
    {
        return commandStore.commandsForKeyCache().referenceAndGetIfLoaded(key);
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
        return commandStore().ranges();
    }

    @Override
    public long latestEpoch()
    {
        return commandStore().time().epoch();
    }

    @Override
    public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        // TODO: Seekables
        // TODO: efficiency
        return ((Keys)keysOrRanges).stream()
                           .map(this::maybeCommandsForKey)
                           .filter(Objects::nonNull)
                           .map(SafeCommandsForKey::current)
                           .filter(Objects::nonNull)
                           .map(CommandsForKey::max)
                           .max(Comparator.naturalOrder())
                           .orElse(Timestamp.NONE);
    }

    private <O> O mapReduce(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, O terminalValue)
    {
        accumulate = commandStore.mapReduceForRange(keysOrRanges, slice, map, accumulate, terminalValue);
        if (accumulate.equals(terminalValue))
            return accumulate;
        return mapReduceForKey(keysOrRanges, slice, map, accumulate, terminalValue);
    }

    private <O> O mapReduceForKey(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandTimeseriesHolder, O, O> map, O accumulate, O terminalValue)
    {
        switch (keysOrRanges.domain())
        {
            default:
                throw new AssertionError("Unknown domain: " + keysOrRanges.domain());
            case Key:
            {
                // TODO: efficiency
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    SafeCommandsForKey forKey = commandsForKey(key);
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
            }
            break;
            case Range:
            {
                // TODO (correctness): if a range is used, then the range must exist in the PreLoadContext, else the commandsForKeys won't be in-sync... can this be detected?
                // Assuming the range provided is in the PreLoadContext, then AsyncLoader has populated commandsForKeys with keys that
                // are contained within the ranges... so walk all keys found in commandsForKeys
                keysOrRanges = keysOrRanges.slice(slice, Routables.Slice.Minimal);
                for (RoutableKey key : commandsForKeys.keySet())
                {
                    //TODO (duplicate code): this is a repeat of Key... only change is checking contains in range
                    if (!keysOrRanges.contains(key)) continue;
                    SafeCommandsForKey forKey = commandsForKey(key);
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
            }
            break;
        }
        return accumulate;
    }

    @Override
    public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
    {
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
            return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
        }, accumulate, terminalValue);

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
                Command current = liveCommand.current();
                if (current.saveStatus() == SaveStatus.NotWitnessed)
                    return attrs; // don't know the range/dependencies, so can't cache
                ranges = ranges.slice(Ranges.single(seekable.asRange()), Routables.Slice.Minimal);
                if (ranges.isEmpty())
                    return attrs;
                PartialDeps deps = current.partialDeps();
                List<TxnId> dependsOn = deps == null ? Collections.emptyList() : deps.txnIds();
                AccordCommandStore.RangeCommandSummary summary = new AccordCommandStore.RangeCommandSummary(liveCommand.txnId(), current.saveStatus(), current.executeAt(), dependsOn);
                if (builder == null)
                    builder = commandStore.unbuild();

                //TODO Interval is BETWEEN semantics, but Range tends not to be... fix this
                //TODO this does NOT update the value, but causes 2 elements to exist (history + update); need to purge history
                for (Range range : ranges)
                    builder.add(new Interval<>(range.start(), range.end(), summary));
            break;
            default:
                throw new UnsupportedOperationException("Unknown domain: " + seekable.domain());
        }
        return attrs;
    }

    @Override
    protected void invalidateSafeState()
    {
        commands.values().forEach(AccordSafeCommand::invalidate);
        commandsForKeys.values().forEach(AccordSafeCommandsForKey::invalidate);
    }

    @Override
    public CommandLoader<?> cfkLoader()
    {
        return CommandsForKeySerializer.loader;
    }

    public void postExecute(Map<TxnId, AccordSafeCommand> commands,
                            Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        postExecute();
        commands.values().forEach(AccordSafeState::postExecute);
        commandsForKeys.values().forEach(AccordSafeState::postExecute);
        if (builder != null)
            commandStore.updateRanges(builder.build());
    }
}
