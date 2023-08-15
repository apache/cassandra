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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.CommandTimeseries;
import accord.impl.CommandTimeseriesHolder;
import accord.local.Command;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.AbstractKeys;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class CommandsForRanges
{
    public enum TxnType
    {
        UNKNOWN, LOCAL, REMOTE;

        private boolean isSafeToMix(TxnType other)
        {
            if (this == UNKNOWN || other == UNKNOWN) return true;
            return this == other;
        }
    }

    public static final class RangeCommandSummary
    {
        public final TxnId txnId;
        public final Ranges ranges;
        public final SaveStatus status;
        public final @Nullable Timestamp executeAt;
        public final List<TxnId> deps;

        RangeCommandSummary(TxnId txnId, Ranges ranges, SaveStatus status, @Nullable Timestamp executeAt, List<TxnId> deps)
        {
            this.txnId = txnId;
            this.ranges = ranges;
            this.status = status;
            this.executeAt = executeAt;
            this.deps = deps;
        }

        public boolean equalsDeep(RangeCommandSummary other)
        {
            return Objects.equals(txnId, other.txnId)
                   && Objects.equals(ranges, other.ranges)
                   && status == other.status
                   && Objects.equals(executeAt, other.executeAt)
                   && Objects.equals(deps, other.deps);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RangeCommandSummary that = (RangeCommandSummary) o;
            return txnId.equals(that.txnId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(txnId);
        }

        @Override
        public String toString()
        {
            return "RangeCommandSummary{" +
                   "txnId=" + txnId +
                   ", status=" + status +
                   ", ranges=" + ranges +
                   '}';
        }

        public RangeCommandSummary withRanges(Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
        {
            return new RangeCommandSummary(txnId, remappingFunction.apply(this.ranges, ranges), status, executeAt, deps);
        }
    }

    private enum RangeCommandSummaryLoader implements CommandTimeseries.CommandLoader<RangeCommandSummary>
    {
        INSTANCE;

        @Override
        public RangeCommandSummary saveForCFK(Command command)
        {
            //TODO split write from read?
            throw new UnsupportedOperationException();
        }

        @Override
        public TxnId txnId(RangeCommandSummary data)
        {
            return data.txnId;
        }

        @Override
        public Timestamp executeAt(RangeCommandSummary data)
        {
            return data.executeAt;
        }

        @Override
        public SaveStatus saveStatus(RangeCommandSummary data)
        {
            return data.status;
        }

        @Override
        public List<TxnId> depsIds(RangeCommandSummary data)
        {
            return data.deps;
        }
    }

    public static abstract class AbstractBuilder<T extends AbstractBuilder<T>>
    {
        protected final Set<TxnId> localTxns = new HashSet<>();
        protected final TreeMap<TxnId, RangeCommandSummary> txnToRange = new TreeMap<>();
        protected final IntervalTree.Builder<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> rangeToTxn = new IntervalTree.Builder<>();

        public TxnType type(TxnId txnId)
        {
            if (!txnToRange.containsKey(txnId)) return TxnType.UNKNOWN;
            return localTxns.contains(txnId) ? TxnType.LOCAL : TxnType.REMOTE;
        }

        public T put(TxnId txnId, Ranges ranges, SaveStatus status, Timestamp execteAt, List<TxnId> dependsOn)
        {
            remove(txnId);
            put(new RangeCommandSummary(txnId, ranges, status, execteAt, dependsOn));
            //noinspection unchecked
            return (T) this;
        }

        private void put(RangeCommandSummary summary)
        {
            TxnId txnId = summary.txnId;
            localTxns.add(txnId);
            txnToRange.put(txnId, summary);
            addRanges(summary);
        }

        private void addRanges(RangeCommandSummary summary)
        {
            for (Range range : summary.ranges)
                rangeToTxn.add(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                               normalize(range.end(), range.endInclusive(), false),
                                               summary));
        }

        public T putAll(CommandsForRanges other)
        {
            for (TxnId id : other.localCommands)
            {
                TxnType thisType = type(id);
                TxnType otherType = other.type(id);
                Invariants.checkArgument(thisType.isSafeToMix(otherType), "Attempted to add %s; expected %s but was %s", id, thisType, otherType);
            }
            localTxns.addAll(other.localCommands);
            txnToRange.putAll(other.commandsToRanges);
            // If "put" was called before for a txn present in "other", to respect the "put" semantics that update must
            // be removed from "rangeToTxn" (as it got removed from "txnToRange").
            // The expected common case is that this method is called on an empty builder, so the removeIf is off an
            // empty list (aka no-op)
            rangeToTxn.removeIf(data -> other.commandsToRanges.containsKey(data.txnId));
            rangeToTxn.addAll(other.rangesToCommands);
            //noinspection unchecked
            return (T) this;
        }

        public T mergeRemote(TxnId txnId, Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
        {
            // TODO (durability) : remote ranges are not made durable for now.  If this command is stored in commands table,
            // then we have a NotWitnessed command with Ranges, which is not expected in accord.local.Command.NotWitnessed.
            // To properly handle this, the long term storage looks like it will need to store these as well.
            Invariants.checkArgument(!localTxns.contains(txnId), "Attempted to merge remote txn %s, but this is a local txn", txnId);
            // accord.impl.CommandTimeseries.mapReduce does the check on status and deps type, and NotWitnessed should match the semantics hard coded in InMemorySafeStore...
            // in that store, the remote history is only ever included when minStauts == null and deps == ANY... but mapReduce sees accord.local.Status.KnownDeps.hasProposedOrDecidedDeps == false
            // as a mis-match, so will be excluded... since NotWitnessed will return false it will only be included IFF deps = ANY.
            // When it comes to the minStatus check, the current usage is "null", "Committed", "Accepted"... so NotWitnessed will only be included in the null case;
            // the only subtle difference is if minStatus = NotWitnessed, this API will include these but InMemoryStore won't
            RangeCommandSummary oldValue = txnToRange.get(txnId);
            RangeCommandSummary newValue = oldValue == null ?
                                           new RangeCommandSummary(txnId, ranges, SaveStatus.NotDefined, null, Collections.emptyList())
                                           : oldValue.withRanges(ranges, remappingFunction);
            if (oldValue == null || !oldValue.equalsDeep(newValue))
            {
                // changes detected... have to update range index
                rangeToTxn.removeIf(data -> data.txnId.equals(txnId));
                addRanges(newValue);
            }
            //noinspection unchecked
            return (T) this;
        }

        public T remove(TxnId txnId)
        {
            if (txnToRange.containsKey(txnId))
            {
                localTxns.remove(txnId);
                txnToRange.remove(txnId);
                rangeToTxn.removeIf(data -> data.txnId.equals(txnId));
            }
            //noinspection unchecked
            return (T) this;
        }

        public T map(Function<? super RangeCommandSummary, ? extends RangeCommandSummary> mapper)
        {
            for (TxnId id : new TreeSet<>(txnToRange.keySet()))
            {
                RangeCommandSummary summary = txnToRange.get(id);
                RangeCommandSummary update = mapper.apply(summary);
                if (summary.equals(update))
                    continue;
                remove(summary.txnId);
                if (update != null)
                    put(update);
            }
            //noinspection unchecked
            return (T) this;
        }
    }

    public static class Builder extends AbstractBuilder<Builder>
    {
        public CommandsForRanges build()
        {
            CommandsForRanges cfr = new CommandsForRanges();
            cfr.set(this);
            return cfr;
        }
    }

    public class Updater extends AbstractBuilder<Updater>
    {
        private Updater()
        {
            putAll(CommandsForRanges.this);
        }

        public void apply()
        {
            CommandsForRanges.this.set(this);
        }
    }

    public static class Listener implements Command.DurableAndIdempotentListener
    {
        public final TxnId txnId;
        private transient SaveStatus saveStatus;

        public Listener(TxnId txnId)
        {
            this.txnId = txnId;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Command current = safeCommand.current();
            if (current.saveStatus() == saveStatus)
                return;
            saveStatus = current.saveStatus();
            PartialDeps deps = current.partialDeps();
            if (deps == null)
                return;
            Seekables<?, ?> keysOrRanges = current.partialTxn().keys();
            Invariants.checkArgument(keysOrRanges.domain() == Routable.Domain.Range, "Expected txn %s to be a Range txn, but was a %s", txnId, keysOrRanges.domain());

            List<TxnId> dependsOn = deps.txnIds();
            ((AccordSafeCommandStore) safeStore).updateRanges()
                                                .put(txnId, (Ranges) keysOrRanges, current.saveStatus(), current.executeAt(), dependsOn);
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return caller.equals(txnId) ? PreLoadContext.contextFor(txnId) : PreLoadContext.contextFor(txnId, Collections.singletonList(caller));
        }

        @Override
        public String toString()
        {
            return "Listener{" +
                   "txnId=" + txnId +
                   ", saveStatus=" + saveStatus +
                   '}';
        }
    }

    private ImmutableSet<TxnId> localCommands;
    private ImmutableSortedMap<TxnId, RangeCommandSummary> commandsToRanges;
    private IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> rangesToCommands;
    @Nullable
    private Timestamp maxRedundant;

    public CommandsForRanges()
    {
        localCommands = ImmutableSet.of();
        commandsToRanges = ImmutableSortedMap.of();
        rangesToCommands = IntervalTree.emptyTree();
    }

    private void set(AbstractBuilder<?> builder)
    {
        this.localCommands = ImmutableSet.copyOf(builder.localTxns);
        this.commandsToRanges = ImmutableSortedMap.copyOf(builder.txnToRange);
        this.rangesToCommands = builder.rangeToTxn.build();
    }

    public TxnType type(TxnId txnId)
    {
        if (!commandsToRanges.containsKey(txnId)) return TxnType.UNKNOWN;
        return localCommands.contains(txnId) ? TxnType.LOCAL : TxnType.REMOTE;
    }

    @VisibleForTesting
    Set<TxnId> knownIds()
    {
        return commandsToRanges.keySet();
    }

    @VisibleForTesting
    IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> tree()
    {
        return rangesToCommands;
    }

    public @Nullable Timestamp maxRedundant()
    {
        return maxRedundant;
    }

    public boolean containsLocally(TxnId txnId)
    {
        return localCommands.contains(txnId);
    }

    public Iterable<CommandTimeseriesHolder> search(AbstractKeys<Key> keys)
    {
        // group by the keyspace, as ranges are based off TokenKey, which is scoped to a range
        Map<String, List<Key>> groupByKeyspace = new TreeMap<>();
        for (Key key : keys)
            groupByKeyspace.computeIfAbsent(((PartitionKey) key).keyspace(), ignore -> new ArrayList<>()).add(key);
        return () -> new AbstractIterator<CommandTimeseriesHolder>()
        {
            Iterator<String> ksIt = groupByKeyspace.keySet().iterator();
            Iterator<Map.Entry<Range, Set<RangeCommandSummary>>> rangeIt;

            @Override
            protected CommandTimeseriesHolder computeNext()
            {
                while (true)
                {
                    if (rangeIt != null && rangeIt.hasNext())
                    {
                        Map.Entry<Range, Set<RangeCommandSummary>> next = rangeIt.next();
                        return result(next.getKey(), next.getValue());
                    }
                    rangeIt = null;
                    if (!ksIt.hasNext())
                    {
                        ksIt = null;
                        return endOfData();
                    }
                    String ks = ksIt.next();
                    List<Key> keys = groupByKeyspace.get(ks);
                    Map<Range, Set<RangeCommandSummary>> groupByRange = new TreeMap<>(Range::compare);
                    for (Key key : keys)
                    {
                        List<Interval<RoutableKey, RangeCommandSummary>> matches = rangesToCommands.matches(key);
                        if (matches.isEmpty())
                            continue;
                        for (Interval<RoutableKey, RangeCommandSummary> interval : matches)
                            groupByRange.computeIfAbsent(toRange(interval), ignore -> new HashSet<>()).add(interval.data);
                    }
                    rangeIt = groupByRange.entrySet().iterator();
                }
            }
        };
    }

    private static Range toRange(Interval<RoutableKey, RangeCommandSummary> interval)
    {
        TokenKey start = (TokenKey) interval.min;
        TokenKey end = (TokenKey) interval.max;
        // TODO (correctness) : accord doesn't support wrap around, so decreaseSlightly may fail in some cases
        // TODO (correctness) : this logic is mostly used for testing, so is it actually safe for all partitioners?
        return new TokenRange(start.withToken(start.token().decreaseSlightly()), end);
    }

    @Nullable
    public CommandTimeseriesHolder search(Range range)
    {
        List<RangeCommandSummary> matches = rangesToCommands.search(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                                                                    normalize(range.end(), range.endInclusive(), false)));
        return result(range, matches);
    }

    private CommandTimeseriesHolder result(Seekable seekable, Collection<RangeCommandSummary> matches)
    {
        if (matches.isEmpty())
            return null;
        return new Holder(seekable, matches);
    }

    public int size()
    {
        return rangesToCommands.intervalCount();
    }

    public Updater update()
    {
        return new Updater();
    }

    @Override
    public String toString()
    {
        return rangesToCommands.unbuild().toString();
    }

    private static RoutingKey normalize(RoutingKey key, boolean inclusive, boolean upOrDown)
    {
        while (true)
        {
            if (inclusive) return key;
            AccordRoutingKey ak = (AccordRoutingKey) key;
            switch (ak.kindOfRoutingKey())
            {
                case SENTINEL:
                    key = ak.asSentinelKey().toTokenKey();
                    continue;
                case TOKEN:
                    TokenKey tk = ak.asTokenKey();
                    return tk.withToken(upOrDown ? tk.token().nextValidToken() : tk.token().decreaseSlightly());
                default:
                    throw new IllegalArgumentException("Unknown kind: " + ak.kindOfRoutingKey());
            }
        }
    }

    private static class Holder implements CommandTimeseriesHolder
    {
        private final Seekable keyOrRange;
        private final Collection<RangeCommandSummary> matches;

        private Holder(Seekable keyOrRange, Collection<RangeCommandSummary> matches)
        {
            this.keyOrRange = keyOrRange;
            this.matches = matches;
        }

        @Override
        public CommandTimeseries<?> byId()
        {
            return build(m -> m.txnId);
        }

        @Override
        public CommandTimeseries<?> byExecuteAt()
        {
            return build(m -> m.executeAt != null ? m.executeAt : m.txnId);
        }

        @Override
        public Timestamp max()
        {
            return byExecuteAt().maxTimestamp();
        }

        private CommandTimeseries<?> build(Function<RangeCommandSummary, Timestamp> fn)
        {
            CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(keyOrRange, RangeCommandSummaryLoader.INSTANCE);
            for (RangeCommandSummary m : matches)
            {
                if (m.status == SaveStatus.Invalidated)
                    continue;
                builder.add(fn.apply(m), m);
            }
            return builder.build();
        }

        @Override
        public String toString()
        {
            return "Holder{" +
                   "keyOrRange=" + keyOrRange +
                   ", matches=" + matches +
                   '}';
        }
    }

    public void prune(TxnId pruneBefore, Ranges pruneRanges)
    {
        class MaxErased { Timestamp v; }
        MaxErased maxErased = new MaxErased();
        Updater update = update();
        update.map(summary -> {
            if (summary.txnId.compareTo(pruneBefore) >= 0)
                return summary;

            Ranges newRanges = summary.ranges.subtract(pruneRanges);
            if (newRanges == summary.ranges || newRanges.equals(summary.ranges))
                return summary;

            maxErased.v = Timestamp.nonNullOrMax(maxErased.v, summary.executeAt);
            if (newRanges.isEmpty())
                return null;
            return new RangeCommandSummary(summary.txnId, newRanges, summary.status, summary.executeAt, summary.deps);
        }).apply();
        maxRedundant = Timestamp.nonNullOrMax(maxRedundant, maxErased.v);
    }

}
