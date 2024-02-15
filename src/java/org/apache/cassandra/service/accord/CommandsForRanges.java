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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.CommandsForKey;
import accord.impl.CommandsSummary;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.AbstractKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

import static accord.local.SafeCommandStore.*;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestStartedAt.ANY;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_BEFORE;
import static accord.local.SafeCommandStore.TestStatus.ANY_STATUS;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;

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

    public static boolean needsUpdate(Command prev, Command updated)
    {
        return CommandsForKey.needsUpdate(prev, updated);
    }

    public boolean containsLocally(TxnId txnId)
    {
        return localCommands.contains(txnId);
    }

    public Iterable<CommandsSummary> search(AbstractKeys<Key> keys)
    {
        // group by the table, as ranges are based off TokenKey, which is scoped to a range
        Map<TableId, List<Key>> groupByTable = new TreeMap<>();
        for (Key key : keys)
            groupByTable.computeIfAbsent(((PartitionKey) key).table(), ignore -> new ArrayList<>()).add(key);
        return () -> new AbstractIterator<CommandsSummary>()
        {
            Iterator<TableId> tblIt = groupByTable.keySet().iterator();
            Iterator<Map.Entry<Range, Set<RangeCommandSummary>>> rangeIt;

            @Override
            protected CommandsSummary computeNext()
            {
                while (true)
                {
                    if (rangeIt != null && rangeIt.hasNext())
                    {
                        Map.Entry<Range, Set<RangeCommandSummary>> next = rangeIt.next();
                        return result(next.getKey(), next.getValue());
                    }
                    rangeIt = null;
                    if (!tblIt.hasNext())
                    {
                        tblIt = null;
                        return endOfData();
                    }
                    TableId tbl = tblIt.next();
                    List<Key> keys = groupByTable.get(tbl);
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
        // TODO (required, correctness) : accord doesn't support wrap around, so decreaseSlightly may fail in some cases
        // TODO (required, correctness) : this logic is mostly used for testing, so is it actually safe for all partitioners?
        return new TokenRange(start.withToken(start.token().decreaseSlightly()), end);
    }

    @Nullable
    public CommandsSummary search(Range range)
    {
        List<RangeCommandSummary> matches = rangesToCommands.search(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                                                                    normalize(range.end(), range.endInclusive(), false)));
        return result(range, matches);
    }

    private CommandsSummary result(Seekable seekable, Collection<RangeCommandSummary> matches)
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
                    // TODO (required, correctness): this doesn't work
                    key = ak.asSentinelKey().toTokenKeyBroken();
                    continue;
                case TOKEN:
                    TokenKey tk = ak.asTokenKey();
                    // TODO (required, correctness): this doesn't work for ordered partitioner
                    return tk.withToken(upOrDown ? tk.token().increaseSlightly() : tk.token().decreaseSlightly());
                default:
                    throw new IllegalArgumentException("Unknown kind: " + ak.kindOfRoutingKey());
            }
        }
    }

    private static class Holder implements CommandsSummary
    {
        private final Seekable keyOrRange;
        private final Collection<RangeCommandSummary> matches;

        private Holder(Seekable keyOrRange, Collection<RangeCommandSummary> matches)
        {
            this.keyOrRange = keyOrRange;
            this.matches = matches;
        }

        @Override
        public <P1, T> T mapReduceFull(TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            return mapReduce(testTxnId, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, accumulate);
        }

        @Override
        public <P1, T> T mapReduceActive(Timestamp startedBefore, Txn.Kind.Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            return mapReduce(startedBefore, null, testKind, STARTED_BEFORE, ANY_DEPS, ANY_STATUS, map, p1, accumulate);
        }

        private <P1, T> T mapReduce(@Nonnull Timestamp testTimestamp, @Nullable TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            // TODO (required): reconsider how we build this, to avoid having to provide range keys in order (or ensure our range search does this for us)
            Map<Range, List<RangeCommandSummary>> collect = new TreeMap<>(Range::compare);
            matches.forEach((summary -> {
                if (summary.status.compareTo(SaveStatus.Erased) >= 0)
                    return;

                switch (testStartedAt)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                        if (summary.txnId.compareTo(testTimestamp) <= 0) return;
                        else break;
                    case STARTED_BEFORE:
                        if (summary.txnId.compareTo(testTimestamp) >= 0) return;
                    case ANY:
                        if (testDep != ANY_DEPS && (summary.executeAt == null || summary.executeAt.compareTo(testTxnId) < 0))
                            return;
                }

                switch (testStatus)
                {
                    default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                    case ANY_STATUS:
                        break;
                    case IS_PROPOSED:
                        switch (summary.status)
                        {
                            default: return;
                            case PreCommitted:
                            case Committed:
                            case Accepted:
                        }
                        break;
                    case IS_STABLE:
                        if (!summary.status.hasBeen(Stable) || summary.status.hasBeen(Truncated))
                            return;
                }

                if (!testKind.test(summary.txnId.kind()))
                    return;

                if (testDep != ANY_DEPS)
                {
                    if (!summary.status.known.deps.hasProposedOrDecidedDeps())
                        return;

                    // TODO (required): we must ensure these txnId are limited to those we intersect in this command store
                    // We are looking for transactions A that have (or have not) B as a dependency.
                    // If B covers ranges [1..3] and A covers [2..3], but the command store only covers ranges [1..2],
                    // we could have A adopt B as a dependency on [3..3] only, and have that A intersects B on this
                    // command store, but also that there is no dependency relation between them on the overlapping
                    // key range [2..2].

                    // This can lead to problems on recovery, where we believe a transaction is a dependency
                    // and so it is safe to execute, when in fact it is only a dependency on a different shard
                    // (and that other shard, perhaps, does not know that it is a dependency - and so it is not durably known)
                    // TODO (required): consider this some more
                    if ((testDep == WITH) == !summary.deps.contains(testTxnId))
                        return;
                }

                // TODO (required): ensure we are excluding any ranges that are now shard-redundant (not sure if this is enforced yet)
                for (Range range : summary.ranges)
                    collect.computeIfAbsent(range, ignore -> new ArrayList<>()).add(summary);
            }));

            for (Map.Entry<Range, List<RangeCommandSummary>> e : collect.entrySet())
            {
                for (RangeCommandSummary command : e.getValue())
                {
                    T initial = accumulate;
                    accumulate = map.apply(p1, e.getKey(), command.txnId, command.executeAt, initial);
                }
            }

            return accumulate;
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
