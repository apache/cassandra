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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.impl.CommandsSummary;
import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.local.StoreParticipants;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.index.accord.RoutesSearcher;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_BEFORE;
import static accord.local.SafeCommandStore.TestStatus.ANY_STATUS;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class CommandsForRanges extends TreeMap<Timestamp, CommandsForRanges.Summary> implements CommandsSummary
{
    public CommandsForRanges(Map<? extends Timestamp, ? extends Summary> m)
    {
        super(m);
    }

    @Override
    public <P1, T> T mapReduceFull(Routables<?> keysOrRanges, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, testTxnId, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, accumulate);
    }

    @Override
    public <P1, T> T mapReduceActive(Routables<?> keysOrRanges, Timestamp startedBefore, Txn.Kind.Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        return mapReduce(keysOrRanges, startedBefore, null, testKind, STARTED_BEFORE, ANY_DEPS, ANY_STATUS, map, p1, accumulate);
    }

    private <P1, T> T mapReduce(Routables<?> keysOrRanges, @Nonnull Timestamp testTimestamp, @Nullable TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
    {
        Map<Range, List<Summary>> collect = new TreeMap<>(Range::compare);
        NavigableMap<Timestamp, Summary> submap;
        switch (testStartedAt)
        {
            case STARTED_AFTER:
                submap = tailMap(testTimestamp, false);
                break;
            case STARTED_BEFORE:
                submap = headMap(testTimestamp, false);
                break;
            case ANY:
                submap = this;
                break;
            default:
                throw new AssertionError("Unknown started at: " + testStartedAt);
        }
        submap.values().forEach((summary -> {
            if (!testKind.test(summary.txnId.kind()))
                return;

            // range specific logic... ranges don't update CommandsForRange based off the life cycle and instead
            // merge the cache with the disk state; so exclude states that should get removed from CommandsFor*
            if (summary.saveStatus != null && summary.saveStatus.compareTo(SaveStatus.Erased) >= 0)
                return;

            // TODO (expected): share this logic with InMemoryCommandsStore for improved BurnTest coverage
            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case ANY_STATUS:
                    break;
                case IS_PROPOSED:
                    switch (summary.saveStatus.status)
                    {
                        default: return;
                        case PreCommitted:
                        case Committed:
                        case Accepted:
                    }
                    break;
                case IS_STABLE:
                    if (!summary.saveStatus.hasBeen(Stable) || summary.saveStatus.hasBeen(Truncated))
                        return;
            }

            if (testDep != ANY_DEPS)
            {
                // ! status.hasInfo
                //TODO (now, reuse): should this just check if known?
                if (!(summary.saveStatus.compareTo(SaveStatus.Accepted) >= 0))
                    return;

                Timestamp executeAt = summary.executeAt;
                if (executeAt.compareTo(testTxnId) <= 0)
                    return;

                // We are looking for transactions A that have (or have not) B as a dependency.
                // If B covers ranges [1..3] and A covers [2..3], but the command store only covers ranges [1..2],
                // we could have A adopt B as a dependency on [3..3] only, and have that A intersects B on this
                // command store, but also that there is no dependency relation between them on the overlapping
                // key range [2..2].

                // This can lead to problems on recovery, where we believe a transaction is a dependency
                // and so it is safe to execute, when in fact it is only a dependency on a different shard
                // (and that other shard, perhaps, does not know that it is a dependency - and so it is not durably known)
                // TODO (required): consider this some more
                if ((testDep == WITH) == !summary.hasAsDep)
                    return;

                Invariants.checkState(testTxnId.equals(summary.findAsDep));
            }

            for (Range range : summary.ranges)
            {
                if (keysOrRanges.intersects(range))
                    collect.computeIfAbsent(range, ignore -> new ArrayList<>()).add(summary);
            }
        }));

        for (Map.Entry<Range, List<Summary>> e : collect.entrySet())
        {
            for (Summary command : e.getValue())
                accumulate = map.apply(p1, e.getKey(), command.txnId, command.executeAt, accumulate);
        }

        return accumulate;
    }

    public static class Summary
    {
        public final @Nonnull TxnId txnId;
        public final @Nonnull Timestamp executeAt;
        public final @Nonnull SaveStatus saveStatus;
        public final @Nonnull Ranges ranges;

        public final TxnId findAsDep;
        public final boolean hasAsDep;

        @VisibleForTesting
        Summary(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull SaveStatus saveStatus, @Nonnull Ranges ranges, TxnId findAsDep, boolean hasAsDep)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.saveStatus = saveStatus;
            this.ranges = ranges;
            this.findAsDep = findAsDep;
            this.hasAsDep = hasAsDep;
        }

        public Summary slice(Ranges slice)
        {
            return new Summary(txnId, executeAt, saveStatus, ranges.slice(slice, Minimal), findAsDep, hasAsDep);
        }

        @Override
        public String toString()
        {
            return "Summary{" +
                   "txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + saveStatus +
                   ", ranges=" + ranges +
                   ", findAsDep=" + findAsDep +
                   ", hasAsDep=" + hasAsDep +
                   '}';
        }
    }

    public static class Manager implements AccordCache.Listener<TxnId, Command>
    {
        private final AccordCommandStore commandStore;
        private final RoutesSearcher searcher = new RoutesSearcher();
        private final NavigableMap<TxnId, Ranges> transitive = new TreeMap<>();
        private final ObjectHashSet<TxnId> cachedRangeTxns = new ObjectHashSet<>();

        public Manager(AccordCommandStore commandStore)
        {
            this.commandStore = commandStore;
            try (AccordCommandStore.ExclusiveCaches caches = commandStore.lockCaches())
            {
                caches.commands().register(this);
            }
        }

        @Override
        public void onAdd(AccordCacheEntry<TxnId, Command> state)
        {
            TxnId txnId = state.key();
            if (txnId.is(Routable.Domain.Range))
                cachedRangeTxns.add(txnId);
        }

        @Override
        public void onEvict(AccordCacheEntry<TxnId, Command> state)
        {
            TxnId txnId = state.key();
            if (txnId.is(Routable.Domain.Range))
                cachedRangeTxns.remove(txnId);
        }

        public CommandsForRanges.Loader loader(@Nullable TxnId primaryTxnId, KeyHistory keyHistory, Unseekables<?> keysOrRanges)
        {
            RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
            TxnId minTxnId = redundantBefore.min(keysOrRanges, e -> e.gcBefore);
            Timestamp maxTxnId = primaryTxnId == null || keyHistory == KeyHistory.RECOVER || !primaryTxnId.is(ExclusiveSyncPoint) ? Timestamp.MAX : primaryTxnId;
            TxnId findAsDep = primaryTxnId != null && keyHistory == KeyHistory.RECOVER ? primaryTxnId : null;
            return new CommandsForRanges.Loader(this, keysOrRanges, redundantBefore, minTxnId, maxTxnId, findAsDep);
        }

        public void mergeTransitive(TxnId txnId, Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
        {
            transitive.merge(txnId, ranges, remappingFunction);
        }

        public void gcBefore(TxnId gcBefore, Ranges ranges)
        {
            Iterator<Map.Entry<TxnId, Ranges>> iterator = transitive.headMap(gcBefore).entrySet().iterator();
            while (iterator.hasNext())
            {
                Map.Entry<TxnId, Ranges> e = iterator.next();
                Ranges newRanges = e.getValue().without(ranges);
                if (newRanges.isEmpty())
                    iterator.remove();
                e.setValue(newRanges);
            }
        }
    }

    public static class Loader
    {
        private final Manager manager;
        final Unseekables<?> searchKeysOrRanges;
        final RedundantBefore redundantBefore;
        final TxnId minTxnId;
        final Timestamp maxTxnId;
        @Nullable final TxnId findAsDep;

        public Loader(Manager manager, Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            this.manager = manager;
            this.searchKeysOrRanges = searchKeysOrRanges;
            this.redundantBefore = redundantBefore;
            this.minTxnId = minTxnId;
            this.maxTxnId = maxTxnId;
            this.findAsDep = findAsDep;
        }

        public void intersects(Consumer<TxnId> forEach)
        {
            switch (searchKeysOrRanges.domain())
            {
                case Range:
                    for (Unseekable range : searchKeysOrRanges)
                        manager.searcher.intersects(manager.commandStore.id(), (TokenRange) range, minTxnId, maxTxnId, forEach);
                    break;
                case Key:
                    for (Unseekable key : searchKeysOrRanges)
                        manager.searcher.intersects(manager.commandStore.id(), (AccordRoutingKey) key, minTxnId, maxTxnId, forEach);
            }

            if (!manager.transitive.isEmpty())
            {
                for (var e : manager.transitive.tailMap(minTxnId, true).entrySet())
                {
                    if (e.getValue().intersects(searchKeysOrRanges))
                        forEach.accept(e.getKey());
                }
            }
        }

        public void forEachInCache(Consumer<Summary> forEach, AccordCommandStore.Caches caches)
        {
            for (TxnId txnId : manager.cachedRangeTxns)
            {
                AccordCacheEntry<TxnId, Command> state = caches.commands().getUnsafe(txnId);
                Summary summary = from(state);
                if (summary != null)
                    forEach.accept(summary);
            }
        }

        public Summary load(TxnId txnId)
        {
            if (findAsDep == null)
            {
                SavedCommand.MinimalCommand cmd = manager.commandStore.loadMinimal(txnId);
                if (cmd != null)
                    return from(cmd);
            }
            else
            {
                Command cmd = manager.commandStore.loadCommand(txnId);
                if (cmd != null)
                    return from(cmd);
            }

            Ranges ranges = manager.transitive.get(txnId);
            if (ranges == null)
                return null;

            ranges = ranges.intersecting(searchKeysOrRanges);
            if (ranges.isEmpty())
                return null;

            return new Summary(txnId, txnId, SaveStatus.NotDefined, ranges, null, false);
        }

        public Summary from(AccordCacheEntry<TxnId, Command> state)
        {
            if (state.key().domain() != Routable.Domain.Range)
                return null;

            switch (state.status())
            {
                default: throw new AssertionError("Unhandled status: " + state.status());
                case LOADING:
                case WAITING_TO_LOAD:
                case UNINITIALIZED:
                    return null;

                case LOADED:
                case MODIFIED:
                case SAVING:
                case FAILED_TO_SAVE:
            }

            TxnId txnId = state.key();
            if (!txnId.isVisible() || txnId.compareTo(minTxnId) < 0 || txnId.compareTo(maxTxnId) >= 0)
                return null;

            Command command = state.getExclusive();
            if (command == null)
                return null;
            return from(command);
        }

        public Summary from(Command cmd)
        {
            return from(cmd.txnId(), cmd.executeAt(), cmd.saveStatus(), cmd.participants(), cmd.partialDeps());
        }

        public Summary from(SavedCommand.MinimalCommand cmd)
        {
            Invariants.checkState(findAsDep == null);
            return from(cmd.txnId, cmd.executeAt, cmd.saveStatus, cmd.participants, null);
        }

        private Summary from(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, StoreParticipants participants, @Nullable PartialDeps partialDeps)
        {
            if (participants == null)
                return null;

            Ranges keysOrRanges = participants.touches().toRanges();
            if (keysOrRanges.domain() != Routable.Domain.Range)
                throw new AssertionError(String.format("Txn keys are not range for %s", participants));
            Ranges ranges = keysOrRanges;

            ranges = ranges.intersecting(searchKeysOrRanges, Minimal);
            if (ranges.isEmpty())
                return null;

            if (redundantBefore != null)
            {
                Ranges newRanges = redundantBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                    if (e.shardAppliedOrInvalidatedBefore.compareTo(txnId) < 0)
                        return accum;
                    return accum.without(Ranges.of(new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end)));
                }, ranges, ignore -> false);

                if (newRanges.isEmpty())
                    return null;

                ranges = newRanges;
            }

            Invariants.checkState(partialDeps != null || findAsDep == null || !saveStatus.known.deps.hasProposedOrDecidedDeps());
            boolean hasAsDep = false;
            if (partialDeps != null)
            {
                Ranges depRanges = partialDeps.rangeDeps.ranges(txnId);
                if (depRanges != null && depRanges.containsAll(ranges))
                    hasAsDep = true;
            }

            return new Summary(txnId, executeAt, saveStatus, ranges, findAsDep, hasAsDep);
        }
    }
}
