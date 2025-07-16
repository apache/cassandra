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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
import accord.local.KeyHistory;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.RangeRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.SymmetricComparator;
import accord.utils.UnhandledEnum;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.btree.IntervalBTree;

import static accord.local.CommandSummaries.SummaryStatus.NOT_DIRECTLY_WITNESSED;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.endWithStart;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.keyEndWithStart;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithEnd;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithStart;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.startWithEnd;
import static org.apache.cassandra.utils.btree.IntervalBTree.InclusiveEndHelper.startWithStart;

// TODO (expected): move to accord-core, merge with existing logic there
public class CommandsForRanges extends TreeMap<Timestamp, Summary> implements CommandSummaries.ByTxnIdSnapshot
{
    static final IntervalComparators COMPARATORS = new IntervalComparators();
    static final IntervalKeyComparators KEY_COMPARATORS = new IntervalKeyComparators();
    static class TxnIdInterval extends TokenRange
    {
        final TxnId txnId;

        TxnIdInterval(RoutingKey start, RoutingKey end, TxnId txnId)
        {
            super((TokenKey) start, (TokenKey) end);
            this.txnId = txnId;
        }

        TxnIdInterval(Range range, TxnId txnId)
        {
            this(range.start(), range.end(), txnId);
        }
    }

    static class IntervalComparators implements IntervalBTree.IntervalComparators<TxnIdInterval>
    {
        @Override
        public Comparator<TxnIdInterval> totalOrder()
        {
            return (a, b) -> {
                int c = a.start().compareTo(b.start());
                if (c == 0) c = a.end().compareTo(b.end());
                if (c == 0) c = a.txnId.compareTo(b.txnId);
                return c;
            };
        }
        @Override public Comparator<TxnIdInterval> endWithEndSorter() { return (a, b) -> a.end().compareTo(b.end()); }

        @Override public SymmetricComparator<TxnIdInterval> startWithStartSeeker() { return (a, b) -> startWithStart(a.start().compareTo(b.start())); }
        @Override public SymmetricComparator<TxnIdInterval> startWithEndSeeker() { return (a, b) -> startWithEnd(a.start().compareTo(b.end())); }
        @Override public SymmetricComparator<TxnIdInterval> endWithStartSeeker() { return (a, b) -> endWithStart(a.end().compareTo(b.start())); }
    }

    static class IntervalKeyComparators implements IntervalBTree.WithIntervalComparators<RoutingKey, TxnIdInterval>
    {
        @Override public AsymmetricComparator<RoutingKey, TxnIdInterval> startWithStartSeeker() { return (a, b) -> keyStartWithStart(a.compareTo(b.start()));}
        @Override public AsymmetricComparator<RoutingKey, TxnIdInterval> startWithEndSeeker() { return (a, b) -> keyStartWithEnd(a.compareTo(b.end())); }
        @Override public AsymmetricComparator<RoutingKey, TxnIdInterval> endWithStartSeeker() { return (a, b) -> keyEndWithStart(a.compareTo(b.start())); }
    }

    public CommandsForRanges(Map<? extends Timestamp, ? extends Summary> m)
    {
        super(m);
    }

    @Override
    public NavigableMap<Timestamp, CommandSummaries.Summary> byTxnId()
    {
        return this;
    }

    public static class Manager implements AccordCache.Listener<TxnId, Command>
    {
        private final AccordCommandStore commandStore;
        private final RangeSearcher searcher;
        private final AtomicReference<NavigableMap<TxnId, Ranges>> transitive = new AtomicReference<>(new TreeMap<>());
        // TODO (desired): manage memory consumed by this auxillary information
        private final Object2ObjectHashMap<TxnId, RangeRoute> cachedRangeTxnsById = new Object2ObjectHashMap<>();
        private Object[] cachedRangeTxnsByRange = IntervalBTree.empty();

        public Manager(AccordCommandStore commandStore)
        {
            this.commandStore = commandStore;
            try (AccordCommandStore.ExclusiveCaches caches = commandStore.lockCaches())
            {
                caches.commands().register(this);
            }
            this.searcher = commandStore.rangeSearcher();
        }

        @Override
        public void onUpdate(AccordCacheEntry<TxnId, Command> state)
        {
            TxnId txnId = state.key();
            if (txnId.is(Routable.Domain.Range))
            {
                Command cmd = state.tryGetExclusive();
                if (cmd != null)
                {
                    RangeRoute upd = (RangeRoute) cmd.route();
                    if (upd != null)
                    {
                        RangeRoute cur = cachedRangeTxnsById.put(cmd.txnId(), upd);
                        if (!upd.equals(cur))
                        {
                            if (cur != null)
                                remove(txnId, cur);
                            cachedRangeTxnsByRange = IntervalBTree.update(cachedRangeTxnsByRange, toMap(txnId, upd), COMPARATORS);
                        }
                    }
                }
            }
        }

        @Override
        public void onEvict(AccordCacheEntry<TxnId, Command> state)
        {
            TxnId txnId = state.key();
            if (txnId.is(Routable.Domain.Range))
            {
                RangeRoute cur = cachedRangeTxnsById.remove(txnId);
                if (cur != null)
                    remove(txnId, cur);
            }
        }

        private void remove(TxnId txnId, RangeRoute route)
        {
            cachedRangeTxnsByRange = IntervalBTree.subtract(cachedRangeTxnsByRange, toMap(txnId, route), COMPARATORS);
        }

        static Object[] toMap(TxnId txnId, RangeRoute route)
        {
            int size = route.size();
            switch (size)
            {
                case 0: return IntervalBTree.empty();
                case 1: return IntervalBTree.singleton(new TxnIdInterval(route.get(0), txnId));
                default:
                {
                    try (IntervalBTree.FastIntervalTreeBuilder<TxnIdInterval> builder = IntervalBTree.fastBuilder(COMPARATORS))
                    {
                        for (int i = 0 ; i < size ; ++i)
                            builder.add(new TxnIdInterval(route.get(i), txnId));
                        return builder.build();
                    }
                }
            }

        }

        public CommandsForRanges.Loader loader(@Nullable TxnId primaryTxnId, KeyHistory keyHistory, Unseekables<?> keysOrRanges)
        {
            RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
            return Loader.loader(redundantBefore, primaryTxnId, keyHistory, keysOrRanges, this::newLoader);
        }

        private Loader newLoader(@Nullable TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            MaxDecidedRX maxDecidedRX = null;
            if (primaryTxnId != null && primaryTxnId.is(Txn.Kind.ExclusiveSyncPoint) && findAsDep == null)
                maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
            return new Loader(this, primaryTxnId, searchKeysOrRanges, redundantBefore, testKind, minTxnId, maxTxnId, findAsDep, maxDecidedRX);
        }

        private void updateTransitive(UnaryOperator<NavigableMap<TxnId, Ranges>> update)
        {
            while (true)
            {
                NavigableMap<TxnId, Ranges> prev = transitive.get();
                NavigableMap<TxnId, Ranges> next = update.apply(prev);
                if (next == null || prev == next)
                    return;
                if (transitive.compareAndSet(prev, next))
                    return;
            }
        }

        public void mergeTransitive(TxnId txnId, Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
        {
            updateTransitive(transitive -> {
                NavigableMap<TxnId, Ranges> next = new TreeMap<>(transitive);
                next.merge(txnId, ranges, remappingFunction);
                return next;
            });
        }

        public void gcBefore(TxnId gcBefore, Ranges ranges)
        {
            updateTransitive(transitive -> {
                NavigableMap<TxnId, Ranges> next = null;
                Iterator<Map.Entry<TxnId, Ranges>> iterator = transitive.headMap(gcBefore).entrySet().iterator();
                while (iterator.hasNext())
                {
                    Map.Entry<TxnId, Ranges> e = iterator.next();
                    Ranges newRanges = e.getValue().without(ranges);
                    if (!newRanges.isEmpty())
                    {
                        if (next == null)
                            next = new TreeMap<>();
                        next.put(e.getKey(), newRanges);
                    }
                }
                return next;
            });
        }
    }

    public static class Loader extends Summary.Loader
    {
        private final Manager manager;
        private final MaxDecidedRX maxDecidedRX;
        private final TxnId primaryTxnId;
        private final TxnId minRelevantId;

        public Loader(Manager manager, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep, MaxDecidedRX maxDecidedRX)
        {
            super(primaryTxnId, searchKeysOrRanges, redundantBefore, testKinds, minTxnId, maxTxnId, findAsDep);
            this.manager = manager;
            this.maxDecidedRX = maxDecidedRX;
            this.primaryTxnId = primaryTxnId;
            this.minRelevantId = MaxDecidedRX.minDecidedDependencyId(maxDecidedRX, searchKeysOrRanges, primaryTxnId);
        }

        public void intersects(Consumer<TxnId> forEach)
        {
            // TODO (expected): use the ranges we find to filter results by MaxDecidedRX (don't just consume the TxnId)
            switch (searchKeysOrRanges.domain())
            {
                case Range:
                    for (Unseekable range : searchKeysOrRanges)
                        manager.searcher.search(manager.commandStore.id(), (TokenRange) range, minTxnId, maxTxnId).consume(forEach);
                    break;
                case Key:
                    for (Unseekable key : searchKeysOrRanges)
                        manager.searcher.search(manager.commandStore.id(), (TokenKey) key, minTxnId, maxTxnId).consume(forEach);
            }

            NavigableMap<TxnId, Ranges> transitive = manager.transitive.get();
            if (!transitive.isEmpty())
            {
                for (Map.Entry<TxnId, Ranges> e : transitive.tailMap(minTxnId, true).entrySet())
                {
                    if (e.getValue().intersects(searchKeysOrRanges))
                        forEach.accept(e.getKey());
                }
            }
        }

        boolean isRelevant(TxnIdInterval txnIdInterval)
        {
            if (maxDecidedRX == null)
                return true;

            if (!isMaybeRelevant(txnIdInterval.txnId))
                return false;

            TxnId minRelevantId = MaxDecidedRX.minDecidedDependencyId(maxDecidedRX, Ranges.of(txnIdInterval), primaryTxnId);
            return isRelevant(minRelevantId, primaryTxnId);
        }

        private boolean isRelevant(@Nullable TxnId minRelevantId, TxnId txnId)
        {
            return minRelevantId == null || minRelevantId.compareTo(txnId) <= 0;
        }

        boolean isMaybeRelevant(TxnId txnId)
        {
            return isRelevant(minRelevantId, txnId);
        }

        public void forEachInCache(Unseekables<?> keysOrRanges, Consumer<Summary> forEach, AccordCommandStore.Caches caches)
        {
            switch (keysOrRanges.domain())
            {
                default: throw new UnhandledEnum(keysOrRanges.domain());
                case Key:
                {
                    for (RoutingKey key : (AbstractUnseekableKeys)keysOrRanges)
                    {
                        IntervalBTree.accumulate(manager.cachedRangeTxnsByRange, KEY_COMPARATORS, key, (f, s, i, c) -> {
                            TxnIdInterval interval = (TxnIdInterval)i;
                            if (isRelevant(interval))
                            {
                                TxnId txnId = ((TxnIdInterval)i).txnId;
                                Summary summary = ifRelevant(c.getUnsafe(txnId));
                                if (summary != null)
                                    f.accept(summary);
                            }
                            return c;
                        }, forEach, this, caches.commands());
                    }
                    break;
                }
                case Range:
                {
                    for (Range range : (AbstractRanges)keysOrRanges)
                    {
                        IntervalBTree.accumulate(manager.cachedRangeTxnsByRange, COMPARATORS, new TxnIdInterval(range.start(), range.end(), TxnId.NONE), (f, s, i, c) -> {
                            if (isRelevant(i))
                            {
                                TxnId txnId = i.txnId;
                                Summary summary = ifRelevant(c.getUnsafe(txnId));
                                if (summary != null)
                                    f.accept(summary);
                            }
                            return c;
                        }, forEach, this, caches.commands());
                    }
                    break;
                }
            }
        }

        public Summary load(TxnId txnId)
        {
            if (!isMaybeRelevant(txnId))
                return null;

            if (findAsDep == null)
            {
                Command.Minimal cmd = manager.commandStore.loadMinimal(txnId);
                if (cmd != null)
                    return ifRelevant(cmd);
            }
            else
            {
                Command cmd = manager.commandStore.loadCommand(txnId);
                if (cmd != null)
                    return ifRelevant(cmd);
            }

            Ranges ranges = manager.transitive.get().get(txnId);
            if (ranges == null)
                return null;

            ranges = ranges.intersecting(searchKeysOrRanges);
            if (ranges.isEmpty())
                return null;

            return new Summary(txnId, txnId, NOT_DIRECTLY_WITNESSED, ranges, null, null);
        }

        public Summary ifRelevant(AccordCacheEntry<TxnId, Command> state)
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
            return ifRelevant(command);
        }

        public Summary ifRelevant(Command cmd)
        {
            return ifRelevant(cmd.txnId(), cmd.executeAt(), cmd.saveStatus(), cmd.participants(), cmd.partialDeps());
        }

        public Summary ifRelevant(Command.Minimal cmd)
        {
            Invariants.require(findAsDep == null);
            return ifRelevant(cmd.txnId, cmd.executeAt, cmd.saveStatus, cmd.participants, null);
        }
    }
}
