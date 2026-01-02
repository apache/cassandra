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
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.impl.RangeIntervalComparators;
import accord.impl.RangeIntervalComparators.InclusiveEndWithKeyComparators;
import accord.impl.RangeIntervalComparators.InclusiveEndWithRangeComparators;
import accord.local.Command;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.LoadKeysFor;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.RangeRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.SemiSyncIntervalTree;
import accord.utils.UnhandledEnum;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.api.TokenKey;
import accord.utils.btree.BTree;
import accord.utils.btree.IntervalBTree;

import static accord.local.CommandSummaries.Relevance.IRRELEVANT;
import static accord.local.LoadKeysFor.RECOVERY;

public class JournalRangeIndex extends SemiSyncIntervalTree<Object[]> implements AccordCache.Listener<TxnId, Command>, Runnable, RangeIndex
{
    static final IntervalBTree.IntervalComparators<TxnIdInterval> ENTRIES = new RangeIntervalComparators.InclusiveEndEntryComparators<>(a -> a, (a, b) -> a.txnId.compareTo(b.txnId));
    static final IntervalBTree.WithIntervalComparators<RoutingKey, TxnIdInterval> WITH_KEY = new InclusiveEndWithKeyComparators<>(a -> a);
    static final IntervalBTree.WithIntervalComparators<Range, TxnIdInterval> WITH_RANGE = new InclusiveEndWithRangeComparators<>(a -> a);

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

        @Override
        public String toString()
        {
            return super.toString() + ':' + txnId;
        }
    }

    public static class Loader extends RangeIndex.Loader
    {
        static class CommandWatcher implements AccordCache.Listener<TxnId, Command>
        {
            final Loader loader;
            final Map<Timestamp, Summary> summaries;

            CommandWatcher(Loader loader, Map<Timestamp, Summary> summaries)
            {
                this.loader = loader;
                this.summaries = summaries;
            }

            @Override
            public void onUpdate(AccordCacheEntry<TxnId, Command> state)
            {
                Summary summary = loader.ifRelevant(state);
                if (summary != null)
                    summaries.put(summary.plainTxnId(), summary);
            }
        }

        private final JournalRangeIndex owner;
        private CommandWatcher commandWatcher;

        public Loader(JournalRangeIndex owner, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, LoadKeysFor loadKeysFor)
        {
            super(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKinds, minTxnId, maxTxnId, loadKeysFor);
            this.owner = owner;
        }

        @Override
        public void loadExclusive(Map<Timestamp, Summary> into, Caches caches)
        {
            forEachInCache(searchFor, summary -> into.put(summary.plainTxnId(), summary), caches);
            commandWatcher = new CommandWatcher(this, into);
            caches.commands().register(commandWatcher);
        }

        public void forEachInCache(Unseekables<?> keysOrRanges, Consumer<Summary> forEach, Caches caches)
        {
            switch (keysOrRanges.domain())
            {
                default: throw new UnhandledEnum(keysOrRanges.domain());
                case Key:
                {
                    for (RoutingKey key : (AbstractUnseekableKeys)keysOrRanges)
                    {
                        IntervalBTree.accumulate(owner.cachedRangeTxnsByRange(), WITH_KEY, key, (f, s, i, c) -> {
                            if (isMaybeRelevant(i))
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
                case Range:
                {
                    for (Range range : (AbstractRanges)keysOrRanges)
                    {
                        IntervalBTree.accumulate(owner.cachedRangeTxnsByRange(), WITH_RANGE, new TxnIdInterval(range.start(), range.end(), TxnId.NONE), (f, s, i, c) -> {
                            if (isMaybeRelevant(i))
                            {
                                TxnId txnId = i.txnId;
                                AccordCacheEntry<TxnId, Command> entry = c.getUnsafe(txnId);
                                Invariants.expect(entry != null, "%s found interval %s but no matching transaction in cache", owner.commandStore, i);
                                if (entry != null)
                                {
                                    Summary summary = ifRelevant(entry);
                                    if (summary != null)
                                        f.accept(summary);
                                }
                            }
                            return c;
                        }, forEach, this, caches.commands());
                    }
                    break;
                }
            }
        }

        public void load(Map<Timestamp, Summary> into, BooleanSupplier abort)
        {
            forEachIntersectingOnDisk(txnId -> {
                if (abort.getAsBoolean())
                    throw new CancellationException();

                if (into.containsKey(txnId))
                    return;

                Summary summary = loadFromDisk(txnId);
                if (summary != null)
                {
                    into.putIfAbsent(txnId, summary);
                    if (shouldRecordFutureRx(txnId, summary.status()))
                        recordFutureRx(txnId, summary.participants());
                }
            });
        }

        @Override
        void finish(Map<Timestamp, Summary> into)
        {
        }

        private void forEachIntersectingOnDisk(Consumer<TxnId> forEach)
        {
            Timestamp maxTxnId = loadKeysFor == RECOVERY || !primaryTxnId.isSyncPoint() ? Timestamp.MAX : primaryTxnId;
            switch (searchFor.domain())
            {
                case Range:
                    for (Unseekable range : searchFor)
                        owner.searcher.search(owner.commandStore.id(), (TokenRange) range, minTxnId, maxTxnId, decidedRx).consume(forEach);
                    break;
                case Key:
                    for (Unseekable key : searchFor)
                        owner.searcher.search(owner.commandStore.id(), (TokenKey) key, minTxnId, maxTxnId, decidedRx).consume(forEach);
            }
        }

        @Override
        protected Summary loadFromDisk(TxnId txnId)
        {
            if (!isMaybeRelevant(txnId))
                return null;

            return super.loadFromDisk(txnId);
        }

        boolean isMaybeRelevant(TxnIdInterval txnIdInterval)
        {
            return relevance(txnIdInterval.txnId, null, null, null, Ranges.of(txnIdInterval)) != IRRELEVANT;
        }

        @Override
        void cleanupExclusive(Caches caches)
        {
            if (commandWatcher != null)
            {
                CommandWatcher unregister = commandWatcher;
                commandWatcher = null;
                caches.commands().unregister(unregister);
            }
        }

        @Override
        AccordCommandStore commandStore()
        {
            return owner.commandStore;
        }
    }

    private final AccordCommandStore commandStore;
    // TODO (expected): do we need one of these per command store?
    private final JournalRangeSearcher searcher;
    private final Object2ObjectHashMap<TxnId, RangeRoute> cachedRangeTxnsById = new Object2ObjectHashMap<>();

    public JournalRangeIndex(AccordCommandStore commandStore)
    {
        super(ENTRIES);
        this.commandStore = commandStore;
        try (AccordCommandStore.ExclusiveCaches caches = commandStore.lockCaches())
        {
            caches.commands().register(this);
        }
        this.searcher = JournalRangeSearcher.extractRangeSearcher(commandStore.journal);
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
                        pushEdit(txnId, toMap(txnId, upd), cur == null ? null : toMap(txnId, cur));
                }
                else
                {
                    RangeRoute cur = cachedRangeTxnsById.remove(cmd.txnId());
                    if (cur != null)
                        pushEdit(txnId, null, toMap(txnId, cur));
                }
            }
        }
    }

    Object[] cachedRangeTxnsByRange()
    {
        return get();
    }

    protected void drainPendingEditsExclusive()
    {
        super.drainPendingEditsExclusive();
        if (Invariants.isParanoid())
        {
            try (AccordCommandStore.ExclusiveCaches caches = commandStore.tryLockCaches())
            {
                if (caches != null)
                {
                    for (TxnIdInterval i : BTree.<TxnIdInterval>iterable(value))
                    {
                        if (caches.commands().getUnsafe(i.txnId) == null)
                        {
                            boolean removed = pendingEdits != null && pendingEdits.foldl((edit, interval, r) -> {
                                return r || (edit.group.equals(i.txnId) && BTree.find(edit.replace, ENTRIES.totalOrder(), i) != null);
                            }, i, false);
                            Invariants.require(removed);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void onNewEdits()
    {
        commandStore.executor().submitExclusive(this);
    }

    @Override
    protected void onRemainingEdits()
    {
        commandStore.executor().submit(this);
    }

    @Override
    public void onEvict(AccordCacheEntry<TxnId, Command> state)
    {
        TxnId txnId = state.key();
        if (txnId.is(Routable.Domain.Range))
        {
            RangeRoute cur = cachedRangeTxnsById.remove(txnId);
            if (cur != null)
                pushEdit(txnId, null, toMap(txnId, cur));
        }
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
                try (IntervalBTree.FastIntervalTreeBuilder<TxnIdInterval> builder = IntervalBTree.fastBuilder(ENTRIES))
                {
                    for (int i = 0 ; i < size ; ++i)
                        builder.add(new TxnIdInterval(route.get(i), txnId));
                    return builder.build();
                }
            }
        }
    }

    public JournalRangeIndex.Loader loader(TxnId primaryTxnId, Timestamp primaryExecuteAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
    {
        RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
        MaxDecidedRX maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
        return SummaryLoader.loader(redundantBefore, maxDecidedRX, primaryTxnId, primaryExecuteAt, loadKeysFor, keysOrRanges, this::newLoader);
    }

    @Override
    public void update(Command prev, Command updated, boolean force)
    {
    }

    @Override
    public void postReplay()
    {
    }

    private Loader newLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, @Nullable TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, LoadKeysFor loadKeysFor)
    {
        return new Loader(this, redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKind, minTxnId, maxTxnId, loadKeysFor);
    }

    @Override
    protected Object[] tree(Object[] edit)
    {
        return edit;
    }
}
