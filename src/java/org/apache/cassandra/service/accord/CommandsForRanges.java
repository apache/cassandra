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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
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
import accord.utils.AsymmetricComparator;
import accord.utils.Invariants;
import accord.utils.SymmetricComparator;
import accord.utils.UnhandledEnum;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.IntervalBTree;
import org.apache.cassandra.utils.concurrent.IntrusiveStack;

import static accord.api.Journal.Load.MINIMAL;
import static accord.api.Journal.Load.MINIMAL_WITH_DEPS;
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

        @Override
        public String toString()
        {
            return super.toString() + ':' + txnId;
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

    public static class Manager implements AccordCache.Listener<TxnId, Command>, Runnable
    {
        static class IntervalTreeEdit extends IntrusiveStack<IntervalTreeEdit>
        {
            final TxnId txnId;
            final @Nullable Object[] update, remove;

            IntervalTreeEdit(TxnId txnId, Object[] update, Object[] remove)
            {
                this.txnId = txnId;
                this.update = update;
                this.remove = remove;
            }

            public static boolean push(IntervalTreeEdit edit, Manager manager)
            {
                return null == IntrusiveStack.getAndPush(pendingEditsUpdater, manager, edit);
            }

            public IntervalTreeEdit reverse()
            {
                return reverse(this);
            }

            boolean isSize(int size)
            {
                return IntrusiveStack.isSize(size, this);
            }

            IntervalTreeEdit merge(IntervalTreeEdit next)
            {
                Invariants.require(this.txnId.equals(next.txnId));
                Object[] remove = this.remove == null ? next.remove : next.remove == null ? this.remove : IntervalBTree.update(this.remove, next.remove, COMPARATORS);
                return new IntervalTreeEdit(txnId, next.update, remove);
            }
        }

        private final AccordCommandStore commandStore;
        private final RangeSearcher searcher;
        // TODO (desired): manage memory consumed by this auxillary information
        private final Object2ObjectHashMap<TxnId, RangeRoute> cachedRangeTxnsById = new Object2ObjectHashMap<>();
        private Object[] cachedRangeTxnsByRange = IntervalBTree.empty();

        private volatile IntervalTreeEdit pendingEdits;
        private final Lock drainPendingEditsLock = new ReentrantLock();
        private static final AtomicReferenceFieldUpdater<Manager, IntervalTreeEdit> pendingEditsUpdater = AtomicReferenceFieldUpdater.newUpdater(Manager.class, IntervalTreeEdit.class, "pendingEdits");

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
                            pushEdit(new IntervalTreeEdit(txnId, toMap(txnId, upd), cur == null ? null : toMap(txnId, cur)));
                    }
                    else
                    {
                        RangeRoute cur = cachedRangeTxnsById.remove(cmd.txnId());
                        if (cur != null)
                            pushEdit(new IntervalTreeEdit(txnId, null, toMap(txnId, cur)));
                    }
                }
            }
        }

        private void pushEdit(IntervalTreeEdit edit)
        {
            if (IntervalTreeEdit.push(edit, this))
                commandStore.executor().submitExclusive(this);
        }

        @Override
        public void run()
        {
            if (drainPendingEditsLock.tryLock())
            {
                try
                {
                    drainPendingEditsInternal();
                }
                finally
                {
                    drainPendingEditsLock.unlock();
                    postUnlock();
                }
            }
        }

        Object[] cachedRangeTxnsByRange()
        {
            drainPendingEditsLock.lock();
            try
            {
                drainPendingEditsInternal();
                return cachedRangeTxnsByRange;
            }
            finally
            {
                drainPendingEditsLock.unlock();
                postUnlock();
            }
        }

        void drainPendingEditsInternal()
        {
            IntervalTreeEdit edits = pendingEditsUpdater.getAndSet(this, null);
            if (edits == null)
                return;

            if (edits.isSize(1))
            {
                if (edits.remove != null) cachedRangeTxnsByRange = IntervalBTree.subtract(cachedRangeTxnsByRange, edits.remove, COMPARATORS);
                if (edits.update != null) cachedRangeTxnsByRange = IntervalBTree.update(cachedRangeTxnsByRange, edits.update, COMPARATORS);
                return;
            }

            edits = edits.reverse();
            Map<TxnId, IntervalTreeEdit> editMap = new HashMap<>();
            for (IntervalTreeEdit edit : edits)
                editMap.merge(edit.txnId, edit, IntervalTreeEdit::merge);

            List<TxnIdInterval> update = new ArrayList<>(), remove = new ArrayList<>();
            for (IntervalTreeEdit edit : editMap.values())
            {
                if (edit.remove != null) remove.addAll(BTreeSet.wrap(edit.remove, COMPARATORS.totalOrder()));
                if (edit.update != null) update.addAll(BTreeSet.wrap(edit.update, COMPARATORS.totalOrder()));
            }

            if (!remove.isEmpty())
            {
                remove.sort(COMPARATORS.totalOrder());
                cachedRangeTxnsByRange = IntervalBTree.subtract(cachedRangeTxnsByRange, IntervalBTree.build(remove, COMPARATORS), COMPARATORS);
            }
            if (!update.isEmpty())
            {
                update.sort(COMPARATORS.totalOrder());
                cachedRangeTxnsByRange = IntervalBTree.update(cachedRangeTxnsByRange, IntervalBTree.build(update, COMPARATORS), COMPARATORS);
            }

            if (Invariants.isParanoid())
            {
                try (AccordCommandStore.ExclusiveCaches caches = commandStore.tryLockCaches())
                {
                    if (caches != null)
                    {
                        for (TxnIdInterval i : BTree.<TxnIdInterval>iterable(cachedRangeTxnsByRange))
                        {
                            if (caches.commands().getUnsafe(i.txnId) == null)
                            {
                                boolean removed = pendingEdits != null && pendingEdits.foldl((edit, interval, r) -> {
                                    return r || (edit.txnId.equals(i.txnId) && BTree.find(edit.remove, COMPARATORS.totalOrder(), i) != null);
                                }, i, false);
                                Invariants.require(removed);
                            }
                        }
                    }
                }
            }
        }

        private void postUnlock()
        {
            if (pendingEdits != null)
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
                    pushEdit(new IntervalTreeEdit(txnId, null, toMap(txnId, cur)));
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
                    try (IntervalBTree.FastIntervalTreeBuilder<TxnIdInterval> builder = IntervalBTree.fastBuilder(COMPARATORS))
                    {
                        for (int i = 0 ; i < size ; ++i)
                            builder.add(new TxnIdInterval(route.get(i), txnId));
                        return builder.build();
                    }
                }
            }
        }

        public CommandsForRanges.Loader loader(@Nullable TxnId primaryTxnId, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
        {
            RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
            MaxDecidedRX maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
            return SummaryLoader.loader(redundantBefore, maxDecidedRX, primaryTxnId, loadKeysFor, keysOrRanges, this::newLoader);
        }

        private Loader newLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, @Nullable TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            return new Loader(this, redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKind, minTxnId, maxTxnId, findAsDep);
        }
    }

    public static class Loader extends SummaryLoader
    {
        private final Manager manager;

        public Loader(Manager manager, RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            super(redundantBefore, maxDecidedRX, primaryTxnId, searchKeysOrRanges, testKinds, minTxnId, maxTxnId, findAsDep);
            this.manager = manager;
        }

        public void intersects(Consumer<TxnId> forEach)
        {
            switch (searchKeysOrRanges.domain())
            {
                case Range:
                    for (Unseekable range : searchKeysOrRanges)
                        manager.searcher.search(manager.commandStore.id(), (TokenRange) range, minTxnId, maxTxnId, decidedRx).consume(forEach);
                    break;
                case Key:
                    for (Unseekable key : searchKeysOrRanges)
                        manager.searcher.search(manager.commandStore.id(), (TokenKey) key, minTxnId, maxTxnId, decidedRx).consume(forEach);
            }
        }

        boolean isMaybeRelevant(TxnIdInterval txnIdInterval)
        {
            return isMaybeRelevant(txnIdInterval.txnId, null, Ranges.of(txnIdInterval));
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
                        IntervalBTree.accumulate(manager.cachedRangeTxnsByRange(), KEY_COMPARATORS, key, (f, s, i, c) -> {
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
                        IntervalBTree.accumulate(manager.cachedRangeTxnsByRange(), COMPARATORS, new TxnIdInterval(range.start(), range.end(), TxnId.NONE), (f, s, i, c) -> {
                            if (isMaybeRelevant(i))
                            {
                                TxnId txnId = i.txnId;
                                AccordCacheEntry<TxnId, Command> entry = c.getUnsafe(txnId);
                                Invariants.expect(entry != null, "%s found interval %s but no matching transaction in cache", manager.commandStore, i);
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
                Command.MinimalWithDeps cmd = manager.commandStore.loadMinimalWithDeps(txnId);
                if (cmd != null)
                    return ifRelevant(cmd);
            }

            return null;
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
                case WAITING_TO_SAVE:
                case FAILED_TO_SAVE:
            }

            TxnId txnId = state.key();
            if (!isMaybeRelevant(txnId))
                return null;

            Object command = state.getOrShrunkExclusive();
            if (command == null)
                return null;

            if (command instanceof Command)
                return ifRelevant((Command) command);

            Invariants.require(command instanceof ByteBuffer);
            AccordJournal.Builder builder = new AccordJournal.Builder(txnId, findAsDep == null ? MINIMAL : MINIMAL_WITH_DEPS);
            ByteBuffer buffer = (ByteBuffer) command;
            buffer.mark();
            try (DataInputBuffer buf = new DataInputBuffer(buffer, false))
            {
                builder.deserializeNext(buf, Version.LATEST);
                if (findAsDep == null) return ifRelevant(builder.asMinimal());
                else return ifRelevant(builder.asMinimalWithDeps());
            }
            catch (UnknownTableException e)
            {
                return null;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                buffer.reset();
            }
        }
    }
}
