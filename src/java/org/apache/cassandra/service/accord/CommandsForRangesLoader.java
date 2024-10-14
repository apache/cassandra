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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.RedundantBefore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.index.accord.RoutesSearcher;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.Pair;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class CommandsForRangesLoader implements AccordStateCache.Listener<TxnId, Command>
{
    private final RoutesSearcher searcher = new RoutesSearcher();
    //TODO (now, durability): find solution for this...
    private final NavigableMap<TxnId, Ranges> historicalTransaction = new TreeMap<>();
    private final AccordCommandStore store;
    private final ObjectHashSet<TxnId> cachedRangeTxns = new ObjectHashSet<>();
    // TODO (required): make this configurable, or perhaps backed by READ stage with concurrency limit

    public CommandsForRangesLoader(AccordCommandStore store)
    {
        this.store = store;
        store.commandCache().register(this);
    }

    @Override
    public void onAdd(AccordCachingState<TxnId, Command> state)
    {
        TxnId txnId = state.key();
        if (txnId.is(Domain.Range))
            cachedRangeTxns.add(txnId);
    }

    @Override
    public void onEvict(AccordCachingState<TxnId, Command> state)
    {
        TxnId txnId = state.key();
        if (txnId.is(Domain.Range))
            cachedRangeTxns.remove(txnId);
    }

    public AsyncResult<Pair<Watcher, NavigableMap<TxnId, Summary>>> get(@Nullable TxnId primaryTxnId, KeyHistory keyHistory, Ranges ranges)
    {
        RedundantBefore redundantBefore = store.unsafeGetRedundantBefore();
        TxnId minTxnId = redundantBefore.minGcBefore(ranges);
        Timestamp maxTxnId = primaryTxnId == null || keyHistory == KeyHistory.RECOVERY || !primaryTxnId.is(ExclusiveSyncPoint) ? Timestamp.MAX : primaryTxnId;
        TxnId findAsDep = primaryTxnId != null && keyHistory == KeyHistory.RECOVERY ? primaryTxnId : null;
        var watcher = fromCache(findAsDep, ranges, minTxnId, maxTxnId, redundantBefore);
        var before = ImmutableMap.copyOf(watcher.get());
        return AsyncChains.ofCallable(Stage.ACCORD_RANGE_LOADER.executor(), () -> get(ranges, before, findAsDep, minTxnId, maxTxnId, redundantBefore))
                          .map(map -> Pair.create(watcher, map), store)
               .beginAsResult();
    }

    private NavigableMap<TxnId, Summary> get(Ranges ranges, Map<TxnId, Summary> cacheHits, @Nullable TxnId findAsDep, TxnId minTxnId, Timestamp maxTxnId, RedundantBefore redundantBefore)
    {
        Set<TxnId> matches = new ObjectHashSet<>();
        for (Range range : ranges)
            matches.addAll(intersects(range, minTxnId, maxTxnId));
        if (matches.isEmpty())
            return new TreeMap<>();
        return load(ranges, cacheHits, matches, findAsDep, redundantBefore);
    }

    private Collection<TxnId> intersects(Range range, TxnId minTxnId, Timestamp maxTxnId)
    {
        assert range instanceof TokenRange : "Require TokenRange but given " + range.getClass();
        Set<TxnId> intersects = searcher.intersects(store.id(), (TokenRange) range, minTxnId, maxTxnId);
        if (!historicalTransaction.isEmpty())
        {
            if (intersects.isEmpty())
                intersects = new ObjectHashSet<>();
            for (var e : historicalTransaction.tailMap(minTxnId, true).entrySet())
            {
                if (e.getValue().intersects(range))
                    intersects.add(e.getKey());
            }
            if (intersects.isEmpty())
                intersects = Collections.emptySet();
        }
        return intersects;
    }

    public class Watcher implements AccordStateCache.Listener<TxnId, Command>, AutoCloseable
    {
        private final Ranges ranges;
        private final @Nullable TxnId findAsDep;
        private final TxnId minTxnId;
        private final Timestamp maxTxnId;
        private final RedundantBefore redundantBefore;

        private NavigableMap<TxnId, Summary> summaries = null;
        private Set<AccordCachingState<TxnId, Command>> needToDoubleCheck = null;

        public Watcher(Ranges ranges, @Nullable TxnId findAsDep, TxnId minTxnId, Timestamp maxTxnId, RedundantBefore redundantBefore)
        {
            this.ranges = ranges;
            this.findAsDep = findAsDep;
            this.minTxnId = minTxnId;
            this.maxTxnId = maxTxnId;
            this.redundantBefore = redundantBefore;
        }

        public NavigableMap<TxnId, Summary> get()
        {
            return summaries == null ? Collections.emptyNavigableMap() : summaries;
        }

        @Override
        public void onAdd(AccordCachingState<TxnId, Command> n)
        {
            if (n.key().domain() != Domain.Range)
                return;
            if (n.key().compareTo(minTxnId) < 0 || n.key().compareTo(maxTxnId) >= 0)
                return;

            var state = n.state();
            if (state instanceof AccordCachingState.Loading)
            {
                if (needToDoubleCheck == null)
                    needToDoubleCheck = new ObjectHashSet<>();
                needToDoubleCheck.add(n);
                return;
            }
            //TODO (required): include FailedToSave?  Most likely need to, but need to improve test coverage to have failed writes
            if (!(state instanceof AccordCachingState.Loaded
                  || state instanceof AccordCachingState.Modified
                  || state instanceof AccordCachingState.Saving))
                return;

            var cmd = state.get();
            if (cmd == null)
                return;
            Summary summary = create(cmd, ranges, findAsDep, redundantBefore);
            if (summary != null)
            {
                if (summaries == null)
                    summaries = new TreeMap<>();
                summaries.put(summary.txnId, summary);
            }
        }

        @Override
        public void onEvict(AccordCachingState<TxnId, Command> state)
        {
            if (needToDoubleCheck == null)
                return;
            if (!needToDoubleCheck.remove(state))
                return;
            if (state.state() instanceof AccordCachingState.Loading)
                return; // can't double check
            onAdd(state);
        }

        @Override
        public void close()
        {
            store.commandCache().unregister(this);
            if (needToDoubleCheck != null)
            {
                var copy = needToDoubleCheck;
                needToDoubleCheck = null;
                copy.forEach(this::onAdd);
            }
            needToDoubleCheck = null;
        }
    }

    private Watcher fromCache(@Nullable TxnId findAsDep, Ranges ranges, TxnId minTxnId, Timestamp maxTxnId, RedundantBefore redundantBefore)
    {
        Watcher watcher = new Watcher(ranges, findAsDep, minTxnId, maxTxnId, redundantBefore);
        for (TxnId rangeTxnId : cachedRangeTxns)
            watcher.onAdd(store.commandCache().getUnsafe(rangeTxnId));
        store.commandCache().register(watcher);
        return watcher;
    }

    private NavigableMap<TxnId, Summary> load(Ranges ranges, Map<TxnId, Summary> cacheHits, Collection<TxnId> possibleTxns, @Nullable TxnId findAsDep, RedundantBefore redundantBefore)
    {
        //TODO (required): this logic is kinda duplicate of org.apache.cassandra.service.accord.CommandsForRange.mapReduce
        // should figure out if this can be improved... also what is correct?
        NavigableMap<TxnId, Summary> map = new TreeMap<>();
        for (TxnId txnId : possibleTxns)
        {
            if (cacheHits.containsKey(txnId))
                continue;
            if (findAsDep == null)
            {
                var cmd = store.loadMinimal(txnId);
                if (cmd == null)
                    continue; // unknown command
                var summary = create(cmd, ranges, redundantBefore);
                if (summary == null)
                    continue;
                map.put(txnId, summary);

            }
            else
            {
                var cmd = store.loadCommand(txnId);
                if (cmd == null)
                    continue; // unknown command
                var summary = create(cmd, ranges, findAsDep, redundantBefore);
                if (summary == null)
                    continue;
                map.put(txnId, summary);
            }
        }
        return map;
    }

    private static Summary create(Command cmd, Ranges cacheRanges, @Nullable TxnId findAsDep, @Nullable RedundantBefore redundantBefore)
    {
        //TODO (required, correctness): C* did Invalidated, accord-core did Erased... what is correct?
        SaveStatus saveStatus = cmd.saveStatus();
        if (saveStatus == SaveStatus.Invalidated
            || saveStatus == SaveStatus.Erased
            || !saveStatus.hasBeen(Status.PreAccepted))
            return null;
        if (cmd.partialTxn() == null)
            return null;

        var keysOrRanges = cmd.participants().touches().toRanges();
        if (keysOrRanges.domain() != Domain.Range)
            throw new AssertionError(String.format("Txn keys are not range for %s", cmd.partialTxn()));
        Ranges ranges = (Ranges) keysOrRanges;

        ranges = ranges.slice(cacheRanges, Routables.Slice.Minimal);
        if (ranges.isEmpty())
            return null;

        if (redundantBefore != null)
        {
            Ranges newRanges = redundantBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                if (e.gcBefore.compareTo(cmd.txnId()) < 0)
                    return accum;
                return accum.without(Ranges.of(new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end)));
            }, ranges, ignore -> false);

            if (newRanges.isEmpty())
                return null;
        }

        var partialDeps = cmd.partialDeps();
        boolean hasAsDep = findAsDep != null && partialDeps != null && partialDeps.rangeDeps.intersects(findAsDep, ranges);
        return new Summary(cmd.txnId(), cmd.executeAt(), saveStatus, ranges, findAsDep, hasAsDep);
    }

    private static Summary create(SavedCommand.MinimalCommand cmd, Ranges cacheRanges, @Nullable RedundantBefore redundantBefore)
    {
        //TODO (required, correctness): C* did Invalidated, accord-core did Erased... what is correct?
        SaveStatus saveStatus = cmd.saveStatus;
        if (saveStatus == null
            || saveStatus == SaveStatus.Invalidated
            || saveStatus == SaveStatus.Erased
            || !saveStatus.hasBeen(Status.PreAccepted))
            return null;

        if (cmd.participants == null)
            return null;

        var keysOrRanges = cmd.participants.touches().toRanges();
        if (keysOrRanges.domain() != Domain.Range)
            throw new AssertionError(String.format("Txn keys are not range for %s", cmd.participants));
        Ranges ranges = (Ranges) keysOrRanges;

        ranges = ranges.slice(cacheRanges, Routables.Slice.Minimal);
        if (ranges.isEmpty())
            return null;

        if (redundantBefore != null)
        {
            Ranges newRanges = redundantBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                if (e.gcBefore.compareTo(cmd.txnId) < 0)
                    return accum;
                return accum.without(Ranges.of(new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end)));
            }, ranges, ignore -> false);

            if (newRanges.isEmpty())
                return null;
        }

        return new Summary(cmd.txnId, cmd.executeAt, saveStatus, ranges, null, false);
    }

    public void mergeHistoricalTransaction(TxnId txnId, Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
    {
        historicalTransaction.merge(txnId, ranges, remappingFunction);
    }

    public static class Summary
    {
        public final TxnId txnId;
        @Nullable public final Timestamp executeAt;
        @Nullable public final SaveStatus saveStatus;
        @Nullable public final Ranges ranges;

        // TODO (required): this logic is still broken (was already): needs to consider exact range matches
        public final TxnId findAsDep;
        public final boolean hasAsDep;

        @VisibleForTesting
        Summary(TxnId txnId, @Nullable Timestamp executeAt, SaveStatus saveStatus, Ranges ranges, TxnId findAsDep, boolean hasAsDep)
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
            return new Summary(txnId, executeAt, saveStatus, ranges.slice(slice, Routables.Slice.Minimal), findAsDep, hasAsDep);
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
}
