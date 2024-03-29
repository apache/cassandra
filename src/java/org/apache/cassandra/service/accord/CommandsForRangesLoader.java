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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import accord.local.Command;
import accord.local.DurableBefore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.index.accord.RoutesSearcher;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.Pair;

public class CommandsForRangesLoader
{
    private final RoutesSearcher searcher = new RoutesSearcher();
    //TODO (now, durability): find solution for this...
    private final Map<TxnId, Ranges> historicalTransaction = new HashMap<>();
    private final AccordCommandStore store;

    public CommandsForRangesLoader(AccordCommandStore store)
    {
        this.store = store;
    }

    public AsyncResult<Pair<Watcher, NavigableMap<TxnId, Summary>>> get(Ranges ranges)
    {
        var watcher = fromCache(ranges);
        var before = ImmutableMap.copyOf(watcher.get());
        return AsyncChains.ofCallable(Stage.READ.executor(), () -> get(ranges, before))
                          .map(map -> Pair.create(watcher, map), store)
               .beginAsResult();
    }

    private NavigableMap<TxnId, Summary> get(Ranges ranges, Map<TxnId, Summary> cacheHits)
    {
        Set<TxnId> matches = new HashSet<>();
        for (Range range : ranges)
            matches.addAll(intersects(range));
        if (matches.isEmpty())
            return new TreeMap<>();
        return load(ranges, cacheHits, matches);
    }

    private Collection<TxnId> intersects(Range range)
    {
        assert range instanceof TokenRange : "Require TokenRange but given " + range.getClass();
        Set<TxnId> intersects = searcher.intersects(store.id(), (TokenRange) range);
        if (!historicalTransaction.isEmpty())
        {
            if (intersects.isEmpty())
                intersects = new HashSet<>();
            for (var e : historicalTransaction.entrySet())
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

        private NavigableMap<TxnId, Summary> summaries = null;
        private List<AccordCachingState<TxnId, Command>> needToDoubleCheck = null;

        public Watcher(Ranges ranges)
        {
            this.ranges = ranges;
        }

        public NavigableMap<TxnId, Summary> get()
        {
            return summaries == null ? Collections.emptyNavigableMap() : summaries;
        }

        @Override
        public void onAdd(AccordCachingState<TxnId, Command> n)
        {
            if (n.key().domain() != Routable.Domain.Range)
                return;
            var state = n.state();
            if (state instanceof AccordCachingState.Loading)
            {
                if (needToDoubleCheck == null)
                    needToDoubleCheck = new ArrayList<>();
                needToDoubleCheck.add(n);
                return;
            }
            //TODO (now): include FailedToSave?  Most likely need to, but need to improve test coverage to have failed writes
            if (!(state instanceof AccordCachingState.Loaded
                  || state instanceof AccordCachingState.Modified
                  || state instanceof AccordCachingState.Saving))
                return;

            var cmd = state.get();
            if (cmd == null)
                return;
            Summary summary = create(cmd, ranges, null);
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

    private Watcher fromCache(Ranges ranges)
    {
        Watcher watcher = new Watcher(ranges);
        store.commandCache().stream().forEach(watcher::onAdd);
        store.commandCache().register(watcher);
        return watcher;
    }

    private NavigableMap<TxnId, Summary> load(Ranges ranges, Map<TxnId, Summary> cacheHits, Collection<TxnId> possibleTxns)
    {
        //TODO (now): this logic is kinda duplicate of org.apache.cassandra.service.accord.CommandsForRange.mapReduce
        // should figure out if this can be improved... also what is correct?
        var durableBefore = store.durableBefore();
        NavigableMap<TxnId, Summary> map = new TreeMap<>();
        for (TxnId txnId : possibleTxns)
        {
            if (cacheHits.containsKey(txnId))
                continue;
            var cmd = store.loadCommand(txnId);
            if (cmd == null)
                continue; // unknown command
            var summary = create(cmd, ranges, durableBefore);
            if (summary == null)
                continue;
            map.put(txnId, summary);
        }
        return map;
    }

    private static Summary create(Command cmd, Ranges cacheRanges, @Nullable DurableBefore durableBefore)
    {
        //TODO (now, correctness): C* did Invalidated, accord-core did Erased... what is correct?
        SaveStatus saveStatus = cmd.saveStatus();
        if (saveStatus == SaveStatus.Invalidated
            || saveStatus == SaveStatus.Erased
            || !saveStatus.hasBeen(Status.PreAccepted))
            return null;
        if (cmd.partialTxn() == null)
            return null;

        var keysOrRanges = cmd.partialTxn().keys();
        if (keysOrRanges.domain() != Routable.Domain.Range)
            throw new AssertionError(String.format("Txn keys are not range for %s", cmd.partialTxn()));
        Ranges ranges = (Ranges) keysOrRanges;

        if (!ranges.intersects(cacheRanges))
            return null;

        if (durableBefore != null)
        {
            Ranges durableAlready = Ranges.of(durableBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                if (e.universalBefore.compareTo(cmd.txnId()) < 0)
                    return accum;
                accum.add(new TokenRange((AccordRoutingKey) start, (AccordRoutingKey) end));
                return accum;
            }, new ArrayList<Range>(), ignore -> false).toArray(Range[]::new));
            Ranges newRanges = ranges.subtract(durableAlready);

            if (newRanges.isEmpty())
                return null;
        }

        var partialDeps = cmd.partialDeps();
        List<TxnId> deps = partialDeps == null ? Collections.emptyList() : partialDeps.txnIds();
        return new Summary(cmd.txnId(), cmd.executeAt(), saveStatus, ranges, deps);
    }

    public void mergeHistoricalTransaction(TxnId txnId, Ranges ranges, BiFunction<? super Ranges, ? super Ranges, ? extends Ranges> remappingFunction)
    {
        historicalTransaction.merge(txnId, ranges, remappingFunction);
    }

    public static class Summary
    {
        public final TxnId txnId;
        @Nullable
        public final Timestamp executeAt;
        public final SaveStatus saveStatus;
        public final Ranges ranges;
        public final List<TxnId> depsIds;

        private Summary(TxnId txnId, @Nullable Timestamp executeAt, SaveStatus saveStatus, Ranges ranges, List<TxnId> depsIds)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.saveStatus = saveStatus;
            this.ranges = ranges;
            this.depsIds = depsIds;
        }

        @Override
        public String toString()
        {
            return "Summary{" +
                   "txnId=" + txnId +
                   ", executeAt=" + executeAt +
                   ", saveStatus=" + saveStatus +
                   ", ranges=" + ranges +
                   ", depsIds=" + depsIds +
                   '}';
        }
    }
}
