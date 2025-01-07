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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
import accord.local.KeyHistory;
import accord.local.RedundantBefore;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.index.accord.RoutesSearcher;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

import static accord.local.CommandSummaries.SummaryStatus.NOT_ACCEPTED;

// TODO (required): move to accord-core, merge with existing logic there
public class CommandsForRanges extends TreeMap<Timestamp, Summary> implements CommandSummaries.Snapshot
{
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
            return Loader.loader(redundantBefore, primaryTxnId, keyHistory, keysOrRanges, this::newLoader);
        }

        private Loader newLoader(Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKind, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            return new Loader(this, searchKeysOrRanges, redundantBefore, testKind, minTxnId, maxTxnId, findAsDep);
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

    public static class Loader extends Summary.Loader
    {
        private final Manager manager;

        public Loader(Manager manager, Unseekables<?> searchKeysOrRanges, RedundantBefore redundantBefore, Kinds testKinds, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            super(searchKeysOrRanges, redundantBefore, testKinds, minTxnId, maxTxnId, findAsDep);
            this.manager = manager;
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
                Summary summary = ifRelevant(state);
                if (summary != null)
                    forEach.accept(summary);
            }
        }

        public Summary load(TxnId txnId)
        {
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

            Ranges ranges = manager.transitive.get(txnId);
            if (ranges == null)
                return null;

            ranges = ranges.intersecting(searchKeysOrRanges);
            if (ranges.isEmpty())
                return null;

            return new Summary(txnId, txnId, NOT_ACCEPTED, ranges, null, null);
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
            Invariants.checkState(findAsDep == null);
            return ifRelevant(cmd.txnId, cmd.executeAt, cmd.saveStatus, cmd.participants, null);
        }
    }
}
