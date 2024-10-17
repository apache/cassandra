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
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.index.accord.RoutesSearcher;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.AccordCommandStore.ExclusiveCaches;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

public class CommandsForRangesLoader implements AccordCache.Listener<TxnId, Command>
{
    private final AccordCommandStore commandStore;
    private final RoutesSearcher searcher = new RoutesSearcher();
    private final NavigableMap<TxnId, Ranges> transitive = new TreeMap<>();
    private final ObjectHashSet<TxnId> cachedRangeTxns = new ObjectHashSet<>();

    public CommandsForRangesLoader(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
        try (ExclusiveCaches caches = commandStore.lockCaches())
        {
            caches.commands().register(this);
        }
    }

    @Override
    public void onAdd(AccordCacheEntry<TxnId, Command> state)
    {
        TxnId txnId = state.key();
        if (txnId.is(Domain.Range))
            cachedRangeTxns.add(txnId);
    }

    @Override
    public void onEvict(AccordCacheEntry<TxnId, Command> state)
    {
        TxnId txnId = state.key();
        if (txnId.is(Domain.Range))
            cachedRangeTxns.remove(txnId);
    }

    public Loader loader(@Nullable TxnId primaryTxnId, KeyHistory keyHistory, Ranges ranges)
    {
        RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
        TxnId minTxnId = redundantBefore.min(ranges, e -> e.gcBefore);
        Timestamp maxTxnId = primaryTxnId == null || keyHistory == KeyHistory.RECOVER || !primaryTxnId.is(ExclusiveSyncPoint) ? Timestamp.MAX : primaryTxnId;
        TxnId findAsDep = primaryTxnId != null && keyHistory == KeyHistory.RECOVER ? primaryTxnId : null;
        return new Loader(ranges, redundantBefore, minTxnId, maxTxnId, findAsDep);
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
            return new Summary(txnId, executeAt, saveStatus, ranges == null ? null : ranges.slice(slice, Minimal), findAsDep, hasAsDep);
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

    public class Loader
    {
        final Ranges searchRanges;
        final RedundantBefore redundantBefore;
        final TxnId minTxnId;
        final Timestamp maxTxnId;
        @Nullable final TxnId findAsDep;

        public Loader(Ranges searchRanges, RedundantBefore redundantBefore, TxnId minTxnId, Timestamp maxTxnId, @Nullable TxnId findAsDep)
        {
            this.searchRanges = searchRanges;
            this.redundantBefore = redundantBefore;
            this.minTxnId = minTxnId;
            this.maxTxnId = maxTxnId;
            this.findAsDep = findAsDep;
        }

        public Collection<TxnId> intersects()
        {
            ObjectHashSet<TxnId> txnIds = new ObjectHashSet<>();
            for (Range range : searchRanges)
            {
                searcher.intersects(commandStore.id(), (TokenRange) range, minTxnId, maxTxnId, txnIds::add);
            }
            if (!transitive.isEmpty())
            {
                for (var e : transitive.tailMap(minTxnId, true).entrySet())
                {
                    if (e.getValue().intersects(searchRanges))
                        txnIds.add(e.getKey());
                }
            }
            return txnIds;
        }

        public void forEachInCache(Consumer<Summary> forEach, Caches caches)
        {
            for (TxnId txnId : cachedRangeTxns)
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
                SavedCommand.MinimalCommand cmd = commandStore.loadMinimal(txnId);
                return cmd == null ? null : from(cmd);
            }
            else
            {
                Command cmd = commandStore.loadCommand(txnId);
                return cmd == null ? null : from(cmd);
            }
        }

        public Summary from(AccordCacheEntry<TxnId, Command> state)
        {
            if (state.key().domain() != Domain.Range)
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
            if (saveStatus == SaveStatus.Invalidated
                || saveStatus == SaveStatus.Erased
                || !saveStatus.hasBeen(Status.PreAccepted))
                return null;

            if (participants == null)
                return null;

            Ranges keysOrRanges = participants.touches().toRanges();
            if (keysOrRanges.domain() != Domain.Range)
                throw new AssertionError(String.format("Txn keys are not range for %s", participants));
            Ranges ranges = keysOrRanges;

            ranges = ranges.slice(searchRanges, Minimal);
            if (ranges.isEmpty())
                return null;

            if (redundantBefore != null)
            {
                Ranges newRanges = redundantBefore.foldlWithBounds(ranges, (e, accum, start, end) -> {
                    if (e.gcBefore.compareTo(txnId) < 0)
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
