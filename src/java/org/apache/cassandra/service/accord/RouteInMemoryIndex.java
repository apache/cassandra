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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.local.MaxDecidedRX;
import accord.local.MaxDecidedRX.DecidedRX;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.index.accord.RouteIndexFormat;
import org.apache.cassandra.index.accord.RouteJournalIndex;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

import static accord.primitives.Routable.Domain.Range;

public class RouteInMemoryIndex<V> implements RangeSearcher
{
    private final Long2ObjectHashMap<SegmentIndex> segmentIndexes = new Long2ObjectHashMap<>();

    public synchronized void update(long segment, int commandStoreId, TxnId id, Route<?> route)
    {
        if (!RouteJournalIndex.allowed(id))
            return;
        Invariants.nonNull(route, "route");
        segmentIndexes.computeIfAbsent(segment, SegmentIndex::new).add(commandStoreId, id, route);
    }

    public synchronized void remove(Collection<StaticSegment<JournalKey, V>> oldSegments)
    {
        // As of this writing compact in accord journal takes StaticSegments, writes them to a SSTable, and pushes to a table;
        // it then stops managing those segments... for this reason compactedSegments is normally empty and none of the
        // oldSegments are expected to be tracked anymore, so this index should remove the reference (there is normal table 2i to pick up the job)
        oldSegments.forEach(s -> segmentIndexes.remove(s.id()));
    }

    @VisibleForTesting
    public synchronized void removeForTests(Collection<Long> oldSegments)
    {
        oldSegments.forEach(segmentIndexes::remove);
    }

    @Override
    public RangeSearcher.Result search(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
    {
        NavigableSet<TxnId> result = search(commandStoreId, range.table(),
                                            OrderedRouteSerializer.serializeTokenOnly(range.start()),
                                            OrderedRouteSerializer.serializeTokenOnly(range.end()),
                                            minTxnId, maxTxnId, decidedRX);
        return new DefaultResult(minTxnId, maxTxnId, decidedRX, CloseableIterator.wrap(result.iterator()));
    }

    private synchronized NavigableSet<TxnId> search(int storeId, TableId tableId, byte[] start, byte[] end, TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
    {
        // store matches in a hash set so add is O(1), and the sorting is done after collecting all matches
        Set<TxnId> matches = new HashSet<>();
        segmentIndexes.values().forEach(s -> s.search(storeId, tableId, start, end, minTxnId, maxTxnId, decidedRX, e -> matches.add(e.getValue())));
        return matches.isEmpty() ? Collections.emptyNavigableSet() : new TreeSet<>(matches);
    }

    @Override
    public RangeSearcher.Result search(int commandStoreId, TokenKey key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
    {
        NavigableSet<TxnId> result = search(commandStoreId, key.table(), OrderedRouteSerializer.serializeTokenOnly(key), minTxnId, maxTxnId, decidedRX);
        return new DefaultResult(minTxnId, maxTxnId, decidedRX, CloseableIterator.wrap(result.iterator()));
    }

    private synchronized NavigableSet<TxnId> search(int storeId, TableId tableId, byte[] key, TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX)
    {
        // store matches in a hash set so add is O(1), and the sorting is done after collecting all matches
        Set<TxnId> matches = new HashSet<>();
        segmentIndexes.values().forEach(s -> s.search(storeId, tableId, key, minTxnId, maxTxnId, decidedRX, e -> matches.add(e.getValue())));
        return matches.isEmpty() ? Collections.emptyNavigableSet() : new TreeSet<>(matches);
    }

    public synchronized void truncateForTesting()
    {
        segmentIndexes.clear();
    }

    private static class SegmentIndex
    {
        private final Int2ObjectHashMap<StoreIndex> storeIndexes = new Int2ObjectHashMap<>();

        private SegmentIndex(long segment)
        {
        }

        private void add(int commandStoreId, TxnId id, Route<?> route)
        {
            storeIndexes.computeIfAbsent(commandStoreId, i -> new StoreIndex()).add(id, route);
        }

        private void search(int storeId, TableId tableId,
                            byte[] start, byte[] end,
                            TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                            Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            StoreIndex idx = storeIndexes.get(storeId);
            if (idx == null) return;
            idx.search(tableId, start, end, minTxnId, maxTxnId, decidedRX, fn);
        }

        private void search(int storeId, TableId tableId,
                            byte[] key,
                            TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                            Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            StoreIndex idx = storeIndexes.get(storeId);
            if (idx == null) return;
            idx.search(tableId, key, minTxnId, maxTxnId, decidedRX, fn);
        }
    }

    private static class StoreIndex
    {
        private final Map<TableId, TableIndex> tableIndex = new HashMap<>();

        private StoreIndex()
        {
        }

        public void add(TxnId id, Route<?> route)
        {
            for (Unseekable keyOrRange : route)
                add(id, keyOrRange);
        }

        private void add(TxnId id, Unseekable keyOrRange)
        {
            Invariants.require(keyOrRange.domain() == Range);
            TokenRange ts = (TokenRange) keyOrRange;
            TableId tableId = ts.table();
            tableIndex.computeIfAbsent(tableId, i -> new TableIndex()).add(id, ts);
        }

        public void search(TableId tableId,
                           byte[] start, byte[] end,
                           TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                           Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            TableIndex index = tableIndex.get(tableId);
            if (index == null) return;
            index.search(start, end, minTxnId, maxTxnId, decidedRX, fn);
        }

        public void search(TableId tableId,
                           byte[] key,
                           TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX,
                           Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            TableIndex index = tableIndex.get(tableId);
            if (index == null) return;
            index.search(key, minTxnId, maxTxnId, decidedRX, fn);
        }
    }

    private static class TableIndex
    {
        private final RangeTree<byte[], IndexRange, TxnId> index = createRangeTree();
        private TxnId min = TxnId.MAX;
        private TxnId max = TxnId.NONE;
        private @Nullable TxnId maxRX = TxnId.NONE;

        private TableIndex()
        {
        }

        private void add(TxnId id, TokenRange ts)
        {
            byte[] start = OrderedRouteSerializer.serializeTokenOnly(ts.start());
            byte[] end = OrderedRouteSerializer.serializeTokenOnly(ts.end());
            IndexRange range = new IndexRange(start, end);

            index.add(range, id);
            if (min.compareTo(id) > 0)
                min = id;
            if (max.compareTo(id) < 0)
                max = id;
            if (maxRX != null)
            {
                if (!id.is(Txn.Kind.ExclusiveSyncPoint)) maxRX = null;
                else if (id.compareTo(maxRX) > 0)        maxRX = id;
            }
        }

        private void search(byte[] start, byte[] end,
                           TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                           Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            if (minTxnId.compareTo(max) > 0) return;
            if (maxTxnId.compareTo(min) < 0) return;
            if (maxRX != null && !RouteIndexFormat.includeByDecidedRX(decidedRX, maxRX)) return;
            index.search(new IndexRange(start, end), e -> {
                if (minTxnId.compareTo(e.getValue()) > 0) return;
                if (maxTxnId.compareTo(e.getValue()) < 0) return;
                fn.accept(e);
            });
        }

        private void search(byte[] key,
                            TxnId minTxnId, Timestamp maxTxnId, @Nullable MaxDecidedRX.DecidedRX decidedRX,
                            Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            if (minTxnId.compareTo(max) > 0) return;
            if (maxTxnId.compareTo(min) < 0) return;
            if (maxRX != null && !RouteIndexFormat.includeByDecidedRX(decidedRX, maxRX)) return;
            index.searchToken(key, e -> {
                if (minTxnId.compareTo(e.getValue()) > 0) return;
                if (maxTxnId.compareTo(e.getValue()) < 0) return;
                fn.accept(e);
            });
        }
    }

    private static RangeTree<byte[], IndexRange, TxnId> createRangeTree()
    {
        return new RTree<>((a, b) -> ByteArrayUtil.compareUnsigned(a, 0, b, 0, a.length), new RangeTree.Accessor<>()
        {
            @Override
            public byte[] start(IndexRange range)
            {
                return range.start;
            }

            @Override
            public byte[] end(IndexRange range)
            {
                return range.end;
            }

            @Override
            public boolean contains(byte[] start, byte[] end, byte[] bytes)
            {
                // bytes are ordered, start is exclusive, end is inclusive
                return FastByteOperations.compareUnsigned(start, bytes) < 0
                       && FastByteOperations.compareUnsigned(end, bytes) >= 0;
            }

            @Override
            public boolean intersects(IndexRange range, byte[] start, byte[] end)
            {
                return range.intersects(start, end);
            }

            @Override
            public boolean intersects(IndexRange left, IndexRange right)
            {
                return left.intersects(right.start, right.end);
            }
        });
    }
}
