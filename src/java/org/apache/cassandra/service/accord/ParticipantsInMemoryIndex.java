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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import accord.impl.CommandChange;
import accord.local.StoreParticipants;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.index.accord.OrderedRouteSerializer;
import org.apache.cassandra.index.accord.ParticipantsJournalIndex;
import org.apache.cassandra.index.accord.RouteIndexFormat;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

public class ParticipantsInMemoryIndex<K extends JournalKey, V> implements AccordJournal.Listener<V>, RangeSearcher
{
    private final Long2ObjectHashMap<ParticipantsInMemorySegmentIndex> segmentIndexes = new Long2ObjectHashMap<>();

    @Override
    public void onWrite(JournalKey id, Journal.Writer writer, Set<Integer> hosts, RecordPointer pointer)
    {
        if (!ParticipantsJournalIndex.allowed(id))
            return;
        AccordJournal.Writer saveCommandWriter = (AccordJournal.Writer) writer;
        if (!saveCommandWriter.hasField(CommandChange.Fields.PARTICIPANTS))
            return;
        StoreParticipants participants = saveCommandWriter.after.participants();
        Route<?> route = participants.route();
        if (route != null)
            update(pointer.segment, id.commandStoreId, id.id, route);
    }

    public synchronized void update(long segment, int commandStoreId, TxnId id, Route<?> route)
    {
        if (!ParticipantsJournalIndex.allowed(id))
            return;
        Invariants.nonNull(route, "route");
        segmentIndexes.computeIfAbsent(segment, ParticipantsInMemorySegmentIndex::new).add(commandStoreId, id, route);
    }

    public void update(long segment, K id, ByteBuffer buffer, int userVersion)
    {
        if (!ParticipantsJournalIndex.allowed(id))
            return;
        StoreParticipants participants = RouteIndexFormat.extract(id.id, buffer, userVersion).participants();
        if (participants == null || participants.route() == null)
            return;
        update(segment, id.commandStoreId, id.id, participants.route());
    }

    @Override
    public synchronized void onCompact(Collection<StaticSegment<JournalKey, V>> oldSegments,
                                       Collection<StaticSegment<JournalKey, V>> compactedSegments)
    {
        oldSegments.forEach(s -> segmentIndexes.remove(s.id()));
    }

    public NavigableMap<IndexRange, Set<TxnId>> search(int storeId, AccordRoutingKey key)
    {
        return search(storeId, key.table(), OrderedRouteSerializer.serializeRoutingKeyNoTable(key));
    }

    private synchronized NavigableMap<IndexRange, Set<TxnId>> search(int storeId, TableId tableId, byte[] key)
    {
        TreeMap<IndexRange, Set<TxnId>> matches = new TreeMap<>();
        segmentIndexes.values().forEach(s -> s.search(storeId, tableId, key, e -> matches.computeIfAbsent(e.getKey(), i -> new HashSet<>()).add(e.getValue())));
        return matches.isEmpty() ? Collections.emptyNavigableMap() : matches;
    }

    public NavigableMap<IndexRange, Set<TxnId>> search(int storeId, AccordRoutingKey start, AccordRoutingKey end)
    {
        return search(storeId, start.table(), OrderedRouteSerializer.serializeRoutingKeyNoTable(start), OrderedRouteSerializer.serializeRoutingKeyNoTable(end));
    }

    private synchronized NavigableMap<IndexRange, Set<TxnId>> search(int storeId, TableId tableId, byte[] start, byte[] end)
    {
        TreeMap<IndexRange, Set<TxnId>> matches = new TreeMap<>();
        segmentIndexes.values().forEach(s -> s.search(storeId, tableId, start, end, e -> matches.computeIfAbsent(e.getKey(), i -> new HashSet<>()).add(e.getValue())));
        return matches.isEmpty() ? Collections.emptyNavigableMap() : matches;
    }

    public synchronized void truncateForTesting()
    {
        segmentIndexes.clear();
    }

    @Override
    public void intersects(int commandStoreId, TokenRange range, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
    {
        NavigableMap<IndexRange, Set<TxnId>> result = search(commandStoreId, range.start(), range.end());
        TreeSet<TxnId> matches = new TreeSet<>();
        result.values().forEach(s -> matches.addAll(s));
        consume(matches.iterator(), minTxnId, maxTxnId, forEach);
    }

    @Override
    public void intersects(int commandStoreId, AccordRoutingKey key, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
    {
        NavigableMap<IndexRange, Set<TxnId>> result = search(commandStoreId, key);
        TreeSet<TxnId> matches = new TreeSet<>();
        result.values().forEach(s -> matches.addAll(s));
        consume(matches.iterator(), minTxnId, maxTxnId, forEach);
    }

    private void consume(Iterator<TxnId> it, TxnId minTxnId, Timestamp maxTxnId, Consumer<TxnId> forEach)
    {
        while (it.hasNext())
        {
            TxnId next = it.next();
            if (next.compareTo(minTxnId) >= 0 && next.compareTo(maxTxnId) < 0)
                forEach.accept(next);
        }
    }

    private static class ParticipantsInMemorySegmentIndex
    {
        private final Int2ObjectHashMap<StoreIndex> storeIndexes = new Int2ObjectHashMap<>();

        private ParticipantsInMemorySegmentIndex(long segment)
        {
        }

        public void add(int commandStoreId, TxnId id, Route<?> route)
        {
            storeIndexes.computeIfAbsent(commandStoreId, StoreIndex::new).add(id, route);
        }

        public void search(int storeId, TableId tableId, byte[] start, byte[] end, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            StoreIndex idx = storeIndexes.get(storeId);
            if (idx == null) return;
            idx.search(tableId, start, end, fn);
        }

        public void search(int storeId, TableId tableId, byte[] key, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            StoreIndex idx = storeIndexes.get(storeId);
            if (idx == null) return;
            idx.search(tableId, key, fn);
        }
    }

    private static class StoreIndex
    {
        private final Map<TableId, TableIndex> tableIndex = new HashMap<>();

        private StoreIndex(int commandStoreId)
        {
        }

        public void add(TxnId id, Route<?> route)
        {
            for (Unseekable keyOrRange : route)
                add(id, keyOrRange);
        }

        private void add(TxnId id, Unseekable keyOrRange)
        {
            if (keyOrRange.domain() != Routable.Domain.Range)
                throw new IllegalArgumentException("Unexpected domain: " + keyOrRange.domain());
            TokenRange ts = (TokenRange) keyOrRange;
            TableId tableId = ts.table();
            tableIndex.computeIfAbsent(tableId, TableIndex::new).add(id, ts);
        }

        public void search(TableId tableId, byte[] start, byte[] end, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            TableIndex index = tableIndex.get(tableId);
            if (index == null) return;
            index.search(start, end, fn);
        }

        public void search(TableId tableId, byte[] key, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            TableIndex index = tableIndex.get(tableId);
            if (index == null) return;
            index.search(key, fn);
        }
    }

    private static class TableIndex
    {
        private final RangeTree<byte[], IndexRange, TxnId> index = createRangeTree();

        private TableIndex(TableId tableId)
        {
        }

        public void add(TxnId id, TokenRange ts)
        {
            byte[] start = OrderedRouteSerializer.serializeRoutingKeyNoTable(ts.start());
            byte[] end = OrderedRouteSerializer.serializeRoutingKeyNoTable(ts.end());
            IndexRange range = new IndexRange(start, end);

            index.add(range, id);
        }

        public void search(byte[] start, byte[] end, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            index.search(new IndexRange(start, end), fn);
        }

        public void search(byte[] key, Consumer<Map.Entry<IndexRange, TxnId>> fn)
        {
            index.searchToken(key, fn);
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
