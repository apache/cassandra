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

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;

import accord.local.MaxDecidedRX.DecidedRX;
import accord.primitives.Participants;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

import static org.apache.cassandra.index.accord.RouteIndexFormat.deserializeTouches;

public class RangeMemoryIndex
{

    @GuardedBy("this")
    private final Map<Key, Group> map = new HashMap<>();

    private static class Group
    {
        private RangeTree<byte[], Range, DecoratedKey> tree = createRangeTree();
        public byte[] minTerm, maxTerm;
        public TxnId minTxnId = TxnId.MAX;
        public TxnId maxTxnId = TxnId.NONE;
        public @Nullable TxnId maxRXId = TxnId.NONE;

        void add(Range range, DecoratedKey key, TxnId txnId, byte[] start, byte[] end)
        {
            tree.add(range, key);
            minTerm = minTerm == null ? start : ByteArrayUtil.compareUnsigned(minTerm, 0, start, 0, minTerm.length) > 0 ? start : minTerm;
            maxTerm = maxTerm == null ? end : ByteArrayUtil.compareUnsigned(maxTerm, 0, end, 0, maxTerm.length) < 0 ? end : maxTerm;
            if (minTxnId.compareTo(txnId) > 0)
                minTxnId = txnId;
            if (maxTxnId.compareTo(txnId) < 0)
                maxTxnId = txnId;
            if (maxRXId != null)
            {
                if (!txnId.is(Txn.Kind.ExclusiveSyncPoint)) maxRXId = null;
                else if (maxRXId.compareTo(txnId) < 0)      maxRXId = txnId;
            }
        }

        void search(byte[] start, byte[] end,
                    TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                    Consumer<Map.Entry<RangeMemoryIndex.Range, DecoratedKey>> fn)
        {
            if (this.minTxnId.compareTo(maxTxnId) > 0 || this.maxTxnId.compareTo(minTxnId) < 0)
                return;
            if (maxRXId != null && !RouteIndexFormat.includeByDecidedRX(decidedRX, maxRXId))
                return;
            tree.search(new Range(start, end), e -> {
                TxnId id = AccordKeyspace.JournalColumns.getJournalKey(e.getValue()).id;
                if (minTxnId.compareTo(id) > 0 || maxTxnId.compareTo(id) < 0) return;
                fn.accept(e);
            });
        }

        void searchToken(byte[] key,
                         TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                         Consumer<Map.Entry<RangeMemoryIndex.Range, DecoratedKey>> fn)
        {
            if (this.minTxnId.compareTo(maxTxnId) > 0 || this.maxTxnId.compareTo(minTxnId) < 0)
                return;
            if (maxRXId != null && !RouteIndexFormat.includeByDecidedRX(decidedRX, maxRXId))
                return;
            tree.searchToken(key, e -> {
                TxnId id = AccordKeyspace.JournalColumns.getJournalKey(e.getValue()).id;
                if (minTxnId.compareTo(id) > 0 || maxTxnId.compareTo(id) < 0) return;
                fn.accept(e);
            });
        }
    }

    private static RangeTree<byte[], Range, DecoratedKey> createRangeTree()
    {
        return new RTree<>((a, b) -> ByteArrayUtil.compareUnsigned(a, 0, b, 0, a.length), new RangeTree.Accessor<>()
        {
            @Override
            public byte[] start(Range range)
            {
                return range.start;
            }

            @Override
            public byte[] end(Range range)
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
            public boolean intersects(Range range, byte[] start, byte[] end)
            {
                return range.intersects(start, end);
            }

            @Override
            public boolean intersects(Range left, Range right)
            {
                return left.intersects(right.start, right.end);
            }
        });
    }

    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        Participants<?> route;
        try
        {
            route = deserializeTouches(value);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        TxnId txnId = AccordKeyspace.JournalColumns.getJournalKey(key).id;
        return add(key, txnId, route);
    }

    public synchronized long add(DecoratedKey key, TxnId txnId, Participants<?> route)
    {
        if (route == null || route.domain() != Routable.Domain.Range)
            return 0;
        long sum = 0;
        for (Unseekable keyOrRange : route)
            sum += add(key, txnId, keyOrRange);
        return sum;
    }

    @VisibleForTesting
    protected long add(DecoratedKey key, TxnId txnId, Unseekable keyOrRange)
    {
        if (keyOrRange.domain() != Routable.Domain.Range)
            throw new IllegalArgumentException("Unexpected domain: " + keyOrRange.domain());
        TokenRange ts = (TokenRange) keyOrRange;

        int storeId = AccordKeyspace.JournalColumns.getStoreId(key);
        TableId tableId = ts.table();
        byte[] start = OrderedRouteSerializer.serializeTokenOnly(ts.start());
        byte[] end = OrderedRouteSerializer.serializeTokenOnly(ts.end());
        Range range = new Range(start, end);
        map.computeIfAbsent(new Key(storeId, tableId), ignore -> new Group()).add(range, key, txnId, start, end);
        return TableId.EMPTY_SIZE + range.unsharedHeapSize();
    }

    public synchronized void search(int storeId, TableId tableId,
                                    byte[] start, byte[] end,
                                    TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                                    Consumer<ByteBuffer> onMatch)
    {
        Group group = map.get(new Key(storeId, tableId));
        if (group == null) return;
        if (group.tree.isEmpty()) return;

        group.search(start, end, minTxnId, maxTxnId, decidedRX, e -> onMatch.accept(e.getValue().getKey()));
    }

    public synchronized void search(int storeId, TableId tableId, byte[] key,
                                    TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX,
                                    Consumer<ByteBuffer> onMatch)
    {
        Group group = map.get(new Key(storeId, tableId));
        if (group == null) return;
        if (group.tree.isEmpty()) return;

        group.searchToken(key, minTxnId, maxTxnId, decidedRX, e -> onMatch.accept(e.getValue().getKey()));
    }

    public synchronized boolean isEmpty()
    {
        return map.isEmpty();
    }

    public synchronized Segment write(IndexDescriptor id) throws IOException
    {
        if (map.isEmpty())
            throw new AssertionError("Unable to write empty index");
        Map<Key, Segment.Metadata> output = new HashMap<>();

        List<Key> groups = new ArrayList<>(map.keySet());
        groups.sort(Comparator.naturalOrder());

        for (Key key : groups)
        {
            Group group = map.get(key);
            RangeTree<byte[], Range, DecoratedKey> submap = group.tree;
            if (submap.isEmpty()) // is this possible?  put here for safty so list is never empty
                continue;

            //TODO (performance): if the RangeTree can return the data in sorted order, then this local can become faster
            // Right now the code is based off RTree, which is undefined order, so we must iterate then sort; in testing this is a good chunk of the time of this method
            List<CheckpointIntervalArrayIndex.Interval> list = submap.stream()
                                                                     .map(e -> new CheckpointIntervalArrayIndex.Interval(e.getKey().start, e.getKey().end, ByteBufferUtil.getArray(e.getValue().getKey())))
                                                                     .sorted(Comparator.naturalOrder())
                                                                     .collect(Collectors.toList());

            CheckpointIntervalArrayIndex.SegmentWriter writer = new CheckpointIntervalArrayIndex.SegmentWriter(id, list.get(0).start.length, list.get(0).value.length);
            EnumMap<IndexDescriptor.IndexComponent, Segment.ComponentMetadata> meta = writer.write(list.toArray(CheckpointIntervalArrayIndex.Interval[]::new));
            if (meta.isEmpty()) // don't include empty segments
                continue;
            output.put(key, new Segment.Metadata(meta, group.minTerm, group.maxTerm, group.minTxnId, group.maxTxnId, group.maxRXId == null ? TxnId.NONE : group.maxRXId));
        }

        return new Segment(output);
    }

    private static class Range implements Comparable<Range>, IMeasurableMemory
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Range(null, null));

        private final byte[] start, end;

        private Range(byte[] start, byte[] end)
        {
            this.start = start;
            this.end = end;
        }

        @Override
        public int compareTo(Range other)
        {
            int rc = ByteArrayUtil.compareUnsigned(start, 0, other.start, 0, start.length);
            if (rc == 0)
                rc = ByteArrayUtil.compareUnsigned(end, 0, other.end, 0, end.length);
            return rc;
        }

        @Override
        public long unsharedHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOfArray(start) * 2;
        }

        public boolean intersects(byte[] start, byte[] end)
        {
            if (ByteArrayUtil.compareUnsigned(this.start, 0, end, 0, end.length) >= 0)
                return false;
            if (ByteArrayUtil.compareUnsigned(this.end, 0, start, 0, start.length) <= 0)
                return false;
            return true;
        }
    }
}
