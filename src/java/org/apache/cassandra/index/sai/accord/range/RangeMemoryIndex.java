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

package org.apache.cassandra.index.sai.accord.range;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Unseekable;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.accord.SaiSerializer;
import org.apache.cassandra.index.sai.accord.UnseekableMemoryIndex;
import org.apache.cassandra.index.sai.accord.range.CheckpointIntervalArrayIndex.Interval;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.memory.InMemoryKeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.RTree;

import static org.apache.cassandra.index.sai.accord.SaiSerializer.deserializeRoutingKey;
import static org.apache.cassandra.index.sai.accord.SaiSerializer.unwrap;

public class RangeMemoryIndex extends UnseekableMemoryIndex
{
    @GuardedBy("this")
    private final Map<Group, RTree<byte[], Range, PrimaryKey>> map = new HashMap<>();
    @GuardedBy("this")
    private byte[] minTerm, maxTerm;

    protected RangeMemoryIndex(StorageAttachedIndex index)
    {
        super(index);
    }

    private static RTree<byte[], Range, PrimaryKey> createRTree()
    {
        return new RTree<>((a, b) -> ByteArrayUtil.compareUnsigned(a, 0, b, 0, a.length), new RTree.Accessor<>()
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
                throw new UnsupportedOperationException();
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

    @Override
    protected boolean isSupported(Routable.Domain domain)
    {
        return domain == Routable.Domain.Range;
    }

    public synchronized long add(PrimaryKey key, Route<?> route)
    {
        long sum = 0;
        for (var keyOrRange : route)
            sum += add(key, keyOrRange);
        return sum;
    }

    @Override
    protected long add(PrimaryKey pk, Unseekable keyOrRange)
    {
        switch (keyOrRange.domain())
        {
            case Key:
                return 0;
            case Range:
                TokenRange ts = (TokenRange) keyOrRange;

                var storeId = AccordKeyspace.CommandRows.getStoreId(pk.partitionKey());
                var tableId = ts.table();
                var group = new Group(storeId, tableId);
                var range = new Range(unwrap((AccordRoutingKey) ts.start()), unwrap((AccordRoutingKey) ts.end()));
                map.computeIfAbsent(group, ignore -> createRTree()).add(range, pk);

                var start = ByteBufferUtil.getArray(SaiSerializer.serializeRoutingKey((AccordRoutingKey) ts.start()));
                var end = ByteBufferUtil.getArray(SaiSerializer.serializeRoutingKey((AccordRoutingKey) ts.end()));
                minTerm = minTerm == null ? start : ByteArrayUtil.compareUnsigned(minTerm, 0, start, 0, minTerm.length) > 0 ? start : minTerm;
                maxTerm = maxTerm == null ? end : ByteArrayUtil.compareUnsigned(maxTerm, 0, end, 0, maxTerm.length) < 0 ? end : maxTerm;
                return TableId.EMPTY_SIZE + range.unsharedHeapSize();
            default:
                throw new IllegalArgumentException("Unknown domain: " + keyOrRange.domain());
        }
    }

    @Override
    public synchronized KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        assert expression.getIndexOperator() == Expression.IndexOperator.RANGE : String.format("Unsupported operator %s", expression.getIndexOperator());
        Optional<RowFilter.Expression> maybeStore = queryContext.readCommand.rowFilter().getExpressions().stream().filter(e -> e.column().name.toString().equals("store_id") && e.operator() == Operator.EQ).findFirst();
        if (maybeStore.isEmpty())
            throw new AssertionError("store_id is not present in the expression: " + expression);
        int storeId = Int32Type.instance.compose(maybeStore.get().getIndexValue());
        TableId table;
        byte[] start;
        {
            var e = deserializeRoutingKey(expression.lower.value.raw);
            table = e.table();
            start = unwrap(e);
        }
        byte[] end = unwrap(deserializeRoutingKey(expression.upper.value.raw));
        var rangesToPks = map.get(new Group(storeId, table));
        if (rangesToPks == null || rangesToPks.isEmpty())
            return KeyRangeIterator.empty();
        var matches = search(rangesToPks, start, end);
        if (matches.isEmpty())
            return KeyRangeIterator.empty();
        TreeSet<PrimaryKey> pks = new TreeSet<>();
        matches.values().forEach(pks::addAll);
        return new InMemoryKeyRangeIterator(pks);
    }

    private TreeMap<Range, Set<PrimaryKey>> search(RTree<byte[], Range, PrimaryKey> tokensToPks, byte[] start, byte[] end)
    {

        TreeMap<Range, Set<PrimaryKey>> matches = new TreeMap<>();
        tokensToPks.search(new Range(start, end), e -> matches.computeIfAbsent(e.getKey(), ignore -> new HashSet<>()).add(e.getValue()));
        return matches;
    }

    @Override
    public synchronized boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public synchronized ByteBuffer getMinTerm()
    {
        return ByteBuffer.wrap(minTerm);
    }

    @Override
    public synchronized ByteBuffer getMaxTerm()
    {
        return ByteBuffer.wrap(maxTerm);
    }

    @Override
    public synchronized SegmentMetadata.ComponentMetadataMap writeDirect(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier, Function<PrimaryKey, Integer> postingTransformer) throws IOException
    {
        if (map.isEmpty())
            throw new AssertionError("Unable to write empty index");
        List<Group> groups = new ArrayList<>(map.keySet());
        groups.sort(Comparator.naturalOrder());

        Map<Group, SegmentMetadata.ComponentMetadataMap> tableOffsets = new HashMap<>();
        for (var group : groups)
        {
            var submap = map.get(group);
            if (submap.isEmpty()) // is this possible?  put here for safty so list is never empty
                continue;
            List<Interval> list = submap.stream()
                                        .map(e -> new Interval(e.getKey().start, e.getKey().end, ByteBufferUtil.getArray(e.getValue().partitionKey().getKey())))
                                        .sorted(Comparator.naturalOrder())
                                        .collect(Collectors.toList());

            var writer = new CheckpointIntervalArrayIndex.SegmentWriter(indexDescriptor, indexIdentifier, list.get(0).start.length, list.get(0).value.length);
            var meta = writer.writeCompleteSegment(list.toArray(Interval[]::new));
            tableOffsets.put(group, meta);
        }

        return GroupToCheckpointIntervals.writeCompleteSegment(indexDescriptor, indexIdentifier, tableOffsets);
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
