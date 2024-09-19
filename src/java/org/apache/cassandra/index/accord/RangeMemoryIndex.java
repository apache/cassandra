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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Unseekable;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

public class RangeMemoryIndex
{
    @GuardedBy("this")
    private final Map<Group, RangeTree<byte[], Range, DecoratedKey>> map = new HashMap<>();
    @GuardedBy("this")
    private final Map<Group, Metadata> groupMetadata = new HashMap<>();

    private static class Metadata
    {
        public byte[] minTerm, maxTerm;
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

    public synchronized long add(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        Route<?> route;
        try
        {
            route = AccordKeyspace.deserializeRouteOrNull(value);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        return add(key, route);
    }


    public synchronized long add(DecoratedKey key, Route<?> route)
    {
        if (route.domain() != Routable.Domain.Range)
            return 0;
        long sum = 0;
        for (var keyOrRange : route)
            sum += add(key, keyOrRange);
        return sum;
    }
    
    protected long add(DecoratedKey key, Unseekable keyOrRange)
    {
        if (keyOrRange.domain() != Routable.Domain.Range)
            throw new IllegalArgumentException("Unexpected domain: " + keyOrRange.domain());
        TokenRange ts = (TokenRange) keyOrRange;

        var storeId = AccordKeyspace.CommandRows.getStoreId(key);
        var tableId = ts.table();
        var group = new Group(storeId, tableId);
        var start = OrderedRouteSerializer.serializeRoutingKeyNoTable((AccordRoutingKey) ts.start());
        var end = OrderedRouteSerializer.serializeRoutingKeyNoTable((AccordRoutingKey) ts.end());
        var range = new Range(start, end);
        map.computeIfAbsent(group, ignore -> createRangeTree()).add(range, key);
        Metadata metadata = groupMetadata.computeIfAbsent(group, ignore -> new Metadata());

        metadata.minTerm = metadata.minTerm == null ? start : ByteArrayUtil.compareUnsigned(metadata.minTerm, 0, start, 0, metadata.minTerm.length) > 0 ? start : metadata.minTerm;
        metadata.maxTerm = metadata.maxTerm == null ? end : ByteArrayUtil.compareUnsigned(metadata.maxTerm, 0, end, 0, metadata.maxTerm.length) < 0 ? end : metadata.maxTerm;
        return TableId.EMPTY_SIZE + range.unsharedHeapSize();
    }

    public NavigableSet<ByteBuffer> search(int storeId, TableId tableId, byte[] start, boolean startInclusive, byte[] end, boolean endInclusive)
    {
        var rangesToPks = map.get(new Group(storeId, tableId));
        if (rangesToPks == null || rangesToPks.isEmpty())
            return Collections.emptyNavigableSet();
        var matches = search(rangesToPks, start, end);
        if (matches.isEmpty())
            return Collections.emptyNavigableSet();
        TreeSet<ByteBuffer> pks = new TreeSet<>();
        matches.values().forEach(s -> s.forEach(d -> pks.add(d.getKey())));
        return pks;
    }

    private TreeMap<Range, Set<DecoratedKey>> search(RangeTree<byte[], Range, DecoratedKey> tokensToPks, byte[] start, byte[] end)
    {

        TreeMap<Range, Set<DecoratedKey>> matches = new TreeMap<>();
        tokensToPks.search(new Range(start, end), e -> matches.computeIfAbsent(e.getKey(), ignore -> new HashSet<>()).add(e.getValue()));
        return matches;
    }

    public synchronized boolean isEmpty()
    {
        return map.isEmpty();
    }

    public Segment write(IndexDescriptor id) throws IOException
    {
        if (map.isEmpty())
            throw new AssertionError("Unable to write empty index");
        Map<Group, Segment.Metadata> output = new HashMap<>();

        List<Group> groups = new ArrayList<>(map.keySet());
        groups.sort(Comparator.naturalOrder());

        for (var group : groups)
        {
            var submap = map.get(group);
            if (submap.isEmpty()) // is this possible?  put here for safty so list is never empty
                continue;
            Metadata metadata = groupMetadata.get(group);

            //TODO (performance): if the RangeTree can return the data in sorted order, then this local can become faster
            // Right now the code is based off RTree, which is undefined order, so we must iterate then sort; in testing this is a good chunk of the time of this method
            List<CheckpointIntervalArrayIndex.Interval> list = submap.stream()
                                                                     .map(e -> new CheckpointIntervalArrayIndex.Interval(e.getKey().start, e.getKey().end, ByteBufferUtil.getArray(e.getValue().getKey())))
                                                                     .sorted(Comparator.naturalOrder())
                                                                     .collect(Collectors.toList());

            var writer = new CheckpointIntervalArrayIndex.SegmentWriter(id, list.get(0).start.length, list.get(0).value.length);
            var meta = writer.write(list.toArray(CheckpointIntervalArrayIndex.Interval[]::new));
            if (meta.isEmpty()) // don't include empty segments
                continue;
            output.put(group, new Segment.Metadata(meta, metadata.minTerm, metadata.maxTerm));
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
