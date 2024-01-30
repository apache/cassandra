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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.accord.CheckpointIntervalArrayIndex.Interval;
import org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CheckpointIntervalArrayIndexTest
{
    private static final Logger logger = LoggerFactory.getLogger(CheckpointIntervalArrayIndexTest.class);

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    private static final byte[] EMPTY = new byte[0];
    private static final TreeSet<Interval> EMPTY_TREE_SET = new TreeSet<>();

    private static final Gen.IntGen MAX_TOKEN_GEN = Gens.pickInt(1 << 14,
                                                                 1 << 16,
                                                                 1 << 20,
                                                                 1 << 30);

    private enum Pattern { RANDOM, NO_OVERLAP, PARTIAL_OVERLAP }

    private static final Gen<Pattern> PATTERN_GEN = Gens.enums().all(Pattern.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private int generation = 0;

    @Test
    public void simple() throws IOException
    {
        int bytesPerKey = Integer.BYTES;
        int bytesPerValue = 0;

        List<Interval> list = new ArrayList<>(10);
        list.add(new Interval(bytes(Integer.MIN_VALUE).array(), bytes(Integer.MAX_VALUE).array(), EMPTY));
        for (int i = 0; i < 10; i++)
            list.add(new Interval(bytes(i).array(), bytes(i + 1).array(), EMPTY));

        try (var searcher = index(bytesPerKey, bytesPerValue, list))
        {
            Set<List<Integer>> expected = Set.of(List.of(-2147483648, 2147483647),
                                                 List.of(2, 3),
                                                 List.of(3, 4));
            Set<List<Integer>> actual = new HashSet<>();
            var stats = searcher.intersects(bytes(2).array(), bytes(4).array(), value -> actual.add(List.of(ByteBuffer.wrap(value.start).getInt(), ByteBuffer.wrap(value.end).getInt())));
            logger.info("Stats: {}", stats);
            Assertions.assertThat(actual).isEqualTo(expected);
        }
    }

    @Test
    public void fuzzSmall()
    {
        var minToken = 0;
        int numRecords = 10;
        qt().check(rs -> fuzz(rs, minToken, MAX_TOKEN_GEN.nextInt(rs), PATTERN_GEN.next(rs), numRecords));
    }

    @Test
    public void fuzzMedium()
    {
        var minToken = 0;
        int numRecords = 1_000;
        qt().check(rs -> fuzz(rs, minToken, MAX_TOKEN_GEN.nextInt(rs), PATTERN_GEN.next(rs), numRecords));
    }

    private void fuzz(RandomSource rs, int minToken, int maxToken, Pattern pattern, int numRecords) throws IOException
    {
        List<DetailedInterval> intervals = buildIntervals(rs, minToken, maxToken, pattern, numRecords);
        List<? extends Interval> nonContainedRanges = findMissingRanges(intervals);

        try (var searcher = index(Integer.BYTES, Integer.BYTES, intervals))
        {
            for (int i = 0, samples = rs.nextInt(Math.min(10, numRecords), Math.min(10, numRecords) * 10); i < samples; i++)
            {
                SearchContext ctx = rs.decide(.2) ? miss(rs, nonContainedRanges)
                                                  : hit(rs, intervals, pattern);
                Set<Interval> actual = new TreeSet<>();
                try
                {
                    var stats = searcher.intersects(ctx.start, ctx.end, interval -> actual.add(new DetailedInterval(interval)));
                    logger.info("[Pattern={}, size={}, expectedMatches={}, query=[{}, {})] Stats: {}", pattern, intervals.size(), ctx.expected.size(), ctx.a, ctx.b, stats);
                }
                catch (Throwable t)
                {
                    throw new AssertionError(String.format("Failure searching for [%d, %d) from %s", ctx.a, ctx.b, intervals), t);
                }
                Assertions.assertThat(actual).describedAs("search(%d, %d) from %s", ctx.a, ctx.b, intervals).isEqualTo(ctx.expected);
            }
        }
    }

    /**
     * mutable/shared ctx to avoid allocating in a loop...
     */
    private SearchContext searchContext = new SearchContext();

    private SearchContext miss(RandomSource rs, List<? extends Interval> nonContainedRanges)
    {
        var range = rs.pick(nonContainedRanges);
        var s = unbc(range.start);
        var e = unbc(range.end);
        int domain = e - s;
        int a, b;
        if (domain == 1)
        {
            // you can not find multiple values within this range!
            a = s;
            b = e;
        }
        else
        {
            a = e == Integer.MAX_VALUE ? rs.nextInt(s, e) : rs.nextInt(s, e) + 1;
            b = e == Integer.MAX_VALUE ? rs.nextInt(s, e) : rs.nextInt(s, e) + 1;
            for (int i = 0; i < 42 && a == b; i++)
                b = e == Integer.MAX_VALUE ? rs.nextInt(s, e) : rs.nextInt(s, e) + 1;
            if (a == b)
                throw new IllegalStateException("Unable to create missing range: " + range);
            if (b < a)
            {
                var tmp = a;
                a = b;
                b = tmp;
            }
        }
        searchContext.a = a;
        searchContext.b = b;
        searchContext.start = bc(a);
        searchContext.end = bc(b);
        searchContext.expected = EMPTY_TREE_SET;
        return searchContext;
    }

    private SearchContext hit(RandomSource rs, List<DetailedInterval> intervals, Pattern pattern)
    {
        int numRecords =  intervals.size();
        DetailedInterval first, second;
        do
        {
            var offset = rs.nextInt(0, numRecords);
            int endOffset;
            switch (pattern)
            {
                case PARTIAL_OVERLAP:
                case RANDOM:
                    endOffset = offset;
                    break;
                case NO_OVERLAP:
                    endOffset = offset == numRecords - 1 ? offset : offset + rs.nextInt(1, Math.min(3, numRecords - offset));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown pattern: " + pattern);
            }
            first = intervals.get(offset);
            second = intervals.get(endOffset);
        }
        while (first.compareTo(second) == 0 && first.size() == 1);
        int a, b;
        a = rs.nextInt(unbc(first.start), unbc(first.end)) + 1;
        b = rs.nextInt(unbc(second.start), unbc(second.end)) + 1;
        while (a == b)
            b = rs.nextInt(unbc(second.start), unbc(second.end)) + 1;
        if (b < a)
        {
            var tmp = b;
            b = a;
            a = tmp;
        }

        searchContext.start = bc(a);
        searchContext.end = bc(b);

        searchContext.expected = intervals.stream().filter(i -> i.intersects(searchContext.start, searchContext.end)).collect(Collectors.toCollection(TreeSet::new));
        Assertions.assertThat(searchContext.expected).isNotEmpty();
        return searchContext;
    }

    private static List<DetailedInterval> buildIntervals(RandomSource rs, int minToken, int maxToken, Pattern pattern, int numRecords)
    {
        List<DetailedInterval> intervals = new ArrayList<>(numRecords);
        {
            var domain = maxToken - minToken + 1;
            var delta = domain / numRecords;
            var sub_delta = delta / 2;
            for (int i = 0; i < numRecords; i++)
            {
                switch (pattern)
                {
                    case RANDOM:
                    {
                        var start = rs.nextInt(minToken, maxToken);
                        var remaining = maxToken - start;
                        var end = start + (remaining == 1 ? 1 : rs.nextInt(1, remaining));
                        intervals.add(new DetailedInterval(bc(start), bc(end), bytes(i).array()));
                    }
                    break;
                    case NO_OVERLAP:
                    {
                        var start = delta * i;
                        var end = start + sub_delta;
                        intervals.add(new DetailedInterval(bc(start), bc(end), bytes(i).array()));
                    }
                    break;
                    case PARTIAL_OVERLAP:
                    {
                        if (i > 1 && rs.decide(.2))
                        {
                            // overlap
                            DetailedInterval start, end;
                            do
                            {
                                int numOverlaps = rs.nextInt(1, Math.min(3, intervals.size()));
                                int offset = rs.nextInt(0, intervals.size() - numOverlaps);
                                start = intervals.get(offset);
                                end = intervals.get(offset + numOverlaps);
                            }
                            while (start.compareTo(end) == 0 && start.size() == 1);
                            var a = rs.nextInt(unbc(start.start), unbc(start.end)) + 1;
                            var b = rs.nextInt(unbc(end.start), unbc(end.end)) + 1;
                            if (a == b && end.size() == 1)
                            {
                                while (a == b)
                                    a = rs.nextInt(unbc(start.start), unbc(start.end)) + 1;
                            }
                            else
                            {
                                while (a == b)
                                    b = rs.nextInt(unbc(end.start), unbc(end.end)) + 1;
                            }
                            if (a > b)
                            {
                                var tmp = a;
                                a = b;
                                b = tmp;
                            }
                            intervals.add(new DetailedInterval(bc(a), bc(b), bytes(i).array()));
                            intervals.sort(Comparator.naturalOrder()); // so partial can work next time
                        }
                        else
                        {
                            // no overlap
                            var start = delta * i;
                            var end = start + sub_delta;
                            intervals.add(new DetailedInterval(bc(start), bc(end), bytes(i).array()));
                        }
                    }
                    break;
                    default:
                        throw new IllegalArgumentException("Unknown pattern: " + pattern);
                }
            }
            intervals.sort(Comparator.naturalOrder());
        }
        return intervals;
    }

    private static List<DetailedInterval> findMissingRanges(List<? extends Interval> intervals)
    {
        List<DetailedInterval> list = new ArrayList<>();
        list.add(new DetailedInterval(bc(Integer.MIN_VALUE), intervals.get(0).start, bytes(0).array()));
        // track current visable coverage
        int end = unbc(intervals.get(0).end);
        for (var i : intervals)
        {
            int istar = unbc(i.start);
            int iend = unbc(i.end);
            if (end >= istar)
            {
                // current scope includes this range
                end = Math.max(end, iend);
            }
            else
            {
                // range doesn't intersect, and a new start/end are formed!
                list.add(new DetailedInterval(bc(end), bc(istar), bytes(list.size()).array()));
                end = iend;
            }
        }
        list.add(new DetailedInterval(bc(end), bc(Integer.MAX_VALUE), bytes(list.size()).array()));
        return list;
    }

    private static class DetailedInterval extends Interval
    {
        public DetailedInterval(byte[] start, byte[] end, byte[] value)
        {
            super(start, end, value);
        }

        public DetailedInterval(Interval other)
        {
            super(other);
        }

        public int size()
        {
            return unbc(end) - unbc(start);
        }

        @Override
        public String toString()
        {
            return "[" + unbc(start) + ", " + unbc(end) + ") -> " + ByteBuffer.wrap(value).getInt();
        }
    }

    private static byte[] bc(int value)
    {
        ByteBuffer bb = bytes(value);
        var bs = Int32Type.instance.asComparableBytes(ByteBufferAccessor.instance, bb, ByteComparable.Version.OSS50);
        return ByteSourceInverse.readBytes(bs);
    }

    private static int unbc(byte[] bc)
    {
        return Int32Type.instance.fromComparableBytes(ByteSource.peekable(ByteSource.fixedLength(bc)), ByteComparable.Version.OSS50).getInt();
    }

    @SuppressWarnings({ "IOResourceOpenedButNotSafelyClosed", "resource" })
    private Searcher index(int bytesPerKey, int bytesPerValue, List<? extends Interval> sortedIntervals) throws IOException
    {
        IndexDescriptor descriptor = nextDescriptor();

        var writer = new CheckpointIntervalArrayIndex.SegmentWriter(descriptor, bytesPerKey, bytesPerValue);
        var metas = writer.write(sortedIntervals.toArray(Interval[]::new));

        // going through the RouteIndexFormat isn't required for this test, but it helps improve coverage there...
        Segment segment = new Segment(ImmutableMap.of(new Group(0, TableId.fromUUID(new UUID(0, 0))), new Segment.Metadata(metas, ByteArrayUtil.EMPTY_BYTE_ARRAY, ByteArrayUtil.EMPTY_BYTE_ARRAY)));
        RouteIndexFormat.appendSegment(descriptor, segment);

        Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
        for (IndexComponent c : descriptor.getLiveComponents())
            files.put(c, new FileHandle.Builder(descriptor.fileFor(c)).mmapped(true).complete());
        List<Segment> segments = RouteIndexFormat.readSegements(files);
        files.remove(IndexComponent.SEGMENT).close();
        files.remove(IndexComponent.METADATA).close();

        var searcher = new CheckpointIntervalArrayIndex.SegmentSearcher(files.get(IndexComponent.CINTIA_SORTED_LIST).sharedCopy(), metas.get(IndexComponent.CINTIA_SORTED_LIST).offset,
                                                                        files.get(IndexComponent.CINTIA_CHECKPOINTS).sharedCopy(), metas.get(IndexComponent.CINTIA_CHECKPOINTS).offset);
        return new Searcher()
        {
            @Override
            public CheckpointIntervalArrayIndex.Stats intersects(byte[] start, byte[] end, Consumer<Interval> callback) throws IOException
            {
                return searcher.intersects(start, end, callback);
            }

            @Override
            public void close()
            {
                searcher.close();
                for (var fh : files.values())
                    fh.close();
            }
        };
    }

    private IndexDescriptor nextDescriptor()
    {
        return IndexDescriptor.create(new Descriptor(new File(folder.getRoot()), "test", "test", new SequenceBasedSSTableId(generation++)),
                                      Murmur3Partitioner.instance,
                                      new ClusteringComparator());
    }

    private static class SearchContext
    {
        TreeSet<Interval> expected;
        byte[] start, end;
        int a, b;
    }

    public interface Searcher extends Closeable
    {
        CheckpointIntervalArrayIndex.Stats intersects(byte[] start, byte[] end, Consumer<Interval> callback) throws IOException;
    }
}