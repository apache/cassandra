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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.dht.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.apache.cassandra.db.ColumnFamilyStore.RING_VERSION_IRRELEVANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ShardManagerTest
{
    final IPartitioner partitioner = Murmur3Partitioner.instance;
    final Token minimumToken = partitioner.getMinimumToken();

    ColumnFamilyStore.VersionedLocalRanges weightedRanges;

    static final double delta = 1e-15;

    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        weightedRanges = new ColumnFamilyStore.VersionedLocalRanges(RING_VERSION_IRRELEVANT, 16);
    }

    @Test
    public void testRangeSpannedFullOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(minimumToken, minimumToken)));
        ShardManager shardManager = new ShardManagerNoDisks(weightedRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.5, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(range(0.3, 0.5)), delta);

        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, Double.NaN)), delta);
        // single-partition correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.0)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, -1)), delta);

        // correction over coverage
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.5, 1e-50)), delta);
    }

    @Test
    public void testRangeSpannedPartialOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.05), tokenAt(0.15))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.3), tokenAt(0.4))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.45), tokenAt(0.5))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.7), tokenAt(0.75))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.75), tokenAt(0.85))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.90), tokenAt(0.91))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.92), tokenAt(0.94))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.98), tokenAt(1.0))));
        double total = weightedRanges.stream().mapToDouble(wr -> wr.range().left.size(wr.range().right)).sum();

        ShardManager shardManager = new ShardManagerNoDisks(weightedRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.15, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.15, shardManager.rangeSpanned(range(0.3, 0.5)), delta);
        assertEquals(0.0, shardManager.rangeSpanned(range(0.5, 0.7)), delta);
        assertEquals(total, shardManager.rangeSpanned(range(0.0, 1.0)), delta);


        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, Double.NaN)), delta);

        // single-partition correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN)), delta);
        // out-of-local-range correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.6, 0.7, Double.NaN)), delta);
        assertEquals(0.001, shardManager.rangeSpanned(mockedTable(0.6, 0.701, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 0.0)), delta);
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, -1)), delta);

        // correction over coverage, no recalculation
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 1e-50)), delta);
    }

    @Test
    public void testRangeSpannedWeighted()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.05), tokenAt(0.15))));
        weightedRanges.add(new Splitter.WeightedRange(0.5, new Range<>(tokenAt(0.3), tokenAt(0.4))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.45), tokenAt(0.5))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.7), tokenAt(0.75))));
        weightedRanges.add(new Splitter.WeightedRange(0.2, new Range<>(tokenAt(0.75), tokenAt(0.85))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.90), tokenAt(0.91))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.92), tokenAt(0.94))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.98), tokenAt(1.0))));
        double total = weightedRanges.stream().mapToDouble(wr -> wr.size()).sum();

        ShardManager shardManager = new ShardManagerNoDisks(weightedRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.10, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.10, shardManager.rangeSpanned(range(0.3, 0.5)), delta);
        assertEquals(0.0, shardManager.rangeSpanned(range(0.5, 0.7)), delta);
        assertEquals(total, shardManager.rangeSpanned(range(0.0, 1.0)), delta);


        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, Double.NaN)), delta);

        // single-partition correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN)), delta);
        // out-of-local-range correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.6, 0.7, Double.NaN)), delta);
        assertEquals(0.001, shardManager.rangeSpanned(mockedTable(0.6, 0.701, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 0.0)), delta);
        assertEquals(0.06, shardManager.rangeSpanned(mockedTable(0.5, 0.8, -1)), delta);

        // correction over coverage, no recalculation
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 1e-50)), delta);
    }

    Token tokenAt(double pos)
    {
        return partitioner.split(minimumToken, minimumToken, pos);
    }

    DecoratedKey keyAt(double pos)
    {
        Token token = tokenAt(pos);
        return new BufferDecoratedKey(token, ByteBuffer.allocate(0));
    }

    Range<Token> range(double start, double end)
    {
        return new Range<>(tokenAt(start), tokenAt(end));
    }

    SSTableReader mockedTable(double start, double end, double reportedCoverage)
    {
        SSTableReader mock = Mockito.mock(SSTableReader.class);
        Mockito.when(mock.getFirst()).thenReturn(keyAt(start));
        Mockito.when(mock.getLast()).thenReturn(keyAt(end));
        Mockito.when(mock.tokenSpaceCoverage()).thenReturn(reportedCoverage);
        return mock;
    }

    @Test
    public void testShardBoundaries()
    {
        // no shards
        testShardBoundaries(ints(), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundaries(ints(30), 1, 2, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundaries(ints(30), 2, 1, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80), 3, 3, ints(0, 90));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(0, 51, 61, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(0, 49, 59, 100));
        testShardBoundaries(ints(12, 23, 33, 45, 56, 70, 80, 90), 3, 3, ints(0, 9, 11, 20, 21, 39, 41, 50, 51, 60, 64, 68, 68, 100));

        // uneven
        testShardBoundaries(ints(8, 16, 24, 32, 42, 52, 62, 72, 79, 86, 93), 4, ints(32, 72, 100), ints(0, 100));
        testShardBoundaries(ints(1, 2, 3, 4, 6, 8, 10, 12, 34, 56, 78), 4, ints(4, 12, 100), ints(0, 100));
    }

    @Test
    public void testShardBoundariesWraparound()
    {
        // no shards
        testShardBoundaries(ints(), 1, 1, ints(50, 10));
        // split on disks at minimum
        testShardBoundaries(ints(70), 1, 2, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70), 1, 5, ints(91, 31, 61, 71));
        // no disks
        testShardBoundaries(ints(70), 2, 1, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70), 5, 1, ints(91, 31, 61, 71));
        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90), 3, 3, ints(81, 71));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(51, 41));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90), 3, 3, ints(21, 11));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90), 3, 3, ints(89, 79));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90), 3, 3, ints(59, 49));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90), 3, 3, ints(29, 19));

        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(91, 51, 61, 91));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(21, 51, 61, 21));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90), 3, 3, ints(71, 51, 61, 71));
    }

    @Test
    public void testShardBoundariesWeighted()
    {
        // no shards
        testShardBoundariesWeighted(ints(), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundariesWeighted(ints(30), 1, 2, ints(10, 50));
        testShardBoundariesWeighted(ints(22, 34, 45, 64), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundariesWeighted(ints(30), 2, 1, ints(10, 50));
        testShardBoundariesWeighted(ints(22, 34, 45, 64), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundariesWeighted(ints(10, 20, 30, 40, 50, 60, 70, 80), 3, 3, ints(0, 90));
        testShardBoundariesWeighted(ints(14, 29, 43, 64, 71, 79, 86, 93), 3, 3, ints(0, 51, 61, 100));
        testShardBoundariesWeighted(ints(18, 36, 50, 63, 74, 83, 91, 96), 3, 3, ints(0, 40, 40, 70, 70, 90, 90, 100));
    }

    private int[] ints(int... values)
    {
        return values;
    }

    private void testShardBoundaries(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
        when(cfs.getPartitioner()).thenReturn(partitioner);

        List<Range<Token>> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1])));
        ranges = Range.sort(ranges);
        ColumnFamilyStore.VersionedLocalRanges sortedRanges = localRanges(ranges.stream().map(x -> new Splitter.WeightedRange(1.0, x)).collect(Collectors.toList()));

        List<Token> diskBoundaries = splitRanges(sortedRanges, numDisks);
        int[] result = getShardBoundaries(cfs, numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private void testShardBoundariesWeighted(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
        when(cfs.getPartitioner()).thenReturn(partitioner);

        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(2.0 / (rangeBounds.length - i), new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        ColumnFamilyStore.VersionedLocalRanges sortedRanges = localRanges(ranges);

        List<Token> diskBoundaries = splitRanges(sortedRanges, numDisks);
        int[] result = getShardBoundaries(cfs, numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private void testShardBoundaries(int[] expected, int numShards, int[] diskPositions, int[] rangeBounds)
    {
        ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
        when(cfs.getPartitioner()).thenReturn(partitioner);

        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(1.0, new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        ColumnFamilyStore.VersionedLocalRanges sortedRanges = localRanges(ranges);

        List<Token> diskBoundaries = Arrays.stream(diskPositions).mapToObj(this::getToken).collect(Collectors.toList());
        int[] result = getShardBoundaries(cfs, numShards, diskBoundaries, sortedRanges);
        Assert.assertArrayEquals("Disks " + Arrays.toString(diskPositions) + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private int[] getShardBoundaries(ColumnFamilyStore cfs, int numShards, List<Token> diskBoundaries, ColumnFamilyStore.VersionedLocalRanges sortedRanges)
    {
        DiskBoundaries db = makeDiskBoundaries(cfs, diskBoundaries);
        when(cfs.localRangesWeighted()).thenReturn(sortedRanges);
        when(cfs.getDiskBoundaries()).thenReturn(db);

        final ShardTracker shardTracker = ShardManager.create(cfs)
                                                      .boundaries(numShards);
        IntArrayList list = new IntArrayList();
        for (int i = 0; i < 100; ++i)
        {
            if (shardTracker.advanceTo(getToken(i)))
                list.addInt(fromToken(shardTracker.shardStart()));
        }
        return list.toIntArray();
    }

    ColumnFamilyStore.VersionedLocalRanges localRanges(List<Splitter.WeightedRange> ranges)
    {
        ColumnFamilyStore.VersionedLocalRanges versionedLocalRanges = new ColumnFamilyStore.VersionedLocalRanges(RING_VERSION_IRRELEVANT, ranges.size());
        versionedLocalRanges.addAll(ranges);
        return versionedLocalRanges;
    }

    ColumnFamilyStore.VersionedLocalRanges localRangesFull()
    {
        List<Splitter.WeightedRange> ranges = ImmutableList.of(new Splitter.WeightedRange(1.0,
                                                                                          new Range<>(partitioner.getMinimumToken(),
                                                                                                      partitioner.getMinimumToken())));
        ColumnFamilyStore.VersionedLocalRanges versionedLocalRanges = new ColumnFamilyStore.VersionedLocalRanges(RING_VERSION_IRRELEVANT, ranges.size());
        versionedLocalRanges.addAll(ranges);
        return versionedLocalRanges;
    }

    List<Token> splitRanges(ColumnFamilyStore.VersionedLocalRanges ranges, int numDisks)
    {
        return ranges.get(0).left().getPartitioner().splitter().get().splitOwnedRanges(numDisks, ranges, false);
    }

    private static DiskBoundaries makeDiskBoundaries(ColumnFamilyStore cfs, List<Token> diskBoundaries)
    {
        List<PartitionPosition> diskPositions = diskBoundaries.stream().map(Token::maxKeyBound).collect(Collectors.toList());
        DiskBoundaries db = new DiskBoundaries(cfs, null, diskPositions, RING_VERSION_IRRELEVANT, -1);
        return db;
    }

    private Token getToken(int x)
    {
        return tokenAt(x / 100.0);
    }

    private int fromToken(Token t)
    {
        return (int) Math.round(partitioner.getMinimumToken().size(t) * 100.0);
    }

    @Test
    public void testRangeEnds()
    {
        ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
        when(cfs.getPartitioner()).thenReturn(partitioner);
        ColumnFamilyStore.VersionedLocalRanges sortedRanges = localRangesFull();

        for (int numDisks = 1; numDisks <= 3; ++numDisks)
        {
            List<Token> diskBoundaries = splitRanges(sortedRanges, numDisks);
            DiskBoundaries db = makeDiskBoundaries(cfs, diskBoundaries);
            when(cfs.localRangesWeighted()).thenReturn(sortedRanges);
            when(cfs.getDiskBoundaries()).thenReturn(db);

            ShardManager shardManager = ShardManager.create(cfs);
            for (int numShards = 1; numShards <= 3; ++numShards)
            {
                ShardTracker iterator = shardManager.boundaries(numShards);
                iterator.advanceTo(partitioner.getMinimumToken());

                int count = 1;
                for (Token end = iterator.shardEnd(); end != null; end = iterator.shardEnd())
                {
                    assertFalse(iterator.advanceTo(end));
                    assertTrue(iterator.advanceTo(end.nextValidToken()));
                    ++count;
                }
                assertEquals(numDisks * numShards, count);
            }
        }
    }


    @Test
    public void testSplitSSTablesInRanges()
    {
        testSplitSSTablesInRanges(8, ints(1, 2, 4));
        testSplitSSTablesInRanges(4, ints(1, 2, 4));
        testSplitSSTablesInRanges(2, ints(1, 2, 4));
        testSplitSSTablesInRanges(5, ints(1, 2, 4));
        testSplitSSTablesInRanges(5, ints(2, 4, 8));
        testSplitSSTablesInRanges(3, ints(1, 3, 5));
        testSplitSSTablesInRanges(3, ints(3, 3, 3));

        testSplitSSTablesInRanges(1, ints(1, 2, 3));

        testSplitSSTablesInRanges(3, ints());
    }

    @Test
    public void testSplitSSTablesInRangesMissingParts()
    {
        // Drop some sstables without losing ranges
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1));

        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(0), ints(2, 7));

        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(1), ints(0), ints(2, 7));
    }

    @Test
    public void testSplitSSTablesInRangesOneRange()
    {
        // Drop second half
        testSplitSSTablesInRanges(2, ints(2, 4, 8),
                                ints(1), ints(2, 3), ints(4, 5, 6, 7));
        // Drop all except center, within shard
        testSplitSSTablesInRanges(3, ints(5, 7, 9),
                                ints(0, 1, 3, 4), ints(0, 1, 2, 4, 5, 6), ints(0, 1, 2, 6, 7, 8));
    }

    @Test
    public void testSplitSSTablesInRangesSkippedRange()
    {
        // Drop all sstables containing the 4/8-5/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2), ints(4));
        // Drop all sstables containing the 4/8-6/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2), ints(4, 5));
        // Drop all sstables containing the 4/8-8/8 range.
        testSplitSSTablesInRanges(8, ints(2, 4, 8),
                                ints(1), ints(2, 3), ints(4, 5, 6, 7));

        // Drop all sstables containing the 0/8-2/8 range.
        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(0), ints(0), ints(0, 1));
        // Drop all sstables containing the 6/8-8/8 range.
        testSplitSSTablesInRanges(5, ints(2, 4, 8),
                                ints(1), ints(3), ints(6, 7));
        // Drop sstables on both ends.
        testSplitSSTablesInRanges(5, ints(3, 4, 8),
                                ints(0, 2), ints(0, 3), ints(0, 1, 6, 7));
    }

    public void testSplitSSTablesInRanges(int numShards, int[] perLevelCounts, int[]... dropsPerLevel)
    {
        weightedRanges.clear();
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(minimumToken, minimumToken)));
        ShardManager manager = new ShardManagerNoDisks(weightedRanges);

        Set<SSTableReader> allSSTables = new HashSet<>();
        int levelNum = 0;
        for (int perLevelCount : perLevelCounts)
        {
            List<SSTableReader> ssTables = mockNonOverlappingSSTables(perLevelCount);
            if (levelNum < dropsPerLevel.length)
            {
                for (int i = dropsPerLevel[levelNum].length - 1; i >= 0; i--)
                    ssTables.remove(dropsPerLevel[levelNum][i]);
            }
            allSSTables.addAll(ssTables);
            ++levelNum;
        }

        var results = new ArrayList<Pair<Range<Token>, Set<SSTableReader>>>();
        manager.splitSSTablesInShards(allSSTables, numShards, (sstables, range) -> results.add(Pair.create(range, Set.copyOf(sstables))));
        int i = 0;
        int[] expectedSSTablesInTasks = new int[results.size()];
        int[] collectedSSTablesPerTask = new int[results.size()];
        for (var t : results)
        {
            collectedSSTablesPerTask[i] = t.right().size();
            expectedSSTablesInTasks[i] = (int) allSSTables.stream().filter(x -> intersects(x, t.left())).count();
            ++i;
        }
        Assert.assertEquals(Arrays.toString(expectedSSTablesInTasks), Arrays.toString(collectedSSTablesPerTask));
        System.out.println(Arrays.toString(expectedSSTablesInTasks));
    }

    private boolean intersects(SSTableReader r, Range<Token> range)
    {
        if (range == null)
            return true;
        return range.intersects(range(r));
    }


    private Bounds<Token> range(SSTableReader x)
    {
        return new Bounds<>(x.getFirst().getToken(), x.getLast().getToken());
    }

    List<SSTableReader> mockNonOverlappingSSTables(int numSSTables)
    {
        if (!partitioner.splitter().isPresent())
            throw new IllegalStateException(String.format("Cannot split ranges with current partitioner %s", partitioner));

        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        List<SSTableReader> sstables = new ArrayList<>(numSSTables);
        for (int i = 0; i < numSSTables; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(numSSTables, i).nextValidToken(), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundary(numSSTables, i+1), emptyBuffer);
            sstables.add(mockSSTable(first, last));
        }

        return sstables;
    }

    private Token boundary(int numSSTables, int i)
    {
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumTokenForSplitting(), i * 1.0 / numSSTables);
    }

    private SSTableReader mockSSTable(DecoratedKey first, DecoratedKey last)
    {
        SSTableReader sstable = Mockito.mock(SSTableReader.class);
        when(sstable.getFirst()).thenReturn(first);
        when(sstable.getLast()).thenReturn(last);
        return sstable;
    }
}
