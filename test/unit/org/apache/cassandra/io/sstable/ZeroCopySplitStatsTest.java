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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** End-to-end coverage for the Statistics.db written for BIG split children. */
public class ZeroCopySplitStatsTest extends CQLTester
{
    private static final long REPAIRED_AT = 8675309L;
    private static final double SOURCE_TOKEN_COVERAGE = 0.375d;

    @Test
    public void splitChildrenPersistExactStatistics() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(80, 5, 480);
        assertEquals("pa", parent.descriptor.version.version);
        assertFalse(parent.descriptor.version.hasSplitPrefixMarker());

        parent.mutateRepairedAndReload(REPAIRED_AT, null, false);
        List<AbstractType<?>> clusteringTypes = parent.metadata().comparator.subtypes();
        parent.descriptor.getMetadataSerializer().mutate(parent.descriptor,
                                                         "seed token coverage for split statistics test",
                                                         stats -> withTokenSpaceCoverage(stats,
                                                                                         clusteringTypes,
                                                                                         SOURCE_TOKEN_COVERAGE));
        parent.reloadSSTableMetadata();

        StatsMetadata parentStats = parent.getSSTableMetadata();
        assertEquals(REPAIRED_AT, parentStats.repairedAt);
        assertEquals(SOURCE_TOKEN_COVERAGE, parentStats.tokenSpaceCoverage, 0.0d);
        assertTrue(parentStats.totalRows > 0);
        assertTrue(parentStats.minTimestamp < parentStats.maxTimestamp);
        assertFalse("an ordinary flushed sstable must not claim a retained split prefix",
                    parent.hasSplitPrefix());
        assertEquals("the ordinary source position must be zero on disk",
                     0, StatsComponent.load(parent.descriptor).statsMetadata().firstPartitionPosition);

        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 4);
        List<Child> children = result.children;
        try
        {
            assertEquals(4, children.size());
            assertEquals("the first child starts at the source's physical beginning",
                         0L, children.get(0).deadPrefixBytes);
            assertFalse("a child without a dead prefix must remain on the ordinary scan path",
                        children.get(0).reader.hasSplitPrefix());

            boolean foundPrefixedChild = false;
            long partitionCount = 0;
            for (Child child : children)
            {
                StatsMetadata persisted = StatsComponent.load(child.descriptor).statsMetadata();
                assertChildStats(parentStats, child, child.reader, persisted);
                assertEquals(persisted, child.reader.getSSTableMetadata());
                assertExactPartitionHistogram(parentStats, child, persisted);

                foundPrefixedChild |= persisted.firstPartitionPosition > 0;
                partitionCount += child.partitionCount;
            }
            assertTrue("the test must exercise a child whose first live partition is inside a retained chunk",
                       foundPrefixedChild);
            assertEquals(parentStats.estimatedPartitionSize.count(), partitionCount);
        }
        finally
        {
            release(result);
        }

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<SSTableReader> reopened = new ArrayList<>(children.size());
        try
        {
            for (Child child : children)
            {
                SSTableReader reader = SSTableReader.open(cfs, child.descriptor, child.components, cfs.metadata);
                reopened.add(reader);

                StatsMetadata persisted = StatsComponent.load(child.descriptor).statsMetadata();
                assertChildStats(parentStats, child, reader, persisted);
                assertEquals("the reopened reader must use the position deserialized from Statistics.db",
                             child.deadPrefixBytes, reader.firstPartitionPosition());
            }
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    @Test
    public void splitChildrenRecordBloomFilterChanceUsedToBuildFilter() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());

        SSTableReader parent = compressedSSTable(80, 5, 480);
        double sourceChance = StatsComponent.load(parent.descriptor).validationMetadata().bloomFilterFPChance;
        double splitChance = 0.2d;
        assertTrue(sourceChance != splitChance);

        execute("ALTER TABLE %s WITH bloom_filter_fp_chance = " + splitChance);
        assertEquals(splitChance, parent.metadata().params.bloomFilterFpChance, 0.0d);

        Result result = ZeroCopySSTableSplitter.splitForTesting(parent, 4);
        try
        {
            for (Child child : result.children)
            {
                assertEquals("ValidationMetadata must describe the bloom filter built for the child",
                             splitChance,
                             StatsComponent.load(child.descriptor).validationMetadata().bloomFilterFPChance,
                             0.0d);
            }
        }
        finally
        {
            release(result);
        }
    }

    private static void assertChildStats(StatsMetadata parentStats,
                                         Child child,
                                         SSTableReader reader,
                                         StatsMetadata childStats)
    {
        assertEquals("pb", child.descriptor.version.version);
        assertTrue(child.descriptor.version.hasSplitPrefixMarker());
        assertEquals("the position is a per-sstable fact, not a version-wide default",
                     child.deadPrefixBytes, childStats.firstPartitionPosition);
        assertEquals(child.deadPrefixBytes, reader.firstPartitionPosition());

        assertEquals(child.first, reader.getFirst());
        assertEquals(child.last, reader.getLast());
        assertEquals(child.first.getKey(), childStats.firstKey);
        assertEquals(child.last.getKey(), childStats.lastKey);
        assertEquals((double) child.onDiskLength() / child.dataLength, childStats.compressionRatio, 0.0d);

        assertEquals(parentStats.estimatedCellPerPartitionCount, childStats.estimatedCellPerPartitionCount);
        assertEquals(parentStats.commitLogIntervals, childStats.commitLogIntervals);
        assertEquals(parentStats.minTimestamp, childStats.minTimestamp);
        assertEquals(parentStats.maxTimestamp, childStats.maxTimestamp);
        assertEquals(parentStats.minLocalDeletionTime, childStats.minLocalDeletionTime);
        assertEquals(parentStats.maxLocalDeletionTime, childStats.maxLocalDeletionTime);
        assertEquals(parentStats.minTTL, childStats.minTTL);
        assertEquals(parentStats.maxTTL, childStats.maxTTL);
        assertEquals(parentStats.estimatedTombstoneDropTime, childStats.estimatedTombstoneDropTime);
        assertEquals(parentStats.sstableLevel, childStats.sstableLevel);
        assertEquals(parentStats.coveredClustering, childStats.coveredClustering);
        assertEquals(parentStats.hasLegacyCounterShards, childStats.hasLegacyCounterShards);
        assertEquals(parentStats.repairedAt, childStats.repairedAt);
        assertEquals(parentStats.totalColumnsSet, childStats.totalColumnsSet);
        assertEquals(parentStats.totalRows, childStats.totalRows);
        assertEquals(parentStats.originatingHostId, childStats.originatingHostId);
        assertEquals(parentStats.pendingRepair, childStats.pendingRepair);
        assertEquals(parentStats.isTransient, childStats.isTransient);
        assertEquals(parentStats.hasPartitionLevelDeletions, childStats.hasPartitionLevelDeletions);
        assertEquals(parentStats.encodingStats, childStats.encodingStats);

        assertTrue("a parent's token coverage cannot be assigned to each disjoint child",
                   Double.isNaN(childStats.tokenSpaceCoverage));
    }

    private static void assertExactPartitionHistogram(StatsMetadata parentStats,
                                                      Child child,
                                                      StatsMetadata childStats) throws IOException
    {
        EstimatedHistogram actual = childStats.estimatedPartitionSize;
        assertArrayEquals("split and writer histograms must use the same bucket grid",
                          parentStats.estimatedPartitionSize.getBucketOffsets(), actual.getBucketOffsets());

        List<Long> positions = readIndexPositions(child);
        assertEquals(child.partitionCount, positions.size());
        assertEquals(child.deadPrefixBytes, positions.get(0).longValue());

        EstimatedHistogram expected = new EstimatedHistogram(ZeroCopySSTableSplitter.PARTITION_SIZE_HISTOGRAM_BUCKETS);
        long previous = positions.get(0);
        for (int i = 1; i < positions.size(); i++)
        {
            long position = positions.get(i);
            expected.add(position - previous);
            previous = position;
        }
        expected.add(child.dataLength - previous);

        assertEquals("Statistics.db must describe the exact partition sizes represented by the child index",
                     expected, actual);
        assertEquals(child.partitionCount, actual.count());
    }

    private static List<Long> readIndexPositions(Child child) throws IOException
    {
        List<Long> positions = new ArrayList<>();
        try (RandomAccessReader index = RandomAccessReader.open(child.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX)))
        {
            while (!index.isEOF())
            {
                ByteBufferUtil.readWithShortLength(index);
                positions.add(index.readUnsignedVInt());
                int promotedSize = index.readUnsignedVInt32();
                if (promotedSize > 0)
                    index.skipBytesFully(promotedSize);
            }
        }
        return positions;
    }

    private static StatsMetadata withTokenSpaceCoverage(StatsMetadata stats,
                                                        List<AbstractType<?>> clusteringTypes,
                                                        double tokenSpaceCoverage)
    {
        return new StatsMetadata(stats.estimatedPartitionSize,
                                 stats.estimatedCellPerPartitionCount,
                                 stats.commitLogIntervals,
                                 stats.minTimestamp,
                                 stats.maxTimestamp,
                                 stats.minLocalDeletionTime,
                                 stats.maxLocalDeletionTime,
                                 stats.minTTL,
                                 stats.maxTTL,
                                 stats.compressionRatio,
                                 stats.estimatedTombstoneDropTime,
                                 stats.sstableLevel,
                                 clusteringTypes,
                                 stats.coveredClustering,
                                 stats.hasLegacyCounterShards,
                                 stats.repairedAt,
                                 stats.totalColumnsSet,
                                 stats.totalRows,
                                 tokenSpaceCoverage,
                                 stats.originatingHostId,
                                 stats.pendingRepair,
                                 stats.isTransient,
                                 stats.hasPartitionLevelDeletions,
                                 stats.firstKey,
                                 stats.lastKey,
                                 stats.firstPartitionPosition);
    }

    private SSTableReader compressedSSTable(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
        disableCompaction();
        for (int p = 0; p < partitions; p++)
        {
            for (int c = 0; c < rowsPerPartition; c++)
            {
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL ?",
                        String.format("k%06d", p), c, randomText(valueBytes),
                        1_000_000L + p * rowsPerPartition + c, 86_400 + c);
            }
        }
        flush();

        Set<SSTableReader> live = getCurrentColumnFamilyStore().getLiveSSTables();
        assertEquals("expected exactly one source sstable", 1, live.size());
        SSTableReader parent = live.iterator().next();
        assertTrue(parent.compression);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));
        assertTrue("the source must span enough chunks to exercise split-prefix children",
                   parent.uncompressedLength() > 20L * parent.getCompressionMetadata().chunkLength());
        return parent;
    }

    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static void release(Result result)
    {
        for (Child child : result.children)
            child.reader.selfRef().release();
    }
}
