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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.KeyspaceParams;
import org.mockito.Mockito;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.validateOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class SizeTieredCompactionStrategyTest
{
    public static final String KEYSPACE1 = "SizeTieredCompactionStrategyTest";
    private static final String CF_STANDARD1 = "Standard1";

    private static final Random random = new Random(98752945723L);

    private final int minThreshold = 4; //same as the default
    private final int maxThreshold = 32; //same as the default
    private final double bucketLow = 0.5; //same as the default
    private final double bucketHigh = 1.5; //same as the default
    private final int minSSTableSize = 10; // small enough not to interfere

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, "1.5");
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, "10000");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "1000.0");
            validateOptions(options);
            fail("bucket_low greater than bucket_high should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }

    @Test
    public void testGetBuckets()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        long[] sstableLengths = { 1L, 4L, 8L, 8L, 4L, 1L };
        for (long len : sstableLengths)
        {
            SSTableReader sstable = Mockito.mock(SSTableReader.class);
            when(sstable.onDiskLength()).thenReturn(len);
            when(sstable.hotness()).thenReturn(0.);
            sstables.add(sstable);
        }

        SizeTieredCompactionStrategyOptions stcsOptions = new SizeTieredCompactionStrategyOptions(2, bucketLow, bucketHigh);
        SizeTieredCompactionStrategy.SizeTieredBuckets sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables, stcsOptions, minThreshold, maxThreshold);
        List<List<SSTableReader>> buckets = sizeTieredBuckets.buckets();
        assertEquals(3, buckets.size());
        for (List<SSTableReader> bucket : buckets)
        {
            assertEquals(2, bucket.size());
        }

        sstables.clear();
        buckets.clear();

        long[] sstableLengths2 = { 3L, 8L, 3L, 8L, 8L, 3L };
        for (long len : sstableLengths2)
        {
            SSTableReader sstable = Mockito.mock(SSTableReader.class);
            when(sstable.onDiskLength()).thenReturn(len);
            when(sstable.hotness()).thenReturn(0.);
            sstables.add(sstable);
        }

        sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables, stcsOptions, minThreshold, maxThreshold);
        buckets = sizeTieredBuckets.buckets();
        assertEquals(2, buckets.size());
        for (List<SSTableReader> bucket : buckets)
        {
            assertEquals(3, bucket.size());
        }

        // Test the "min" functionality
        sstables.clear();
        buckets.clear();

        long[] sstableLengths3 = { 3L, 8L, 3L, 8L, 8L, 3L };
        for (long len : sstableLengths3)
        {
            SSTableReader sstable = Mockito.mock(SSTableReader.class);
            when(sstable.onDiskLength()).thenReturn(len);
            when(sstable.hotness()).thenReturn(0.);
            sstables.add(sstable);
        }

        stcsOptions = new SizeTieredCompactionStrategyOptions(10, bucketLow, bucketHigh);
        sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables, stcsOptions, minThreshold, maxThreshold);
        buckets = sizeTieredBuckets.buckets();
        assertEquals(1, buckets.size());
    }

    @Test
    public void testSingleBucketWith3IdenticalFilesRealSSTables()
    {
        String ksname = KEYSPACE1;
        String cfname = "Standard1";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 3 sstables
        int numSSTables = 3;
        for (int r = 0; r < numSSTables; r++)
        {
            String key = String.valueOf(r);
            new RowUpdateBuilder(cfs.metadata(), 0, key)
                .clustering("column").add("val", value)
                .build().applyUnsafe();
            cfs.forceBlockingFlush(UNIT_TESTS);
        }
        cfs.forceBlockingFlush(UNIT_TESTS);

        SizeTieredCompactionStrategyOptions stcsOptions = new SizeTieredCompactionStrategyOptions();
        List<SSTableReader> sstrs = new ArrayList<>(cfs.getLiveSSTables());
        SizeTieredCompactionStrategy.SizeTieredBuckets sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstrs.subList(0, 2), stcsOptions, 4, 32);

        List<SSTableReader> interestingBucket = new ArrayList<>(CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates()).sstables);
        assertTrue("nothing should be returned when all buckets are below the min threshold", interestingBucket.isEmpty());

        sstrs.get(0).overrideReadMeter(new RestorableMeter(100.0, 100.0));
        sstrs.get(1).overrideReadMeter(new RestorableMeter(200.0, 200.0));
        sstrs.get(2).overrideReadMeter(new RestorableMeter(300.0, 300.0));

        long estimatedKeys = sstrs.get(0).estimatedKeys();

        // if we have more than the max threshold, the coldest should be dropped
        sizeTieredBuckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstrs, stcsOptions, 1, 2);
        sizeTieredBuckets.aggregate();

        List<CompactionPick> compactions = sizeTieredBuckets.getCompactions();
        CompactionPick selected = CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates());
        if (!selected.isEmpty())
            assertEquals(selected, compactions.get(0));
        List<CompactionPick> pending = compactions.isEmpty() ? ImmutableList.of() : compactions.subList(1, compactions.size());

        assertEquals("one bucket should have been dropped", 2, selected.sstables.size());
        assertEquals("there should be one pending task", 1, pending.size());

        double expectedBucketHotness = (200.0 + 300.0) / estimatedKeys;
        assertEquals(String.format("bucket hotness (%f) should be close to %f",
                                   CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates()).hotness, expectedBucketHotness),
                     expectedBucketHotness, CompactionAggregate.getSelected(sizeTieredBuckets.getAggregates()).hotness, 1.0);
    }


    @Test
    public void testTwoBucketsDifferentHotness()
    {
        List<SSTableReader> bucket1 = mockBucket(8, 2000, 100);
        List<SSTableReader> bucket2 = mockBucket(8, 1000, 200); // hottest bucket with hotness 200 per table should be selected

        List<SSTableReader> sstables = Stream.concat(bucket1.stream(), bucket2.stream()).collect(Collectors.toList());
        for (int i = 0; i < 5; i++)
        {
            Collections.shuffle(sstables, random);
            testBuckets(sstables, bucket2, ImmutableList.of(bucket1), 2);
        }
    }

    @Test
    public void testTwoBucketsSameHotness()
    {
        List<SSTableReader> bucket1 = mockBucket(8, 1000, 100);
        List<SSTableReader> bucket2 = mockBucket(8, 4000, 100); // bucket with largest sstables should be selected if same hotness

        List<SSTableReader> sstables = Stream.concat(bucket1.stream(), bucket2.stream()).collect(Collectors.toList());
        for (int i = 0; i < 5; i++)
        {
            Collections.shuffle(sstables, random);
            testBuckets(sstables, bucket2, ImmutableList.of(bucket1), 2);
        }
    }

    @Test
    public void testSplitLargeBucketExactMultiple()
    {
        List<SSTableReader> bucket1 = mockBucket(maxThreshold, 1000, 100);
        List<SSTableReader> bucket2 = mockBucket(maxThreshold, 1000, 200);
        List<SSTableReader> bucket3 = mockBucket(maxThreshold, 1000, 300);
        List<SSTableReader> bucket4 = mockBucket(maxThreshold, 1000, 400); // hottest bucket

        List<SSTableReader> largeBucket = new ArrayList<>(maxThreshold * 4);
        largeBucket.addAll(bucket1);
        largeBucket.addAll(bucket2);
        largeBucket.addAll(bucket3);
        largeBucket.addAll(bucket4);

        Collections.shuffle(largeBucket, random);

        testBuckets(largeBucket, bucket4, ImmutableList.of(bucket3, bucket2, bucket1), 1);
    }

    @Test
    public void testSplitLargeBucketNotExactMultiple()
    {
        List<SSTableReader> bucket1 = mockBucket(maxThreshold / 2, 1000, 100);
        List<SSTableReader> bucket2 = mockBucket(maxThreshold, 1000, 200);
        List<SSTableReader> bucket3 = mockBucket(maxThreshold, 1000, 300);
        List<SSTableReader> bucket4 = mockBucket(maxThreshold, 1000, 400); // hottest bucket

        List<SSTableReader> largeBucket = new ArrayList<>(maxThreshold * 4);
        largeBucket.addAll(bucket1);
        largeBucket.addAll(bucket2);
        largeBucket.addAll(bucket3);
        largeBucket.addAll(bucket4);

        Collections.shuffle(largeBucket, random);

        testBuckets(largeBucket, bucket4, ImmutableList.of(bucket3, bucket2, bucket1), 1);
    }

    @Test
    public void testSplitLargeBucketWithLeftOverBelowMinThreshold()
    {
        List<SSTableReader> bucket1 = mockBucket(minThreshold - 1, 1000, 100); // should be ignored
        List<SSTableReader> bucket2 = mockBucket(maxThreshold, 1000, 200); // hottest bucket

        List<SSTableReader> largeBucket = new ArrayList<>(maxThreshold * 4);
        largeBucket.addAll(bucket1);
        largeBucket.addAll(bucket2);

        Collections.shuffle(largeBucket, random);

        testBuckets(largeBucket, bucket2, ImmutableList.of(), 1);
    }

    @Test
    public void testIgnoreBucketsBelowMinThreshold()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        long bytesOnDisk = 1000;
        double hotness = 200;
        for (int i = 0; i < minThreshold; i++)
        {
            sstables.addAll(mockBucket(i, bytesOnDisk, hotness));
            bytesOnDisk *= 2;
            hotness *= 2;
        }

        // all buckets with sstables should be considered and so the number of expected aggregates
        // is minThreshold - 1 (because one has no sstables)
        testBuckets(sstables, ImmutableList.of(), ImmutableList.of(), minThreshold - 1);
    }

    @Test
    public void testIgnoreBucketsBelowMinThresholdExceptOne()
    {
        List<SSTableReader> sstables = new ArrayList<>();
        long bytesOnDisk = 1000;
        double hotness = 200;
        for (int i = 0; i < minThreshold; i++)
        {
            sstables.addAll(mockBucket(i, bytesOnDisk, hotness));
            bytesOnDisk *= 2;
            hotness *= 2;
        }

        List<SSTableReader> bucket = mockBucket(minThreshold, bytesOnDisk, hotness);
        sstables.addAll(bucket); // this is the only bucket that should be picked up

        // all buckets with sstables should be considered and so the number of expected aggregates
        // is minThreshold (because one has no sstables)
        testBuckets(sstables, bucket, ImmutableList.of(), minThreshold);
    }

    @Test
    public void testManySmallSSTables()
    {
        // SStables smaller than minSSTableSize should all be grouped in the same bucket

        int minSSTableSize = 1000;
        List<SSTableReader> sstables = new ArrayList<>();

        for (int i = 0; i < 10; i++)
        {
            List<SSTableReader> bucket = mockBucket(minThreshold + random.nextInt(maxThreshold), random.nextInt(minSSTableSize), 100);
            sstables.addAll(bucket);
        }

        Collections.sort(sstables, Comparator.comparing(sstable -> sstable.onDiskLength()));

        List<List<SSTableReader>> buckets = new ArrayList<>();
        int i = 0;
        while ((sstables.size() - i) >= minThreshold)
        {
            buckets.add(sstables.subList(i, Math.min(i+ maxThreshold, sstables.size())));
            i += maxThreshold;
        }

        SizeTieredCompactionStrategyOptions stcsOptions = new SizeTieredCompactionStrategyOptions(minSSTableSize, bucketLow, bucketHigh);
        testBuckets(stcsOptions, sstables, buckets.get(0), buckets.subList(1, buckets.size()), 1);
    }

    @Test
    public void testThreeBucketsOnlyLargestSizeHasComps()
    {
        List<SSTableReader> bucket1 = mockBucket(2, 1000, 0); // no compaction
        List<SSTableReader> bucket2 = mockBucket(2, 4000, 0); // no compaction
        List<SSTableReader> bucket3 = mockBucket(4, 8000, 0); // one compaction

        List<SSTableReader> sstables = new ArrayList<>(bucket1.size() + bucket2.size() + bucket3.size());
        sstables.addAll(bucket1);
        sstables.addAll(bucket2);
        sstables.addAll(bucket3);

        for (int i = 0; i < 5; i++)
        {
            Collections.shuffle(sstables, random);
            testBuckets(sstables, bucket3, ImmutableList.of(), 3);
        }
    }

    @Test
    public void testThreeBucketsOnlySmallestSizeHasComps()
    {
        List<SSTableReader> bucket1 = mockBucket(4, 1000, 0); // one compaction
        List<SSTableReader> bucket2 = mockBucket(2, 4000, 0); // no compaction
        List<SSTableReader> bucket3 = mockBucket(2, 8000, 0); // no compaction

        List<SSTableReader> sstables = new ArrayList<>(bucket1.size() + bucket2.size() + bucket3.size());
        sstables.addAll(bucket1);
        sstables.addAll(bucket2);
        sstables.addAll(bucket3);

        for (int i = 0; i < 5; i++)
        {
            Collections.shuffle(sstables, random);
            testBuckets(sstables, bucket1, ImmutableList.of(), 3);
        }
    }

    /**
     * Sort the buckets by calling {@link SizeTieredCompactionStrategy.SizeTieredBuckets#aggregate()} and then verify
     * that the selected bucket is {@code expectedBucket} and that the pending buckets are {@code expectedPending}.
     *
     * @param sstables - the input sstables to aggregate into buckets
     * @param expectedSelected - the expected bucket that should be selected for compaction
     * @param expectedPending - the expected pending buckets
     */
    private void testBuckets(List<SSTableReader> sstables, List<SSTableReader> expectedSelected, List<List<SSTableReader>> expectedPending, int numExpectedAggregates)
    {
        SizeTieredCompactionStrategyOptions stcsOptions = new SizeTieredCompactionStrategyOptions(minSSTableSize, bucketLow, bucketHigh);
        testBuckets(stcsOptions, sstables, expectedSelected, expectedPending, numExpectedAggregates);
    }

    private void testBuckets(SizeTieredCompactionStrategyOptions stcsOptions,
                             List<SSTableReader> sstables,
                             List<SSTableReader> expectedSelected,
                             List<List<SSTableReader>> expectedPending,
                             int numExpectedAggregates)
    {
        SizeTieredCompactionStrategy.SizeTieredBuckets buckets = new SizeTieredCompactionStrategy.SizeTieredBuckets(sstables,
                                                                                                                    stcsOptions,
                                                                                                                    minThreshold,
                                                                                                                    maxThreshold);
        buckets.aggregate();

        List<CompactionPick> compactions = buckets.getCompactions();
        CompactionPick selected = CompactionAggregate.getSelected(buckets.getAggregates());
        if (!selected.isEmpty())
            assertEquals(selected, compactions.get(0));
        List<CompactionPick> pending = compactions.isEmpty() ? ImmutableList.of() : compactions.subList(1, compactions.size());

        compareBucketToCandidate(expectedSelected, selected);
        assertEquals(expectedPending.size(), pending.size());

        for (int i = 0; i < expectedPending.size(); i++)
            compareBucketToCandidate(expectedPending.get(i), pending.get(i));

        assertEquals(numExpectedAggregates, buckets.getAggregates().size());
    }

    private List<SSTableReader> mockBucket(int numSSTables, long bytesOnDisk, double hotness)
    {
        List<SSTableReader> ret = new ArrayList<>(numSSTables);
        int h = 0;
        for (int i = 0; i < numSSTables; i++)
            ret.add(mockSSTable(bytesOnDisk, hotness));

        return ret;
    }

    private SSTableReader mockSSTable(long bytesOnDisk, double hotness)
    {
        SSTableReader ret = Mockito.mock(SSTableReader.class);
        when(ret.hotness()).thenReturn(hotness);
        when(ret.onDiskLength()).thenReturn(bytesOnDisk);
        when(ret.bytesOnDisk()).thenReturn(bytesOnDisk);
        when(ret.toString()).thenReturn(String.format("Bytes on disk: %d, hotness %f, hashcode %d", bytesOnDisk, hotness, ret.hashCode()));

        return ret;
    }

    private void compareBucketToCandidate(Collection<SSTableReader> bucket, CompactionPick candidate)
    {
        List<SSTableReader> sortedBucket = new ArrayList<>(bucket);
        List<SSTableReader> sortedCandidate = new ArrayList<>(candidate.sstables);

        // Sort by hash code because sorting by hotness may not work if several sstables have the
        // same hotness and length on disk
        Collections.sort(sortedBucket, Comparator.comparingLong(SSTableReader::hashCode));
        Collections.sort(sortedCandidate, Comparator.comparingLong(SSTableReader::hashCode));

        assertEquals(sortedBucket, sortedCandidate);
        assertEquals(getBucketHotness(bucket), candidate.hotness, 0.000001);
        assertEquals(bucket.size() > 0 ? getBucketSize(bucket) / (double) bucket.size() : 0, candidate.avgSizeInBytes, 1);
    }

    private double getBucketHotness(Collection<SSTableReader> bucket)
    {
        double ret = 0;
        for (SSTableReader sstable : bucket)
            ret += sstable.hotness();

        return ret;
    }

    private long getBucketSize(Collection<SSTableReader> bucket)
    {
        long ret = 0;
        for (SSTableReader sstable : bucket)
            ret += sstable.onDiskLength();

        return ret;
    }
}
