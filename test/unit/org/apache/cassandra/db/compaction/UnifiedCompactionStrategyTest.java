/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * The unified compaction strategy is described in this design document:
 *
 * TODO: link to design doc or SEP
 *
 * It has properties of  both tiered and leveled compactions and it adapts to the workload
 * by switching between strategies or increasing / decreasing the fanout factor.
 *
 * The essential formulae are the calculations of buckets:
 *
 * S = ⌊log_oF(size / m)⌋ = ⌊(ln size - ln m) / (ln F + ln o)⌋
 *
 * where log_oF is the log with oF as the base
 * o is the survival factor, currently fixed to 1
 * F is the fanout factor calculated below
 * m is the minimal size, fixed in the strategy options
 * size is the sorted run size (sum of all the sizes of the sstables in the sorted run)
 *
 * Also, T is the number of sstables that trigger compaction.
 *
 * Give a parameter W, which is fixed in these tests, then T and F are calculated as follows:
 *
 * - W < 0 then T = 2 and F = 2 - W (leveled merge policy)
 * - W > 0 then T = F and F = 2 + W (tiered merge policy)
 * - W = 0 then T = F = 2 (middle ground)
 */
@RunWith(Parameterized.class)
public class UnifiedCompactionStrategyTest extends BaseCompactionStrategyTest
{
    private final static long ONE_MB = 1 << 20;

    // Multiple disks can be used both with and without disk boundaries. We want to test both cases.

    @Parameterized.Parameters(name = "useDiskBoundaries {0}")
    public static Iterable<Object[]> params()
    {
        return Arrays.asList(new Object[][] { {false}, {true} });
    }

    @Parameterized.Parameter
    public boolean useDiskBoundaries = true;

    @BeforeClass
    public static void setUpClass()
    {
        BaseCompactionStrategyTest.setUpClass();
    }

    @Before
    public void setUp()
    {
        super.setUp();
    }

    @Test
    public void testNoSSTables()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(4);
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        assertTrue(strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).isEmpty());
        assertEquals(0, strategy.getEstimatedRemainingTasks());
    }

    @Test
    public void testGetBucketsSameWUniqueArena()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i = 0; i < 20; i++)
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(m * i, numSSTables);
        }

        // W = 3, o = 1 => F = 5, T = 5 => expected T sstables and 2 buckets: 0-10m, 10-50m
        testGetBucketsOneArena(sstables, new int[] { 3 }, m, new int[] { 5, 5});

        // W = 2, o = 1 => F = 4, T = 4 => expected T sstables and 3 buckets: 0-8m, 8-32m, 32-128m
        testGetBucketsOneArena(sstables, new int[] { 2 }, m, new int[] { 4, 4, 4});

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBucketsOneArena(sstables, new int[] { 0 }, m, new int[] { 2, 2, 2, 2, 2});

        // W = -2, o = 1 => F = 4, T = 2 => expected 2 sstables and 3 buckets: 0-8mb, 8-32m, 32-128m
        testGetBucketsOneArena(sstables, new int[] { -2 }, m, new int[] { 2, 2, 2});

        // W = -3, o = 1 => F = 5, T = 2 => expected 2 sstables and 2 buckets: 0-10m, 10-50m
        testGetBucketsOneArena(sstables, new int[] { -3 }, m, new int[] { 2, 2});

        // remove sstables from 4m to 8m to create an empty bucket in the next call
        sstables.remove(4); // 4m
        sstables.remove(6); // 6m
        sstables.remove(8); // 8m

        // W = 0, o = 1 => F = 2, T = 2 => expected 2 sstables and 5 buckets: 0-4m, 4-8m, 8-16m, 16-32m, 32-64m
        testGetBucketsOneArena(sstables, new int[] { 0 }, m, new int[] { 2, 2, 2, 2, 2});
    }

    @Test
    public void testGetBucketsDifferentWsUniqueArena()
    {
        final int m = 2; // minimal sorted run size in MB m
        final Map<Integer, Integer> sstables = new TreeMap<>();

        for (int i : new int[] { 50, 100, 200, 400, 600, 800, 1000})
        {
            int numSSTables = 2 + random.nextInt(18);
            sstables.put(i, numSSTables);
        }

        // W = [30, 2, -6], o = 1 => F = [32, 4, 8] , T = [32, 4, 2]  => expected 3 buckets: 0-64m, 64-256m 256-2048m
        testGetBucketsOneArena(sstables, new int[]{ 30, 2, -6 }, m, new int[] { 32, 4, 2});

        // W = [30, 6, -8], o = 1 => F = [32, 8, 10] , T = [32, 8, 2]  => expected 3 buckets: 0-64m, 64-544m 544-5440m
        testGetBucketsOneArena(sstables, new int[]{ 30, 6, -8 }, m, new int[] { 32, 8, 2});

        // W = [0, 0, 0, -2, -2], o = 1 => F = [2, 2, 2, 4, 4] , T = [2, 2, 2, 2, 2]  => expected 6 buckets: 0-4m, 4-8m, 8-16m, 16-64m, 64-256m, 256-1024m
        testGetBucketsOneArena(sstables, new int[]{ 0, 0, 0, -2, -2 }, m, new int[] { 2, 2, 2, 2, 2, 2});
    }

    private void testGetBucketsOneArena(Map<Integer, Integer> sstableMap, int[] Ws, int m, int[] expectedTs)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getNumShards()).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);

        when(controller.getScalingParameter(anyInt())).thenAnswer(answer -> {
            int index = answer.getArgument(0);
            return Ws[index < Ws.length ? index : Ws.length - 1];
        });
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();

        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = cfs.getPartitioner();
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMaximumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        long dataSetSizeBytes = 0;
        for (Map.Entry<Integer, Integer> entry : sstableMap.entrySet())
        {
            for (int i = 0; i < entry.getValue(); i++)
            {
                // we want a number > 0 and < 1 so that the sstable has always some size and never crosses the boundary to the next bucket
                // so we leave a 1% margin, picking a number from 0.01 to 0.99
                double rand = 0.01 + 0.98 * random.nextDouble();
                long sizeOnDiskBytes = (entry.getKey() << 20) + (long) (minimalSizeBytes * rand);
                dataSetSizeBytes += sizeOnDiskBytes;
                sstables.add(mockSSTable(sizeOnDiskBytes, System.currentTimeMillis(), first, last));
            }
        }
        dataTracker.addInitialSSTables(sstables);

        Map<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> arenas = strategy.getShardsWithBuckets();
        assertNotNull(arenas);
        assertEquals(1, arenas.size());

        for (Map.Entry<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Bucket> buckets = entry.getValue();
            assertEquals(expectedTs.length, buckets.size());

            for (int i = 0; i < expectedTs.length; i++)
            {
                UnifiedCompactionStrategy.Bucket bucket = buckets.get(i);
                if (bucket.sstables.size() >= expectedTs[i])
                    assertFalse(bucket.getCompactionAggregate(entry.getKey(), Collections.EMPTY_SET, controller, dataSetSizeBytes).isEmpty());
                else
                    assertTrue(bucket.getCompactionAggregate(entry.getKey(), Collections.EMPTY_SET, controller, dataSetSizeBytes).isEmpty());
            }
        }
    }

    @Test
    public void testOversizedCompactions_limitingTriggered_maxSpaceOverhead1pct()
    {
        testLimitOversizedCompactions(true, 0.01);
    }

    @Test
    public void testOversizedCompactions_limitingTriggered_maxSpaceOverhead10pct()
    {
        testLimitOversizedCompactions(true, 0.1);
    }

    @Test
    public void testOversizedCompactions_limitingTriggered_maxSpaceOverhead20pct()
    {
        testLimitOversizedCompactions(true, 0.2);
    }

    @Test
    public void testOversizedCompactions_limitingTriggered_maxSpaceOverhead50pct()
    {
        testLimitOversizedCompactions(true, 0.5);
    }

    @Test
    public void testOversizedCompactions_limitingTriggered_maxSpaceOverhead90pct()
    {
        testLimitOversizedCompactions(true, 0.9);
    }

    void testLimitOversizedCompactions(boolean triggerOversizedLimiting, double maxSpaceOverhead)
    {
        testLimitCompactions(1000, true, triggerOversizedLimiting, maxSpaceOverhead);
    }

    @Test
    public void testLimitCompactions_noLimiting()
    {
        testLimitCompactionsCount(true, 1000);
    }

    @Test
    public void testLimitCompactionsCount_1()
    {
        testLimitCompactionsCount(false, 1);
    }

    @Test
    public void testLimitCompactionsCount_3()
    {
        testLimitCompactionsCount(false, 3);
    }

    @Test
    public void testLimitCompactionsCount_PerLevel_1()
    {
        testLimitCompactionsCount(true, 1);
    }

    @Test
    public void testLimitCompactionsCount_PerLevel_5()
    {
        testLimitCompactionsCount(true, 5);
    }

    @Test
    public void testLimitCompactionsCount_PerLevel_11()
    {
        testLimitCompactionsCount(true, 11);
    }

    void testLimitCompactionsCount(boolean topLevelOnly, int count)
    {
        testLimitCompactions(count, topLevelOnly, false, 1.0);
    }

    public void testLimitCompactions(int maxCount, boolean topLevelOnly, boolean triggerOversizedLimiting, double maxSpaceOverhead)
    {
        final int numBuckets = 4;
        UnifiedCompactionStrategy strategy = prepareStrategyWithLimits(maxCount,
                                                                       topLevelOnly,
                                                                       triggerOversizedLimiting,
                                                                       maxSpaceOverhead,
                                                                       Double.MAX_VALUE,
                                                                       numBuckets);

        int numShards = strategy.getController().getNumShards();
        // Without limiting oversized compactions kicking in, we expect one compaction per shard, otherwise we expect
        // a fraction of the number of all shards, proportional to the max allowed space amplification fraction.
        int expectedCompactionTasks = triggerOversizedLimiting
                                      ? (int) (Math.floor(numShards * maxSpaceOverhead))
                                      : topLevelOnly
                                        ? Math.min((maxCount + numBuckets - 1) / numBuckets, numShards)
                                        : Math.min(maxCount, numShards);
        // TODO: Check that a warning was issued if space overhead limit was too low.
        assertEquals(expectedCompactionTasks, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());
    }

    @Test
    public void testPreserveLayout_W2_947()
    {
        testPreserveLayout(2, 947);
    }

    @Test
    public void testPreserveLayout_WM2_947()
    {
        testPreserveLayout(-2, 947);
    }

    @Test
    public void testPreserveLayout_W2_251()
    {
        testPreserveLayout(2, 251);
    }

    @Test
    public void testPreserveLayout_WM2_251()
    {
        testPreserveLayout(-2, 251);
    }

    @Test
    public void testPreserveLayout_W2_320()
    {
        testPreserveLayout(2, 320);
    }

    @Test
    public void testPreserveLayout_WM2_320()
    {
        testPreserveLayout(-2, 320);
    }

    @Test
    public void testPreserveLayout_WM2_947_128()
    {
        testLayout(-2, 947, 128);
    }

    @Test
    public void testPreserveLayout_WM2_947_64()
    {
        testLayout(-2, 947, 64);
    }

    public void testPreserveLayout(int W, int numSSTables)
    {
        testLayout(W, numSSTables, 10000);
    }

    @Test
    public void testMaxSSTablesToCompact()
    {
        testLayout(2, 944,  60);
        testLayout(2, 944, 1000);
        testLayout(2, 944,  100);
        testLayout(2, 803,  200);
    }

    public void testLayout(int W, int numSSTables, int maxSSTablesToCompact)
    {
        int F = 2 + Math.abs(W);
        int T = W < 0 ? 2 : F;
        final long minSstableSizeBytes = 2L << 20; // 2 MB
        final int numShards = 1;
        final int levels = (int) Math.floor(Math.log(numSSTables) / Math.log(F)) + 1;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minSstableSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(numShards);
        when(controller.getMaxSpaceOverhead()).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);

        if (maxSSTablesToCompact >= numSSTables)
            when(controller.maxConcurrentCompactions()).thenReturn(levels * (W < 0 ? 1 : F)); // make sure the work is assigned to different levels
        else
            when(controller.maxConcurrentCompactions()).thenReturn(1000); // make sure the work is assigned to different levels

        when(controller.maxCompactionSpaceBytes()).thenCallRealMethod();
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getDataSetSizeBytes()).thenReturn(minSstableSizeBytes * numSSTables * numShards);
        when(controller.maxSSTablesToCompact()).thenReturn(maxSSTablesToCompact);
        Random random = Mockito.mock(Random.class);
        when(random.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(random);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        List<SSTableReader> allSstables = new ArrayList<>();

        List<SSTableReader> sstables = mockSSTables(numSSTables,
                                                    minSstableSizeBytes,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    0,
                                                    true,
                                                    null);
        allSstables.addAll(sstables);
        dataTracker.addInitialSSTables(allSstables);

        int num = numSSTables;
        Collection<AbstractCompactionTask> tasks;
        boolean headerPrinted = false;
        while (true)
        {
            tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
            if (tasks.isEmpty())
                break;

            for (CompactionAggregate aggregate : strategy.getAggregates())
            {
                if (!headerPrinted)
                    System.out.println(aggregate.getStatistics().header());
                headerPrinted = true;
                System.out.println(aggregate.getStatistics().data());
            }

            boolean layout = Math.min(num, maxSSTablesToCompact) > F * F;
            int limit;
            if (layout)
            {
                int forLimitLevel = (int) (Math.pow(F, Math.floor(Math.log(maxSSTablesToCompact) / Math.log(F))));
                // for clarification see W < 0 case in layoutCompactions method
                limit = W < 0 ? maxSSTablesToCompact / forLimitLevel * forLimitLevel : forLimitLevel;
            }
            else
                limit = maxSSTablesToCompact;

            for (AbstractCompactionTask task : tasks)
            {
                int expected = num;
                if (layout)
                {
                    int forTopLevel = (int) (Math.pow(F, Math.floor(Math.log(num) / Math.log(F))));
                    expected = W > 0
                               ? forTopLevel
                               : num / forTopLevel * forTopLevel;

                }
                expected = Math.min(expected, limit);

                int count = task.transaction.originals().size();
                assertEquals(expected, count);
                num -= count;
            }
        }
        // Check that we issue all the compactions
        assertTrue(num < T);
    }

    @Test
    public void testLimitCompactionsThroughput_1()
    {
        testLimitCompactionsThroughput(1000, 1);
    }

    @Test
    public void testLimitCompactionsThroughput_3()
    {
        testLimitCompactionsThroughput(1000, 3);
    }

    @Test
    public void testOversizedCompactions_limitingNotTriggered()
    {
        testLimitOversizedCompactions(false, 1.0);
    }

    void testLimitCompactionsThroughput(int maxCount, int maxThroughput)
    {
        UnifiedCompactionStrategy strategy = prepareStrategyWithLimits(maxCount, false, false, 1.0, maxThroughput, 4);

        // first call should return a pilot task
        assertEquals(1, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        // if task hasn't progressed, no new tasks should be produced
        assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        for (CompactionPick pick : strategy.backgroundCompactions.getCompactionsInProgress())
            strategy.backgroundCompactions.onInProgress(mockProgress(strategy, pick.id));

        // now that we have a rate, make sure we produce tasks to fill up the limit
        assertEquals(Math.min(maxThroughput, maxCount) - 1, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        // and don't create any new ones when the limit is filled, before they make progress
        assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        for (CompactionPick pick : strategy.backgroundCompactions.getCompactionsInProgress())
            if (pick.progress == null)
                strategy.backgroundCompactions.onInProgress(mockProgress(strategy, pick.id));

        // and also when they do
        assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        for (int remaining = strategy.getController().getNumShards() - Math.min(maxThroughput, maxCount);
             remaining > 0;
             --remaining)
        {
            // mark a task as completed
            strategy.backgroundCompactions.onCompleted(strategy, Iterables.get(strategy.backgroundCompactions.getCompactionsInProgress(), 0).id);

            // and check that we get a new one
            assertEquals(1, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());
        }
    }

    private UnifiedCompactionStrategy prepareStrategyWithLimits(int maxCount,
                                                                boolean topBucketOnly,
                                                                boolean triggerOversizedLimiting,
                                                                double maxSpaceOverhead,
                                                                double maxThroughput,
                                                                int numBuckets)
    {
        int W = 2; // W = 2 => T = F = 4
        int T = 4;
        int F = 4;
        final long minSstableSizeBytes = 2L << 20; // 2 MB
        final int numShards = 5;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minSstableSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(numShards);
        when(controller.getMaxSpaceOverhead()).thenReturn(maxSpaceOverhead);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(maxCount);
        when(controller.maxCompactionSpaceBytes()).thenCallRealMethod();
        when(controller.maxThroughput()).thenReturn(maxThroughput);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        // Calculate the minimum shard size such that the top bucket compactions won't be considered "oversized" and
        // all will be allowed to run. The calculation below assumes (1) that compactions are considered "oversized"
        // if they are more than 1/2 of the max shard size; (2) that mockSSTables uses 15% less than the max SSTable
        // size for that bucket.
        long topBucketMaxSstableSize = (long) (minSstableSizeBytes * Math.pow(F, numBuckets));
        long topBucketMaxCompactionSize = T * topBucketMaxSstableSize;
        when(controller.getDataSetSizeBytes()).thenReturn(topBucketMaxCompactionSize * numShards);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        List<SSTableReader> allSstables = new ArrayList<>();

        for (int i = numBuckets; i > 0; i--)
        {
            // Set compactions only in the top bucket of each shard
            int numSstables = (!topBucketOnly || i == numBuckets) ? T : T - 1;
            long size = (long) (minSstableSizeBytes * Math.pow(F, i));
            // Simulate shards by using different disk indexes
            for (int j = numShards; j > 0; j--)
            {
                List<SSTableReader> sstables = mockSSTables(numSstables,
                                                            size,
                                                            0,
                                                            System.currentTimeMillis(),
                                                            j - 1,
                                                            true,
                                                            null);
                allSstables.addAll(sstables);
            }
        }
        dataTracker.addInitialSSTables(allSstables);
        return strategy;
    }

    private CompactionProgress mockProgress(UnifiedCompactionStrategy strategy, UUID id)
    {
        CompactionProgress progress = Mockito.mock(CompactionProgress.class);
        when(progress.durationInNanos()).thenReturn(1000L*1000*1000);
        when(progress.outputDiskSize()).thenReturn(1L);
        when(progress.operationId()).thenReturn(id);
        return progress;
    }

    private static final class ArenaSpecs
    {
        private List<SSTableReader> sstables;
        private int[] expectedBuckets;

        ArenaSpecs(int[] expectedBuckets)
        {
            this.sstables = new ArrayList<>();
            this.expectedBuckets = expectedBuckets;
        }
    }

    private ArenaSpecs mockArena(Token min,
                                 Token max,
                                 Map<Long, Integer> sstables,
                                 boolean repaired,
                                 UUID pendingRepair,
                                 int diskIndex,
                                 int[] expectedBuckets)
    {
        ArenaSpecs arena = new ArenaSpecs(expectedBuckets);
        ByteBuffer bb = ByteBuffer.allocate(0);

        sstables.forEach((size, num) -> {
            // Generate a key inside the shard, but make sure it's not too close to the boundaries to compensate for
            // rounding differences between splitting directly and splitting first by disk and then by shard.
            Token first = min.getPartitioner().split(min, max, 0.01 + random.nextDouble() * 0.98);
            Token last = min.getPartitioner().split(min, max, 0.99);

            for (int i = 0; i < num; i++)
            {
                arena.sstables.add(mockSSTable(0,
                                               size,
                                               System.currentTimeMillis(),
                                               0.0,
                                               new BufferDecoratedKey(first, bb),
                                               new BufferDecoratedKey(last, bb),
                                               diskIndex,
                                               repaired,
                                               pendingRepair,
                                               0));
                first = first.increaseSlightly();
            }
        });

        return arena;
    }

    private List<PartitionPosition> makeBoundaries(int numShards, int numDisks)
    {
        IPartitioner partitioner = cfs.getPartitioner();
        assert numShards >= 1;
        assert numDisks >= 1;

        if (numShards * numDisks == 1)
            return ImmutableList.of(partitioner.getMaximumToken().maxKeyBound());

        Splitter splitter = partitioner.splitter().orElse(null);
        assertNotNull("The partitioner must support a splitter", splitter);

        int numBoundaries = useDiskBoundaries ? numDisks * numShards : numShards;
        Splitter.WeightedRange range = new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        final List<PartitionPosition> shards = splitter.splitOwnedRanges(numBoundaries, ImmutableList.of(range), Splitter.SplitType.ALWAYS_SPLIT)
                                               .boundaries
                                               .stream()
                                               .map(Token::maxKeyBound)
                                               .collect(Collectors.toList());
        if (useDiskBoundaries)
        {
            diskBoundaryPositions = new ArrayList<>(numDisks);
            for (int i = 0; i < numDisks; ++i)
                diskBoundaryPositions.add(shards.get((i + 1) * numShards - 1));
        }
        return shards;
    }

    private List<ArenaSpecs> mockArenas(int diskIndex,
                                        int diskCount,
                                        boolean repaired,
                                        UUID pendingRepair,
                                        List<PartitionPosition> boundaries,
                                        Map<Long, Integer> sstables,
                                        int[] buckets)
    {
        List<ArenaSpecs> arenasList = new ArrayList<>();

        int numShards = boundaries.size() / diskCount;
        List<PartitionPosition> shardPositions = useDiskBoundaries
                                                 ? boundaries.subList(diskIndex * numShards, (diskIndex + 1) * numShards)
                                                 : boundaries;
        Token min = useDiskBoundaries && diskIndex > 0
                    ? boundaries.get(diskIndex * numShards - 1).getToken()
                    : partitioner.getMinimumToken();

        for (PartitionPosition boundary : shardPositions)
        {
            Token max = boundary.getToken();

            // what matters is the first key, which must be less than max
            arenasList.add(mockArena(min, max, sstables, repaired, pendingRepair, diskIndex, buckets));

            min = max;
        }

        return arenasList;
    }

    private static Map<Long, Integer> mapFromPair(Pair<Long, Integer> ... pairs)
    {
        Map<Long, Integer> ret = new HashMap<>();
        for (Pair<Long, Integer> pair : pairs)
        {
            ret.put(pair.left, pair.right);
        }

        return ret;
    }

    @Test
    public void testAllArenasOneBucket_NoShards()
    {
        testAllArenasOneBucket(1);
    }

    @Test
    public void testAllArenasOneBucket_MultipleShards()
    {
        testAllArenasOneBucket(5);
    }

    private void testAllArenasOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        UUID pendingRepair = UUID.randomUUID();
        arenasList.addAll(mockArenas(0, 2, false, pendingRepair, boundaries, sstables, buckets)); // pending repair

        arenasList.addAll(mockArenas(0, 2, false, null, boundaries, sstables, buckets)); // unrepaired
        arenasList.addAll(mockArenas(1, 2, false, null, boundaries, sstables, buckets)); // unrepaired, next disk

        arenasList.addAll(mockArenas(0, 2, true, null, boundaries, sstables, buckets)); // repaired
        arenasList.addAll(mockArenas(1, 2, true, null, boundaries, sstables, buckets)); // repaired, next disk

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedOneDiskOneBucket_NoShards()
    {
        testRepairedOneDiskOneBucket(1);
    }

    @Test
    public void testRepairedOneDiskOneBucket_MultipleShards()
    {
        testRepairedOneDiskOneBucket(5);
    }

    private void testRepairedOneDiskOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 1);
        List<ArenaSpecs> arenas = mockArenas(0, 1, true, null, boundaries, sstables, buckets);
        testGetBucketsMultipleArenas(arenas, W, m, boundaries);
    }

    @Test
    public void testRepairedTwoDisksOneBucket_NoShards()
    {
        testRepairedTwoDisksOneBucket(1);
    }

    @Test
    public void testRepairedTwoDisksOneBucket_MultipleShards()
    {
        testRepairedTwoDisksOneBucket(5);
    }

    private void testRepairedTwoDisksOneBucket(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        Map<Long, Integer> sstables = mapFromPair(Pair.create(4 * ONE_MB, 4));
        int[] buckets = new int[]{4};

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenas = new ArrayList<>();

        arenas.addAll(mockArenas(0, 2, true, null, boundaries, sstables, buckets));
        arenas.addAll(mockArenas(1, 2, true, null, boundaries, sstables, buckets));

        testGetBucketsMultipleArenas(arenas, W, m, boundaries);
    }

    @Test
    public void testRepairedMultipleDisksMultipleBuckets_NoShards()
    {
        testRepairedMultipleDisksMultipleBuckets(1);
    }

    @Test
    public void testRepairedMultipleDisksMultipleBuckets_MultipleShards()
    {
        testRepairedMultipleDisksMultipleBuckets(15);
    }

    private void testRepairedMultipleDisksMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 6);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        for (int i = 0; i < 6; i++)
        {
            if (i % 2 == 0)
                arenasList.addAll(mockArenas(i, 6, true, null, boundaries, sstables1, buckets1));
            else
                arenasList.addAll(mockArenas(i, 6, true, null, boundaries, sstables2, buckets2));

        }

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedUnrepairedOneDiskMultipleBuckets_NoShards()
    {
        testRepairedUnrepairedOneDiskMultipleBuckets(1);
    }

    @Test
    public void testRepairedUnrepairedOneDiskMultipleBuckets_MultipleShards()
    {
        testRepairedUnrepairedOneDiskMultipleBuckets(10);
    }

    private void testRepairedUnrepairedOneDiskMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 1);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        arenasList.addAll(mockArenas(0, 1, true, null, boundaries, sstables2, buckets2)); // repaired
        arenasList.addAll(mockArenas(0, 1, false, null, boundaries, sstables1, buckets1)); // unrepaired

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    @Test
    public void testRepairedUnrepairedTwoDisksMultipleBuckets_NoShards()
    {
        testRepairedUnrepairedTwoDisksMultipleBuckets(1);
    }

    @Test
    public void testRepairedUnrepairedTwoDisksMultipleBuckets_MultipleShards()
    {
        testRepairedUnrepairedTwoDisksMultipleBuckets(5);
    }

    private void testRepairedUnrepairedTwoDisksMultipleBuckets(int numShards)
    {
        final int m = 2; // minimal sorted run size in MB
        final int W = 2; // => o = 1 => F = 4, T = 4: 0-8m, 8-32m, 32-128m

        List<PartitionPosition> boundaries = makeBoundaries(numShards, 2);
        List<ArenaSpecs> arenasList = new ArrayList<>();

        Map<Long, Integer> sstables1 = mapFromPair(Pair.create(2 * ONE_MB, 4), Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 4));
        int[] buckets1 = new int[]{4,4,4};

        Map<Long, Integer> sstables2 = mapFromPair(Pair.create(8 * ONE_MB, 4), Pair.create(32 * ONE_MB, 8));
        int[] buckets2 = new int[]{0,4,8};

        arenasList.addAll(mockArenas(0, 2, true, null, boundaries, sstables2, buckets2));  // repaired, first disk
        arenasList.addAll(mockArenas(1, 2, true, null, boundaries, sstables1, buckets1));  // repaired, second disk

        arenasList.addAll(mockArenas(0, 2, false, null, boundaries, sstables1, buckets1));  // unrepaired, first disk
        arenasList.addAll(mockArenas(1, 2, false, null, boundaries, sstables2, buckets2));  // unrepaired, second disk

        testGetBucketsMultipleArenas(arenasList, W, m, boundaries);
    }

    private void testGetBucketsMultipleArenas(List<ArenaSpecs> arenaSpecs, int W, int m, List<PartitionPosition> shards)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.getNumShards()).thenReturn(shards.size());
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        List<SSTableReader> sstables = arenaSpecs.stream().flatMap(a -> a.sstables.stream()).collect(Collectors.toList());
        dataTracker.addInitialSSTables(sstables);

        Map<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> arenas = strategy.getShardsWithBuckets();
        assertNotNull(arenas);
        assertEquals(arenaSpecs.size(), arenas.size());

        int idx = 0;
        for (Map.Entry<UnifiedCompactionStrategy.Shard, List<UnifiedCompactionStrategy.Bucket>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Bucket> buckets = entry.getValue();
            ArenaSpecs currentArenaSpecs = arenaSpecs.get(idx++);

            assertEquals(currentArenaSpecs.expectedBuckets.length, buckets.size());
            for (int i = 0; i < currentArenaSpecs.expectedBuckets.length; i++)
                assertEquals(currentArenaSpecs.expectedBuckets[i], buckets.get(i).sstables.size());
        }
    }

    @Test
    public void testShardBoundaries()
    {
        // no shards
        testShardBoundaries(ints(100), 1, 1, ints(10, 50));
        // split on disks at minimum
        testShardBoundaries(ints(30, 100), 1, 2, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50, 100), 1, 5, ints(10, 51, 61, 70));

        // no disks
        testShardBoundaries(ints(30, 100), 2, 1, ints(10, 50));
        testShardBoundaries(ints(20, 30, 40, 50, 100), 5, 1, ints(10, 51, 61, 70));

        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80, 100), 9, 3, ints(0, 90));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(0, 51, 61, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(0, 49, 59, 100));
        testShardBoundaries(ints(12, 23, 33, 45, 56, 70, 80, 90, 100), 9, 3, ints(0, 9, 11, 20, 21, 39, 41, 50, 51, 60, 64, 68, 68, 100));

        // uneven
        testShardBoundaries(ints(11, 22, 33, 42, 50, 58, 67, 78, 89, 100), 10, 3, ints(0, 100));
        testShardBoundaries(ints(8, 17, 25, 38, 50, 58, 67, 75, 88, 100), 10, 4, ints(0, 100));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 80, 90, 100), 10, 5, ints(0, 100));
        testShardBoundaries(ints(8, 17, 33, 42, 50, 58, 67, 83, 92, 100), 10, 6, ints(0, 100));
        testShardBoundaries(ints(14, 21, 29, 43, 50, 57, 71, 79, 86, 100), 10, 7, ints(0, 100));
        testShardBoundaries(ints(13, 19, 25, 38, 50, 63, 69, 75, 88, 100), 10, 8, ints(0, 100));
        testShardBoundaries(ints(11, 22, 33, 44, 50, 56, 67, 78, 89, 100), 10, 9, ints(0, 100));

        // uneven again, where x0 are the disk boundaries and the others are inserted shard boundaries
        testShardBoundaries(ints(3, 7, 10, 13, 15, 18, 20, 23, 27, 100), 10, 3, ints(0, 30));
        testShardBoundaries(ints(3, 7, 10, 15, 20, 23, 27, 30, 35, 100), 10, 4, ints(0, 40));
        testShardBoundaries(ints(5, 10, 15, 20, 25, 30, 35, 40, 45, 100), 10, 5, ints(0, 50));
        testShardBoundaries(ints(5, 10, 20, 25, 30, 35, 40, 50, 55, 100), 10, 6, ints(0, 60));
        testShardBoundaries(ints(10, 15, 20, 30, 35, 40, 50, 55, 60, 100), 10, 7, ints(0, 70));
        testShardBoundaries(ints(10, 15, 20, 30, 40, 50, 55, 60, 70, 100), 10, 8, ints(0, 80));
        testShardBoundaries(ints(10, 20, 30, 40, 45, 50, 60, 70, 80, 100), 10, 9, ints(0, 90));
    }

    @Test
    public void testShardBoundariesWraparound()
    {
        // no shards
        testShardBoundaries(ints(100), 1, 1, ints(50, 10));
        // split on disks at minimum
        testShardBoundaries(ints(70, 100), 1, 2, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70, 100), 1, 5, ints(91, 31, 61, 71));
        // no disks
        testShardBoundaries(ints(70, 100), 2, 1, ints(50, 10));
        testShardBoundaries(ints(10, 20, 30, 70, 100), 5, 1, ints(91, 31, 61, 71));
        // split
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90, 100), 9, 3, ints(81, 71));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(51, 41));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90, 100), 9, 3, ints(21, 11));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 60, 70, 90, 100), 9, 3, ints(89, 79));
        testShardBoundaries(ints(10, 20, 30, 40, 60, 70, 80, 90, 100), 9, 3, ints(59, 49));
        testShardBoundaries(ints(10, 30, 40, 50, 60, 70, 80, 90, 100), 9, 3, ints(29, 19));

        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(91, 51, 61, 91));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(21, 51, 61, 21));
        testShardBoundaries(ints(10, 20, 30, 40, 50, 70, 80, 90, 100), 9, 3, ints(71, 51, 61, 71));
    }

    private int[] ints(int... values)
    {
        return values;
    }

    private void testShardBoundaries(int[] expected, int numShards, int numDisks, int[] rangeBounds)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        for (int i = 0; i < rangeBounds.length; i += 2)
            ranges.add(new Splitter.WeightedRange(1.0, new Range<>(getToken(rangeBounds[i + 0]), getToken(rangeBounds[i + 1]))));
        SortedLocalRanges sortedRanges = SortedLocalRanges.forTesting(cfs, ranges);

        List<PartitionPosition> diskBoundaries = sortedRanges.split(numDisks);

        int[] result = UnifiedCompactionStrategy.computeShardBoundaries(sortedRanges, diskBoundaries, numShards, partitioner)
                                                .stream()
                                                .map(PartitionPosition::getToken)
                                                .mapToInt(this::fromToken)
                                                .toArray();

        Assert.assertArrayEquals("Disks " + numDisks + " shards " + numShards + " expected " + Arrays.toString(expected) + " was " + Arrays.toString(result), expected, result);
    }

    private Token getToken(int x)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), x * 0.01);
    }

    private int fromToken(Token t)
    {
        IPartitioner partitioner = Murmur3Partitioner.instance;
        return (int) Math.round(partitioner.getMinimumToken().size(t) * 100.0);
    }

    @Test
    public void testGetNextBackgroundTasks()
    {
        assertCompactionTask(1, 3, CompactionTask.class);
        assertCompactionTask(3, 3, UnifiedCompactionTask.class);
    }

    private void assertCompactionTask(final int numShards, final int expectedNumOfTasks, Class expectedClass)
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(0);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(numShards);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = cfs.getPartitioner();

        List<SSTableReader> sstables = createSStables(partitioner);

        dataTracker.addInitialSSTables(sstables);

        Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals("Expecting number of next background tasks:", expectedNumOfTasks, tasks.size());
        for (AbstractCompactionTask task : tasks)
        {
            assertSame(expectedClass, task.getClass());
        }
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner)
    {
        return createSStables(partitioner, mapFromPair(Pair.create(4 * ONE_MB, 4)), 10000, UUID.randomUUID());
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner, int ttl, UUID pendingRepair)
    {
        return createSStables(partitioner, mapFromPair(Pair.create(4 * ONE_MB, 4)), ttl, pendingRepair);
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner, Map<Long, Integer> sstablesMap)
    {
        return createSStables(partitioner, sstablesMap, 10000, UUID.randomUUID());
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner,
                                               Map<Long, Integer> sstablesMap,
                                               int ttl,
                                               UUID pendingRepair)
    {
        List<SSTableReader> mockSSTables = new ArrayList<>();
        Token min = partitioner.getMinimumToken();
        Token max = partitioner.getMaximumToken();
        ByteBuffer bb = ByteBuffer.allocate(0);
        sstablesMap.forEach((size, num) -> {
            Token first = min.getPartitioner().split(min, max, 0.01 + random.nextDouble() * 0.98);

            for (int i = 0; i < num; i++)
            {
                // pending repair
                mockSSTables.add(mockSSTable(0,
                                             size,
                                             System.currentTimeMillis(),
                                             0.0,
                                             new BufferDecoratedKey(first, bb),
                                             new BufferDecoratedKey(max, bb),
                                             0,
                                             false,
                                             pendingRepair,
                                             ttl));
                first = first.increaseSlightly();
            }

            for (int i = 0; i < num; i++)
            {
                // unrepaired
                mockSSTables.add(mockSSTable(0,
                                             size,
                                             System.currentTimeMillis(),
                                             0.0,
                                             new BufferDecoratedKey(first, bb),
                                             new BufferDecoratedKey(max, bb),
                                             0,
                                             false,
                                             null,
                                             ttl));
                first = first.increaseSlightly();
            }

            for (int i = 0; i < num; i++)
            {
                // repaired
                mockSSTables.add(mockSSTable(0,
                                             size,
                                             System.currentTimeMillis(),
                                             0.0,
                                             new BufferDecoratedKey(first, bb),
                                             new BufferDecoratedKey(max, bb),
                                             0,
                                             true,
                                             null,
                                             ttl));
                first = first.increaseSlightly();
            }
        });
        return mockSSTables;
    }

    @Test
    public void testDropExpiredSSTables1Shard()
    {
        testDropExpiredFromBucket(1);
        testDropExpiredAndCompactNonExpired();
    }

    @Test
    public void testDropExpiredSSTables3Shards()
    {
        testDropExpiredFromBucket(3);
    }

    private void testDropExpiredFromBucket(int numShards)
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(3); // T=5
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(numShards);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        List<SSTableReader> sstables = createSStables(cfs.getPartitioner());
        // Tracker#addSSTables also tries to backup SSTables, so we use addInitialSSTables and notify explicitly
        dataTracker.addInitialSSTables(sstables);

        try
        {
            // nothing to compact yet
            assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

            int timestamp = sstables.get(sstables.size() - 1).getMaxLocalDeletionTime();
            int expirationPoint = timestamp + 1;

            assertEquals(3, strategy.getNextBackgroundTasks(expirationPoint).size()); // repaired, unrepaired, pending
            Collection<CompactionPick> picks = strategy.backgroundCompactions.getCompactionsInProgress();
            for (CompactionPick pick : picks)
            {
                // expired SSTables don't contribute to total size
                assertTrue(pick.hasExpiredOnly());
                assertEquals(sstables.size() / 3, pick.expired.size());
                assertEquals(0L, pick.totSizeInBytes);
                assertEquals(0L, pick.avgSizeInBytes);
                assertEquals(0, pick.parent);
            }
        }
        finally
        {
            strategy.shutdown();
            dataTracker.dropSSTables();
        }
    }

    private void testDropExpiredAndCompactNonExpired()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(2);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        UUID pendingRepair = UUID.randomUUID();
        List<SSTableReader> expiredSSTables = createSStables(cfs.getPartitioner(), 1000, pendingRepair);
        List<SSTableReader> nonExpiredSSTables = createSStables(cfs.getPartitioner(), 0, pendingRepair);
        List<SSTableReader> allSSTables = Stream.concat(expiredSSTables.stream(), nonExpiredSSTables.stream())
                                                .collect(Collectors.toList());
        dataTracker.addInitialSSTables(allSSTables);

        int timestamp = expiredSSTables.get(expiredSSTables.size() - 1).getMaxLocalDeletionTime();
        int expirationPoint = timestamp + 1;

        try
        {
            strategy.getNextBackgroundTasks(expirationPoint);
            Collection<CompactionPick> picks = strategy.backgroundCompactions.getCompactionsInProgress();

            for (CompactionPick pick : picks)
            {
                assertFalse(pick.hasExpiredOnly());
                assertEquals(pick.sstables.size() / 2, pick.expired.size());
                Set<SSTableReader> nonExpired = pick.sstables.stream()
                                                             .filter(sstable -> !pick.expired.contains(sstable))
                                                             .collect(Collectors.toSet());
                assertEquals(pick.sstables.size() / 2, nonExpired.size());
                long expectedTotSize = nonExpired.stream()
                                                 .mapToLong(SSTableReader::onDiskLength)
                                                 .sum();
                assertEquals(expectedTotSize, pick.totSizeInBytes);
                assertEquals(expectedTotSize / nonExpired.size(), pick.avgSizeInBytes);
                assertEquals(0, pick.parent);
            }
        }
        finally
        {
            strategy.shutdown();
            dataTracker.dropSSTables();
        }
    }

    @Test
    public void testPending()
    {
        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(-8); // F=10, T=2
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.maxSSTablesToCompact()).thenReturn(10); // same as fanout

        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getNumShards()).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        List<SSTableReader> sstables = createSStables(cfs.getPartitioner(),
                                                      mapFromPair(Pair.create(4 * ONE_MB, 91)));
        dataTracker.addInitialSSTables(sstables);

        assertEquals(3, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size()); // repaired, unrepaired, pending
        Collection<CompactionAggregate> aggregates = strategy.backgroundCompactions.getAggregates();
        assertEquals(3, aggregates.size());
        for (CompactionAggregate aggregate : aggregates)
            assertEquals(8, aggregate.getPending().size());
    }
}
