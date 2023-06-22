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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.Reservations;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

/**
 * The unified compaction strategy is described in this design document:
 *
 * See CEP-26: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy
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
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
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
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);

        when(controller.getScalingParameter(anyInt())).thenAnswer(answer -> {
            int index = answer.getArgument(0);
            return Ws[index < Ws.length ? index : Ws.length - 1];
        });
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();

        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = realm.getPartitioner();
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

        Map<UnifiedCompactionStrategy.Arena, List<UnifiedCompactionStrategy.Level>> arenas = strategy.getLevels();
        assertNotNull(arenas);
        assertEquals(1, arenas.size());

        for (Map.Entry<UnifiedCompactionStrategy.Arena, List<UnifiedCompactionStrategy.Level>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Level> levels = entry.getValue();
            assertEquals(expectedTs.length, levels.size());

            for (int i = 0; i < expectedTs.length; i++)
            {
                UnifiedCompactionStrategy.Level level = levels.get(i);
                assertEquals(i, level.getIndex());

                Collection<CompactionAggregate.UnifiedAggregate> compactionAggregates =
                    level.getCompactionAggregates(entry.getKey(), Collections.EMPTY_SET, controller, dataSetSizeBytes);
                long selectedCount = compactionAggregates.stream()
                                                         .filter(a -> !a.isEmpty())
                                                         .count();
                int expectedCount = level.getSSTables().size() >= expectedTs[i] ? 1 : 0;
                assertEquals(expectedCount, selectedCount);
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

        int numArenas = strategy.getController().getNumShards(1);
        // Without limiting oversized compactions kicking in, we expect one compaction per shard, otherwise we expect
        // a fraction of the number of all shards, proportional to the max allowed space amplification fraction.
        int expectedCompactionTasks = triggerOversizedLimiting
                                      ? (int) (Math.floor(numArenas * maxSpaceOverhead))
                                      : topLevelOnly
                                        ? Math.min((maxCount + numBuckets - 1) / numBuckets, numArenas)
                                        : Math.min(maxCount, numArenas);
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSpaceOverhead()).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);

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
            strategy.backgroundCompactions.onInProgress(mockProgress(strategy, pick.id()));

        // now that we have a rate, make sure we produce tasks to fill up the limit
        assertEquals(Math.min(maxThroughput, maxCount) - 1, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        // and don't create any new ones when the limit is filled, before they make progress
        assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        for (CompactionPick pick : strategy.backgroundCompactions.getCompactionsInProgress())
            if (!pick.inProgress())
                strategy.backgroundCompactions.onInProgress(mockProgress(strategy, pick.id()));

        // and also when they do
        assertEquals(0, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size());

        for (int remaining = strategy.getController().getNumShards(1) - Math.min(maxThroughput, maxCount);
             remaining > 0;
             --remaining)
        {
            // mark a task as completed
            strategy.backgroundCompactions.onCompleted(strategy, Iterables.get(strategy.backgroundCompactions.getCompactionsInProgress(), 0).id());

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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSpaceOverhead()).thenReturn(maxSpaceOverhead);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(maxCount);
        when(controller.maxCompactionSpaceBytes()).thenCallRealMethod();
        when(controller.maxThroughput()).thenReturn(maxThroughput);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);
        // Calculate the minimum shard size such that the top bucket compactions won't be considered "oversized" and
        // all will be allowed to run. The calculation below assumes (1) that compactions are considered "oversized"
        // if they are more than 1/2 of the max shard size; (2) that mockSSTables uses 15% less than the max SSTable
        // size for that bucket.
        long topBucketMaxSstableSize = (long) (minSstableSizeBytes * Math.pow(F, numBuckets));
        long topBucketMaxCompactionSize = T * topBucketMaxSstableSize;
        when(controller.getDataSetSizeBytes()).thenReturn(topBucketMaxCompactionSize * numShards);
        when(controller.random()).thenCallRealMethod();

        when(controller.getOverheadSizeInBytes(any(CompactionPick.class))).thenAnswer(inv -> ((CompactionPick)inv.getArgument(0)).totSizeInBytes());

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
            Token first = min.getPartitioner().split(min, max, 0.01);
            Token last = min.getPartitioner().split(min, max, 0.99);
            double tokenSpan = first.size(last);

            for (int i = 0; i < num; i++)
            {
                arena.sstables.add(mockSSTable(0,
                                               (long) (size * tokenSpan * 1.01),    // adjust slightly bigger to avoid rounding issues
                                               System.currentTimeMillis(),
                                               0.0,
                                               new BufferDecoratedKey(first, bb),
                                               new BufferDecoratedKey(last, bb),
                                               diskIndex,
                                               repaired,
                                               pendingRepair,
                                               0));
                first = first.nextValidToken();
            }
        });

        return arena;
    }

    private List<Token> makeBoundaries(int numShards, int numDisks)
    {
        IPartitioner partitioner = realm.getPartitioner();
        assert numShards >= 1;
        assert numDisks >= 1;

        if (numShards * numDisks == 1)
            return ImmutableList.of(partitioner.getMaximumToken());

        Splitter splitter = partitioner.splitter().orElse(null);
        assertNotNull("The partitioner must support a splitter", splitter);

        int numBoundaries = useDiskBoundaries ? numDisks * numShards : numShards;
        Splitter.WeightedRange range = new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        final List<Token> shards = splitter.splitOwnedRanges(numBoundaries, ImmutableList.of(range), Splitter.SplitType.ALWAYS_SPLIT)
                                   .boundaries
                                   .stream()
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
                                        List<Token> boundaries,
                                        Map<Long, Integer> sstables,
                                        int[] buckets)
    {
        List<ArenaSpecs> arenasList = new ArrayList<>();

        int numShards = boundaries.size() / diskCount;
        List<Token> shardPositions = useDiskBoundaries
                                                 ? boundaries.subList(diskIndex * numShards, (diskIndex + 1) * numShards)
                                                 : boundaries;
        Token min = useDiskBoundaries && diskIndex > 0
                    ? boundaries.get(diskIndex * numShards - 1).getToken()
                    : partitioner.getMinimumToken();
        Token max = shardPositions.get(shardPositions.size() - 1).getToken();

        arenasList.add(mockArena(min, max, sstables, repaired, pendingRepair, diskIndex, buckets));

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

        List<Token> boundaries = makeBoundaries(numShards, 2);
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

        List<Token> boundaries = makeBoundaries(numShards, 1);
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

        List<Token> boundaries = makeBoundaries(numShards, 2);
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

        List<Token> boundaries = makeBoundaries(numShards, 6);
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

        List<Token> boundaries = makeBoundaries(numShards, 1);
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

        List<Token> boundaries = makeBoundaries(numShards, 2);
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

    private void testGetBucketsMultipleArenas(List<ArenaSpecs> arenaSpecs, int W, int m, List<Token> shards)
    {
        long minimalSizeBytes = m << 20;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.getNumShards(anyDouble())).thenReturn(shards.size());
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        List<SSTableReader> sstables = arenaSpecs.stream().flatMap(a -> a.sstables.stream()).collect(Collectors.toList());
        dataTracker.addInitialSSTables(sstables);

        Map<UnifiedCompactionStrategy.Arena, List<UnifiedCompactionStrategy.Level>> arenas = strategy.getLevels();
        assertNotNull(arenas);
        assertEquals(arenaSpecs.size(), arenas.size());

        int idx = 0;
        for (Map.Entry<UnifiedCompactionStrategy.Arena, List<UnifiedCompactionStrategy.Level>> entry : arenas.entrySet())
        {
            List<UnifiedCompactionStrategy.Level> levels = entry.getValue();
            ArenaSpecs currentArenaSpecs = arenaSpecs.get(idx++);

            assertEquals(currentArenaSpecs.expectedBuckets.length, levels.size());
            for (int i = 0; i < currentArenaSpecs.expectedBuckets.length; i++)
                assertEquals(currentArenaSpecs.expectedBuckets[i], levels.get(i).sstables.size());
        }
    }
    @Test
    public void testGetNextBackgroundTasks()
    {
        assertCompactionTask(1, 3, UnifiedCompactionTask.class);
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        IPartitioner partitioner = realm.getPartitioner();

        List<SSTableReader> sstables = createSStables(partitioner);

        dataTracker.addInitialSSTables(sstables);

        Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertEquals("Expecting number of next background tasks:", expectedNumOfTasks, tasks.size());
        for (AbstractCompactionTask task : tasks)
        {
            assertSame(expectedClass, task.getClass());
        }
    }

    @Test
    public void testGetNextCompactionAggregates()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(0);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.random()).thenCallRealMethod();
        when(controller.getMaxRecentAdaptiveCompactions()).thenReturn(-1);
        when(controller.isRecentAdaptive(any())).thenReturn(true);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        CompactionPick compaction = Mockito.mock(CompactionPick.class);
        when(compaction.isEmpty()).thenReturn(false);
        when(compaction.hasExpiredOnly()).thenReturn(false);
        List<SSTableReader> nonExpiredSSTables = createSStables(realm.getPartitioner());
        when(compaction.sstables()).thenReturn(ImmutableSet.copyOf(nonExpiredSSTables));

        CompactionAggregate.UnifiedAggregate aggregate = Mockito.mock(CompactionAggregate.UnifiedAggregate.class);
        when(aggregate.getSelected()).thenReturn(compaction);

        Collection<CompactionAggregate> compactionAggregates = strategy.getNextCompactionAggregates(ImmutableList.of(aggregate), 1000);
        assertNotNull(compactionAggregates);
        assertEquals(1, compactionAggregates.size());
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
            Token first = min.getPartitioner().split(min, max, 0.01);

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
                first = first.nextValidToken();
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
                first = first.nextValidToken();
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
                first = first.nextValidToken();
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(3); // T=5
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        List<SSTableReader> sstables = createSStables(realm.getPartitioner());
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
                assertEquals(sstables.size() / 3, pick.expired().size());
                assertEquals(0L, pick.totSizeInBytes());
                assertEquals(0L, pick.avgSizeInBytes());
                assertEquals(-1, pick.parent());
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(2);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);

        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        UUID pendingRepair = UUID.randomUUID();
        List<SSTableReader> expiredSSTables = createSStables(realm.getPartitioner(), 1000, pendingRepair);
        List<SSTableReader> nonExpiredSSTables = createSStables(realm.getPartitioner(), 0, pendingRepair);
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
                if (pick.hasExpiredOnly())
                {
                    assertEquals(4, pick.sstables().size());
                    assertEquals(4, pick.expired().size());
                    assertEquals(0L, pick.totSizeInBytes());
                    assertEquals(0L, pick.avgSizeInBytes());
                    assertEquals(-1, pick.parent());
                }
                else
                {
                    assertEquals(4, pick.sstables().size());
                    assertEquals(0, pick.expired().size());
                    Set<CompactionSSTable> nonExpired = pick.sstables();
                    long expectedTotSize = nonExpired.stream()
                                                     .mapToLong(CompactionSSTable::onDiskLength)
                                                     .sum();
                    assertEquals(expectedTotSize, pick.totSizeInBytes());
                    assertEquals(expectedTotSize / nonExpired.size(), pick.avgSizeInBytes());
                    assertEquals(0, pick.parent());
                }
            }
        }
        finally
        {
            strategy.shutdown();
            dataTracker.dropSSTables();
        }
    }

    @Test
    public void testPrioritizeLocallyAvailableSSTables()
    {
        Set<SSTableReader> sstables0 = new HashSet<>(createSSTalesWithDiskIndex(realm.getPartitioner(), 0));
        Set<SSTableReader> sstables1 = new HashSet<>(createSSTalesWithDiskIndex(realm.getPartitioner(), 1));
        Set<SSTableReader> sstables = Sets.union(sstables0, sstables1);
        dataTracker.addInitialSSTables(sstables);

        for (SSTableReader sstable : sstables)
        {
            long onDiskLength;
            if (sstables1.contains(sstable))
                onDiskLength = sstable.onDiskLength();
            else
                onDiskLength = 0L;
            when(sstable.onDiskLength()).thenReturn(onDiskLength);
        }

        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getScalingParameter(anyInt())).thenReturn(0);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1);
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(2);
        when(controller.prioritize(anyList())).thenAnswer(answ -> {
            List<CompactionAggregate.UnifiedAggregate> pending = answ.getArgument(0);
            pending.sort(Comparator.comparingLong(a -> ((CompactionAggregate.UnifiedAggregate) a).sstables.stream().mapToLong(CompactionSSTable::onDiskLength).sum()).reversed());
            return pending;
        });
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());

        assertEquals(1, tasks.size());
        Set<SSTableReader> compacting = tasks.iterator().next().transaction.getCompacting();
        assertEquals(2, compacting.size());
        assertEquals(new HashSet<>(sstables1), compacting);
    }

    private Set<SSTableReader> createSSTalesWithDiskIndex(IPartitioner partitioner, int diskIndex)
    {
        Set<SSTableReader> mockSSTables = new HashSet<>();
        Map<Long, Integer> sstablesMap = mapFromPair(Pair.create(4 * ONE_MB, 2));
        Token min = partitioner.getMinimumToken();
        Token max = partitioner.getMaximumToken();
        ByteBuffer bb = ByteBuffer.allocate(0);
        int ttl = 0;
        UUID pendingRepair = null;
        sstablesMap.forEach((size, num) -> {
            Token first = min.getPartitioner().split(min, max, 0.01);

            for (int i = 0; i < num; i++)
            {
                mockSSTables.add(mockSSTable(0,
                                             size,
                                             FBUtilities.nowInSeconds(),
                                             0.0,
                                             new BufferDecoratedKey(first, bb),
                                             new BufferDecoratedKey(max, bb),
                                             diskIndex,
                                             false,
                                             pendingRepair,
                                             ttl
                ));
                first = first.nextValidToken();
            }
        });
        return mockSSTables;
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getMinSstableSizeBytes()).thenReturn(minimalSizeBytes);
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();
        when(controller.prioritize(anyList())).thenAnswer(answ -> answ.getArgument(0));
        when(controller.getReservedThreads()).thenReturn(Integer.MAX_VALUE);
        when(controller.getReservationsType()).thenReturn(Reservations.Type.PER_LEVEL);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        strategy.startup();

        List<SSTableReader> sstables = createSStables(realm.getPartitioner(),
                                                      mapFromPair(Pair.create(4 * ONE_MB, 91)));
        dataTracker.addInitialSSTables(sstables);

        assertEquals(3, strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds()).size()); // repaired, unrepaired, pending
        Collection<CompactionAggregate> aggregates = strategy.backgroundCompactions.getAggregates();
        assertEquals(3, aggregates.size());
        for (CompactionAggregate aggregate : aggregates)
            assertEquals(8, aggregate.getPending().size());
    }

    @Test
    public void testMaximalSelection()
    {
        Set<SSTableReader> allSSTables = new HashSet<>();
        allSSTables.addAll(mockNonOverlappingSSTables(10, 0, 100 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(15, 1, 200 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(25, 2, 400 << 20));
        dataTracker.addInitialSSTables(allSSTables);

        Controller controller = Mockito.mock(Controller.class);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        CompactionTasks tasks = strategy.getMaximalTasks(0, false);
        assertEquals(5, tasks.size());  // 5 (gcd of 10,15,25) common boundaries
        for (AbstractCompactionTask task : tasks)
        {
            Set<SSTableReader> compacting = task.getTransaction().originals();
            assertEquals(2 + 3 + 5, compacting.size()); // count / gcd sstables of each level
            assertEquals((2 * 100L + 3 * 200 + 5 * 400) << 20, compacting.stream().mapToLong(CompactionSSTable::onDiskLength).sum());

            // None of the selected sstables may intersect any in any other set.
            for (AbstractCompactionTask task2 : tasks)
            {
                if (task == task2)
                    continue;

                Set<SSTableReader> compacting2 = task2.getTransaction().originals();
                for (SSTableReader r1 : compacting)
                    for (SSTableReader r2 : compacting2)
                        assertTrue(r1 + " intersects " + r2, r1.getFirst().compareTo(r2.getLast()) > 0 || r1.getLast().compareTo(r2.getFirst()) < 0);
            }
        }
    }

    @Test
    public void testBucketSelectionSimple()
    {
        testBucketSelection(repeats(4, 10), repeats(10, 4), Overlaps.InclusionMethod.TRANSITIVE);
    }

    @Test
    public void testBucketSelectionHalved()
    {
        testBucketSelection(repeats(4, arr(10, 5)), repeats(5, 6), Overlaps.InclusionMethod.TRANSITIVE);
        testBucketSelection(repeats(4, arr(10, 5)), repeats(5, 6), Overlaps.InclusionMethod.SINGLE);
        // When we take large sstables for one compaction, remaining overlaps don't have enough to trigger next
        testBucketSelection(repeats(4, arr(10, 5)), repeats(5, 4), Overlaps.InclusionMethod.NONE, 10);
    }

    @Test
    public void testBucketSelectionFives()
    {
        testBucketSelection(arr(25, 15, 10), repeats(5, arr(10)), Overlaps.InclusionMethod.TRANSITIVE);
        testBucketSelection(arr(25, 15, 10), concat(repeats(5, 6), repeats(5, 4)), Overlaps.InclusionMethod.SINGLE);
        // When we take large sstables for one compaction, remaining overlaps don't have enough to trigger next
        testBucketSelection(arr(25, 15, 10), repeats(10, arr(3)), Overlaps.InclusionMethod.NONE, 20);
    }

    @Test
    public void testBucketSelectionMissing()
    {
        testBucketSelection(repeats(4,5), repeats(4, 4), Overlaps.InclusionMethod.TRANSITIVE, 3, 1);
    }

    @Test
    public void testBucketSelectionHalvesMissing()
    {
        // Drop one half: still compact because of overlap
        testBucketSelection(repeats(4, arr(6, 3)), arr(5, 6, 6), Overlaps.InclusionMethod.TRANSITIVE, 0, 1);
        // Drop one full: don't compact
        testBucketSelection(repeats(4, arr(3, 6)), arr(6, 6), Overlaps.InclusionMethod.TRANSITIVE, 5, 1);
        // Drop two adjacent halves: don't compact
        testBucketSelection(repeats(4, arr(6, 3)), arr(6, 6), Overlaps.InclusionMethod.TRANSITIVE, 4, 2, 3);
    }


    private int[] arr(int... values)
    {
        return values;
    }

    private int[] repeats(int count, int... values)
    {
        int[] rep = new int[count];
        for (int i = 0; i < count; ++i)
            rep[i] = values[i % values.length];
        return rep;
    }

    private int[] concat(int[]... arrays)
    {
        int total = 0;
        for (int[] array : arrays)
            total += array.length;
        int[] result = new int[total];
        int offset = 0;
        for (int[] array : arrays)
        {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public void testBucketSelection(int[] counts, int[] expecteds, Overlaps.InclusionMethod overlapInclusionMethod)
    {
        testBucketSelection(counts, expecteds, overlapInclusionMethod, 0);
    }

    public void testBucketSelection(int[] counts, int[] expecteds, Overlaps.InclusionMethod overlapInclusionMethod, int expectedRemaining, int... dropFromFirst)
    {
        Set<SSTableReader> allSSTables = new HashSet<>();
        int fanout = counts.length;
        for (int i = 0; i < fanout; ++i)
        {
            final int count = counts[i];
            final List<SSTableReader> list = mockNonOverlappingSSTables(count, /*ignored*/ 0, (100 << 20) / count);
            if (i == 0)
            {
                for (int k = dropFromFirst.length - 1; k >= 0; --k)
                    list.remove(dropFromFirst[k]);
            }
            allSSTables.addAll(list);
        }
        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(fanout - 2); // F=T=fanout
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) (90 << 20));
        when(controller.overlapInclusionMethod()).thenReturn(overlapInclusionMethod);
        Random randomMock = Mockito.mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(randomMock);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        dataTracker.addInitialSSTables(allSSTables);

        List<CompactionPick> picks = new ArrayList<>();
        boolean compactionFound;
        do
        {
            Collection<CompactionAggregate.UnifiedAggregate> aggregates = strategy.getPendingCompactionAggregates(0);
            compactionFound = false;
            for (CompactionAggregate a : aggregates)
            {
                CompactionPick pick = a.getSelected();
                if (pick == null || pick.isEmpty())
                    continue;
                if (realm.tryModify(pick.sstables(), OperationType.COMPACTION) != null) // This will fail if the sstables are in multiple buckets (for Overlaps.InclusionMethod.NONE)
                {
                    compactionFound = true;
                    picks.add(pick);
                }
            }
        }
        while (compactionFound);
        assertEquals(expectedRemaining, Iterables.size(dataTracker.getNoncompacting()));

        assertEquals(expecteds.length, picks.size());
        int buckIdx = 0;
        for (CompactionPick pick : picks)
        {
            int expectedCount = expecteds[buckIdx++];
            assertEquals(expectedCount, pick.sstables().size()); // count / gcd sstables of each level

            if (overlapInclusionMethod == Overlaps.InclusionMethod.TRANSITIVE)
            {
                // None of the selected sstables may intersect any in any other set.
                for (CompactionPick pick2 : picks)
                {
                    if (pick == pick2)
                        continue;

                    for (CompactionSSTable r1 : pick.sstables())
                        for (CompactionSSTable r2 : pick2.sstables())
                            assertTrue(r1 + " intersects " + r2, r1.getFirst().compareTo(r2.getLast()) > 0 || r1.getLast().compareTo(r2.getFirst()) < 0);
                }
            }
        }
        dataTracker.removeUnsafe(allSSTables);
    }

    @Test
    public void testGetLevel()
    {
        Controller controller = Mockito.mock(Controller.class);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        UnifiedCompactionStrategy.Level level = strategy.getLevel(1, 0.25d, 0.5d);
        assertEquals(1, level.index);
        assertEquals(0.25d, level.min, 0);
        assertEquals(0.5d, level.max, 0);
    }

    @Test
    public void testGetExpiredLevel()
    {
        Controller controller = Mockito.mock(Controller.class);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        assertEquals(strategy.getLevel(-1, 0, 0), UnifiedCompactionStrategy.EXPIRED_TABLES_LEVEL);
    }
}
