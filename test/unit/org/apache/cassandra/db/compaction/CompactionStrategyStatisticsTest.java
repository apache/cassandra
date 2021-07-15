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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.mockito.Mockito;

import static org.apache.cassandra.db.compaction.LeveledManifest.MAX_COMPACTING_L0;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

/**
 * Test for the compaction statistics for all strategies that support them.
 */
public class CompactionStrategyStatisticsTest extends BaseCompactionStrategyTest
{
    private final int minCompactionThreshold = 4;
    private final int maxCompactionThreshold = 32;

    @BeforeClass
    public static void setUpClass()
    {
        BaseCompactionStrategyTest.setUpClass();
    }

    @Before
    public void setUp()
    {
        super.setUp();

        when(cfs.getMinimumCompactionThreshold()).thenReturn(minCompactionThreshold);
        when(cfs.getMaximumCompactionThreshold()).thenReturn(maxCompactionThreshold);
    }

    /**
     * Creates 5 buckets with T sorted runs in each using W = 2 and o = 1 (the default)
     */
    @Test
    public void testUnifiedCompactionStrategy_tiered_twoShards_fiveBuckets_W2()
    {
        int W = 2; // W = 2 => T = F = 4
        int T = 4;
        int F = 4;
        final long minSstableSizeBytes = 2L << 20; // 2 MB
        final int numBuckets = 5;

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenReturn(F);
        when(controller.getThreshold(anyInt())).thenReturn(T);
        when(controller.getMinSstableSizeBytes()).thenReturn(minSstableSizeBytes);
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.random()).thenCallRealMethod();
        // Calculate the minimum shard size such that the top bucket compactions won't be considered "oversized" and
        // all will be allowed to run. The calculation below assumes (1) that compactions are considered "oversized"
        // if they are more than 1/2 of the max shard size; (2) that mockSSTables uses 15% less than the max SSTable
        // size for that bucket.
        long topBucketMaxSstableSize = (long) (minSstableSizeBytes * Math.pow(F, numBuckets));
        long minShardSizeWithoutOversizedCompactions = T * topBucketMaxSstableSize * 2;
        when(controller.getShardSizeBytes()).thenReturn(minShardSizeWithoutOversizedCompactions);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);
        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(numBuckets * 2);

        // The order is repaired false, disk 0, then repaired true, disk 1, one bucket per shard, lowest to highest
        // because the test picks from the end of the test buckets, we need to revert this order
        for (int i = numBuckets - 1; i >= 0; i--)
        {
            for (boolean repaired : new boolean[] { false, true })
            {
                for (int diskIndex = 1; diskIndex >= 0; diskIndex--)
                {
                    // calculate the max size then mockSSTables will remove 15% to this value,
                    // this assumes o = 1, which is the default
                    long size = (long) (minSstableSizeBytes * Math.pow(F, i + 1));
                    List<SSTableReader> sstables = mockSSTables(T,
                                                                size,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                diskIndex,
                                                                repaired,
                                                                null);
                    testBuckets.add(sstables);
                }
            }
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * Creates 5 buckets with T sorted runs in each using W = 2 and o = 1 (the default)
     */
    @Test
    public void testUnifiedCompactionStrategy_leveled_one_shard_oneBucket_F8()
    {
        int W = -6; // W = 2 => T = 2, F = 8
        int T = 2;
        int F = 8;
        int m = 2; // m = 2 MB
        long minSize = m << 20; // MB to bytes

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenReturn(F);
        when(controller.getThreshold(anyInt())).thenReturn(T);
        when(controller.getMinSstableSizeBytes()).thenReturn(minSize);
        when(controller.getSurvivalFactor()).thenReturn(1.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSize);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxCompactionSpaceBytes()).thenReturn(Long.MAX_VALUE);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(strategyFactory, controller);

        // put F sstables in the first bucket
        List<SSTableReader> ssTablesList = new LinkedList<>();
        for (int i = 0; i < F; i++)
            ssTablesList.addAll(mockSSTables(1, minSize, 0, System.currentTimeMillis()));

        Collections.sort(ssTablesList, Comparator.comparing(SSTableReader::onDiskLength).reversed());
        Set<SSTableReader> sstables = new LinkedHashSet<>(F);
        sstables.addAll(ssTablesList);

        // sort by size and add 2 by 2 from largest to smallest, normally the sstable resulting from the 1 compaction
        // would be added back to the same bucket and be selected for the next compaction but we don't simulate this
        // so next time a compaction is invoked it will pick the next two largest sstables, that's why there will be
        // F/2 compactions rather than F-1
//        LinkedList<Collection<SSTableReader>> compactions = new LinkedList();
//        for (int i = 0; (i + T) <= ssTablesList.size(); i += T)
//        {
//            List<SSTableReader> candidates = ssTablesList.subList(i, i + T);
//            compactions.addFirst(candidates); // we want the first 2 sstables (the largest) to be the last in the list
//        }

        testCompactionStatistics(sstables, ImmutableList.of(sstables), 1, strategy);
    }

    /**
     * Creates 5 STCS buckets with a single compaction pick (<= max threshold tables) and
     * increasing hotness so that the highest test bucket will be compacted first.
     */
    @Test
    public void testSizeTieredCompactionStrategy_fiveBucketsOnePick()
    {
        Map<String, String> options = new HashMap<>();
        addSizeTieredOptions(options);

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(strategyFactory, options);

        final int numCompactions = 5;
        long minSize = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE;
        double hotness = 1000;

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(numCompactions);
        for (int i = 0; i < numCompactions; i++)
        {
            List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold,
                                                        minSize,
                                                        hotness,
                                                        System.currentTimeMillis());
            testBuckets.add(sstables);

            minSize *= 10;
            hotness *= 2;
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * Creates a single STCS bucket with enough sstables to fill 5 picks.
     */
    @Test
    public void testSizeTieredCompactionStrategy_oneBucketFivePicks()
    {
        Map<String, String> options = new HashMap<>();
        addSizeTieredOptions(options);

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(strategyFactory, options);

        final int numCompactions = 5;
        long size = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE * 2;
        double hotness = 1000;

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(numCompactions);
        for (int i = 0; i < numCompactions; i++)
        {
            List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold,
                                                        size,
                                                        hotness,
                                                        System.currentTimeMillis());
            testBuckets.add(sstables);
            hotness *= 2;
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * Creates 3 STCS buckets with enough sstables to have 2 compactions per bucket and increasing
     * hotness so that the highest test buckets will be compacted first.
     */
    @Test
    public void testSizeTieredCompactionStrategy_threeBucketsTwoPicks()
    {
        Map<String, String> options = new HashMap<>();
        addSizeTieredOptions(options);

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(strategyFactory, options);

        long minSize = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE;
        double hotness = 1000;

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) // STCS buckets
        {
            for (int j = 0; j < 2; j++) // picks
            {
                List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold,
                                                            minSize,
                                                            hotness,
                                                            System.currentTimeMillis());
                testBuckets.add(sstables);
                hotness *= 2;
            }

            minSize *= 10;
        }

        testCompactionStatistics(testBuckets, strategy);
    }


    /**
     * Creates 5 TWCS buckets with increasing timestamp so that the higher buckets will be compacted first.
     * Each bucket only has a single compaction pick (<= max threshold tables).
     */
    @Test
    public void testTimeWindowCompactionStrategy_fiveBucketsOnePick()
    {
        Map<String, String> options = new HashMap<>();
        addTimeTieredOptions(options);

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(strategyFactory, options);

        final int numCompactions = 5;
        long size = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE * 5;
        double hotness = 1000;
        long timestap = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10); // 10 hours ago

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(numCompactions);
        for (int i = 0; i < numCompactions; i++)
        {
            List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold, size, hotness, timestap);
            testBuckets.add(sstables);

            timestap += TimeUnit.HOURS.toMillis(2);
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * Creates a single TWCS bucket with enough sstables to fill 5 picks.
     */
    @Test
    public void testTimeWindowCompactionStrategy_oneBucketFivePicks()
    {
        Map<String, String> options = new HashMap<>();
        addTimeTieredOptions(options);

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(strategyFactory, options);

        final int numCompactions = 5;
        long size = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE * 5;
        double hotness = 100;
        long timestap = System.currentTimeMillis();

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(numCompactions);
        for (int i = 0; i < numCompactions; i++)
        {
            List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold, size, hotness, timestap);
            testBuckets.add(sstables);

            hotness *= 2; // hottest tables should be picked first because TWCS uses STCS in the latest bucket
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * Creates 3 TWCS buckets with enough sstables to have 2 compactions per bucket.
     */
    @Test
    public void testTimeWindowCompactionStrategy_threeBucketsTwoPicks()
    {
        Map<String, String> options = new HashMap<>();
        addTimeTieredOptions(options);

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(strategyFactory, options);

        long size = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE * 10;
        double hotness = 1000;
        long timestap = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10); // 10 hours ago

        List<Collection<SSTableReader>> testBuckets = new ArrayList<>(3 * 2);
        for (int i = 0; i < 3; i++)
        {
            for (int j = 0; j < 2; j++)
            {
                List<SSTableReader> sstables = mockSSTables(maxCompactionThreshold, size, hotness, timestap);
                testBuckets.add(sstables);

                hotness *= 2; // hottest tables should be picked first in the newest bucket because of STCS
                size -= SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE; // smaller sstables are picked first in other TWCS buckets
            }

            size = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE * 10;
            timestap += TimeUnit.HOURS.toMillis(2);
        }

        testCompactionStatistics(testBuckets, strategy);
    }

    /**
     * A utility method for determining the overlapping sstables similarly to what {@link LeveledManifest} does
     * when selecting sstables from the next level that overlap with a candidate of the previous level.
     *
     * @param sstable the sstables from the previous level
     * @param candidates the candidates sstables from the next level
     *
     * @return a set containing the sstable passed in and all the sstables that overlap from the candidates
     */
    private static Set<SSTableReader> overlapping(SSTableReader sstable, List<SSTableReader> candidates)
    {
        Map<SSTableReader, Bounds<Token>> candidatesWithBounds = LeveledManifest.genBounds(candidates);
        return Sets.union(Collections.singleton(sstable), LeveledManifest.overlappingWithBounds(sstable, candidatesWithBounds));
    }

    /**
     * Creates 3 LCS levels. Checks that L2 and L3 are able to compact in parallel but L0 gets blocked by the
     * L1 compaction. Once the L2 and L3 compactions have finished, then the L0 compaction can proceed.
     */
    @Test
    public void testLeveledCompactionStrategy_threeLevels()
    {
        Map<String, String> options = new HashMap<>();
        long maxSSTableSize = 160 << 20; // 160 MB in bytes
        addLeveledOptions(options, maxSSTableSize);

        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(strategyFactory, options);

        final int numLevels = 3;
        List<List<SSTableReader>> ssTablesByLevel = new ArrayList<>(numLevels);
        for (int i = 0; i < numLevels; i++)
        {
            // level zero maximum size is 4 times maxSSTableSize, and for other levels it is
            // the fan-out size (10) to the power of the level number, times maxSSTableSize.
            long maxLevelSize = (long) ((i == 0 ? 4 : Math.pow(10, i)) * maxSSTableSize);

            // we add one to ensure the score will be > 1 so that only one sstable (and no more!) will be selected for compaction
            int numSSTables = (int) Math.ceil(maxLevelSize / maxSSTableSize) + 1;

            List<SSTableReader> sstables = mockNonOverlappingSSTables(numSSTables, i, maxSSTableSize);
            ssTablesByLevel.add(sstables);
        }

        // all sstables flattened
        Set<SSTableReader> sstables = ssTablesByLevel.stream().flatMap(bucket -> bucket.stream()).collect(Collectors.toSet());

        // Organize the sstables into the expected compactions
        // LCS will always compact the highest level first unless L0 has more than 32 sstables in which case
        // it compacts using STCS
        List<Collection<SSTableReader>> compactions = new ArrayList(3);

        //L0 will compact all its sstables and the ones of L1 since they all overlap and the total is below the max threshold
        compactions.add(Sets.union(Sets.newLinkedHashSet(ssTablesByLevel.get(0)), Sets.newLinkedHashSet(ssTablesByLevel.get(1))));

        // L1 will compact the first sstable that it finds not overlapping with L2 sstables that are not suspect or already
        // compacting. Because the next line will select the first sstable in L2 to compact, L1 will pick the first sstable
        // that does not overlap with it
        SSTableReader candidate = null;
        for (SSTableReader c : ssTablesByLevel.get(1))
        {
            if (c.getFirst().compareTo(ssTablesByLevel.get(2).get(0).getLast()) > 0)
            {
                candidate = c;
                break;
            }
        }
        assertNotNull(candidate);
        // compact the candidate with all the overlapping sstables of L2
        compactions.add(overlapping(candidate, ssTablesByLevel.get(2)));

        // L2 will compact the first sstable because the score is > 1 but no other overlapping sstables since L3 is empty
        compactions.add(overlapping(ssTablesByLevel.get(2).get(0), ImmutableList.of()));

        // L2 and L1 compactions can proceed in parallel but L0 will refuse to compact due to overlapping sstables in L1
        // already compacting, hence we can only test 2 compactions initially
        testCompactionStatistics(sstables, compactions, 2, strategy);

        // Now check L0 compaction can proceed, the other levels won't compact since the score should be <= 1
        ssTablesByLevel.get(1).remove(candidate); // remove the L1 sstable that was already compacted
        Set<SSTableReader> candidates = Sets.union(Sets.newLinkedHashSet(ssTablesByLevel.get(0)), Sets.newLinkedHashSet(ssTablesByLevel.get(1)));
        long totLength = totUncompressedLength(candidates);

        Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
        assertFalse(tasks.isEmpty());

        for (AbstractCompactionTask task : tasks)
        {
            assertNotNull(task);
            UUID id = task.transaction().opId();

            verifyStatistics(strategy,
                             1,
                             1,
                             candidates.size(),
                             candidates.size(),
                             totLength,
                             0,
                             0,
                             0);

            CompactionProgress progress = mockCompletedCompactionProgress(candidates, id);
            strategy.onInProgress(progress);

            verifyStatistics(strategy,
                             1,
                             1,
                             candidates.size(),
                             candidates.size(),
                             totLength,
                             totLength,
                             totLength,
                             0);

            strategy.backgroundCompactions.onCompleted(strategy, id);
        }

        // Now we should have L1 again...
    }

    /**
     *  Test the case where L0 has enough sstables to trigger STCS, plus also add some tables in L1.
     */
    @Test
    public void testLeveledCompactionStrategy_stcsL0()
    {
        Map<String, String> options = new HashMap<>();
        long maxSSTableSize = 160 << 20; // 160 MB in bytes
        addLeveledOptions(options, maxSSTableSize);

        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(strategyFactory, options);

        int level = 1;
        long maxLevelSize = (long) (Math.pow(10, level) * maxSSTableSize);
        int numSSTables = (int) Math.ceil(maxLevelSize / maxSSTableSize) + 1;
        List<SSTableReader> l1SSTables = mockNonOverlappingSSTables(numSSTables, level, maxSSTableSize);

        List<SSTableReader> l0SSTables = mockSSTables(MAX_COMPACTING_L0 + 1, maxSSTableSize, 0.0, System.currentTimeMillis());

        Set<SSTableReader> sstables = Sets.newHashSet(l0SSTables);
        sstables.addAll(l1SSTables);

        // Organize the sstables into the expected compactions
        // LCS will always compact the highest level first unless L0 has more than 32 sstables in which case
        // it compacts using STCS
        List<Collection<SSTableReader>> compactions = new ArrayList(2);

        // L1 will compact the first sstable because the score is > 1 but no other overlapping sstables since L2 is empty
        compactions.add(overlapping(l1SSTables.get(0), ImmutableList.of()));

        // L0 should use STCS to compact them all up to the max threshold, since all sstables have the same hotness,
        // they will be sorted by size
        Collections.sort(l0SSTables, Comparator.comparing(SSTableReader::onDiskLength));
        compactions.add(l0SSTables.subList(0, Math.min(maxCompactionThreshold, l0SSTables.size())));

        testCompactionStatistics(sstables, compactions, compactions.size(), strategy);
    }

    private void testCompactionStatistics(List<Collection<SSTableReader>> compactions, AbstractCompactionStrategy strategy)
    {
        Set<SSTableReader> sstables = compactions.stream().flatMap(Collection::stream).collect(Collectors.toSet());
        testCompactionStatistics(sstables, compactions, compactions.size(), strategy);
    }

    /**
     * Tests the statistics for a given strategy. It is expected that the compactions passed in will contain a set of sstables
     * to be compacted together, with the highest index being picked first, then the second highest and so forth.
     *
     * @param compactions sstables grouped by compaction, each compaction is expected to be compacted fully (no splitting currently
     *                    supported), the highest index compaction should be picked first by the strategy
     * @param numExpectedCompactions the expected number of compactions that can occur in parallel
     * @param strategy the compaction strategy
     */
    private void testCompactionStatistics(Set<SSTableReader> sstables,
                                          List<Collection<SSTableReader>> compactions,
                                          int numExpectedCompactions,
                                          AbstractCompactionStrategy strategy)
    {
        // Add the tables to the strategy and the data tracker
        addSSTablesToStrategy(strategy, sstables);

        List<SSTableReader> sstablesForCompaction = compactions.stream().flatMap(Collection::stream).collect(Collectors.toList());

        int numSSTables = sstablesForCompaction.size();
        long totLength = totUncompressedLength(sstablesForCompaction);
        double totHotness = totHotness(sstablesForCompaction);

        Set<SSTableReader> compacting = new HashSet<>();
        List<Pair<Set<SSTableReader>, UUID>> submittedCompactions = new ArrayList<>(compactions.size());

        long totRead = 0;
        long totWritten = 0;
        int numSSTablesCompacting = 0;
        int numCompactions = compactions.size();
        int numCompactionsInProgress = 0;

        // Create a compaction task and start the compaction for each bucket starting with the highest index
        int i = 0;
        while (i < numExpectedCompactions)
        {
            List<Pair<Set<SSTableReader>, UUID>> tasksCompactions = new ArrayList<>(compactions.size());
            Collection<AbstractCompactionTask> tasks = strategy.getNextBackgroundTasks(FBUtilities.nowInSeconds());
            assertFalse(tasks.isEmpty());

            // Keep track of all the tasks that were submitted (one per shard)
            for (AbstractCompactionTask task : tasks)
            {
                Set<SSTableReader> candidates = Sets.newHashSet(task.transaction.originals());

                i++;

                assertNotNull(task);
                UUID id = task.transaction().opId();

                numCompactionsInProgress++;
                numSSTablesCompacting += candidates.size();
                tasksCompactions.add(Pair.create(candidates, id));
            }

            // after mocking the compactions the list of pending compactions has been updated in the strategy
            // and this will be reflected in the statistics but the compaction task has not started yet
            verifyStatistics(strategy,
                             numCompactions,
                             numCompactionsInProgress,
                             numSSTables,
                             numSSTablesCompacting,
                             totLength,
                             totRead,
                             totWritten,
                             totHotness);

            // Start the compactions and check the statistics are updated
            for (Pair<Set<SSTableReader>, UUID> pair : tasksCompactions)
            {
                UUID id = pair.right;
                Set<SSTableReader> candidates = pair.left;

                // Now we simulate starting the compaction task
                CompactionProgress progress = mockCompletedCompactionProgress(candidates, id);
                strategy.onInProgress(progress);

                // The compaction has started and so we must updated the following expected values
                totRead += progress.uncompressedBytesRead();
                totWritten += progress.uncompressedBytesWritten();

                // Now check that the statistics reflect the compaction in progress
                verifyStatistics(strategy,
                                 numCompactions,
                                 numCompactionsInProgress,
                                 numSSTables,
                                 numSSTablesCompacting,
                                 totLength,
                                 totRead,
                                 totWritten,
                                 totHotness);

                // update compacting for the next iteration
                compacting.addAll(candidates);
            }

            submittedCompactions.addAll(tasksCompactions);
        }

        assertEquals(numExpectedCompactions, submittedCompactions.size());

        // Terminate the compactions one by one by closing the AutoCloseable and check
        // that the statistics are updated
        for (Pair<Set<SSTableReader>, UUID> pair : submittedCompactions)
        {
            Set<SSTableReader> compSSTables = pair.left;
            long totSSTablesLen = totUncompressedLength(compSSTables);
            strategy.onCompleted(pair.right);

            numCompactions--;
            numCompactionsInProgress--;
            numSSTables -= compSSTables.size();
            numSSTablesCompacting -= compSSTables.size();

            totLength -= totSSTablesLen;
            totRead -= totSSTablesLen;
            totWritten -= totSSTablesLen;
            totHotness -= totHotness(compSSTables);

            removeSSTablesFromStrategy(strategy, pair.left);
            sstables.removeAll(pair.left);
            compacting.removeAll(pair.left);

            verifyStatistics(strategy,
                             numCompactions,
                             numCompactionsInProgress,
                             numSSTables,
                             numSSTablesCompacting,
                             totLength,
                             totRead,
                             totWritten,
                             totHotness);
        }

        assertTrue(String.format("Data tracker still had compacting sstables: %s", dataTracker.getCompacting()),
                   dataTracker.getCompacting().isEmpty());
    }

    private void verifyStatistics(CompactionStrategy strategy,
                                  int expectedCompactions,
                                  int expectedCompacting,
                                  int expectedSSTables,
                                  int expectedSSTablesCompacting,
                                  long expectedTotBytes,
                                  long expectedReadBytes,
                                  long expectedWrittenBytes,
                                  double expectedTotHotness)
    {
        CompactionStrategyStatistics stats = strategy.getStatistics().get(0);
        System.out.println(stats.toString());

        assertEquals(keyspace, stats.keyspace());
        assertEquals(table, stats.table());
        assertEquals(strategy.getClass().getSimpleName(), stats.strategy());

        assertEquals(expectedCompactions, strategy.getTotalCompactions());

        int numCompactions = 0;
        int numCompacting = 0;
        int numSSTables = 0;
        int numCompactingSSTables = 0;
        long totBytes = 0;
        long writtenBytes = 0;
        long readBytes = 0;
        double hotness = 0;

        for (CompactionAggregateStatistics compactionStatistics : stats.aggregates())
        {
            numCompactions += compactionStatistics.numCompactions();
            numCompacting += compactionStatistics.numCompactionsInProgress();
            numSSTables += compactionStatistics.numCandidateSSTables();
            numCompactingSSTables += compactionStatistics.numCompactingSSTables();

            if (compactionStatistics instanceof TieredCompactionStatistics)
            {
                TieredCompactionStatistics tieredStatistics = (TieredCompactionStatistics) compactionStatistics;

                totBytes += tieredStatistics.tot();
                writtenBytes += tieredStatistics.written();
                readBytes += tieredStatistics.read();
                hotness += tieredStatistics.hotness;
            }
            else if (compactionStatistics instanceof LeveledCompactionStatistics)
            {
                LeveledCompactionStatistics leveledStatistics = (LeveledCompactionStatistics) compactionStatistics;

                totBytes += leveledStatistics.tot();
                writtenBytes += leveledStatistics.written();
                readBytes += leveledStatistics.read();
            }
            else
            {
                UnifiedCompactionStatistics tieredStatistics = (UnifiedCompactionStatistics) compactionStatistics;

                totBytes += tieredStatistics.tot();
                writtenBytes += tieredStatistics.written();
                readBytes += tieredStatistics.read();
            }
        }

        assertEquals(expectedCompactions, numCompactions);
        assertEquals(expectedCompacting, numCompacting);

        if (!(strategy instanceof LeveledCompactionStrategy))
        { // LCS won't report pending sstables but only pending tasks
            assertEquals(expectedSSTables, numSSTables);
            assertEquals(expectedSSTablesCompacting, numCompactingSSTables);
            assertEquals(expectedTotBytes, totBytes);
        }

        assertEquals(expectedReadBytes, readBytes);
        assertEquals(expectedWrittenBytes, writtenBytes);

        if (expectedTotHotness > 0)
            assertEquals(expectedTotHotness, hotness, epsilon);
    }
}