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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.LeveledManifest.MAX_COMPACTING_L0;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test for the compaction statistics for all strategies that support them.
 */
public class CompactionStrategyStatisticsTest
{
    private static final double epsilon = 0.00000001;
    private static final Random random = new Random(87689624525L);
    private static final AtomicInteger generation = new AtomicInteger(1);

    private final String keyspace = "ks";
    private final String table = "table";
    private final int minCompactionThreshold = 4;
    private final int maxCompactionThreshold = 32;
    private final long minSSTableSize = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE;

    private long repairedAt;

    @Mock
    private ColumnFamilyStore cfs;

    @Mock
    private Tracker dataTracker;

    @Mock
    private CompactionStrategyManager strategyManager;

    private CompactionLogger compactionLogger;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);

        TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                              .addPartitionKeyColumn("pk", AsciiType.instance)
                                              .build();
        repairedAt = System.currentTimeMillis();

        when(cfs.getMinimumCompactionThreshold()).thenReturn(minCompactionThreshold);
        when(cfs.getMaximumCompactionThreshold()).thenReturn(maxCompactionThreshold);
        when(cfs.metadata()).thenReturn(metadata);
        when(cfs.getKeyspaceName()).thenReturn(keyspace);
        when(cfs.getTableName()).thenReturn(table);
        when(cfs.getTracker()).thenReturn(dataTracker);
        when(cfs.getPartitioner()).thenReturn(DatabaseDescriptor.getPartitioner());
        when(cfs.getCompactionStrategyManager()).thenReturn(strategyManager);

        // use a real compaction logger to execute that code too even though we don't really check
        // the content of the files, at least we cover the code. The files will be overwritten next
        // time the test is run or by a gradle clean task, so they will not grow indefinitely
        compactionLogger = new CompactionLogger(cfs, strategyManager);
        compactionLogger.enable();
        when(strategyManager.compactionLogger()).thenReturn(compactionLogger);
    }

    private void addSizeTieredOptions(Map<String, String> options)
    {
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, Long.toString(minSSTableSize));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_LOW));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_HIGH));
    }

    private void addTimeTieredOptions(Map<String, String> options)
    {
        addSizeTieredOptions(options);

        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, TimeUnit.MILLISECONDS.toString());
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, Long.toString(Long.MAX_VALUE)); // disable check for expired sstables
    }

    private void addLeveledOptions(Map<String, String> options, long maxSSTableSizeBytes)
    {
        addSizeTieredOptions(options);

        options.put(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, Long.toString(maxSSTableSizeBytes >> 20)); // Bytes to MB
        options.put(LeveledCompactionStrategy.LEVEL_FANOUT_SIZE_OPTION, "10");
    }

    private SSTableReader mockSSTable(int level, long bytesOnDisk, long timestamp, double hotness, DecoratedKey first, DecoratedKey last)
    {
        SSTableReader ret = Mockito.mock(SSTableReader.class);

        when(ret.bytesOnDisk()).thenReturn(bytesOnDisk);
        when(ret.onDiskLength()).thenReturn(bytesOnDisk);
        when(ret.uncompressedLength()).thenReturn(bytesOnDisk); // let's assume no compression
        when(ret.hotness()).thenReturn(hotness);
        when(ret.getSSTableLevel()).thenReturn(level);
        when(ret.getMaxTimestamp()).thenReturn(timestamp);
        when(ret.getMinTimestamp()).thenReturn(timestamp);
        when(ret.getFirst()).thenReturn(first);
        when(ret.getLast()).thenReturn(last);
        when(ret.isMarkedSuspect()).thenReturn(false);
        when(ret.isRepaired()).thenReturn(true);
        when(ret.getRepairedAt()).thenReturn(repairedAt);
        when(ret.getId()).thenReturn(new SequenceBasedSSTableId(generation.getAndIncrement()));
        when(ret.toString()).thenReturn(String.format("Bytes on disk: %s, level %d, hotness %f, timestamp %d, first %s, last %s",
                                                      FBUtilities.prettyPrintMemory(bytesOnDisk), level, hotness, timestamp, first, last));

        return ret;
    }

    private List<SSTableReader> mockSSTables(int numSSTables, long bytesOnDisk, double hotness, long timestamp)
    {
        IPartitioner partitioner = cfs.getPartitioner();
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < numSSTables; i++)
        {
            long b = (long)(bytesOnDisk * 0.8 + bytesOnDisk * 0.05 * random.nextDouble()); // leave 5% variability
            double h = hotness * 0.8 + hotness * 0.05 * random.nextDouble(); // leave 5% variability
            sstables.add(mockSSTable(0, b, timestamp, h, first, last));
        }

        return sstables;
    }

    private List<SSTableReader> mockNonOverlappingSSTables(int numSSTables, int level, long bytesOnDisk)
    {
        IPartitioner partitioner = cfs.getPartitioner(); // mocked same as DD.getPartitioner()
        if (!partitioner.splitter().isPresent())
            fail(String.format("Cannot split ranges with current partitioner %s", partitioner));

        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        Splitter.WeightedRange weightedRange = new Splitter.WeightedRange(1.0, range);
        Splitter splitter = partitioner.splitter().get();
        List<Token> boundaries = splitter.splitOwnedRanges(numSSTables,
                                                           ImmutableList.of(weightedRange),
                                                           false);
        assertEquals(numSSTables, boundaries.size());
        boundaries.add(0, partitioner.getMinimumToken());
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>(numSSTables);
        for (int i = 0; i < numSSTables; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundaries.get(i).increaseSlightly(), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundaries.get(i+1), emptyBuffer);
            sstables.add(mockSSTable(level, bytesOnDisk, timestamp, 0., first, last));

            timestamp+=10;
        }

        return sstables;
    }

    private long totUncompressedLength(Collection<SSTableReader> sstables)
    {
        long ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.uncompressedLength();

        return ret;
    }

    private double totHotness(Collection<SSTableReader> sstables)
    {
        double ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.hotness();

        return ret;
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

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(cfs, options);

        final int numCompactions = 5;
        long minSize = minSSTableSize;
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

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(cfs, options);

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

        SizeTieredCompactionStrategy strategy = new SizeTieredCompactionStrategy(cfs, options);

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

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(cfs, options);

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

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(cfs, options);

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

        TimeWindowCompactionStrategy strategy = new TimeWindowCompactionStrategy(cfs, options);

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

        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(cfs, options);

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

        // L1 will compact the first sstable because the score is > 1 plus the overlapping sstables from L2
        compactions.add(overlapping(ssTablesByLevel.get(1).get(0), ssTablesByLevel.get(2)));

        // L2 will compact the first sstable because the score is > 1 but no other overlapping sstables since L3 is empty
        compactions.add(overlapping(ssTablesByLevel.get(2).get(0), ImmutableList.of()));

        // L2 and L1 compactions can proceed in parallel but L0 will refuse to compact due to overlapping sstables in L1
        // already compacting, hence we can only test 2 compactions initially
        testCompactionStatistics(sstables, compactions, 2, strategy);

        // Now check L0 compaction can proceed, the other levels won't compact since the score should be < 1
        ssTablesByLevel.get(1).remove(0); // the first one must have been compacted
        Set<SSTableReader> candidates = Sets.union(Sets.newLinkedHashSet(ssTablesByLevel.get(0)), Sets.newLinkedHashSet(ssTablesByLevel.get(1)));
        long totLength = totUncompressedLength(candidates);
        UUID id = mockCompaction(strategy, sstables, candidates, Collections.emptySet());

        verifyStatistics(strategy,
                         1,
                         1,
                         candidates.size(),
                         candidates.size(),
                         totLength,
                         0,
                         0,
                         0);

        CompactionProgress progress = mockCompactionProgress(candidates, id);
        strategy.getBackgroundCompactions().setInProgress(progress);

        verifyStatistics(strategy,
                         1,
                         1,
                         candidates.size(),
                         candidates.size(),
                         totLength,
                         totLength,
                         totLength,
                         0);

        strategy.backgroundCompactions.setCompleted(id);

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

        LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(cfs, options);

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
        Set<SSTableReader> sstables = compactions.stream().flatMap(bucket -> bucket.stream()).collect(Collectors.toSet());
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
        // Add the tables to the strategy
        for (SSTableReader sstable : sstables)
            strategy.addSSTable(sstable);

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
        for (int i = 0; i < numExpectedCompactions; i++)
        {
            int compactingLevel = compactions.size() - i - 1;
            Set<SSTableReader> candidates = Sets.newHashSet(compactions.get(compactingLevel));

            UUID id = mockCompaction(strategy, sstables, candidates, compacting);

            numCompactionsInProgress++;
            numSSTablesCompacting += candidates.size();
            submittedCompactions.add(Pair.create(candidates, id));

            // after mocking the compaction the list of pending compactions has been updated in the strategy
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

            // Now we simulate starting the compaction task
            CompactionProgress progress = mockCompactionProgress(candidates, id);
            strategy.getBackgroundCompactions().setInProgress(progress);

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

        // Terminate the compactions one by one by closing the AutoCloseable and check
        // that the statistics are updated
        for (Pair<Set<SSTableReader>, UUID> pair : submittedCompactions)
        {
            Set<SSTableReader> compSSTables = pair.left;
            long totSSTablesLen = totUncompressedLength(compSSTables);
            strategy.getBackgroundCompactions().setCompleted(pair.right);

            numCompactions--;
            numCompactionsInProgress--;
            numSSTables -= compSSTables.size();
            numSSTablesCompacting -= compSSTables.size();

            totLength -= totSSTablesLen;
            totRead -= totSSTablesLen;
            totWritten -= totSSTablesLen;
            totHotness -= totHotness(compSSTables);

            for (SSTableReader sstable : pair.left)
                strategy.removeSSTable(sstable);

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
    }

    private UUID mockCompaction(AbstractCompactionStrategy strategy, Set<SSTableReader> live, Set<SSTableReader> candidates, Set<SSTableReader> compacting)
    {
        final UUID id = UUID.randomUUID();
        final AtomicReference<LifecycleTransaction> txn = new AtomicReference<>();

        when(dataTracker.tryModify(anyIterable(), eq(OperationType.COMPACTION))).thenAnswer(invocation -> {
            assertNull(txn.get());

            LifecycleTransaction ret = Mockito.mock(LifecycleTransaction.class);
            when(ret.opId()).thenReturn(id);
            when(ret.originals()).thenReturn(candidates);
            when(ret.getCompacting()).thenReturn(Sets.union(compacting, candidates));

            txn.set(ret);
            return ret;
        });

        when(cfs.getSSTables(eq(SSTableSet.LIVE))).thenReturn(live);
        when(cfs.getNoncompactingSSTables()).thenAnswer(invocation -> Sets.difference(live, txn.get() == null ? compacting : Sets.union(compacting, candidates)));
        when(cfs.getNoncompactingSSTables(anyIterable())).thenAnswer(invocation -> Sets.difference(Sets.newHashSet((Iterable<SSTableReader>)invocation.getArguments()[0]),
                                                                                                  txn.get() == null ? compacting : Sets.union(compacting, candidates)));
        when(cfs.getCompactingSSTables()).thenAnswer(invocation -> txn.get() == null ? compacting : Sets.union(compacting, candidates));

        // Ask for a background compaction
        AbstractCompactionTask task = strategy.getNextBackgroundTask((int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
        assertNotNull(task);
        return id;
    }

    private CompactionProgress mockCompactionProgress(Set<SSTableReader> compacting, UUID id)
    {
        CompactionProgress progress = Mockito.mock(CompactionProgress.class);

        long compactingLen = totUncompressedLength(compacting);
        when(progress.operationId()).thenReturn(id);
        when(progress.inSSTables()).thenReturn(compacting);
        when(progress.uncompressedBytesRead()).thenReturn(compactingLen);
        when(progress.uncompressedBytesWritten()).thenReturn(compactingLen);
        when(progress.durationInNanos()).thenReturn(TimeUnit.SECONDS.toNanos(30));

        return progress;
    }

    private void verifyStatistics(AbstractCompactionStrategy strategy,
                                  int expectedCompactions,
                                  int expectedCompacting,
                                  int expectedSSTables,
                                  int expectedSSTablesCompacting,
                                  long expectedTotBytes,
                                  long expectedReadBytes,
                                  long expectedWrittenBytes,
                                  double expectedTotHotness)
    {
        CompactionStrategyStatistics stats = strategy.getStatistics();
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
            else
            {
                LeveledCompactionStatistics leveledStatistics = (LeveledCompactionStatistics) compactionStatistics;

                totBytes += leveledStatistics.tot();
                writtenBytes += leveledStatistics.written();
                readBytes += leveledStatistics.read();
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

        if (hotness > 0)
            assertEquals(expectedTotHotness, hotness, epsilon);

    }
}