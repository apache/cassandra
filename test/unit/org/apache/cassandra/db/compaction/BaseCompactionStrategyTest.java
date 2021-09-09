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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math3.random.JDKRandomGenerator;

import org.junit.Ignore;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * A class that contains common mocks and test utilities for unit tests of compaction strategies
 * that involve mocking compactions and sstables.
 */
@Ignore
public class BaseCompactionStrategyTest
{
    static final double epsilon = 0.00000001;
    static final JDKRandomGenerator random = new JDKRandomGenerator();

    final String keyspace = "ks";
    final String table = "tbl";

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    CompactionRealm realm;

    @Mock
    CompactionStrategyFactory strategyFactory;

    @Mock
    DiskBoundaries diskBoundaries;

    // Returned by diskBoundaries.getPositions() and modified by UnifiedCompactionStrategyTest
    protected List<PartitionPosition> diskBoundaryPositions = null;

    int diskIndexes = 0;

    SortedLocalRanges localRanges;

    Tracker dataTracker;

    long repairedAt;

    CompactionLogger compactionLogger;

    IPartitioner partitioner;

    Splitter splitter;

    protected static void setUpClass()
    {
        long seed = System.currentTimeMillis();
        random.setSeed(seed);
        System.out.println("Random seed: " + seed);

        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    protected void setUp()
    {
        setUp(1);
    }

    protected void setUp(int numShards)
    {
        MockitoAnnotations.initMocks(this);

        TableMetadata metadata = TableMetadata.builder(keyspace, table)
                                              .addPartitionKeyColumn("pk", AsciiType.instance)
                                              .build();

        dataTracker = Tracker.newDummyTracker();
        repairedAt = System.currentTimeMillis();
        partitioner = DatabaseDescriptor.getPartitioner();
        splitter = partitioner.splitter().orElse(null);
        if (numShards > 1)
            assertNotNull("Splitter is required with multiple compaction shards", splitter);

        localRanges = SortedLocalRanges.forTesting(realm, ImmutableList.of(new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()))));

        when(realm.metadata()).thenReturn(metadata);
        when(realm.getKeyspaceName()).thenReturn(keyspace);
        when(realm.getTableName()).thenReturn(table);
        when(realm.getDiskBoundaries()).thenReturn(diskBoundaries);
        when(diskBoundaries.getLocalRanges()).thenReturn(localRanges);
        when(realm.getPartitioner()).thenReturn(partitioner);
        when(realm.getLiveSSTables()).thenAnswer(request -> dataTracker.getLiveSSTables());
        when(realm.getCompactingSSTables()).thenAnswer(request -> dataTracker.getCompacting());
        when(realm.getSSTables(any())).thenAnswer(request -> dataTracker.getView().select(request.getArgument(0)));
        when(realm.getNoncompactingSSTables(anyIterable())).thenAnswer(request -> dataTracker.getNoncompacting(request.getArgument(0)));
        when(realm.tryModify(anyIterable(), any())).thenAnswer(
            request -> dataTracker.tryModify(request.getArgument(0, Iterable.class),
                                             request.getArgument(1)));

        // use a real compaction logger to execute that code too, even though we don't really check
        // the content of the files, at least we cover the code. The files will be overwritten next
        // time the test is run or by a gradle clean task, so they will not grow indefinitely
        compactionLogger = new CompactionLogger(realm.metadata());
        compactionLogger.enable();

        when(strategyFactory.getRealm()).thenReturn(realm);
        when(strategyFactory.getCompactionLogger()).thenReturn(compactionLogger);

        when(diskBoundaries.getNumBoundaries()).thenAnswer(invocation -> diskIndexes);
        when(diskBoundaries.getPositions()).thenAnswer(invocationOnMock -> diskBoundaryPositions);
    }

    /**
     * Add sstables to the tracker, which is enough for {@link UnifiedCompactionStrategy}, but for
     * {@link LegacyAbstractCompactionStrategy} we also need to add the sstables directly to the strategy.
     */
    void addSSTablesToStrategy(AbstractCompactionStrategy strategy, Iterable<SSTableReader> sstables)
    {
        dataTracker.addInitialSSTables(sstables);

        if (strategy instanceof LegacyAbstractCompactionStrategy)
        {
            LegacyAbstractCompactionStrategy legacyStrategy = (LegacyAbstractCompactionStrategy) strategy;
            for (SSTableReader sstable : sstables)
                legacyStrategy.addSSTable(sstable);
        }
    }

    /**
     * Remove sstables from the tracker, which should be enough for {@link UnifiedCompactionStrategy}, but for
     * {@link LegacyAbstractCompactionStrategy} we also need to remove the sstables directly from the strategy.
     */
    void removeSSTablesFromStrategy(AbstractCompactionStrategy strategy, Set<SSTableReader> sstables)
    {
        dataTracker.removeCompactingUnsafe(sstables);

        if (strategy instanceof LegacyAbstractCompactionStrategy)
        {
            LegacyAbstractCompactionStrategy legacyStrategy = (LegacyAbstractCompactionStrategy) strategy;
            for (SSTableReader sstable : sstables)
                legacyStrategy.removeSSTable(sstable);
        }
    }

    SSTableReader mockSSTable(int level, long bytesOnDisk, long timestamp, double hotness, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(level, bytesOnDisk, timestamp, hotness, first, last,  0, true, null, 0);
    }

    SSTableReader mockSSTable(long bytesOnDisk, long timestamp, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(0, bytesOnDisk, timestamp, 0, first, last,  0, true, null, 0);
    }

    SSTableReader mockSSTable(int level,
                              long bytesOnDisk,
                              long timestamp,
                              double hotness,
                              DecoratedKey first,
                              DecoratedKey last,
                              int diskIndex,
                              boolean repaired,
                              UUID pendingRepair,
                              int ttl)
    {
        // We create a ton of mock SSTables that mockito is going to keep until the end of the test suite without stubOnly.
        // Mockito keeps them alive to preserve the history of invocations which is not available for stubs. If we ever
        // need history of invocations and remove stubOnly, we should also manually reset mocked SSTables in tearDown.
        // FIXME: This should eventually be CompactionSSTable
        SSTableReader ret = Mockito.mock(SSTableReader.class, withSettings().stubOnly()
                                                                            .defaultAnswer(RETURNS_SMART_NULLS));

        when(ret.isSuitableForCompaction()).thenReturn(true);
        when(ret.getSSTableLevel()).thenReturn(level);
        when(ret.onDiskLength()).thenReturn(bytesOnDisk);
        when(ret.uncompressedLength()).thenReturn(bytesOnDisk); // let's assume no compression
        when(ret.hotness()).thenReturn(hotness);
        when(ret.getMaxTimestamp()).thenReturn(timestamp);
        when(ret.getMinTimestamp()).thenReturn(timestamp);
        when(ret.getFirst()).thenReturn(first);
        when(ret.getLast()).thenReturn(last);
        when(ret.isMarkedSuspect()).thenReturn(false);
        when(ret.isRepaired()).thenReturn(repaired);
        when(ret.getRepairedAt()).thenReturn(repairedAt);
        when(ret.getPendingRepair()).thenReturn(pendingRepair);
        when(ret.isPendingRepair()).thenReturn(pendingRepair != null);
        when(ret.getColumnFamilyName()).thenReturn(table);
        when(ret.getGeneration()).thenReturn(new SequenceBasedSSTableUniqueIdentifier(level));
        when(ret.toString()).thenReturn(String.format("Bytes on disk: %s, level %d, hotness %f, timestamp %d, first %s, last %s, disk index: %d, repaired: %b, pend. repair: %b",
                                                      FBUtilities.prettyPrintMemory(bytesOnDisk), level, hotness, timestamp, first, last, diskIndex, repaired, pendingRepair));
        int deletionTime;
        if (ttl > 0)
            deletionTime = (int) TimeUnit.MILLISECONDS.toSeconds(timestamp) + ttl;
        else
            deletionTime = Integer.MAX_VALUE;

        when(ret.getMinLocalDeletionTime()).thenReturn(deletionTime);
        when(ret.getMaxLocalDeletionTime()).thenReturn(deletionTime);
        when(ret.getMinTTL()).thenReturn(ttl);
        when(ret.getMaxTTL()).thenReturn(ttl);

        when(diskBoundaries.getDiskIndexFromKey(ret)).thenReturn(diskIndex);
        if (diskIndex >= diskIndexes)
            diskIndexes = diskIndex + 1;
        return ret;
    }

    List<SSTableReader> mockSSTables(int numSSTables, long bytesOnDisk, double hotness, long timestamp)
    {
        return mockSSTables(numSSTables, bytesOnDisk, hotness, timestamp, 0, true,null);
    }

    List<SSTableReader> mockSSTables(int numSSTables, long bytesOnDisk, double hotness, long timestamp, int diskIndex, boolean repaired, UUID pendingRepair)
    {
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < numSSTables; i++)
        {
            long b = (long)(bytesOnDisk * 0.95 + bytesOnDisk * 0.05 * random.nextDouble()); // leave 5% variability
            double h = hotness * 0.95 + hotness * 0.05 * random.nextDouble(); // leave 5% variability
            sstables.add(mockSSTable(0, b, timestamp, h, first, last, diskIndex, repaired, pendingRepair, 0));
        }

        return sstables;
    }

    List<SSTableReader> mockNonOverlappingSSTables(int numSSTables, int level, long bytesOnDisk)
    {
        if (!partitioner.splitter().isPresent())
            throw new IllegalStateException(String.format("Cannot split ranges with current partitioner %s", partitioner));

        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        Splitter.WeightedRange weightedRange = new Splitter.WeightedRange(1.0, range);
        Splitter splitter = partitioner.splitter().get();
        List<Token> boundaries = splitter.splitOwnedRanges(numSSTables,
                                                           ImmutableList.of(weightedRange),
                                                           Splitter.SplitType.ALWAYS_SPLIT)
                                 .boundaries;
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

    CompactionProgress mockCompletedCompactionProgress(Set<SSTableReader> compacting, UUID id)
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

    void addSizeTieredOptions(Map<String, String> options)
    {
        addSizeTieredOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE);
    }

    void addSizeTieredOptions(Map<String, String> options, long minSSTableSize)
    {
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, Long.toString(minSSTableSize));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_LOW));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, Double.toString(SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_HIGH));
    }

    void addTimeTieredOptions(Map<String, String> options)
    {
        addSizeTieredOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE);

        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, TimeUnit.MILLISECONDS.toString());
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, Long.toString(Long.MAX_VALUE)); // disable check for expired sstables
    }

    void addLeveledOptions(Map<String, String> options, long maxSSTableSizeBytes)
    {
        addLeveledOptions(options, SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE, maxSSTableSizeBytes, 10);
    }

    void addLeveledOptions(Map<String, String> options, long minSSTableSizeBytes, long maxSSTableSizeBytes, int fanout)
    {
        addSizeTieredOptions(options, minSSTableSizeBytes);

        options.put(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, Long.toString(maxSSTableSizeBytes >> 20)); // Bytes to MB
        options.put(LeveledCompactionStrategy.LEVEL_FANOUT_SIZE_OPTION, Integer.toString(fanout));
    }

    long totUncompressedLength(Collection<? extends CompactionSSTable> sstables)
    {
        long ret = 0;
        for (CompactionSSTable sstable : sstables)
            ret += sstable.uncompressedLength();

        return ret;
    }

    double totHotness(Collection<? extends CompactionSSTable> sstables)
    {
        double ret = 0;
        for (CompactionSSTable sstable : sstables)
            ret += sstable.hotness();

        return ret;
    }

}
