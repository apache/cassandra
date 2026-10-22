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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.collect.ImmutableList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.io.sstable.format.SSTableReader.UNIQUE_IDENTIFIER_FACTORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * The unified compaction strategy is described in this design document:
 *
 * See CEP-26: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy
 */
public class UnifiedCompactionStrategyTest
{
    private final static long ONE_MB = 1 << 20;

    // Multiple disks can be used both with and without disk boundaries. We want to test both cases.

    final String keyspace = "ks";
    final String table = "tbl";

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    ColumnFamilyStore cfs;

    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    CompactionStrategyManager csm;

    ColumnFamilyStore.VersionedLocalRanges localRanges;

    Tracker dataTracker;

    long repairedAt;

    IPartitioner partitioner;

    Splitter splitter;

    @BeforeClass
    public static void setUpClass()
    {
        long seed = System.currentTimeMillis();
        random.setSeed(seed);
        System.out.println("Random seed: " + seed);

        DatabaseDescriptor.daemonInitialization(); // because of all the static initialization in CFS
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }


    static final JDKRandomGenerator random = new JDKRandomGenerator();

    @Before
    public void setUp()
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

        when(cfs.getPartitioner()).thenReturn(partitioner);
        localRanges = cfs.fullWeightedRange(0, partitioner);

        when(cfs.metadata()).thenReturn(metadata);
        when(cfs.getTableName()).thenReturn(table);
        when(cfs.localRangesWeighted()).thenReturn(localRanges);
        when(cfs.getTracker()).thenReturn(dataTracker);
        when(cfs.getLiveSSTables()).thenAnswer(request -> dataTracker.getView().select(SSTableSet.LIVE));
        when(cfs.getSSTables(any())).thenAnswer(request -> dataTracker.getView().select(request.getArgument(0)));
        when(cfs.getCompactionStrategyManager()).thenReturn(csm);

        DiskBoundaries db = new DiskBoundaries(cfs, new Directories.DataDirectory[0], 0);
        when(cfs.getDiskBoundaries()).thenReturn(db);

        when(csm.onlyPurgeRepairedTombstones()).thenReturn(false);
    }

    @Test
    public void testNoSSTables()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getScalingParameter(anyInt())).thenReturn(4);
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        assertNull(strategy.getNextBackgroundTask(FBUtilities.nowInSeconds()));
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
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
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
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();

        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

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
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        List<UnifiedCompactionStrategy.Level> levels = strategy.getLevels();
        assertEquals(expectedTs.length, levels.size());

        for (int i = 0; i < expectedTs.length; i++)
        {
            UnifiedCompactionStrategy.Level level = levels.get(i);
            assertEquals(i, level.getIndex());
            UnifiedCompactionStrategy.SelectionContext context = new UnifiedCompactionStrategy.SelectionContext(strategy.getController(), strategy.getShardManager());
            UnifiedCompactionStrategy.CompactionPick pick = level.getCompactionPick(context);

            assertEquals(level.getSSTables().size() >= expectedTs[i], pick != null);
        }
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
        when(controller.getScalingParameter(anyInt())).thenReturn(W);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minSstableSizeBytes);

        if (maxSSTablesToCompact >= numSSTables)
            when(controller.maxConcurrentCompactions()).thenReturn(levels * (W < 0 ? 1 : F)); // make sure the work is assigned to different levels
        else
            when(controller.maxConcurrentCompactions()).thenReturn(1000); // make sure the work is assigned to different levels

        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(maxSSTablesToCompact);
        Random random = Mockito.mock(Random.class);
        when(random.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(random);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        List<SSTableReader> allSstables = new ArrayList<>();

        List<SSTableReader> sstables = mockSSTables(numSSTables,
                                                    0,
                                                    System.currentTimeMillis(),
                                                    0);
        allSstables.addAll(sstables);
        strategy.addSSTables(allSstables);
        dataTracker.addInitialSSTables(allSstables);

        int num = numSSTables;
        UnifiedCompactionStrategy.CompactionPick task;
        while (true)
        {
            task = strategy.getNextCompactionPick(0); // do not check expiration
            if (task == null)
                break;

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

            int expected = num;
            if (layout)
            {
                int forTopLevel = (int) (Math.pow(F, Math.floor(Math.log(num) / Math.log(F))));
                expected = W > 0
                           ? forTopLevel
                           : num / forTopLevel * forTopLevel;

            }
            expected = Math.min(expected, limit);

            int count = task.size();
            assertEquals(expected, count);
            for (SSTableReader rdr : task)
                strategy.removeSSTable(rdr);
            num -= count;
        }
        // Check that we issue all the compactions
        assertTrue(num < T);
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
    public void testGetNextBackgroundTasks()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getScalingParameter(anyInt())).thenReturn(0);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        IPartitioner partitioner = cfs.getPartitioner();

        List<SSTableReader> sstables = createSStables(partitioner);

        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        AbstractCompactionTask task = strategy.getNextBackgroundTask(0);
        assertSame(UnifiedCompactionTask.class, task.getClass());
        task.transaction.abort();
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner)
    {
        return createSStables(partitioner, mapFromPair(Pair.create(4 * ONE_MB, 4)), 10000);
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner, int ttl)
    {
        return createSStables(partitioner, mapFromPair(Pair.create(4 * ONE_MB, 4)), ttl);
    }

    private List<SSTableReader> createSStables(IPartitioner partitioner, Map<Long, Integer> sstablesMap)
    {
        return createSStables(partitioner, sstablesMap, 10000);
    }

    // Used to make sure timestamps are not exactly the same, which disables expiration
    int millisAdjustment = 0;

    private List<SSTableReader> createSStables(IPartitioner partitioner,
                                               Map<Long, Integer> sstablesMap,
                                               int ttl)
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
                                             System.currentTimeMillis() + millisAdjustment++,
                                             0.0,
                                             new BufferDecoratedKey(first, bb),
                                             new BufferDecoratedKey(max, bb),
                                             ttl));
                first = first.nextValidToken();
            }
        });
        return mockSSTables;
    }

    @Test
    public void testDropExpiredSSTables()
    {
        testDropExpiredFromBucket(1);
        testDropExpiredAndCompactNonExpired();
    }

    private void testDropExpiredFromBucket(int numShards)
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getScalingParameter(anyInt())).thenReturn(3); // T=5
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.getExpiredSSTableCheckFrequency()).thenReturn(Long.MIN_VALUE); // check expiration on every pick
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();

        List<SSTableReader> sstables = createSStables(cfs.getPartitioner());
        // Tracker#addSSTables also tries to backup SSTables, so we use addInitialSSTables and notify explicitly
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        try
        {
            // nothing to compact yet
            assertNull(strategy.getNextCompactionPick(0));

            long timestamp = sstables.get(sstables.size() - 1).getMaxLocalDeletionTime();
            long expirationPoint = timestamp + 1;

            UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(expirationPoint);
            assertNotNull(pick);
            assertEquals(sstables.size(), pick.size());
            assertEquals(-1, pick.level);
        }
        finally
        {
            strategy.shutdown();
        }
    }

    private void testDropExpiredAndCompactNonExpired()
    {
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 2 << 20;
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getScalingParameter(anyInt())).thenReturn(2);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.getExpiredSSTableCheckFrequency()).thenReturn(Long.MIN_VALUE); // check expiration on every pick
        when(controller.maxSSTablesToCompact()).thenReturn(1000);

        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();

        List<SSTableReader> expiredSSTables = createSStables(cfs.getPartitioner(), 1000);
        List<SSTableReader> nonExpiredSSTables = createSStables(cfs.getPartitioner(), 0);
        strategy.addSSTables(expiredSSTables);
        strategy.addSSTables(nonExpiredSSTables.subList(0, 3));
        dataTracker.addInitialSSTables(ImmutableList.copyOf(Iterables.concat(expiredSSTables, nonExpiredSSTables)));

        long timestamp = expiredSSTables.get(expiredSSTables.size() - 1).getMaxLocalDeletionTime();
        long expirationPoint = timestamp + 1;

        try
        {
            UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(expirationPoint);

            assertEquals(expiredSSTables.size(), pick.size());
            assertEquals(-1, pick.level);

            strategy.addSSTables(nonExpiredSSTables);   // duplicates should be skipped
            pick = strategy.getNextCompactionPick(expirationPoint);

            assertEquals(expiredSSTables.size() + nonExpiredSSTables.size(), pick.size());
            assertEquals(0, pick.level);
        }
        finally
        {
            strategy.shutdown();
        }
    }

    @Test
    public void testPending()
    {
        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(8); // F=10, T=10
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.maxSSTablesToCompact()).thenReturn(10); // same as fanout

        long minimalSizeBytes = 2 << 20;
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.random()).thenCallRealMethod();

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();

        int count = 91;
        List<SSTableReader> sstables = createSStables(cfs.getPartitioner(),
                                                      mapFromPair(Pair.create(4 * ONE_MB, count)));
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(9, strategy.getEstimatedRemainingTasks());
    }

    @Test
    public void testMaximalSelection()
    {
        Set<SSTableReader> allSSTables = new HashSet<>();
        allSSTables.addAll(mockNonOverlappingSSTables(10, 0, 100 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(15, 1, 200 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(25, 2, 400 << 20));

        Controller controller = Mockito.mock(Controller.class);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.addSSTables(allSSTables);
        dataTracker.addInitialSSTables(allSSTables);

        Collection<AbstractCompactionTask> tasks = strategy.getMaximalTask(0, false);
        assertEquals(5, tasks.size());  // 5 (gcd of 10,15,25) common boundaries
        for (AbstractCompactionTask task : tasks)
        {
            Set<SSTableReader> compacting = task.transaction.originals();
            assertEquals(2 + 3 + 5, compacting.size()); // count / gcd sstables of each level
            assertEquals((2 * 100L + 3 * 200 + 5 * 400) << 20, compacting.stream().mapToLong(SSTableReader::onDiskLength).sum());

            // None of the selected sstables may intersect any in any other set.
            for (AbstractCompactionTask task2 : tasks)
            {
                if (task == task2)
                    continue;

                Set<SSTableReader> compacting2 = task2.transaction.originals();
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
        testBucketSelection(arr(25, 15, 10), repeats(10, arr(6, 4)), Overlaps.InclusionMethod.SINGLE);
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
        // Note: picks are returned right-to-left because the random mock always returns 0, picking the last bucket.
        testBucketSelection(repeats(4, arr(6, 3)), arr(6, 6, 5), Overlaps.InclusionMethod.TRANSITIVE, 0, 1);
        // Drop one full: don't compact
        testBucketSelection(repeats(4, arr(3, 6)), arr(6, 6), Overlaps.InclusionMethod.TRANSITIVE, 5, 1);
        // Drop two adjacent halves: don't compact
        testBucketSelection(repeats(4, arr(6, 3)), arr(6, 6), Overlaps.InclusionMethod.TRANSITIVE, 4, 2, 3);
    }

    // We use a level with only non-overlapping SSTables, so there won't be any standard bucketing compaction.
    // The per-shard threshold is fanout(3) * maxSstablesPerShardFactor(10) = 30.
    // We use a number of SSTables per shard one more than the per-shard threshold.
    // We use a total number of SSTables one more than numShards * sstablesPerShard so the SSTables are not aligned
    // with the shard boundaries. This means that all shards will have an SSTable in common with the next shard.
    // We drop the SSTables that cross the boundaries between the first and second shard, and between the second and
    // third shard.
    // In the end, we get 3 groups of shards with SSTables in common:
    // - a group with the first shard which has perShardThreshold + 1 SSTables, so will get compacted.
    // - a group with the second shard which has perShardThreshold SSTables, so will not get compacted.
    // - a group with the rest of the shards, one of those shards having perShardThreshold + 1 SSTables, so will get compacted.
    // Note: picks are returned last group first because the random mock always returns 0, picking the last group.
    @Test
    public void testBucketSelectionNonOverlapping()
    {
        int numShards = 16;
        double maxSstablesPerShardFactor = 10.0;
        int perShardThreshold = (int) (3 * maxSstablesPerShardFactor);
        int sstablesPerShard = perShardThreshold + 1;
        testBucketSelection(arr(numShards * sstablesPerShard + 1),
                            arr(sstablesPerShard * (numShards - 2), sstablesPerShard),
                            Overlaps.InclusionMethod.TRANSITIVE,
                            numShards,
                            maxSstablesPerShardFactor,
                            perShardThreshold,
                            sstablesPerShard, sstablesPerShard * 2);
    }

    // Same as testBucketSelectionNonOverlapping, but with an infinite maxSstablesPerShardFactor
    // to deactivate compaction of oversize shards.
    @Test
    public void testBucketSelectionNonOverlappingInfiniteFactor()
    {
        int numShards = 16;
        int sstablesPerShard = (3 * 10) + 1;
        testBucketSelection(arr(numShards * sstablesPerShard + 1),
                            arr(),
                            Overlaps.InclusionMethod.TRANSITIVE,
                            numShards,
                            Double.POSITIVE_INFINITY,
                            numShards * sstablesPerShard - 1,
                            sstablesPerShard, sstablesPerShard * 2);
    }

    // Same layout as testBucketSelectionNonOverlapping, with the random mock choosing the first oversize group
    // instead of the last: nextInt(1) is 0 for the first group, nextInt(2) is 1 so the last group does not replace it.
    @Test
    public void testOversizeShardPickRandomGroupChoice()
    {
        int numShards = 16;
        int perShardThreshold = 3 * 10;
        int sstablesPerShard = perShardThreshold + 1;
        List<SSTableReader> list = mockNonOverlappingSSTables(numShards * sstablesPerShard + 1, 0, (100 << 20) / (numShards * sstablesPerShard + 1));
        list.remove(sstablesPerShard * 2);
        list.remove(sstablesPerShard);

        Controller controller = mockOversizeShardController(-1, numShards, 10.0); // F=3, T=2
        Random randomMock = Mockito.mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(0);
        when(randomMock.nextInt(2)).thenReturn(1);
        when(controller.random()).thenReturn(randomMock);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.addSSTables(list);
        dataTracker.addInitialSSTables(list);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(sstablesPerShard, pick.size());
        assertEquals(ImmutableSet.copyOf(list.subList(0, sstablesPerShard)), ImmutableSet.copyOf(pick));
        strategy.removeSSTables(pick);

        pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(sstablesPerShard * (numShards - 2), pick.size());
        strategy.removeSSTables(pick);

        assertNull(strategy.getNextCompactionPick(0));
        assertEquals(perShardThreshold, strategy.getSSTables().size());
    }

    // Overlapping sstables below the level threshold still count toward the per-shard limit, and the pick reports
    // the level's overlap and one remaining task.
    @Test
    public void testOversizeShardPickWithOverlapsBelowThreshold()
    {
        Controller controller = mockOversizeShardController(2, 1, 2.0); // F=T=4, per-shard threshold 8
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        // Five disjoint sstables over the first half of the ring plus four that each overlap exactly one of them,
        // all with the same density so they form a single level with max overlap 2.
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(100, i * 10).nextValidToken(), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(boundary(100, i * 10 + 10), emptyBuffer);
            sstables.add(mockSSTable(0, 10 * ONE_MB, timestamp + i, 0.0, first, last));
        }
        for (int i = 0; i < 4; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(100, i * 10 + 5), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(boundary(100, i * 10 + 8), emptyBuffer);
            sstables.add(mockSSTable(0, 3 * ONE_MB, timestamp + 10 + i, 0.0, first, last));
        }
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(ImmutableSet.copyOf(sstables), ImmutableSet.copyOf(pick));
        assertEquals(0, pick.level);
        assertEquals(2, pick.overlap);
        assertEquals(1, strategy.getEstimatedRemainingTasks());

        strategy.removeSSTables(pick);
        assertNull(strategy.getNextCompactionPick(0));
        assertEquals(0, strategy.getEstimatedRemainingTasks());
    }

    // An oversize shard pick carries the level's overlap, so a level whose overlap reaches its threshold is
    // compacted first, whichever of the two is lower.
    @Test
    public void testOversizeShardPickYieldsToHigherOverlapLevel()
    {
        // Level 0 holds nine disjoint sstables (over the per-shard limit of 8), level 1 holds four overlapping ones.
        Controller controller = mockOversizeShardController(2, 1, 2.0); // F=T=4, per-shard threshold 8
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        List<SSTableReader> disjoint = mockNonOverlappingSSTables(9, 0, (100 << 20) / 9);
        List<SSTableReader> overlapping = createSStables(partitioner, mapFromPair(Pair.create(700 * ONE_MB, 4)));
        strategy.addSSTables(disjoint);
        strategy.addSSTables(overlapping);
        dataTracker.addInitialSSTables(disjoint);
        dataTracker.addInitialSSTables(overlapping);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(1, pick.level);
        assertEquals(ImmutableSet.copyOf(overlapping), ImmutableSet.copyOf(pick));
        strategy.removeSSTables(pick);

        pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(0, pick.level);
        assertEquals(ImmutableSet.copyOf(disjoint), ImmutableSet.copyOf(pick));
        strategy.removeSSTables(pick);
        assertNull(strategy.getNextCompactionPick(0));

        // The mirror: four overlapping sstables on level 0, nine disjoint ones on level 1.
        strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        overlapping = createSStables(partitioner, mapFromPair(Pair.create(99 * ONE_MB, 4)));
        disjoint = mockNonOverlappingSSTables(9, 1, (700 << 20) / 9);
        strategy.addSSTables(overlapping);
        strategy.addSSTables(disjoint);
        dataTracker.addInitialSSTables(overlapping);
        dataTracker.addInitialSSTables(disjoint);

        pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(0, pick.level);
        assertEquals(ImmutableSet.copyOf(overlapping), ImmutableSet.copyOf(pick));
        strategy.removeSSTables(pick);

        pick = strategy.getNextCompactionPick(0);
        assertNotNull(pick);
        assertEquals(1, pick.level);
        assertEquals(ImmutableSet.copyOf(disjoint), ImmutableSet.copyOf(pick));
    }

    @Test
    public void testOversizeShardPickThresholdBoundary()
    {
        // F=4: factor 2 gives a per-shard threshold of 8, factor 2.25 one of 9. A shard at the threshold is left alone.
        testOversizeShardPickThreshold(2.0, 8, false);
        testOversizeShardPickThreshold(2.0, 9, true);
        testOversizeShardPickThreshold(2.25, 9, false);
        testOversizeShardPickThreshold(2.25, 10, true);
        // 0 disables the check whatever the count
        testOversizeShardPickThreshold(0, 50, false);
    }

    private void testOversizeShardPickThreshold(double maxSstablesPerShardFactor, int numSSTables, boolean expectPick)
    {
        Controller controller = mockOversizeShardController(2, 1, maxSstablesPerShardFactor); // F=T=4
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        List<SSTableReader> sstables = mockNonOverlappingSSTables(numSSTables, 0, (100 << 20) / numSSTables);
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertEquals("factor " + maxSstablesPerShardFactor + " with " + numSSTables + " sstables", expectPick, pick != null);
        if (expectPick)
            assertEquals(numSSTables, pick.size());
    }

    // getOversizeShardPick scales the level density by the shard set coverage before asking the controller for a
    // shard count, as compaction tasks do. The node owns 1/8 of the ring and level 0 has a maximum density of 8.8 GiB.
    // Scaled, that is 1.1 GiB, one shard at a 1 GiB target holding all eight sstables. Unscaled, it would be eight
    // shards with one sstable each and no pick.
    @Test
    public void testOversizeShardPickScalesDensityToShardSetCoverage()
    {
        Token ownedEnd = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), 1.0 / 8);
        ColumnFamilyStore.VersionedLocalRanges ownedRanges = new ColumnFamilyStore.VersionedLocalRanges(ColumnFamilyStore.RING_VERSION_IRRELEVANT, 1);
        ownedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), ownedEnd)));
        when(cfs.localRangesWeighted()).thenReturn(ownedRanges);

        Map<String, String> options = new HashMap<>();
        options.put("scaling_parameters", "T2"); // F=T=2, with a factor of 1 a shard may hold 2 sstables
        options.put("max_sstables_per_shard_factor", "1");
        options.put("flush_size_override", "1GiB"); // level 0 maximum density 1 GiB * 0.55 * 8 * 2 = 8.8 GiB
        options.put("target_sstable_size", "1GiB");
        options.put("base_shard_count", "1");
        options.put("sstable_growth", "0");
        options.put("min_sstable_size", "0B");
        Controller controller = Controller.fromOptions(cfs, options);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        // Eight disjoint sstables, each inside its own eighth of the owned range, with a density of 4 GiB.
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < 8; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(64, i).nextValidToken(), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(boundary(128, 2 * i + 1), emptyBuffer);
            sstables.add(mockSSTable(0, 32 * ONE_MB, timestamp + i, 0.0, first, last));
        }
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertEquals(1.0 / 8, strategy.getShardManager().shardSetCoverage(), 0.001); // shard manager is built by the pick
        assertNotNull(pick);
        assertEquals(0, pick.level);
        assertEquals(ImmutableSet.copyOf(sstables), ImmutableSet.copyOf(pick));
    }

    // With two disks each disk is a shard set, so the level density is scaled to one disk's share of the ring. The
    // node owns 1/4 of the ring, each disk 1/8, and level 0 has a maximum density of 4.4 GiB. Scaled to a disk, that
    // is 0.55 GiB, one shard per disk at a 512 MiB target. Scaled to the node's share it would give two shards per
    // disk, and unscaled eight. In both cases no shard holds more than two sstables.
    @Test
    public void testOversizeShardPickScalesDensityToShardSetCoverageWithDisks()
    {
        Token diskEnd = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), 1.0 / 8);
        Token ownedEnd = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), 1.0 / 4);
        ColumnFamilyStore.VersionedLocalRanges ownedRanges = new ColumnFamilyStore.VersionedLocalRanges(ColumnFamilyStore.RING_VERSION_IRRELEVANT, 1);
        ownedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), ownedEnd)));
        when(cfs.localRangesWeighted()).thenReturn(ownedRanges);
        List<PartitionPosition> diskPositions = new ArrayList<>();
        diskPositions.add(diskEnd.maxKeyBound());
        diskPositions.add(ownedEnd.maxKeyBound());
        when(cfs.getDiskBoundaries()).thenReturn(new DiskBoundaries(cfs, null, diskPositions, ColumnFamilyStore.RING_VERSION_IRRELEVANT, 0));

        Map<String, String> options = new HashMap<>();
        options.put("scaling_parameters", "T2"); // F=T=2, with a factor of 1 a shard may hold 2 sstables
        options.put("max_sstables_per_shard_factor", "1");
        options.put("flush_size_override", "1GiB"); // level 0 maximum density 1 GiB * 0.55 * 4 * 2 = 4.4 GiB
        options.put("target_sstable_size", "512MiB");
        options.put("base_shard_count", "1");
        options.put("sstable_growth", "0");
        options.put("min_sstable_size", "0B");
        Controller controller = Controller.fromOptions(cfs, options);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        // Positions in 256ths of the ring: the first disk ends at 32 and the second at 64. Three disjoint sstables on
        // the first disk, two in its first half and one in its second, and two on the second disk. Each spans 1/128 of
        // the ring with a density of 2 GiB.
        int[] starts = { 1, 5, 17, 33, 41 };
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < starts.length; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(256, starts[i]).nextValidToken(), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(boundary(256, starts[i] + 2), emptyBuffer);
            sstables.add(mockSSTable(0, 16 * ONE_MB, timestamp + i, 0.0, first, last));
        }
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertEquals(1.0 / 8, strategy.getShardManager().shardSetCoverage(), 0.001); // shard manager is built by the pick
        assertNotNull(pick);
        assertEquals(0, pick.level);
        assertEquals(ImmutableSet.copyOf(sstables.subList(0, 3)), ImmutableSet.copyOf(pick));
    }

    // getOversizeShardPick scales the level density with the default sharding options (base shard count 4, minimum
    // sstable size 100 MiB, growth 0.333). The node owns 1/8 of the ring and level 0 has a maximum density of
    // 1760 MiB. Scaled, that is 220 MiB, which the minimum size turns into two shards. Unscaled, it takes the growth
    // path and gives four shards, and no shard holds more than two sstables.
    @Test
    public void testOversizeShardPickScalesDensityToShardSetCoverageWithDefaultOptions()
    {
        Token ownedEnd = partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), 1.0 / 8);
        ColumnFamilyStore.VersionedLocalRanges ownedRanges = new ColumnFamilyStore.VersionedLocalRanges(ColumnFamilyStore.RING_VERSION_IRRELEVANT, 1);
        ownedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(partitioner.getMinimumToken(), ownedEnd)));
        when(cfs.localRangesWeighted()).thenReturn(ownedRanges);

        Map<String, String> options = new HashMap<>();
        options.put("scaling_parameters", "T2"); // F=T=2, with a factor of 1 a shard may hold 2 sstables
        options.put("max_sstables_per_shard_factor", "1");
        options.put("flush_size_override", "200MiB"); // level 0 maximum density 200 MiB * 0.55 * 8 * 2 = 1760 MiB
        Controller controller = Controller.fromOptions(cfs, options);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        // Positions in 256ths of the ring: the owned range ends at 32, two shards split it at 16 and four at 8, 16
        // and 24. Two sstables in the first quarter of the owned range and one in each of the other three, so the
        // first half holds three. Each spans 1/256 of the ring with a density of 1 GiB.
        int[] starts = { 1, 3, 9, 17, 25 };
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < starts.length; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(256, starts[i]).nextValidToken(), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(boundary(256, starts[i] + 1), emptyBuffer);
            sstables.add(mockSSTable(0, 4 * ONE_MB, timestamp + i, 0.0, first, last));
        }
        strategy.addSSTables(sstables);
        dataTracker.addInitialSSTables(sstables);

        UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
        assertEquals(1.0 / 8, strategy.getShardManager().shardSetCoverage(), 0.001); // shard manager is built by the pick
        assertNotNull(pick);
        assertEquals(0, pick.level);
        assertEquals(ImmutableSet.copyOf(sstables.subList(0, 3)), ImmutableSet.copyOf(pick));
    }

    // Expired sstables leave the candidate set before levels form, so they do not count toward the per-shard limit,
    // and they are added to the oversize shard pick like to any other pick.
    @Test
    public void testExpiredSSTablesJoinOversizeShardPick()
    {
        Controller controller = mockOversizeShardController(2, 1, 2.0); // F=T=4, per-shard threshold 8
        when(controller.getExpiredSSTableCheckFrequency()).thenReturn(-1L); // check on every call
        when(controller.random()).thenCallRealMethod();
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();

        List<SSTableReader> nonExpired = mockNonOverlappingSSTables(9, 0, (100 << 20) / 9);
        // The expired sstables must be older than every live one, or the overlap check keeps them.
        List<SSTableReader> expired = expiredSSTables(2, nonExpired.get(0).getMinTimestamp() - 1000);
        strategy.addSSTables(nonExpired);
        strategy.addSSTables(expired);
        dataTracker.addInitialSSTables(nonExpired);
        dataTracker.addInitialSSTables(expired);
        long expirationPoint = expired.get(expired.size() - 1).getMaxLocalDeletionTime() + 1;

        try
        {
            UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(expirationPoint);
            assertNotNull(pick);
            assertEquals(0, pick.level);
            assertEquals(ImmutableSet.copyOf(Iterables.concat(nonExpired, expired)), ImmutableSet.copyOf(pick));
        }
        finally
        {
            strategy.shutdown();
        }

        // Eight live sstables plus one expired do not exceed the per-shard limit: only the expired one is picked.
        strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();
        nonExpired = mockNonOverlappingSSTables(8, 0, (100 << 20) / 8);
        expired = expiredSSTables(1, nonExpired.get(0).getMinTimestamp() - 1000);
        strategy.addSSTables(nonExpired);
        strategy.addSSTables(expired);
        dataTracker.addInitialSSTables(nonExpired);
        dataTracker.addInitialSSTables(expired);
        expirationPoint = expired.get(0).getMaxLocalDeletionTime() + 1;

        try
        {
            UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(expirationPoint);
            assertNotNull(pick);
            assertEquals(-1, pick.level);
            assertEquals(ImmutableSet.copyOf(expired), ImmutableSet.copyOf(pick));
        }
        finally
        {
            strategy.shutdown();
        }
    }

    private List<SSTableReader> expiredSSTables(int count, long timestamp)
    {
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        DecoratedKey first = new BufferDecoratedKey(partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), 0.01), emptyBuffer);
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMaximumToken(), emptyBuffer);
        List<SSTableReader> sstables = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            sstables.add(mockSSTable(0, 4 * ONE_MB, timestamp + i, 0.0, first, last, 1000));
        return sstables;
    }

    // The controller stubs shared by the oversize shard tests, matching testBucketSelection: the base sstable size
    // of 90 MiB puts sstables of density 100 MiB on level 0 and the random mock picks the last matching group.
    private Controller mockOversizeShardController(int scalingParameter, int numShards, double maxSstablesPerShardFactor)
    {
        Controller controller = Mockito.mock(Controller.class);
        when(controller.getScalingParameter(anyInt())).thenReturn(scalingParameter);
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(maxSstablesPerShardFactor);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) (90 << 20));
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.overlapInclusionMethod()).thenReturn(Overlaps.InclusionMethod.TRANSITIVE);
        Random randomMock = Mockito.mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(randomMock);
        return controller;
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

    public void testBucketSelection(int[] counts, int[] expecteds, Overlaps.InclusionMethod overlapInclusionMethod)
    {
        testBucketSelection(counts, expecteds, overlapInclusionMethod, 0);
    }

    public void testBucketSelection(int[] counts, int[] expecteds, Overlaps.InclusionMethod overlapInclusionMethod, int expectedRemaining, int... dropFromFirst)
    {
        testBucketSelection(counts, expecteds, overlapInclusionMethod, 1, 10.0, expectedRemaining, dropFromFirst);
    }

    public void testBucketSelection(int[] counts, int[] expecteds, Overlaps.InclusionMethod overlapInclusionMethod, int numShards, double maxSstablesPerShardFactor, int expectedRemaining, int... dropFromFirst)
    {
        Set<SSTableReader> allSSTables = new HashSet<>();
        int fanout = counts.length;
        for (int i = 0; i < fanout; ++i)
        {
            final int count = counts[i];
            final List<SSTableReader> list = mockNonOverlappingSSTables(count, 0, (100 << 20) / count);
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
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(maxSstablesPerShardFactor);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) (90 << 20));
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
        when(controller.overlapInclusionMethod()).thenReturn(overlapInclusionMethod);
        Random randomMock = Mockito.mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(randomMock);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.addSSTables(allSSTables);
        dataTracker.addInitialSSTables(allSSTables);

        List<UnifiedCompactionStrategy.CompactionPick> picks = new ArrayList<>();
        while (true)
        {
            UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
            if (pick == null)
                break;
            strategy.removeSSTables(pick);
            picks.add(pick);
        }
        assertEquals(expectedRemaining, strategy.getSSTables().size());

        assertEquals(expecteds.length, picks.size());
        int buckIdx = 0;
        for (UnifiedCompactionStrategy.CompactionPick pick : picks)
        {
            int expectedCount = expecteds[buckIdx++];
            assertEquals(expectedCount, pick.size()); // count / gcd sstables of each level

            if (overlapInclusionMethod == Overlaps.InclusionMethod.TRANSITIVE)
            {
                // None of the selected sstables may intersect any in any other set.
                for (UnifiedCompactionStrategy.CompactionPick pick2 : picks)
                {
                    if (pick == pick2)
                        continue;

                    for (SSTableReader r1 : pick)
                        for (SSTableReader r2 : pick2)
                            assertTrue(r1 + " intersects " + r2, r1.getFirst().compareTo(r2.getLast()) > 0 || r1.getLast().compareTo(r2.getFirst()) < 0);
                }
            }
        }
    }

    @Test
    public void testTinyNearPointCoverageSSTablesAreEventuallyCompacted()
    {
        // Repair streams produce sstables holding a handful of adjacent keys, with a token span far
        // below any shard's but above ShardManager.MINIMUM_TOKEN_COVERAGE. Without a per-partition
        // span floor (CASSANDRA-21615) their density (size / span) is orders of magnitude higher
        // than any real sstable's and they land on high, otherwise empty levels where no overlap
        // set of size >= 2 can form. Background selection must still consume them eventually.
        Controller controller = Mockito.mock(Controller.class);
        long minimalSizeBytes = 4 << 20;
        when(controller.getScalingParameter(anyInt())).thenReturn(2); // T4: F=4, T=4
        when(controller.getFanout(anyInt())).thenCallRealMethod();
        when(controller.getThreshold(anyInt())).thenCallRealMethod();
        when(controller.getMaxLevelDensity(anyInt(), anyDouble())).thenCallRealMethod();
        when(controller.getSurvivalFactor(anyInt())).thenReturn(1.0);
        when(controller.getNumShards(anyDouble())).thenReturn(1);
        when(controller.getMaxSstablesPerShardFactor()).thenReturn(10.0);
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000);
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(64);
        when(controller.overlapInclusionMethod()).thenReturn(Overlaps.InclusionMethod.TRANSITIVE);
        Random randomMock = Mockito.mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(0);
        when(controller.random()).thenReturn(randomMock);

        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);

        Token min = partitioner.getMinimumToken();
        Token max = partitioner.getMaximumToken();
        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);
        long timestamp = System.currentTimeMillis();

        // Tiny sstables of a few hundred bytes spanning ~1e-11 of the token space, mutually
        // disjoint, scattered across the range, each holding only a couple of partitions.
        final int tinyCount = 16;
        final double tinySpan = 1e-11;
        List<SSTableReader> tiny = new ArrayList<>();
        for (int i = 0; i < tinyCount; i++)
        {
            double position = (i + 1) / (double) (tinyCount + 1);
            DecoratedKey first = new BufferDecoratedKey(partitioner.split(min, max, position), emptyBuffer);
            DecoratedKey last = new BufferDecoratedKey(partitioner.split(min, max, position + tinySpan), emptyBuffer);
            SSTableReader fragment = mockSSTable(0, 256 + 128 * i, timestamp + 100 + i, 0.0, first, last);
            when(fragment.estimatedKeys()).thenReturn(2L);
            tiny.add(fragment);
        }
        strategy.addSSTables(tiny);
        dataTracker.addInitialSSTables(tiny);

        // Simulate normal table activity: each round flushes one wide sstable covering the whole
        // local range, then applies every available background pick, feeding each compaction's
        // output back into the strategy the way a live table would. The tiny sstables must be
        // swept into the hierarchy within a bounded number of rounds.
        DecoratedKey wideFirst = new BufferDecoratedKey(partitioner.split(min, max, 0.001).nextValidToken(), emptyBuffer);
        DecoratedKey wideLast = new BufferDecoratedKey(max, emptyBuffer);
        Set<SSTableReader> compacted = new HashSet<>();
        long ts = timestamp + 1000;
        for (int round = 0; round < 200 && !compacted.containsAll(tiny); round++)
        {
            SSTableReader flushed = mockSSTable(0, minimalSizeBytes, ts++, 0.0, wideFirst, wideLast);
            strategy.addSSTables(ImmutableList.of(flushed));
            dataTracker.addInitialSSTables(ImmutableList.of(flushed));

            for (int picks = 0; picks < 50; picks++)
            {
                UnifiedCompactionStrategy.CompactionPick pick = strategy.getNextCompactionPick(0);
                if (pick == null)
                    break;
                compacted.addAll(pick);
                strategy.removeSSTables(pick);

                long mergedSize = pick.stream().mapToLong(SSTableReader::onDiskLength).sum();
                DecoratedKey mergedFirst = pick.stream().map(SSTableReader::getFirst).min(DecoratedKey::compareTo).get();
                DecoratedKey mergedLast = pick.stream().map(SSTableReader::getLast).max(DecoratedKey::compareTo).get();
                SSTableReader merged = mockSSTable(0, mergedSize, ts++, 0.0, mergedFirst, mergedLast);
                strategy.addSSTables(ImmutableList.of(merged));
                dataTracker.addInitialSSTables(ImmutableList.of(merged));
            }
        }

        List<SSTableReader> missed = new ArrayList<>(tiny);
        missed.removeAll(compacted);
        assertTrue("Background selection never consumed " + missed.size() + " of " + tinyCount +
                   " tiny near-point-coverage sstables: " + missed,
                   missed.isEmpty());
    }

    SSTableReader mockSSTable(int level, long bytesOnDisk, long timestamp, double hotness, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(level, bytesOnDisk, timestamp, hotness, first, last, 0);
    }

    SSTableReader mockSSTable(long bytesOnDisk, long timestamp, DecoratedKey first, DecoratedKey last)
    {
        return mockSSTable(0, bytesOnDisk, timestamp, 0, first, last, 0);
    }

    SSTableReader mockSSTable(int level,
                              long bytesOnDisk,
                              long timestamp,
                              double hotness,
                              DecoratedKey first,
                              DecoratedKey last,
                              int ttl)
    {
        // We create a ton of mock SSTables that mockito is going to keep until the end of the test suite without stubOnly.
        // Mockito keeps them alive to preserve the history of invocations which is not available for stubs. If we ever
        // need history of invocations and remove stubOnly, we should also manually reset mocked SSTables in tearDown.
        SSTableReader ret = Mockito.mock(SSTableReader.class, withSettings().stubOnly()
                                                                            .defaultAnswer(RETURNS_SMART_NULLS));

        when(ret.getSSTableLevel()).thenReturn(level);
        when(ret.onDiskLength()).thenReturn(bytesOnDisk);
        when(ret.uncompressedLength()).thenReturn(bytesOnDisk); // let's assume no compression
        when(ret.getMaxTimestamp()).thenReturn(timestamp);
        when(ret.getMinTimestamp()).thenReturn(timestamp);
        when(ret.getFirst()).thenReturn(first);
        when(ret.getLast()).thenReturn(last);
        when(ret.getInterval()).thenReturn(new Interval<>(first, last, ret));
        when(ret.getEstimatedPartitionSize()).thenReturn(null); // partitionCount falls back to estimatedKeys
        when(ret.estimatedKeys()).thenReturn(ShardManager.PER_PARTITION_SPAN_THRESHOLD * 2);
        when(ret.instanceId()).thenReturn(TimeUUID.Generator.nextTimeUUID(UNIQUE_IDENTIFIER_FACTORY));
        when(ret.compareTo(any())).thenCallRealMethod();
        when(ret.isMarkedSuspect()).thenReturn(false);
        when(ret.isRepaired()).thenReturn(false);
        when(ret.getRepairedAt()).thenReturn(repairedAt);
        when(ret.getPendingRepair()).thenReturn(null);
        when(ret.isPendingRepair()).thenReturn(false);
        when(ret.getColumnFamilyName()).thenReturn(table);
        when(ret.toString()).thenReturn(String.format("Bytes on disk: %s, level %d, hotness %f, timestamp %d, first %s, last %s",
                                                      FBUtilities.prettyPrintMemory(bytesOnDisk), level, hotness, timestamp, first, last));
        long deletionTime;
        if (ttl > 0)
            deletionTime = TimeUnit.MILLISECONDS.toSeconds(timestamp) + ttl;
        else
            deletionTime = Long.MAX_VALUE;

        when(ret.getMinLocalDeletionTime()).thenReturn(deletionTime);
        when(ret.getMaxLocalDeletionTime()).thenReturn(deletionTime);
        when(ret.getMinTTL()).thenReturn(ttl);
        when(ret.getMaxTTL()).thenReturn(ttl);

        return ret;
    }

    List<SSTableReader> mockSSTables(int numSSTables, long bytesOnDisk, double hotness, long timestamp)
    {
        DecoratedKey first = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMinimumToken(), ByteBuffer.allocate(0));

        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < numSSTables; i++)
        {
            long b = (long)(bytesOnDisk * 0.95 + bytesOnDisk * 0.05 * random.nextDouble()); // leave 5% variability
            double h = hotness * 0.95 + hotness * 0.05 * random.nextDouble(); // leave 5% variability
            sstables.add(mockSSTable(0, b, timestamp, h, first, last, 0));
        }

        return sstables;
    }

    List<SSTableReader> mockNonOverlappingSSTables(int numSSTables, int level, long bytesOnDisk)
    {
        if (!partitioner.splitter().isPresent())
            throw new IllegalStateException(String.format("Cannot split ranges with current partitioner %s", partitioner));

        ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

        long timestamp = System.currentTimeMillis();
        List<SSTableReader> sstables = new ArrayList<>(numSSTables);
        for (int i = 0; i < numSSTables; i++)
        {
            DecoratedKey first = new BufferDecoratedKey(boundary(numSSTables, i).nextValidToken(), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundary(numSSTables, i+1), emptyBuffer);
            sstables.add(mockSSTable(level, bytesOnDisk, timestamp, 0., first, last));

            timestamp+=10;
        }

        return sstables;
    }

    private Token boundary(int numSSTables, int i)
    {
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumToken(), i * 1.0 / numSSTables);
    }
}
