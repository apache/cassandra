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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.collect.ImmutableList;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.PartialLifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.apache.cassandra.io.sstable.format.SSTableReader.UNIQUE_IDENTIFIER_FACTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        ServerTestUtils.prepareServerNoRegister();
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
        localRanges = cfs.fullWeightedRange(ClusterMetadata.current().epoch, partitioner);

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

        long gcBefore = FBUtilities.nowInSeconds();
        assertNull(Iterables.getOnlyElement(strategy.getNextBackgroundTasks(gcBefore), null));
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
        DecoratedKey last = new BufferDecoratedKey(partitioner.getMaximumTokenForSplitting(), ByteBuffer.allocate(0));

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
            UnifiedCompactionStrategy.SelectionContext context = new UnifiedCompactionStrategy.SelectionContext(strategy.getController());
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

        AbstractCompactionTask task = Iterables.getOnlyElement(strategy.getNextBackgroundTasks(0), null);
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
        Token max = partitioner.getMaximumTokenForSplitting();
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
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.maxSSTablesToCompact()).thenReturn(1000);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
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
        when(controller.getBaseSstableSize(anyInt())).thenReturn((double) minimalSizeBytes);
        when(controller.maxConcurrentCompactions()).thenReturn(1000); // let it generate as many candidates as it can
        when(controller.maxThroughput()).thenReturn(Double.MAX_VALUE);
        when(controller.getIgnoreOverlapsInExpirationCheck()).thenReturn(false);
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
    public void testCreateParallelTasks()
    {
        testCreateParallelTasks(8, arr(1, 2, 4));
        testCreateParallelTasks(4, arr(1, 2, 4));
        testCreateParallelTasks(2, arr(1, 2, 4));
        testCreateParallelTasks(5, arr(1, 2, 4));
        testCreateParallelTasks(5, arr(2, 4, 8));
        testCreateParallelTasks(3, arr(1, 3, 5));
        testCreateParallelTasks(3, arr(3, 3, 3));

        testCreateParallelTasks(1, arr(1, 2, 3));
    }

    @Test
    public void testCreateParallelTasksMissingParts()
    {
        // Drop some sstables without losing ranges
        testCreateParallelTasks(8, arr(2, 4, 8),
                                arr(1));

        testCreateParallelTasks(8, arr(2, 4, 8),
                                arr(1), arr(0), arr(2, 7));

        testCreateParallelTasks(5, arr(2, 4, 8),
                                arr(1), arr(0), arr(2, 7));
    }

    @Test
    public void testCreateParallelTasksOneRange()
    {
        // Drop second half
        testCreateParallelTasks(2, arr(2, 4, 8),
                                arr(1), arr(2, 3), arr(4, 5, 6, 7));
        // Drop all except center, within shard
        testCreateParallelTasks(3, arr(5, 7, 9),
                                arr(0, 1, 3, 4), arr(0, 1, 2, 4, 5, 6), arr(0, 1, 2, 6, 7, 8));
    }

    @Test
    public void testCreateParallelTasksSkippedRange()
    {
        // Drop all sstables containing the 4/8-5/8 range.
        testCreateParallelTasks(8, arr(2, 4, 8),
                                arr(1), arr(2), arr(4));
        // Drop all sstables containing the 4/8-6/8 range.
        testCreateParallelTasks(8, arr(2, 4, 8),
                                arr(1), arr(2), arr(4, 5));
        // Drop all sstables containing the 4/8-8/8 range.
        testCreateParallelTasks(8, arr(2, 4, 8),
                                arr(1), arr(2, 3), arr(4, 5, 6, 7));

        // Drop all sstables containing the 0/8-2/8 range.
        testCreateParallelTasks(5, arr(2, 4, 8),
                                arr(0), arr(0), arr(0, 1));
        // Drop all sstables containing the 6/8-8/8 range.
        testCreateParallelTasks(5, arr(2, 4, 8),
                                arr(1), arr(3), arr(6, 7));
        // Drop sstables on both ends.
        testCreateParallelTasks(5, arr(3, 4, 8),
                                arr(0, 2), arr(0, 3), arr(0, 1, 6, 7));
    }

    public void testCreateParallelTasks(int numShards, int[] perLevelCounts, int[]... dropsPerLevel)
    {
        // Note: This test has a counterpart in ShardManagerTest that exercises splitSSTablesInShards directly and more thoroughly.
        // This one ensures the data is correctly passed to and presented in compaction tasks.
        Set<SSTableReader> allSSTables = new HashSet<>();
        int levelNum = 0;
        for (int perLevelCount : perLevelCounts)
        {
            List<SSTableReader> ssTables = mockNonOverlappingSSTables(perLevelCount, levelNum, 100 << (20 + levelNum));
            if (levelNum < dropsPerLevel.length)
            {
                for (int i = dropsPerLevel[levelNum].length - 1; i >= 0; i--)
                    ssTables.remove(dropsPerLevel[levelNum][i]);
            }
            allSSTables.addAll(ssTables);
            ++levelNum;
        }
        dataTracker.addInitialSSTables(allSSTables);

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.parallelizeOutputShards()).thenReturn(true);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();
        LifecycleTransaction txn = dataTracker.tryModify(allSSTables, OperationType.COMPACTION);
        var tasks = strategy.createCompactionTasks(0, txn);
        int i = 0;
        int[] expectedSSTablesInTasks = new int[tasks.size()];
        int[] collectedSSTablesPerTask = new int[tasks.size()];
        for (AbstractCompactionTask act : tasks)
        {
            CompactionTask t = (CompactionTask) act;
            assertTrue(t instanceof UnifiedCompactionTask);
            assertFalse(t.inputSSTables().isEmpty());
            collectedSSTablesPerTask[i] = t.inputSSTables().size();
            expectedSSTablesInTasks[i] = (int) allSSTables.stream().filter(x -> intersects(x, t.tokenRange())).count();
            t.rejected(); // close transaction
            ++i;
        }
        if (tasks.size() == 1)
            assertNull(((CompactionTask) tasks.get(0)).tokenRange()); // make sure single-task compactions are not ranged
        Assert.assertEquals(Arrays.toString(expectedSSTablesInTasks), Arrays.toString(collectedSSTablesPerTask));
        System.out.println(Arrays.toString(expectedSSTablesInTasks));
        assertThat(tasks.size()).isLessThanOrEqualTo(numShards);
        assertEquals(allSSTables, tasks.stream().flatMap(t -> ((CompactionTask) t).inputSSTables().stream()).collect(Collectors.toSet()));
        for (var t : tasks)
            for (var q : tasks)
                if (t != q)
                    assertFalse("Subranges " + ((CompactionTask)t).tokenRange() + " and " + ((CompactionTask)q).tokenRange() + "intersect", ((CompactionTask)t).tokenRange().intersects(((CompactionTask)q).tokenRange()));

        // make sure the composite transaction has the correct number of tasks
        assertEquals(Transactional.AbstractTransactional.State.ABORTED, txn.state());
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

    @Test
    public void testDontCreateParallelTasks()
    {
        int numShards = 5;
        Set<SSTableReader> allSSTables = new HashSet<>();
        allSSTables.addAll(mockNonOverlappingSSTables(10, 0, 100 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(15, 1, 200 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(25, 2, 400 << 20));
        dataTracker.addInitialSSTables(allSSTables);
        Controller controller = Mockito.mock(Controller.class);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.parallelizeOutputShards()).thenReturn(false);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.startup();
        LifecycleTransaction txn = dataTracker.tryModify(allSSTables, OperationType.COMPACTION);
        var tasks = strategy.createCompactionTasks(0, txn);
        assertEquals(1, tasks.size());
        assertEquals(allSSTables, ((CompactionTask) tasks.get(0)).inputSSTables());
    }
    @Test
    public void testMaximalSelection()
    {
        // shared transaction, all tasks refer to the same input sstables
        testMaximalSelection(1, 1, 0, false, 12 + 18 + 30, ((12 * 100L + 18 * 200 + 30 * 400) << 20));
        testMaximalSelection(5, 5, 0, true, 12 + 18 + 30, ((12 * 100L + 18 * 200 + 30 * 400) << 20));
        // when there's a common split point of existing and new sharding (i.e. gcd(num_shards,12,18,30) > 1), it should be used
        testMaximalSelection(3, 3, 0, false, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(9, 3, 0, false, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(9, 9, 0, true, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(2, 2, 0, false, 6 + 9 + 15, ((6 * 100L + 9 * 200 + 15 * 400) << 20));
        testMaximalSelection(4, 2, 0, false, 6 + 9 + 15, ((6 * 100L + 9 * 200 + 15 * 400) << 20));
        testMaximalSelection(4, 4, 0, true, 6 + 9 + 15, ((6 * 100L + 9 * 200 + 15 * 400) << 20));
        testMaximalSelection(18, 6, 0, false, 2 + 3 + 5, ((2 * 100L + 3 * 200 + 5 * 400) << 20));
        testMaximalSelection(18, 18, 0, true, 2 + 3 + 5, ((2 * 100L + 3 * 200 + 5 * 400) << 20));
    }

    @Test
    public void testMaximalSelectionWithLimit()
    {
        // shared transaction, all tasks refer to the same input sstables
        testMaximalSelection(5, 5, 2, true, 12 + 18 + 30, ((12 * 100L + 18 * 200 + 30 * 400) << 20));
        // when there's a common split point of existing and new sharding (i.e. gcd(num_shards,12,18,30) > 1), it should be used
        testMaximalSelection(3, 3, 2, false, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(9, 3, 1, false, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(9, 9, 3, true, 4 + 6 + 10, ((4 * 100L + 6 * 200 + 10 * 400) << 20));
        testMaximalSelection(18, 6, 4, false, 2 + 3 + 5, ((2 * 100L + 3 * 200 + 5 * 400) << 20));
        testMaximalSelection(18, 18, 5, true, 2 + 3 + 5, ((2 * 100L + 3 * 200 + 5 * 400) << 20));
    }

    private void testMaximalSelection(int numShards, int expectedTaskCount, int parallelismLimit, boolean parallelize, int originalsCount, long onDiskLength)
    {
        if (parallelismLimit == 0)
            parallelismLimit = Integer.MAX_VALUE;
        Set<SSTableReader> allSSTables = new HashSet<>();
        allSSTables.addAll(mockNonOverlappingSSTables(12, 0, 100 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(18, 1, 200 << 20));
        allSSTables.addAll(mockNonOverlappingSSTables(30, 2, 400 << 20));
        dataTracker.addInitialSSTables(allSSTables);

        Controller controller = Mockito.mock(Controller.class);
        when(controller.getNumShards(anyDouble())).thenReturn(numShards);
        when(controller.parallelizeOutputShards()).thenReturn(parallelize);
        when(controller.maxConcurrentCompactions()).thenReturn(1000);
        UnifiedCompactionStrategy strategy = new UnifiedCompactionStrategy(cfs, new HashMap<>(), controller);
        strategy.addSSTables(allSSTables);

        List<AbstractCompactionTask> allTasks = strategy.getMaximalTasks(0, false);
        List<AbstractCompactionTask> limitedParallelismTasks = CompositeCompactionTask.applyParallelismLimit(allTasks, parallelismLimit);
        assertEquals(expectedTaskCount, allTasks.size());
        for (AbstractCompactionTask task : allTasks)
        {
            Set<SSTableReader> compacting = task.transaction.originals();
            assertEquals(originalsCount, compacting.size()); // count / gcd sstables of each level
            assertEquals(onDiskLength, compacting.stream().mapToLong(SSTableReader::onDiskLength).sum());

            if (!(task.transaction instanceof PartialLifecycleTransaction))
            {
                // None of the selected sstables may intersect any in any other set.
                for (AbstractCompactionTask task2 : allTasks)
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

        if (parallelismLimit > 0)
            assertTrue(limitedParallelismTasks.size() <= parallelismLimit);
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
        when(controller.getNumShards(anyDouble())).thenReturn(1);
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
            DecoratedKey first = new BufferDecoratedKey(boundary(numSSTables, i + 0.0001), emptyBuffer);
            DecoratedKey last =  new BufferDecoratedKey(boundary(numSSTables, i + 0.9999), emptyBuffer);
            sstables.add(mockSSTable(level, bytesOnDisk, timestamp, 0., first, last));

            timestamp+=10;
        }

        return sstables;
    }

    private Token boundary(int numSSTables, double i)
    {
        return partitioner.split(partitioner.getMinimumToken(), partitioner.getMaximumTokenForSplitting(), i / numSSTables);
    }

}
