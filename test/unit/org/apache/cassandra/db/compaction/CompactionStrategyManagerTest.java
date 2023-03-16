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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.AbstractStrategyHolder.GroupedSSTableContainer;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionStrategyManagerTest
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManagerTest.class);

    private static final String KS_PREFIX = "Keyspace1";
    private static final String TABLE_PREFIX = "CF_STANDARD";

    private static IPartitioner originalPartitioner;
    private static boolean backups;

    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServer();
        backups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        /**
         * We use byte ordered partitioner in this test to be able to easily infer an SSTable
         * disk assignment based on its generation - See {@link this#getSSTableIndex(Integer[], SSTableReader)}
         */
        originalPartitioner = StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
        SchemaLoader.createKeyspace(KS_PREFIX,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KS_PREFIX, TABLE_PREFIX)
                                                .compaction(CompactionParams.stcs(Collections.emptyMap())));
    }

    @Before
    public void setUp() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        cfs.truncateBlocking();
    }

    @AfterClass
    public static void afterClass()
    {
        DatabaseDescriptor.setPartitionerUnsafe(originalPartitioner);
        DatabaseDescriptor.setIncrementalBackupsEnabled(backups);
    }

    @Test
    public void testSSTablesAssignedToCorrectCompactionStrategy() throws IOException
    {
        // Creates 100 SSTables with keys 0-99
        int numSSTables = 100;
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        cfs.disableAutoCompaction();
        Set<SSTableReader> previousSSTables = cfs.getLiveSSTables();
        for (int i = 0; i < numSSTables; i++)
        {
            createSSTableWithKey(KS_PREFIX, TABLE_PREFIX, i);
            Set<SSTableReader> currentSSTables = cfs.getLiveSSTables();
            Set<SSTableReader> newSSTables = Sets.difference(currentSSTables, previousSSTables);
            assertEquals(1, newSSTables.size());
            if (i % 3 == 0)
            {
                //make 1 third of sstables repaired
                cfs.getCompactionStrategyManager().mutateRepaired(newSSTables, System.currentTimeMillis(), null, false);
            }
            else if (i % 3 == 1)
            {
                //make 1 third of sstables pending repair
                cfs.getCompactionStrategyManager().mutateRepaired(newSSTables, 0, nextTimeUUID(), false);
            }
            previousSSTables = currentSSTables;
        }

        // Creates a CompactionStrategymanager with different numbers of disks and check
        // if the SSTables are assigned to the correct compaction strategies
        for (int numDisks = 2; numDisks < 10; numDisks++)
        {
            testSSTablesAssignedToCorrectCompactionStrategy(numSSTables, numDisks);
        }
    }

    public void testSSTablesAssignedToCorrectCompactionStrategy(int numSSTables, int numDisks)
    {
        // Create a mock CFS with the given number of disks
        MockCFS cfs = createJBODMockCFS(numDisks);
        //Check that CFS will contain numSSTables
        assertEquals(numSSTables, cfs.getLiveSSTables().size());

        // Creates a compaction strategy manager with an external boundary supplier
        final Integer[] boundaries = computeBoundaries(numSSTables, numDisks);

        MockBoundaryManager mockBoundaryManager = new MockBoundaryManager(cfs, boundaries);
        logger.debug("Boundaries for {} disks is {}", numDisks, Arrays.toString(boundaries));
        CompactionStrategyManager csm = new CompactionStrategyManager(cfs, mockBoundaryManager::getBoundaries,
                                                                      true);

        // Check that SSTables are assigned to the correct Compaction Strategy
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            verifySSTableIsAssignedToCorrectStrategy(boundaries, csm, reader);
        }

        for (int delta = 1; delta <= 3; delta++)
        {
            // Update disk boundaries
            Integer[] previousBoundaries = Arrays.copyOf(boundaries, boundaries.length);
            updateBoundaries(mockBoundaryManager, boundaries, delta);

            // Check that SSTables are still assigned to the previous boundary layout
            logger.debug("Old boundaries: {} New boundaries: {}", Arrays.toString(previousBoundaries), Arrays.toString(boundaries));
            for (SSTableReader reader : cfs.getLiveSSTables())
            {
                verifySSTableIsAssignedToCorrectStrategy(previousBoundaries, csm, reader);
            }

            // Reload CompactionStrategyManager so new disk boundaries will be loaded
            csm.maybeReloadDiskBoundaries();

            for (SSTableReader reader : cfs.getLiveSSTables())
            {
                // Check that SSTables are assigned to the new boundary layout
                verifySSTableIsAssignedToCorrectStrategy(boundaries, csm, reader);

                // Remove SSTable and check that it will be removed from the correct compaction strategy
                csm.handleNotification(new SSTableDeletingNotification(reader), this);
                assertFalse(((SizeTieredCompactionStrategy)csm.compactionStrategyFor(reader)).sstables.contains(reader));

                // Add SSTable again and check that is correctly assigned
                csm.handleNotification(new SSTableAddedNotification(Collections.singleton(reader), null), this);
                verifySSTableIsAssignedToCorrectStrategy(boundaries, csm, reader);
            }
        }
    }

    @Test
    public void testAutomaticUpgradeConcurrency() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);

        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger(0);
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);
        CompactionStrategyManager mgr = mock.getCompactionStrategyManager();
        // basic idea is that we start a thread which will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade tasks which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t.start();
        Thread.sleep(100); // let the thread start and grab the task
        assertEquals(1, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        latch.countDown();
        t.join();
        assertEquals(1, upgradeTaskCount.get()); // we should only call findUpgradeSSTableTask once when concurrency = 1
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    @Test
    public void testAutomaticUpgradeConcurrency2() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(2);
        // latch to block CompactionManager.BackgroundCompactionCandidate#maybeRunUpgradeTask
        // inside the currentlyBackgroundUpgrading check - with max_concurrent_auto_upgrade_tasks = 1 this will make
        // sure that BackgroundCompactionCandidate#maybeRunUpgradeTask returns false until the latch has been counted down
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger upgradeTaskCount = new AtomicInteger();
        MockCFSForCSM mock = new MockCFSForCSM(cfs, latch, upgradeTaskCount);

        CompactionManager.BackgroundCompactionCandidate r = CompactionManager.instance.getBackgroundCompactionCandidate(mock);
        CompactionStrategyManager mgr = mock.getCompactionStrategyManager();

        // basic idea is that we start 2 threads who will be able to get in to the currentlyBackgroundUpgrading-guarded
        // code in CompactionManager, then we try to run a bunch more of the upgrade task which should return false
        // due to the currentlyBackgroundUpgrading count being >= max_concurrent_auto_upgrade_tasks
        Thread t = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t.start();
        Thread t2 = new Thread(() -> r.maybeRunUpgradeTask(mgr));
        t2.start();
        Thread.sleep(100); // let the threads start and grab the task
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertFalse(r.maybeRunUpgradeTask(mgr));
        assertEquals(2, CompactionManager.instance.currentlyBackgroundUpgrading.get());
        latch.countDown();
        t.join();
        t2.join();
        assertEquals(2, upgradeTaskCount.get());
        assertEquals(0, CompactionManager.instance.currentlyBackgroundUpgrading.get());

        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(1);
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    private static void assertHolderExclusivity(boolean isRepaired, boolean isPendingRepair, boolean isTransient, Class<? extends AbstractStrategyHolder> expectedType)
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        CompactionStrategyManager csm = cfs.getCompactionStrategyManager();

        AbstractStrategyHolder holder = csm.getHolder(isRepaired, isPendingRepair, isTransient);
        assertNotNull(holder);
        assertSame(expectedType, holder.getClass());

        int matches = 0;
        for (AbstractStrategyHolder other : csm.getHolders())
        {
            if (other.managesRepairedGroup(isRepaired, isPendingRepair, isTransient))
            {
                assertSame("holder assignment should be mutually exclusive", holder, other);
                matches++;
            }
        }
        assertEquals(1, matches);
    }

    private static void assertInvalieHolderConfig(boolean isRepaired, boolean isPendingRepair, boolean isTransient)
    {
        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        CompactionStrategyManager csm = cfs.getCompactionStrategyManager();
        try
        {
            csm.getHolder(isRepaired, isPendingRepair, isTransient);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    /**
     * If an sstable can be be assigned to a strategy holder, it shouldn't be possibly to
     * assign it to any of the other holders.
     */
    @Test
    public void testMutualExclusiveHolderClassification() throws Exception
    {
        assertHolderExclusivity(false, false, false, CompactionStrategyHolder.class);
        assertHolderExclusivity(true, false, false, CompactionStrategyHolder.class);
        assertHolderExclusivity(false, true, false, PendingRepairHolder.class);
        assertHolderExclusivity(false, true, true, PendingRepairHolder.class);
        assertInvalieHolderConfig(true, true, false);
        assertInvalieHolderConfig(true, true, true);
        assertInvalieHolderConfig(false, false, true);
        assertInvalieHolderConfig(true, false, true);
    }

    PartitionPosition forKey(int key)
    {
        DecoratedKey dk = Util.dk(String.format("%04d", key));
        return dk.getToken().minKeyBound();
    }

    /**
     * Test that csm.groupSSTables correctly groups sstables by repaired status and directory
     */
    @Test
    public void groupSSTables() throws Exception
    {
        final int numDir = 4;
        ColumnFamilyStore cfs = createJBODMockCFS(numDir);
        Keyspace.open(cfs.getKeyspaceName()).getColumnFamilyStore(cfs.name).disableAutoCompaction();
        assertTrue(cfs.getLiveSSTables().isEmpty());
        List<SSTableReader> transientRepairs = new ArrayList<>();
        List<SSTableReader> pendingRepair = new ArrayList<>();
        List<SSTableReader> unrepaired = new ArrayList<>();
        List<SSTableReader> repaired = new ArrayList<>();

        for (int i = 0; i < numDir; i++)
        {
            int key = 100 * i;
            transientRepairs.add(createSSTableWithKey(cfs.getKeyspaceName(), cfs.name, key++));
            pendingRepair.add(createSSTableWithKey(cfs.getKeyspaceName(), cfs.name, key++));
            unrepaired.add(createSSTableWithKey(cfs.getKeyspaceName(), cfs.name, key++));
            repaired.add(createSSTableWithKey(cfs.getKeyspaceName(), cfs.name, key++));
        }

        cfs.getCompactionStrategyManager().mutateRepaired(transientRepairs, 0, nextTimeUUID(), true);
        cfs.getCompactionStrategyManager().mutateRepaired(pendingRepair, 0, nextTimeUUID(), false);
        cfs.getCompactionStrategyManager().mutateRepaired(repaired, 1000, null, false);

        DiskBoundaries boundaries = new DiskBoundaries(cfs, cfs.getDirectories().getWriteableLocations(),
                                                       Lists.newArrayList(forKey(100), forKey(200), forKey(300)),
                                                       10, 10);

        CompactionStrategyManager csm = new CompactionStrategyManager(cfs, () -> boundaries, true);

        List<GroupedSSTableContainer> grouped = csm.groupSSTables(Iterables.concat( transientRepairs, pendingRepair, repaired, unrepaired));

        for (int x=0; x<grouped.size(); x++)
        {
            GroupedSSTableContainer group = grouped.get(x);
            AbstractStrategyHolder holder = csm.getHolders().get(x);
            for (int y=0; y<numDir; y++)
            {
                SSTableReader sstable = Iterables.getOnlyElement(group.getGroup(y));
                assertTrue(holder.managesSSTable(sstable));
                SSTableReader expected;
                if (sstable.isRepaired())
                    expected = repaired.get(y);
                else if (sstable.isPendingRepair())
                {
                    if (sstable.isTransient())
                    {
                        expected = transientRepairs.get(y);
                    }
                    else
                    {
                        expected = pendingRepair.get(y);
                    }
                }
                else
                    expected = unrepaired.get(y);

                assertSame(expected, sstable);
            }
        }
    }

    @Test
    public void testCountsByBuckets()
    {
        Assert.assertArrayEquals(
            new int[] {2, 2, 4},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1),
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1)), CompactionStrategyManager.TWCS_BUCKET_COUNT_MAX));
        Assert.assertArrayEquals(
            new int[] {1, 1, 3},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                ImmutableMap.of(60000L, 1, 0L, 1),
                ImmutableMap.of(0L, 2, 180000L, 1)), CompactionStrategyManager.TWCS_BUCKET_COUNT_MAX));
        Assert.assertArrayEquals(
            new int[] {1, 1},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                ImmutableMap.of(60000L, 1, 0L, 1),
                ImmutableMap.of()), CompactionStrategyManager.TWCS_BUCKET_COUNT_MAX));
        Assert.assertArrayEquals(
            new int[] {8, 4},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                ImmutableMap.of(60000L, 2, 0L, 1, 180000L, 4),
                ImmutableMap.of(60000L, 2, 0L, 1, 180000L, 4)), 2));
        Assert.assertArrayEquals(
            new int[] {1, 1, 2},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                Collections.emptyMap(),
                ImmutableMap.of(60000L, 1, 0L, 2, 180000L, 1)), CompactionStrategyManager.TWCS_BUCKET_COUNT_MAX));
        Assert.assertArrayEquals(
            new int[] {},
            CompactionStrategyManager.sumCountsByBucket(ImmutableList.of(
                Collections.emptyMap(),
                Collections.emptyMap()), CompactionStrategyManager.TWCS_BUCKET_COUNT_MAX));
    }

    private MockCFS createJBODMockCFS(int disks)
    {
        // Create #disks data directories to simulate JBOD
        Directories.DataDirectory[] directories = new Directories.DataDirectory[disks];
        for (int i = 0; i < disks; ++i)
        {
            File tempDir = new File(Files.createTempDir());
            tempDir.deleteOnExit();
            directories[i] = new Directories.DataDirectory(tempDir);
        }

        ColumnFamilyStore cfs = Keyspace.open(KS_PREFIX).getColumnFamilyStore(TABLE_PREFIX);
        MockCFS mockCFS = new MockCFS(cfs, new Directories(cfs.metadata(), directories));
        mockCFS.disableAutoCompaction();
        mockCFS.addSSTables(cfs.getLiveSSTables());
        return mockCFS;
    }

    /**
     * Updates the boundaries with a delta
     */
    private void updateBoundaries(MockBoundaryManager boundaryManager, Integer[] boundaries, int delta)
    {
        for (int j = 0; j < boundaries.length - 1; j++)
        {
            if ((j + delta) % 2 == 0)
                boundaries[j] -= delta;
            else
                boundaries[j] += delta;
        }
        boundaryManager.invalidateBoundaries();
    }

    private void verifySSTableIsAssignedToCorrectStrategy(Integer[] boundaries, CompactionStrategyManager csm, SSTableReader reader)
    {
        // Check that sstable is assigned to correct disk
        int index = getSSTableIndex(boundaries, reader);
        assertEquals(index, csm.compactionStrategyIndexFor(reader));
        // Check that compaction strategy actually contains SSTable
        assertTrue(((SizeTieredCompactionStrategy)csm.compactionStrategyFor(reader)).sstables.contains(reader));
    }

    /**
     * Creates disk boundaries such that each disk receives
     * an equal amount of SSTables
     */
    private Integer[] computeBoundaries(int numSSTables, int numDisks)
    {
        Integer[] result = new Integer[numDisks];
        int sstablesPerRange = numSSTables / numDisks;
        result[0] = sstablesPerRange;
        for (int i = 1; i < numDisks; i++)
        {
            result[i] = result[i - 1] + sstablesPerRange;
        }
        result[numDisks - 1] = numSSTables; // make last boundary alwyays be the number of SSTables to prevent rounding errors
        return result;
    }

    private int getSSTableIndex(Integer[] boundaries, SSTableReader reader)
    {
        int index = 0;
        int firstKey = Integer.parseInt(new String(ByteBufferUtil.getArray(reader.getFirst().getKey())));
        while (boundaries[index] <= firstKey)
            index++;
        logger.debug("Index for SSTable {} on boundary {} is {}", reader.descriptor.id, Arrays.toString(boundaries), index);
        return index;
    }

    class MockBoundaryManager
    {
        private final ColumnFamilyStore cfs;
        private Integer[] positions;
        private DiskBoundaries boundaries;

        public MockBoundaryManager(ColumnFamilyStore cfs, Integer[] positions)
        {
            this.cfs = cfs;
            this.positions = positions;
            this.boundaries = createDiskBoundaries(cfs, positions);
        }

        public void invalidateBoundaries()
        {
            boundaries.invalidate();
        }

        public DiskBoundaries getBoundaries()
        {
            if (boundaries.isOutOfDate())
                boundaries = createDiskBoundaries(cfs, positions);
            return boundaries;
        }

        private DiskBoundaries createDiskBoundaries(ColumnFamilyStore cfs, Integer[] boundaries)
        {
            List<PartitionPosition> positions = Arrays.stream(boundaries).map(b -> Util.token(String.format(String.format("%04d", b))).minKeyBound()).collect(Collectors.toList());
            return new DiskBoundaries(cfs, cfs.getDirectories().getWriteableLocations(), positions, 0, 0);
        }
    }

    private static SSTableReader createSSTableWithKey(String keyspace, String table, int key)
    {
        long timestamp = System.currentTimeMillis();
        DecoratedKey dk = Util.dk(String.format("%04d", key));
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        new RowUpdateBuilder(cfs.metadata(), timestamp, dk.getKey())
        .clustering(Integer.toString(key))
        .add("val", "val")
        .build()
        .applyUnsafe();
        Set<SSTableReader> before = cfs.getLiveSSTables();
        Util.flush(cfs);
        Set<SSTableReader> after = cfs.getLiveSSTables();
        return Iterables.getOnlyElement(Sets.difference(after, before));
    }

    // just to be able to override the data directories
    private static class MockCFS extends ColumnFamilyStore
    {
        MockCFS(ColumnFamilyStore cfs, Directories dirs)
        {
            super(cfs.keyspace, cfs.getTableName(), Util.newSeqGen(), cfs.metadata, dirs, false, false, true);
        }
    }

    private static class MockCFSForCSM extends ColumnFamilyStore
    {
        private final CountDownLatch latch;
        private final AtomicInteger upgradeTaskCount;

        private MockCFSForCSM(ColumnFamilyStore cfs, CountDownLatch latch, AtomicInteger upgradeTaskCount)
        {
            super(cfs.keyspace, cfs.name, Util.newSeqGen(10), cfs.metadata, cfs.getDirectories(), true, false, false);
            this.latch = latch;
            this.upgradeTaskCount = upgradeTaskCount;
        }
        @Override
        public CompactionStrategyManager getCompactionStrategyManager()
        {
            return new MockCSM(this, latch, upgradeTaskCount);
        }
    }

    private static class MockCSM extends CompactionStrategyManager
    {
        private final CountDownLatch latch;
        private final AtomicInteger upgradeTaskCount;

        private MockCSM(ColumnFamilyStore cfs, CountDownLatch latch, AtomicInteger upgradeTaskCount)
        {
            super(cfs);
            this.latch = latch;
            this.upgradeTaskCount = upgradeTaskCount;
        }

        @Override
        public AbstractCompactionTask findUpgradeSSTableTask()
        {
            try
            {
                latch.await();
                upgradeTaskCount.incrementAndGet();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            return null;
        }
    }
}
