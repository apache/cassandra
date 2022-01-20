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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.MockSchema;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LeveledCompactionStrategyTest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategyTest.class);

    private static final String KEYSPACE1 = "LeveledCompactionStrategyTest";
    private static final String CF_STANDARDDLEVELED = "StandardLeveled";
    private Keyspace keyspace;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1"))));
        }

    @Before
    public void enableCompaction()
    {
        keyspace = Keyspace.open(KEYSPACE1);
        cfs = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED);
        cfs.enableAutoCompaction();
    }

    /**
     * Since we use StandardLeveled CF for every test, we want to clean up after the test.
     */
    @After
    public void truncateSTandardLeveled()
    {
        cfs.truncateBlocking();
    }

    /**
     * Ensure that the grouping operation preserves the levels of grouped tables
     */
    @Test
    public void testGrouperLevels() throws Exception{
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        //Need entropy to prevent compression so size is predictable with compression enabled/disabled
        new Random().nextBytes(value.array());

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        int l1Count = strategy.getSSTableCountPerLevel()[1];
        int l2Count = strategy.getSSTableCountPerLevel()[2];
        if (l1Count == 0 || l2Count == 0)
        {
            logger.error("L1 or L2 has 0 sstables. Expected > 0 on both.");
            logger.error("L1: " + l1Count);
            logger.error("L2: " + l2Count);
            Assert.fail();
        }

        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(cfs.getLiveSSTables());
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            int groupLevel = -1;
            Iterator<SSTableReader> it = sstableGroup.iterator();
            while (it.hasNext())
            {

                SSTableReader sstable = it.next();
                int tableLevel = sstable.getSSTableLevel();
                if (groupLevel == -1)
                    groupLevel = tableLevel;
                assert groupLevel == tableLevel;
            }
        }

    }

    /*
     * This exercises in particular the code of #4142
     */
    @Test
    public void testValidationMultipleSSTablePerLevel() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED).gcBefore(FBUtilities.nowInSeconds());
        UUID parentRepSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepSession, FBUtilities.getBroadcastAddress(), Arrays.asList(cfs), Arrays.asList(range), false, System.currentTimeMillis(), true);
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, UUID.randomUUID(), KEYSPACE1, CF_STANDARDDLEVELED, Arrays.asList(range));
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddress(), gcBefore);
        CompactionManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    public static void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException
    {
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        while (true)
        {
            // since we run several compaction strategies we wait until L0 in all strategies is empty and
            // atleast one L1+ is non-empty. In these tests we always run a single data directory with only unrepaired data
            // so it should be good enough
            boolean allL0Empty = true;
            boolean anyL1NonEmpty = false;
            for (AbstractCompactionStrategy strategy : strategyManager.getStrategies())
            {
                if (!(strategy instanceof LeveledCompactionStrategy))
                    return;
                // note that we check > 1 here, if there is too little data in L0, we don't compact it up to L1
                if (((LeveledCompactionStrategy)strategy).getLevelSize(0) > 1)
                    allL0Empty = false;
                for (int i = 1; i < 5; i++)
                    if (((LeveledCompactionStrategy)strategy).getLevelSize(i) > 0)
                        anyL1NonEmpty = true;
            }
            if (allL0Empty && anyL1NonEmpty)
                return;
            Thread.sleep(100);
        }
    }

    @Test
    public void testCompactionProgress() throws Exception
    {
        // make sure we have SSTables in L1
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 2;
        int columns = 10;
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) (cfs.getCompactionStrategyManager()).getStrategies().get(1);
        assert strategy.getLevelSize(1) > 0;

        // get LeveledScanner for level 1 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(1);
        List<ISSTableScanner> scanners = strategy.getScanners(sstables).scanners;
        assertEquals(1, scanners.size()); // should be one per level
        ISSTableScanner scanner = scanners.get(0);
        // scan through to the end
        while (scanner.hasNext())
            scanner.next();

        // scanner.getCurrentPosition should be equal to total bytes of L1 sstables
        assertEquals(scanner.getCurrentPosition(), SSTableReader.getTotalUncompressedBytes(sstables));
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        cfs.disableAutoCompaction();
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ( cfs.getCompactionStrategyManager()).getStrategies().get(1);
        cfs.forceMajorCompaction();

        for (SSTableReader s : cfs.getLiveSSTables())
        {
            assertTrue(s.getSSTableLevel() != 6 && s.getSSTableLevel() > 0);
            strategy.manifest.remove(s);
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, 6);
            s.reloadSSTableMetadata();
            strategy.manifest.add(s);
        }
        // verify that all sstables in the changed set is level 6
        for (SSTableReader s : cfs.getLiveSSTables())
            assertEquals(6, s.getSSTableLevel());

        int[] levels = strategy.manifest.getAllLevelSize();
        // verify that the manifest has correct amount of sstables
        assertEquals(cfs.getLiveSSTables().size(), levels[6]);
    }

    @Test
    public void testNewRepairedSSTable() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata, String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
            Thread.sleep(100);

        CompactionStrategyManager strategy =  cfs.getCompactionStrategyManager();
        List<AbstractCompactionStrategy> strategies = strategy.getStrategies();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) strategies.get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) strategies.get(1);
        assertEquals(0, repaired.manifest.getLevelCount() );
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.isRepaired());

        int sstableCount = 0;
        for (List<SSTableReader> level : unrepaired.manifest.generations)
            sstableCount += level.size();
        // we only have unrepaired sstables:
        assertEquals(sstableCount, cfs.getLiveSSTables().size());

        SSTableReader sstable1 = unrepaired.manifest.generations[2].get(0);
        SSTableReader sstable2 = unrepaired.manifest.generations[1].get(0);

        sstable1.descriptor.getMetadataSerializer().mutateRepairedAt(sstable1.descriptor, System.currentTimeMillis());
        sstable1.reloadSSTableMetadata();
        assertTrue(sstable1.isRepaired());

        strategy.handleNotification(new SSTableRepairStatusChanged(Arrays.asList(sstable1)), this);

        int repairedSSTableCount = 0;
        for (List<SSTableReader> level : repaired.manifest.generations)
            repairedSSTableCount += level.size();
        assertEquals(1, repairedSSTableCount);
        // make sure the repaired sstable ends up in the same level in the repaired manifest:
        assertTrue(repaired.manifest.generations[2].contains(sstable1));
        // and that it is gone from unrepaired
        assertFalse(unrepaired.manifest.generations[2].contains(sstable1));

        unrepaired.removeSSTable(sstable2);
        strategy.handleNotification(new SSTableAddedNotification(Collections.singleton(sstable2)), this);
        assertTrue(unrepaired.manifest.getLevel(1).contains(sstable2));
        assertFalse(repaired.manifest.getLevel(1).contains(sstable2));
    }

    @Test
    public void testReduceScopeL0L1() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        cfs.setCompactionParameters(localOptions);
        List<SSTableReader> l1sstables = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            SSTableReader l1sstable = MockSchema.sstable(i, 1 * 1024 * 1024, cfs);
            l1sstable.descriptor.getMetadataSerializer().mutateLevel(l1sstable.descriptor, 1);
            l1sstable.reloadSSTableMetadata();
            l1sstables.add(l1sstable);
        }
        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 10; i < 20; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs));

        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, Iterables.concat(l0sstables, l1sstables)))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 1, 0, 1024*1024, false);
            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(0);
                SSTableReader removedSSTable = Sets.difference(before, txn.originals()).stream().findFirst().orElse(null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l1after = sstables.right;

                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    Set<SSTableReader> l1after = sstables.right;
                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertFalse(removed);
        }
    }

    @Test
    public void testReduceScopeL0()
    {

        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 10; i < 20; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs));

        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, l0sstables))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 0, 0, 1024*1024, false);

            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(0);
                SSTableReader removedSSTable = Sets.difference(before, txn.originals()).stream().findFirst().orElse(null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertFalse(removed);
        }
    }

    private Pair<Set<SSTableReader>, Set<SSTableReader>> groupByLevel(Iterable<SSTableReader> sstables)
    {
        Set<SSTableReader> l1after = new HashSet<>();
        Set<SSTableReader> l0after = new HashSet<>();
        for (SSTableReader kept : sstables)
        {
            switch (kept.getSSTableLevel())
            {
                case 0:
                    l0after.add(kept);
                    break;
                case 1:
                    l1after.add(kept);
                    break;
                default:
                    throw new RuntimeException("only l0 & l1 sstables");
            }
        }
        return Pair.create(l0after, l1after);
    }
}
