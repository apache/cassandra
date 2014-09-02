/**
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LeveledCompactionStrategyTest
{
    private static final String KEYSPACE1 = "LeveledCompactionStrategyTest";
    private static final String CF_STANDARDDLEVELED = "StandardLeveled";
    private Keyspace keyspace;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED)
                                                .compactionStrategyClass(LeveledCompactionStrategy.class)
                                                .compactionStrategyOptions(leveledOptions));
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

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(CF_STANDARDDLEVELED, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        // Checking we're not completely bad at math
        assert strategy.getLevelSize(1) > 0;
        assert strategy.getLevelSize(2) > 0;

        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategy().groupSSTablesForAntiCompaction(cfs.getSSTables());
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
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(CF_STANDARDDLEVELED, Util.cellname("column" + c), value, 0);
            }
            rm.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        // Checking we're not completely bad at math
        assert strategy.getLevelSize(1) > 0;
        assert strategy.getLevelSize(2) > 0;

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED).gcBefore(System.currentTimeMillis());
        UUID parentRepSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepSession, Arrays.asList(cfs), Arrays.asList(range));
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, UUID.randomUUID(), KEYSPACE1, CF_STANDARDDLEVELED, range);
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddress(), gcBefore);
        CompactionManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    private void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException
    {
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        // L0 is the lowest priority, so when that's done, we know everything is done
        while (strategy.getLevelSize(0) > 1)
            Thread.sleep(100);
    }

    @Test
    public void testCompactionProgress() throws Exception
    {
        // make sure we have SSTables in L1
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]);
        int rows = 2;
        int columns = 10;
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(CF_STANDARDDLEVELED, Util.cellname("column" + c), value, 0);
            }
            rm.applyUnsafe();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        assert strategy.getLevelSize(1) > 0;

        // get LeveledScanner for level 1 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(1);
        List<ICompactionScanner> scanners = strategy.getScanners(sstables).scanners;
        assertEquals(1, scanners.size()); // should be one per level
        ICompactionScanner scanner = scanners.get(0);
        // scan through to the end
        while (scanner.hasNext())
            scanner.next();

        // scanner.getCurrentPosition should be equal to total bytes of L1 sstables
        assert scanner.getCurrentPosition() == SSTableReader.getTotalBytes(sstables);
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(CF_STANDARDDLEVELED, Util.cellname("column" + c), value, 0);
            }
            rm.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.forceBlockingFlush();
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
            Thread.sleep(100);

        for (SSTableReader s : cfs.getSSTables())
        {
            assertTrue(s.getSSTableLevel() != 6);
            strategy.manifest.remove(s);
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, 6);
            s.reloadSSTableMetadata();
            strategy.manifest.add(s);
        }
        // verify that all sstables in the changed set is level 6
        for (SSTableReader s : cfs.getSSTables())
            assertEquals(6, s.getSSTableLevel());

        int[] levels = strategy.manifest.getAllLevelSize();
        // verify that the manifest has correct amount of sstables
        assertEquals(cfs.getSSTables().size(), levels[6]);
    }

    @Test
    public void testNewRepairedSSTable() throws Exception
    {
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(CF_STANDARDDLEVELED, Util.cellname("column" + c), value, 0);
            }
            rm.applyUnsafe();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
            Thread.sleep(100);

        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        assertTrue(strategy.getLevelSize(1) > 0);
        assertTrue(strategy.getLevelSize(2) > 0);

        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse(sstable.isRepaired());
        }
        int sstableCount = 0;
        for (List<SSTableReader> level : strategy.manifest.generations)
            sstableCount += level.size();

        assertEquals(sstableCount, cfs.getSSTables().size());

        assertFalse(strategy.manifest.hasRepairedData());
        assertTrue(strategy.manifest.unrepairedL0.size() == 0);

        SSTableReader sstable1 = strategy.manifest.generations[2].get(0);
        SSTableReader sstable2 = strategy.manifest.generations[1].get(0);

        // "repair" an sstable:
        strategy.manifest.remove(sstable1);
        sstable1.descriptor.getMetadataSerializer().mutateRepairedAt(sstable1.descriptor, System.currentTimeMillis());
        sstable1.reloadSSTableMetadata();
        assertTrue(sstable1.isRepaired());

        // make sure adding a repaired sstable makes the manifest contain only repaired data;
        strategy.manifest.add(sstable1);
        assertTrue(strategy.manifest.hasRepairedData());
        assertTrue(strategy.manifest.generations[2].contains(sstable1));
        assertFalse(strategy.manifest.generations[1].contains(sstable2));
        assertTrue(strategy.manifest.unrepairedL0.contains(sstable2));
        sstableCount = 0;
        for (int i = 0; i < strategy.manifest.generations.length; i++)
        {
            sstableCount += strategy.manifest.generations[i].size();
            if (i != 2)
                assertEquals(strategy.manifest.generations[i].size(), 0);
            else
                assertEquals(strategy.manifest.generations[i].size(), 1);
        }
        assertEquals(1, sstableCount);

        // make sure adding an unrepaired sstable puts it in unrepairedL0:
        strategy.manifest.remove(sstable2);
        strategy.manifest.add(sstable2);
        assertTrue(strategy.manifest.unrepairedL0.contains(sstable2));
        assertEquals(strategy.manifest.unrepairedL0.size(), cfs.getSSTables().size() - 1);

        // make sure repairing an sstable takes it away from unrepairedL0 and puts it in the correct level:
        strategy.manifest.remove(sstable2);
        sstable2.descriptor.getMetadataSerializer().mutateRepairedAt(sstable2.descriptor, System.currentTimeMillis());
        sstable2.reloadSSTableMetadata();
        strategy.manifest.add(sstable2);
        assertFalse(strategy.manifest.unrepairedL0.contains(sstable2));
        assertTrue(strategy.manifest.generations[1].contains(sstable2));
    }
}
