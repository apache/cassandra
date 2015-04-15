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
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LeveledCompactionStrategyTest extends SchemaLoader
{
    private String ksname = "Keyspace1";
    private String cfname = "StandardLeveled";
    private Keyspace keyspace = Keyspace.open(ksname);
    private ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

    @Before
    public void enableCompaction()
    {
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
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        WrappingCompactionStrategy strategy = (WrappingCompactionStrategy) cfs.getCompactionStrategy();
        // Checking we're not completely bad at math
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(cfname).gcBefore(System.currentTimeMillis());
        UUID parentRepSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepSession, Arrays.asList(cfs), Arrays.asList(range));
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, UUID.randomUUID(), ksname, cfname, range);
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddress(), gcBefore);
        CompactionManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    private void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException
    {
        WrappingCompactionStrategy strategy = (WrappingCompactionStrategy) cfs.getCompactionStrategy();
        // L0 is the lowest priority, so when that's done, we know everything is done
        while (strategy.getSSTableCountPerLevel()[0] > 1)
            Thread.sleep(100);
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
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ((WrappingCompactionStrategy) cfs.getCompactionStrategy()).getWrappedStrategies().get(1);
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
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.forceBlockingFlush();
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ((WrappingCompactionStrategy) cfs.getCompactionStrategy()).getWrappedStrategies().get(1);
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
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs)))
            Thread.sleep(100);

        WrappingCompactionStrategy strategy = (WrappingCompactionStrategy) cfs.getCompactionStrategy();
        List<AbstractCompactionStrategy> strategies = strategy.getWrappedStrategies();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) strategies.get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) strategies.get(1);
        assertEquals(0, repaired.manifest.getLevelCount() );
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertTrue(strategy.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategy.getSSTableCountPerLevel()[2] > 0);

        for (SSTableReader sstable : cfs.getSSTables())
            assertFalse(sstable.isRepaired());

        int sstableCount = 0;
        for (List<SSTableReader> level : unrepaired.manifest.generations)
            sstableCount += level.size();
        // we only have unrepaired sstables:
        assertEquals(sstableCount, cfs.getSSTables().size());

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
        strategy.handleNotification(new SSTableAddedNotification(sstable2), this);
        assertTrue(unrepaired.manifest.getLevel(1).contains(sstable2));
        assertFalse(repaired.manifest.getLevel(1).contains(sstable2));
    }
}
