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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.UUID;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LeveledCompactionStrategyTest extends SchemaLoader
{
    /*
     * This exercises in particular the code of #4142
     */
    @Test
    public void testValidationMultipleSSTablePerLevel() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            RowMutation rm = new RowMutation(ksname, key.key);
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, ByteBufferUtil.bytes("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        // Checking we're not completely bad at math
        assert strategy.getLevelSize(1) > 0;
        assert strategy.getLevelSize(2) > 0;

        Range<Token> range = new Range<Token>(Util.token(""), Util.token(""));
        int gcBefore = keyspace.getColumnFamilyStore(cfname).gcBefore(System.currentTimeMillis());
        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), ksname, cfname, range);
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddress(), gcBefore);
        CompactionManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    private void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException, ExecutionException
    {
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        // L0 is the lowest priority, so when that's done, we know everything is done
        while (strategy.getLevelSize(0) > 1)
            Thread.sleep(100);
    }

    @Test
    public void testCompactionProgress() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        // make sure we have SSTables in L1
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]);
        int rows = 2;
        int columns = 10;
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            RowMutation rm = new RowMutation(ksname, key.key);
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, ByteBufferUtil.bytes("column" + c), value, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategy();
        assert strategy.getLevelSize(1) > 0;

        // get LeveledScanner for level 1 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(1);
        List<ICompactionScanner> scanners = strategy.getScanners(sstables);
        assertEquals(1, scanners.size()); // should be one per level
        ICompactionScanner scanner = scanners.get(0);
        // scan through to the end
        while (scanner.hasNext())
            scanner.next();

        // scanner.getCurrentPosition should be equal to total bytes of L1 sstables
        assert scanner.getCurrentPosition() == SSTable.getTotalBytes(sstables);
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 20;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            RowMutation rm = new RowMutation(ksname, key.key);
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, ByteBufferUtil.bytes("column" + c), value, 0);
            }
            rm.apply();
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
            LeveledManifest.mutateLevel(Pair.create(s.getSSTableMetadata(), s.getAncestors()), s.descriptor, s.descriptor.filenameFor(Component.STATS), 6);
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
}
