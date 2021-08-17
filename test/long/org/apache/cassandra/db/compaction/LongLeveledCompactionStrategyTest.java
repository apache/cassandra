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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongLeveledCompactionStrategyTest
{
    public static final String KEYSPACE1 = "LongLeveledCompactionStrategyTest";
    public static final String CF_STANDARDLVL = "StandardLeveled";
    public static final String CF_STANDARDLVL2 = "StandardLeveled2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL)
                                                .compaction(CompactionParams.lcs(leveledOptions)),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDLVL2)
                                                .compaction(CompactionParams.lcs(leveledOptions)));
    }

    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();
        CompactionStrategyManager mgr = store.getCompactionStrategyManager();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) mgr.getStrategies().get(1).get(0);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        populateSSTables(store);

        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());
        List<Runnable> tasks = new ArrayList<Runnable>();
        while (true)
        {
            while (true)
            {
                final AbstractCompactionTask nextTask = lcs.getNextBackgroundTask(Integer.MIN_VALUE);
                if (nextTask == null)
                    break;
                tasks.add(new Runnable()
                {
                    public void run()
                    {
                        nextTask.execute(ActiveCompactionsTracker.NOOP);
                    }
                });
            }
            if (tasks.isEmpty())
                break;

            List<Future<?>> futures = new ArrayList<Future<?>>(tasks.size());
            for (Runnable r : tasks)
                futures.add(executor.submit(r));
            FBUtilities.waitOnFutures(futures);

            tasks.clear();
        }

        // Assert all SSTables are lined up correctly.
        LeveledManifest manifest = lcs.manifest;
        int levels = manifest.getLevelCount();
        for (int level = 0; level < levels; level++)
        {
            Set<SSTableReader> sstables = manifest.getLevel(level);
            // score check
            assert (double) SSTableReader.getTotalBytes(sstables) / manifest.maxBytesForLevel(level, 1 * 1024 * 1024) < 1.00;
            // overlap check for levels greater than 0
            for (SSTableReader sstable : sstables)
            {
                // level check
                assert level == sstable.getSSTableLevel();

                if (level > 0)
                {// overlap check for levels greater than 0
                    Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable.first.getToken(), sstable.last.getToken(), sstables);
                    assert overlaps.size() == 1 && overlaps.contains(sstable);
                }
            }
        }
    }

    @Test
    public void testLeveledScanner() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARDLVL2);
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        populateSSTables(store);

        LeveledCompactionStrategyTest.waitForLeveling(store);
        store.disableAutoCompaction();
        CompactionStrategyManager mgr = store.getCompactionStrategyManager();
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) mgr.getStrategies().get(1).get(0);

        value = ByteBuffer.wrap(new byte[10 * 1024]); // 10 KB value

        // Adds 10 partitions
        for (int r = 0; r < 10; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata(), key);
            for (int c = 0; c < 10; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
        }

        //Flush sstable
        store.forceBlockingFlush();

        store.runWithCompactionsDisabled(new Callable<Void>()
        {
            public Void call() throws Exception
            {
                Iterable<SSTableReader> allSSTables = store.getSSTables(SSTableSet.LIVE);
                for (SSTableReader sstable : allSSTables)
                {
                    if (sstable.getSSTableLevel() == 0)
                    {
                        System.out.println("Mutating L0-SSTABLE level to L1 to simulate a bug: " + sstable.getFilename());
                        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
                        sstable.reloadSSTableMetadata();
                    }
                }

                try (AbstractCompactionStrategy.ScannerList scannerList = lcs.getScanners(Lists.newArrayList(allSSTables)))
                {
                    //Verify that leveled scanners will always iterate in ascending order (CASSANDRA-9935)
                    for (ISSTableScanner scanner : scannerList.scanners)
                    {
                        DecoratedKey lastKey = null;
                        while (scanner.hasNext())
                        {
                            UnfilteredRowIterator row = scanner.next();
                            if (lastKey != null)
                            {
                                assertTrue("row " + row.partitionKey() + " received out of order wrt " + lastKey, row.partitionKey().compareTo(lastKey) >= 0);
                            }
                            lastKey = row.partitionKey();
                        }
                    }
                }
                return null;
            }
        }, true, true);


    }

    @Test
    public void testRepairStatusChanges() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();

        CompactionStrategyManager mgr = store.getCompactionStrategyManager();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) mgr.getStrategies().get(0).get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) mgr.getStrategies().get(1).get(0);

        // populate repaired sstables
        populateSSTables(store);
        assertTrue(repaired.getSSTables().isEmpty());
        assertFalse(unrepaired.getSSTables().isEmpty());
        mgr.mutateRepaired(store.getLiveSSTables(), FBUtilities.nowInSeconds(), null, false);
        assertFalse(repaired.getSSTables().isEmpty());
        assertTrue(unrepaired.getSSTables().isEmpty());

        // populate unrepaired sstables
        populateSSTables(store);
        assertFalse(repaired.getSSTables().isEmpty());
        assertFalse(unrepaired.getSSTables().isEmpty());

        // compact them into upper levels
        store.forceMajorCompaction();
        assertFalse(repaired.getSSTables().isEmpty());
        assertFalse(unrepaired.getSSTables().isEmpty());

        // mark unrepair
        mgr.mutateRepaired(store.getLiveSSTables().stream().filter(s -> s.isRepaired()).collect(Collectors.toList()),
                           ActiveRepairService.UNREPAIRED_SSTABLE,
                           null,
                           false);
        assertTrue(repaired.getSSTables().isEmpty());
        assertFalse(unrepaired.getSSTables().isEmpty());
    }

    private void populateSSTables(ColumnFamilyStore store)
    {
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 128;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            UpdateBuilder builder = UpdateBuilder.create(store.metadata(), key);
            for (int c = 0; c < columns; c++)
                builder.newRow("column" + c).add("val", value);

            Mutation rm = new Mutation(builder.build());
            rm.apply();
            store.forceBlockingFlush();
        }
    }
}
