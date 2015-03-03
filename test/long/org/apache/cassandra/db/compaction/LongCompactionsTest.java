/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.Util;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import static org.junit.Assert.assertEquals;

public class LongCompactionsTest
{
    public static final String KEYSPACE1 = "Keyspace1";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> compactionOptions = new HashMap<>();
        compactionOptions.put("tombstone_compaction_interval", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                .compactionStrategyOptions(compactionOptions));
    }

    @Before
    public void cleanupFiles()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.truncateBlocking();
    }

    /**
     * Test compaction with a very wide row.
     */
    @Test
    public void testCompactionWide() throws Exception
    {
        testCompaction(2, 1, 200000);
    }

    /**
     * Test compaction with lots of skinny rows.
     */
    @Test
    public void testCompactionSlim() throws Exception
    {
        testCompaction(2, 200000, 1);
    }

    /**
     * Test compaction with lots of small sstables.
     */
    @Test
    public void testCompactionMany() throws Exception
    {
        testCompaction(100, 800, 5);
    }

    protected void testCompaction(int sstableCount, int rowsPerSSTable, int colsPerRow) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");

        ArrayList<SSTableReader> sstables = new ArrayList<>();
        for (int k = 0; k < sstableCount; k++)
        {
            SortedMap<String,ColumnFamily> rows = new TreeMap<String,ColumnFamily>();
            for (int j = 0; j < rowsPerSSTable; j++)
            {
                String key = String.valueOf(j);
                Cell[] cols = new Cell[colsPerRow];
                for (int i = 0; i < colsPerRow; i++)
                {
                    // last sstable has highest timestamps
                    cols[i] = Util.column(String.valueOf(i), String.valueOf(i), k);
                }
                rows.put(key, SSTableUtils.createCF(KEYSPACE1, CF_STANDARD, Long.MIN_VALUE, Integer.MIN_VALUE, cols));
            }
            SSTableReader sstable = SSTableUtils.prepare().write(rows);
            sstables.add(sstable);
            store.addSSTable(sstable);
        }

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        long start = System.nanoTime();
        final int gcBefore = (int) (System.currentTimeMillis() / 1000) - Schema.instance.getCFMetaData(KEYSPACE1, "Standard1").getGcGraceSeconds();
        assert store.getDataTracker().markCompacting(sstables): "Cannot markCompacting all sstables";
        new CompactionTask(store, sstables, gcBefore, false).execute(null);
        System.out.println(String.format("%s: sstables=%d rowsper=%d colsper=%d: %d ms",
                                         this.getClass().getName(),
                                         sstableCount,
                                         rowsPerSSTable,
                                         colsPerRow,
                                         TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
    }

    @Test
    public void testStandardColumnCompactions()
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.clearUnsafe();

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata.getMinIndexInterval() * 3 / ROWS_PER_SSTABLE;

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                Mutation rm = new Mutation(KEYSPACE1, key.getKey());
                long timestamp = j * ROWS_PER_SSTABLE + i;
                rm.add("Standard1", Util.cellname(String.valueOf(i / 2)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp);
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                rm.apply();
                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(cfs).size());
        }

        forceCompactions(cfs);

        assertEquals(inserted.size(), Util.getRangeSlice(cfs).size());

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
        cfs.truncateBlocking();
    }

    private void forceCompactions(ColumnFamilyStore cfs)
    {
        // re-enable compaction with thresholds low enough to force a few rounds
        cfs.setCompactionThresholds(2, 4);

        // loop submitting parallel compactions until they all return 0
        do
        {
            ArrayList<Future<?>> compactions = new ArrayList<Future<?>>();
            for (int i = 0; i < 10; i++)
                compactions.addAll(CompactionManager.instance.submitBackground(cfs));
            // another compaction attempt will be launched in the background by
            // each completing compaction: not much we can do to control them here
            FBUtilities.waitOnFutures(compactions);
        } while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0);

        if (cfs.getSSTables().size() > 1)
        {
            CompactionManager.instance.performMaximal(cfs);
        }
    }
}
