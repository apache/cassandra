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

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.Util;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;

public class LongCompactionsTest
{
    public static final String KEYSPACE1 = "Keyspace1";
    public static final String CF_STANDARD = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        Map<String, String> compactionOptions = Collections.singletonMap("tombstone_compaction_interval", "1");
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                .compaction(CompactionParams.stcs(compactionOptions)));
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

    protected void testCompaction(int sstableCount, int partitionsPerSSTable, int rowsPerPartition) throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");

        ArrayList<SSTableReader> sstables = new ArrayList<>();
        for (int k = 0; k < sstableCount; k++)
        {
            SortedMap<String, PartitionUpdate> rows = new TreeMap<>();
            for (int j = 0; j < partitionsPerSSTable; j++)
            {
                String key = String.valueOf(j);
                // last sstable has highest timestamps
                UpdateBuilder builder = UpdateBuilder.create(store.metadata(), String.valueOf(j))
                                                     .withTimestamp(k);
                for (int i = 0; i < rowsPerPartition; i++)
                    builder.newRow(String.valueOf(i)).add("val", String.valueOf(i));
                rows.put(key, builder.build());
            }
            Collection<SSTableReader> readers = SSTableUtils.prepare().write(rows);
            sstables.addAll(readers);
            store.addSSTables(readers);
        }

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        long start = nanoTime();
        final long gcBefore = (currentTimeMillis() / 1000) - Schema.instance.getTableMetadata(KEYSPACE1, "Standard1").params.gcGraceSeconds;
        try (LifecycleTransaction txn = store.getTracker().tryModify(sstables, OperationType.COMPACTION))
        {
            assert txn != null : "Cannot markCompacting all sstables";
            new CompactionTask(store, txn, gcBefore).execute(ActiveCompactionsTracker.NOOP);
        }
        System.out.println(String.format("%s: sstables=%d rowsper=%d colsper=%d: %d ms",
                                         this.getClass().getName(),
                                         sstableCount,
                                         partitionsPerSSTable,
                                         rowsPerPartition,
                                         TimeUnit.NANOSECONDS.toMillis(nanoTime() - start)));
    }

    @Test
    public void testStandardColumnCompactions()
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        cfs.clearUnsafe();

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata().params.minIndexInterval * 3 / ROWS_PER_SSTABLE;

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                long timestamp = j * ROWS_PER_SSTABLE + i;
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                UpdateBuilder.create(cfs.metadata(), key)
                             .withTimestamp(timestamp)
                             .newRow(String.valueOf(i / 2)).add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                             .apply();

                inserted.add(key);
            }
            Util.flush(cfs);
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);

            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        forceCompactions(cfs);
        assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());

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

        if (cfs.getLiveSSTables().size() > 1)
        {
            CompactionManager.instance.performMaximal(cfs, false);
        }
    }
}
