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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;

public class LongCompactionsTest extends SchemaLoader
{
    public static final String TABLE1 = "Keyspace1";

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

        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        ArrayList<SSTableReader> sstables = new ArrayList<SSTableReader>();
        for (int k = 0; k < sstableCount; k++)
        {
            SortedMap<String,ColumnFamily> rows = new TreeMap<String,ColumnFamily>();
            for (int j = 0; j < rowsPerSSTable; j++)
            {
                String key = String.valueOf(j);
                IColumn[] cols = new IColumn[colsPerRow];
                for (int i = 0; i < colsPerRow; i++)
                {
                    // last sstable has highest timestamps
                    cols[i] = Util.column(String.valueOf(i), String.valueOf(i), k);
                }
                rows.put(key, SSTableUtils.createCF(Long.MIN_VALUE, Integer.MIN_VALUE, cols));
            }
            SSTableReader sstable = SSTableUtils.prepare().write(rows);
            sstables.add(sstable);
            store.addSSTable(sstable);
        }

        // give garbage collection a bit of time to catch up
        Thread.sleep(1000);

        long start = System.currentTimeMillis();
        final int gcBefore = (int) (System.currentTimeMillis() / 1000) - Schema.instance.getCFMetaData(TABLE1, "Standard1").getGcGraceSeconds();
        new CompactionTask(store, sstables, gcBefore).execute(null);
        System.out.println(String.format("%s: sstables=%d rowsper=%d colsper=%d: %d ms",
                                         this.getClass().getName(),
                                         sstableCount,
                                         rowsPerSSTable,
                                         colsPerRow,
                                         System.currentTimeMillis() - start));
    }

    @Test
    public void testStandardColumnCompactions() throws IOException, ExecutionException, InterruptedException
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Standard1");
        cfs.clearUnsafe();

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = DatabaseDescriptor.getIndexInterval() * 3 / ROWS_PER_SSTABLE;

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                RowMutation rm = new RowMutation(TABLE1, key.key);
                long timestamp = j * ROWS_PER_SSTABLE + i;
                rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(String.valueOf(i / 2))),
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
        cfs.truncate();
    }

    @Test
    public void testSuperColumnCompactions() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open(TABLE1);
        ColumnFamilyStore cfs = table.getColumnFamilyStore("Super1");

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = DatabaseDescriptor.getIndexInterval() * 3 / ROWS_PER_SSTABLE;

        //disable compaction while flushing
        cfs.disableAutoCompaction();

        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        ByteBuffer superColumn = ByteBufferUtil.bytes("TestSuperColumn");
        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                RowMutation rm = new RowMutation(TABLE1, key.key);
                long timestamp = j * ROWS_PER_SSTABLE + i;
                rm.add(new QueryPath("Super1", superColumn, ByteBufferUtil.bytes((long)(i / 2))),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp);
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                rm.apply();
                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(cfs, superColumn).size());
        }

        forceCompactions(cfs);

        assertEquals(inserted.size(), Util.getRangeSlice(cfs, superColumn).size());

        // make sure max timestamp of compacted sstables is recorded properly after compaction.
        CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
    }

    private void forceCompactions(ColumnFamilyStore cfs) throws ExecutionException, InterruptedException
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
