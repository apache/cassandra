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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import static junit.framework.Assert.assertEquals;

public class CompactionsTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";
    public static final String TABLE2 = "Keyspace2";
    public static final InetAddress LOCAL = FBUtilities.getLocalAddress();

    public static final int MIN_COMPACTION_THRESHOLD = 2;

    @Test
    public void testCompactions() throws IOException, ExecutionException, InterruptedException
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = (DatabaseDescriptor.getIndexInterval() * 3 / ROWS_PER_SSTABLE);

        // disable compaction while flushing
        store.disableAutoCompaction();

        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                RowMutation rm = new RowMutation(TABLE1, key.key);
                rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes(String.valueOf(i / 2))), ByteBufferUtil.EMPTY_BYTE_BUFFER, j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(store).size());
        }
        // re-enable compaction with thresholds low enough to force a few rounds
        store.setMinimumCompactionThreshold(2);
        store.setMaximumCompactionThreshold(4);
        // loop submitting parallel compactions until they all return 0
        while (true)
        {
            ArrayList<Future<Integer>> compactions = new ArrayList<Future<Integer>>();
            for (int i = 0; i < 10; i++)
                compactions.add(CompactionManager.instance.submitMinorIfNeeded(store));
            // another compaction attempt will be launched in the background by
            // each completing compaction: not much we can do to control them here
            boolean progress = false;
            for (Future<Integer> compaction : compactions)
               if (compaction.get() > 0)
                   progress = true;
            if (!progress)
                break;
        }
        if (store.getSSTables().size() > 1)
        {
            CompactionManager.instance.performMajor(store);
        }
        assertEquals(inserted.size(), Util.getRangeSlice(store).size());
    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = new ArrayList<Pair<String, Long>>();
        String[] strings = { "a", "bbbb", "cccccccc", "cccccccc", "bbbb", "a" };
        for (String st : strings)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        Set<List<String>> buckets = CompactionManager.getBuckets(pairs, 2);
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0).length(), bucket.get(1).length());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
        }

        pairs.clear();
        buckets.clear();

        String[] strings2 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings2)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = CompactionManager.getBuckets(pairs, 2);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(3, bucket.size());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
            assertEquals(bucket.get(1).charAt(0), bucket.get(2).charAt(0));
        }

        // Test the "min" functionality
        pairs.clear();
        buckets.clear();

        String[] strings3 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings3)
        {
            Pair<String, Long> pair = new Pair<String, Long>(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = CompactionManager.getBuckets(pairs, 10); // notice the min is 10
        assertEquals(1, buckets.size());
    }

    @Test
    public void testEchoedRow() throws IOException, ExecutionException, InterruptedException
    {
        // This test check that EchoedRow doesn't skipp rows: see CASSANDRA-2653

        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");

        // disable compaction while flushing
        store.disableAutoCompaction();

        // Insert 4 keys in two sstables. We need the sstables to have 2 rows
        // at least to trigger what was causing CASSANDRA-2653
        for (int i=1; i < 5; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes(String.valueOf(i))), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
            rm.apply();

            if (i % 2 == 0)
                store.forceBlockingFlush();
        }
        Collection<SSTableReader> toCompact = store.getSSTables();

        // Reinserting the same keys. We will compact only the previous sstable, but we need those new ones
        // to make sure we use EchoedRow, otherwise it won't be used because purge can be done.
        for (int i=1; i < 5; i++)
        {
            DecoratedKey key = Util.dk(String.valueOf(i));
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(new QueryPath("Standard2", null, ByteBufferUtil.bytes(String.valueOf(i))), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
            rm.apply();
        }
        store.forceBlockingFlush();
        SSTableReader tmpSSTable = null;
        for (SSTableReader sstable : store.getSSTables())
            if (!toCompact.contains(sstable))
                tmpSSTable = sstable;

        // Force compaction on first sstables. Since each row is in only one sstable, we will be using EchoedRow.
        CompactionManager.instance.doCompaction(store, toCompact, (int) (System.currentTimeMillis() / 1000) - store.metadata.getGcGraceSeconds());

        // Now, we remove the sstable that was just created to force the use of EchoedRow (so that it doesn't hide the problem)
        store.markCompacted(Collections.singleton(tmpSSTable));

        // Now assert we do have the 4 keys
        assertEquals(4, Util.getRangeSlice(store).size());
    }

    @Test
    public void testDontPurgeAccidentaly() throws IOException, ExecutionException, InterruptedException
    {
        // Testing with and without forcing deserialization. Without deserialization, EchoedRow will be used.
        testDontPurgeAccidentaly("test1", false);
        testDontPurgeAccidentaly("test2", true);
    }

    private void testDontPurgeAccidentaly(String k, boolean forceDeserialize) throws IOException, ExecutionException, InterruptedException
    {
        // This test catches the regression of CASSANDRA-2786
        Table table = Table.open(TABLE1);
        String cfname = "Super5";
        ColumnFamilyStore store = table.getColumnFamilyStore(cfname);

        // disable compaction while flushing
        store.removeAllSSTables();
        store.disableAutoCompaction();

        // Add test row
        DecoratedKey key = Util.dk(k);
        RowMutation rm = new RowMutation(TABLE1, key.key);
        rm.add(new QueryPath(cfname, ByteBufferUtil.bytes("sc"), ByteBufferUtil.bytes("c")), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        rm.apply();

        store.forceBlockingFlush();

        Collection<SSTableReader> sstablesBefore = store.getSSTables();

        QueryFilter filter = QueryFilter.getIdentityFilter(key, new QueryPath(cfname, null, null));
        assert !store.getColumnFamily(filter).isEmpty();

        // Remove key
        rm = new RowMutation(TABLE1, key.key);
        rm.delete(new QueryPath(cfname, null, null), 2);
        rm.apply();

        ColumnFamily cf = store.getColumnFamily(filter);
        assert cf.isEmpty() : "should be empty: " + cf;

        store.forceBlockingFlush();

        Collection<SSTableReader> sstablesAfter = store.getSSTables();
        Collection<SSTableReader> toCompact = new ArrayList<SSTableReader>();
        for (SSTableReader sstable : sstablesAfter)
            if (!sstablesBefore.contains(sstable))
                toCompact.add(sstable);

        String location = store.table.getDataFileLocation(1);
        CompactionManager.instance.doCompactionWithoutSizeEstimation(store, toCompact, (int) (System.currentTimeMillis() / 1000) - store.metadata.getGcGraceSeconds(), location, forceDeserialize);

        cf = store.getColumnFamily(filter);
        assert cf.isEmpty() : "should be empty: " + cf;
    }
}
