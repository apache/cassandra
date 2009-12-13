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
package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.utils.FBUtilities;
import static junit.framework.Assert.assertEquals;

public class CompactionsTest extends CleanupHelper
{
    public static final String TABLE1 = "Keyspace1";
    public static final String TABLE2 = "Keyspace2";
    public static final InetAddress LOCAL = FBUtilities.getLocalAddress();

    @Test
    public void testCompactions() throws IOException, ExecutionException, InterruptedException
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open(TABLE1);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < (SSTableReader.indexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                String key = String.valueOf(i % 2);
                RowMutation rm = new RowMutation(TABLE1, key);
                rm.add(new QueryPath("Standard1", null, String.valueOf(i / 2).getBytes()), new byte[0], j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(inserted.size(), table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size());
        }
        while (true)
        {
            Future<Integer> ft = CompactionManager.instance.submitMinor(store);
            if (ft.get() == 0)
                break;
        }
        if (store.getSSTables().size() > 1)
        {
            store.doCompaction(2, store.getSSTables().size());
        }
        assertEquals(inserted.size(), table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size());
    }

    @Test
    public void testCompactionPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableCompactions();

        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore store = table.getColumnFamilyStore(cfName);

        String key = "key1";
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();
        store.forceBlockingFlush();

        // deletes
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation(TABLE1, key);
            rm.delete(new QueryPath(cfName, null, String.valueOf(i).getBytes()), 1);
            rm.apply();
        }
        store.forceBlockingFlush();

        // resurrect one column
        rm = new RowMutation(TABLE1, key);
        rm.add(new QueryPath(cfName, null, String.valueOf(5).getBytes()), new byte[0], 2);
        rm.apply();
        store.forceBlockingFlush();

        // verify that non-major compaction does no GC to ensure correctness (see CASSANDRA-604)
        Collection<SSTableReader> sstablesIncomplete = store.getSSTables();
        rm = new RowMutation(TABLE1, key + "x");
        rm.add(new QueryPath(cfName, null, "0".getBytes()), new byte[0], 0);
        rm.apply();
        store.forceBlockingFlush();
        store.doFileCompaction(sstablesIncomplete, Integer.MAX_VALUE);
        ColumnFamily cf = table.getColumnFamilyStore(cfName).getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfName)));
        assert cf.getColumnCount() == 10;

        // major compact and test that all columns but the resurrected one is completely gone
        store.doFileCompaction(store.getSSTables(), Integer.MAX_VALUE);
        cf = table.getColumnFamilyStore(cfName).getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfName)));
        assert cf.getColumnCount() == 1;
        assert cf.getColumn(String.valueOf(5).getBytes()) != null;
    }

    @Test
    public void testCompactionPurgeOneFile() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableCompactions();

        Table table = Table.open(TABLE1);
        String cfName = "Standard2";
        ColumnFamilyStore store = table.getColumnFamilyStore(cfName);

        String key = "key1";
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key);
        for (int i = 0; i < 5; i++)
        {
            rm.add(new QueryPath(cfName, null, String.valueOf(i).getBytes()), new byte[0], 0);
        }
        rm.apply();

        // deletes
        for (int i = 0; i < 5; i++)
        {
            rm = new RowMutation(TABLE1, key);
            rm.delete(new QueryPath(cfName, null, String.valueOf(i).getBytes()), 1);
            rm.apply();
        }
        store.forceBlockingFlush();

        assert store.getSSTables().size() == 1 : store.getSSTables(); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        store.doFileCompaction(store.getSSTables(), Integer.MAX_VALUE);
        assert store.getSSTables().isEmpty();
        ColumnFamily cf = table.getColumnFamilyStore(cfName).getColumnFamily(new IdentityQueryFilter(key, new QueryPath(cfName)));
        assert cf == null : cf;
    }

    @Test
    public void testCompactionReadonly() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open(TABLE2);
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < (SSTableReader.indexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                String key = String.valueOf(i % 2);
                RowMutation rm = new RowMutation(TABLE2, key);
                rm.add(new QueryPath("Standard1", null, String.valueOf(i / 2).getBytes()), new byte[0], j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(inserted.size(), table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size());
        }

        // perform readonly compaction and confirm that no sstables changed
        ArrayList<SSTableReader> oldsstables = new ArrayList<SSTableReader>(store.getSSTables());
        store.doReadonlyCompaction(LOCAL);
        assertEquals(oldsstables, new ArrayList<SSTableReader>(store.getSSTables()));
        assertEquals(inserted.size(), table.getColumnFamilyStore("Standard1").getKeyRange("", "", 10000).keys.size());
    }
}
