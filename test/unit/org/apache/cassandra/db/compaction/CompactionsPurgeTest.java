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

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.db.KeyspaceTest.assertColumns;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

import static org.apache.cassandra.Util.cellname;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;


public class CompactionsPurgeTest extends SchemaLoader
{
    public static final String KEYSPACE1 = "Keyspace1";
    public static final String KEYSPACE2 = "Keyspace2";

    @Test
    public void testMajorCompactionPurge() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        Mutation rm;

        // inserts
        rm = new Mutation(KEYSPACE1, key.getKey());
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();
        cfs.forceBlockingFlush();

        // deletes
        for (int i = 0; i < 10; i++)
        {
            rm = new Mutation(KEYSPACE1, key.getKey());
            rm.delete(cfName, cellname(String.valueOf(i)), 1);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        // resurrect one column
        rm = new Mutation(KEYSPACE1, key.getKey());
        rm.add(cfName, cellname(String.valueOf(5)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        // major compact and test that all columns but the resurrected one is completely gone
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE));
        cfs.invalidateCachedRow(key);
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
        assertColumns(cf, "5");
        assert cf.getColumn(cellname(String.valueOf(5))) != null;
    }

    @Test
    public void testMinorCompactionPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        Mutation rm;
        for (int k = 1; k <= 2; ++k) {
            DecoratedKey key = Util.dk("key" + k);

            // inserts
            rm = new Mutation(KEYSPACE2, key.getKey());
            for (int i = 0; i < 10; i++)
            {
                rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();

            // deletes
            for (int i = 0; i < 10; i++)
            {
                rm = new Mutation(KEYSPACE2, key.getKey());
                rm.delete(cfName, cellname(String.valueOf(i)), 1);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");

        // flush, remember the current sstable and then resurrect one column
        // for first key. Then submit minor compaction on remembered sstables.
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();
        rm = new Mutation(KEYSPACE2, key1.getKey());
        rm.add(cfName, cellname(String.valueOf(5)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2);
        rm.apply();
        cfs.forceBlockingFlush();
        cfs.getCompactionStrategy().getUserDefinedTask(sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        // verify that minor compaction does GC when key is provably not
        // present in a non-compacted sstable
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key2, cfName, System.currentTimeMillis()));
        assert cf == null;

        // verify that minor compaction still GC when key is present
        // in a non-compacted sstable but the timestamp ensures we won't miss anything
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key1, cfName, System.currentTimeMillis()));
        assertEquals(1, cf.getColumnCount());
    }

    /**
     * verify that we don't drop tombstones during a minor compaction that might still be relevant
     */
    @Test
    public void testMinTimestampPurge()
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        Mutation rm;
        DecoratedKey key3 = Util.dk("key3");

        // inserts
        rm = new Mutation(KEYSPACE2, key3.getKey());
        rm.add(cfName, cellname("c1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8);
        rm.add(cfName, cellname("c2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8);
        rm.apply();
        cfs.forceBlockingFlush();
        // delete c1
        rm = new Mutation(KEYSPACE2, key3.getKey());
        rm.delete(cfName, cellname("c1"), 10);
        rm.apply();
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();

        // delete c2 so we have new delete in a diffrent SSTable
        rm = new Mutation(KEYSPACE2, key3.getKey());
        rm.delete(cfName, cellname("c2"), 9);
        rm.apply();
        cfs.forceBlockingFlush();

        // compact the sstables with the c1/c2 data and the c1 tombstone
        cfs.getCompactionStrategy().getUserDefinedTask(sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        // We should have both the c1 and c2 tombstones still. Since the min timestamp in the c2 tombstone
        // sstable is older than the c1 tombstone, it is invalid to throw out the c1 tombstone.
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key3, cfName, System.currentTimeMillis()));
        assertFalse(cf.getColumn(cellname("c2")).isLive());
        assertEquals(2, cf.getColumnCount());
    }

    @Test
    public void testCompactionPurgeOneFile() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        String cfName = "Standard2";
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        Mutation rm;

        // inserts
        rm = new Mutation(KEYSPACE1, key.getKey());
        for (int i = 0; i < 5; i++)
        {
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // deletes
        for (int i = 0; i < 5; i++)
        {
            rm = new Mutation(KEYSPACE1, key.getKey());
            rm.delete(cfName, cellname(String.valueOf(i)), 1);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        assert cfs.getSSTables().size() == 1 : cfs.getSSTables(); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assert cfs.getSSTables().isEmpty();
        ColumnFamily cf = keyspace.getColumnFamilyStore(cfName).getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
        assert cf == null : cf;
    }

    @Test
    public void testCompactionPurgeCachedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = "RowCacheSpace";
        String cfName = "CachedCF";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key3");
        Mutation rm;

        // inserts
        rm = new Mutation(keyspaceName, key.getKey());
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // move the key up in row cache
        cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));

        // deletes row
        rm = new Mutation(keyspaceName, key.getKey());
        rm.delete(cfName, 1);
        rm.apply();

        // flush and major compact
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();

        // re-inserts with timestamp lower than delete
        rm = new Mutation(keyspaceName, key.getKey());
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // Check that the second insert did went in
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis()));
        assertEquals(10, cf.getColumnCount());
        for (Cell c : cf)
            assert c.isLive();
    }

    @Test
    public void testCompactionPurgeTombstonedRow() throws ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String keyspaceName = "Keyspace1";
        String cfName = "Standard1";
        Keyspace keyspace = Keyspace.open(keyspaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        DecoratedKey key = Util.dk("key3");
        Mutation rm;
        QueryFilter filter = QueryFilter.getIdentityFilter(key, cfName, System.currentTimeMillis());

        // inserts
        rm = new Mutation(keyspaceName, key.getKey());
        for (int i = 0; i < 10; i++)
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
        rm.apply();

        // deletes row with timestamp such that not all columns are deleted
        rm = new Mutation(keyspaceName, key.getKey());
        rm.delete(cfName, 4);
        rm.apply();
        ColumnFamily cf = cfs.getColumnFamily(filter);
        assertTrue(cf.isMarkedForDelete());

        // flush and major compact (with tombstone purging)
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertFalse(cfs.getColumnFamily(filter).isMarkedForDelete());

        // re-inserts with timestamp lower than delete
        rm = new Mutation(keyspaceName, key.getKey());
        for (int i = 0; i < 5; i++)
            rm.add(cfName, cellname(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
        rm.apply();

        // Check that the second insert went in
        cf = cfs.getColumnFamily(filter);
        assertEquals(10, cf.getColumnCount());
        for (Cell c : cf)
            assert c.isLive();
    }

    @Test
    public void testRowTombstoneObservedBeforePurging() throws InterruptedException, ExecutionException
    {
        String keyspace = "cql_keyspace";
        String table = "table1";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        // write a row out to one sstable
        executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                      keyspace, table, 1, "foo", 1));
        cfs.forceBlockingFlush();

        UntypedResultSet result = executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a second sstable
        executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        cfs.forceBlockingFlush();

        // basic check that the row is considered deleted
        assertEquals(2, cfs.getSSTables().size());
        result = executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // compact the two sstables with a gcBefore that does *not* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) - 10000));

        // the data should be gone, but the tombstone should still exist
        assertEquals(1, cfs.getSSTables().size());
        result = executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());

        // write a row out to one sstable
        executeInternal(String.format("INSERT INTO %s.%s (k, v1, v2) VALUES (%d, '%s', %d)",
                                      keyspace, table, 1, "foo", 1));
        cfs.forceBlockingFlush();
        assertEquals(2, cfs.getSSTables().size());
        result = executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(1, result.size());

        // write a row tombstone out to a different sstable
        executeInternal(String.format("DELETE FROM %s.%s WHERE k = %d", keyspace, table, 1));
        cfs.forceBlockingFlush();

        // compact the two sstables with a gcBefore that *does* allow the row tombstone to be purged
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, (int) (System.currentTimeMillis() / 1000) + 10000));

        // both the data and the tombstone should be gone this time
        assertEquals(0, cfs.getSSTables().size());
        result = executeInternal(String.format("SELECT * FROM %s.%s WHERE k = %d", keyspace, table, 1));
        assertEquals(0, result.size());
    }
}
