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
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.WrappedRunnable;

import java.net.InetAddress;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.CollatingOrderPreservingPartitioner;
import org.apache.cassandra.io.sstable.SSTableReader;

public class ColumnFamilyStoreTest extends CleanupHelper
{
    static byte[] bytes1, bytes2;

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Test
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace1", "key1".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rm.add(new QueryPath("Standard1", null, "Column2".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTables();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceFilterFailures();
        ColumnFamily cf = store.getColumnFamily(QueryFilter.getIdentityFilter(Util.dk("key2"), new QueryPath("Standard1", null, "Column1".getBytes())));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "key1".getBytes());
        rm.delete(new QueryPath("Standard2", null, null), new TimestampClock(System.currentTimeMillis()));
        rm.apply();

        Runnable r = new WrappedRunnable()
        {
            public void runMayThrow() throws IOException
            {
                QueryFilter sliceFilter = QueryFilter.getSliceFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, null, false, 1);
                ColumnFamily cf = store.getColumnFamily(sliceFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();

                QueryFilter namesFilter = QueryFilter.getNamesFilter(Util.dk("key1"), new QueryPath("Standard2", null, null), "a".getBytes());
                cf = store.getColumnFamily(namesFilter);
                assert cf.isMarkedForDelete();
                assert cf.getColumnsMap().isEmpty();
            }
        };

        TableTest.reTest(store, r);
    }

    /**
     * Writes out a bunch of keys into an SSTable, then runs anticompaction on a range.
     * Checks to see if anticompaction returns true.
     */
    private void testAntiCompaction(String columnFamilyName, int insertsPerTable) throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new ArrayList<RowMutation>();
        for (int j = 0; j < insertsPerTable; j++)
        {
            String key = String.valueOf(j);
            RowMutation rm = new RowMutation("Keyspace1", key.getBytes());
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], new TimestampClock(j));
            rms.add(rm);
        }
        ColumnFamilyStore store = Util.writeColumnFamily(rms);

        List<Range> ranges  = new ArrayList<Range>();
        IPartitioner partitioner = new CollatingOrderPreservingPartitioner();
        Range r = Util.range(partitioner, "0", "zzzzzzz");
        ranges.add(r);

        List<SSTableReader> fileList = CompactionManager.instance.submitAnticompaction(store, ranges, InetAddress.getByName("127.0.0.1")).get();
        assert fileList.size() >= 1;
    }

    @Test
    public void testAntiCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testAntiCompaction("Standard1", 100);
    }

    @Test
    public void testWrappedRangeQuery() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(ArrayUtils.EMPTY_BYTE_ARRAY,
                                             Util.range(p, "key15", "key1"),
                                             10,
                                             new NamesQueryFilter("asdf".getBytes()));
        assertEquals(2, result.size());
    }

    @Test
    public void testSkipStartKey() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = insertKey1Key2();

        IPartitioner p = StorageService.getPartitioner();
        List<Row> result = cfs.getRangeSlice(ArrayUtils.EMPTY_BYTE_ARRAY,
                                             Util.range(p, "key1", "key2"),
                                             10,
                                             new NamesQueryFilter("asdf".getBytes()));
        assertEquals(1, result.size());
        assert Arrays.equals(result.get(0).key.key, "key2".getBytes());
    }

    private ColumnFamilyStore insertKey1Key2() throws IOException, ExecutionException, InterruptedException
    {
        List<RowMutation> rms = new LinkedList<RowMutation>();
        RowMutation rm;
        rm = new RowMutation("Keyspace2", "key1".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        Util.writeColumnFamily(rms);

        rm = new RowMutation("Keyspace2", "key2".getBytes());
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), new TimestampClock(0));
        rms.add(rm);
        return Util.writeColumnFamily(rms);
    }
}
