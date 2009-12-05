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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import java.net.InetAddress;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.CollatingOrderPreservingPartitioner;
import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.io.SSTableReader;

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
        rm = new RowMutation("Keyspace1", "key1");
        rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), "asdf".getBytes(), 0);
        rm.add(new QueryPath("Standard1", null, "Column2".getBytes()), "asdf".getBytes(), 0);
        rms.add(rm);
        ColumnFamilyStore store = ColumnFamilyStoreUtils.writeColumnFamily(rms);

        Table table = Table.open("Keyspace1");
        List<SSTableReader> ssTables = table.getAllSSTablesOnDisk();
        assertEquals(1, ssTables.size());
        ssTables.get(0).forceBloomFilterFailures();
        ColumnFamily cf = store.getColumnFamily(new IdentityQueryFilter("key2", new QueryPath("Standard1", null, "Column1".getBytes())));
        assertNull(cf);
    }

    @Test
    public void testEmptyRow() throws Exception
    {
        Table table = Table.open("Keyspace1");
        final ColumnFamilyStore store = table.getColumnFamilyStore("Standard2");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "key1");
        rm.delete(new QueryPath("Standard2", null, null), System.currentTimeMillis());
        rm.apply();

        TableTest.Runner r = new TableTest.Runner()
        {
            public void run() throws IOException
            {
                SliceQueryFilter sliceFilter = new SliceQueryFilter("key1", new QueryPath("Standard2", null, null), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
                assertNull(store.getColumnFamily(sliceFilter));
                NamesQueryFilter namesFilter = new NamesQueryFilter("key1", new QueryPath("Standard2", null, null), "a".getBytes());
                assertNull(store.getColumnFamily(namesFilter));
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
            RowMutation rm = new RowMutation("Keyspace1", key);
            rm.add(new QueryPath(columnFamilyName, null, "0".getBytes()), new byte[0], j);
            rms.add(rm);
        }
        ColumnFamilyStore store = ColumnFamilyStoreUtils.writeColumnFamily(rms);

        List<Range> ranges  = new ArrayList<Range>();
        IPartitioner partitioner = new CollatingOrderPreservingPartitioner();
        Range r = new Range(partitioner.getToken("0"), partitioner.getToken("zzzzzzz"));
        ranges.add(r);

        List<SSTableReader> fileList = store.forceAntiCompaction(ranges, InetAddress.getByName("127.0.0.1"));
        assert fileList.size() >= 1;
    }

    @Test
    public void testAntiCompaction1() throws IOException, ExecutionException, InterruptedException
    {
        testAntiCompaction("Standard1", 100);
    }    
}
