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

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.dk;

public class RangeTombstoneTest extends SchemaLoader
{
    private static final String KSNAME = "Keyspace1";
    private static final String CFNAME = "StandardInteger1";

    @Test
    public void simpleQueryWithRangeTombstoneTest() throws Exception
    {
        Table table = Table.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k1";
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 40; i += 2)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 10, 22, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 1; i < 40; i += 2)
            add(rm, i, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 19, 27, 3);
        rm.apply();
        // We don't flush to test with both a range tomsbtone in memtable and in sstable

        QueryPath path = new QueryPath(CFNAME);

        // Queries by name
        int[] live = new int[]{ 4, 9, 11, 17, 28 };
        int[] dead = new int[]{ 12, 19, 21, 24, 27 };
        SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
        for (int i : live)
            columns.add(b(i));
        for (int i : dead)
            columns.add(b(i));
        cf = cfs.getColumnFamily(QueryFilter.getNamesFilter(dk(key), path, columns));

        for (int i : live)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i : dead)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";

        // Queries by slices
        cf = cfs.getColumnFamily(QueryFilter.getSliceFilter(dk(key), path, b(7), b(30), false, Integer.MAX_VALUE));

        for (int i : new int[]{ 7, 8, 9, 11, 13, 15, 17, 28, 29, 30 })
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i : new int[]{ 10, 12, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 })
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";
    }

    @Test
    public void overlappingRangeTest() throws Exception
    {
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(KSNAME);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(CFNAME);

        // Inserting data
        String key = "k2";
        RowMutation rm;
        ColumnFamily cf;

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        for (int i = 0; i < 20; i++)
            add(rm, i, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 15, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 10, 1);
        rm.apply();
        cfs.forceBlockingFlush();

        rm = new RowMutation(KSNAME, ByteBufferUtil.bytes(key));
        cf = rm.addOrGet(CFNAME);
        delete(cf, 5, 8, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        QueryPath path = new QueryPath(CFNAME);
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), path));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";

        // Compact everything and re-test
        CompactionManager.instance.performMaximal(cfs);
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(dk(key), path));

        for (int i = 0; i < 5; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 16; i < 20; i++)
            assert isLive(cf, cf.getColumn(b(i))) : "Column " + i + " should be live";
        for (int i = 5; i <= 15; i++)
            assert !isLive(cf, cf.getColumn(b(i))) : "Column " + i + " shouldn't be live";
    }

    private static boolean isLive(ColumnFamily cf, IColumn c)
    {
        return c != null && !c.isMarkedForDelete() && !cf.deletionInfo().isDeleted(c);
    }

    private static ByteBuffer b(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static void insertData(ColumnFamilyStore cfs, String key) throws Exception
    {
    }

    private static void add(RowMutation rm, int value, long timestamp)
    {
        rm.add(new QueryPath(CFNAME, null, b(value)), b(value), timestamp);
    }

    private static void delete(ColumnFamily cf, int from, int to, long timestamp)
    {
        cf.delete(new DeletionInfo(b(from),
                                   b(to),
                                   cf.getComparator(),
                                   timestamp,
                                   (int)(System.currentTimeMillis() / 1000)));
    }
}
