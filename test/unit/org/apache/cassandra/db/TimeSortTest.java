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
import java.util.concurrent.ExecutionException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.CleanupHelper;
import static org.apache.cassandra.Util.getBytes;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.LongType;

public class TimeSortTest extends CleanupHelper
{
    @Test
    public void testMixedSources() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("StandardLong1");
        RowMutation rm;

        rm = new RowMutation("Keyspace1", "key0");
        rm.add(new QueryPath("StandardLong1", null, getBytes(100)), "a".getBytes(), 100);
        rm.apply();
        cfStore.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", "key0");
        rm.add(new QueryPath("StandardLong1", null, getBytes(0)), "b".getBytes(), 0);
        rm.apply();

        ColumnFamily cf = cfStore.getColumnFamily("key0", new QueryPath("StandardLong1"), getBytes(10), ArrayUtils.EMPTY_BYTE_ARRAY, false, 1000);
        Collection<IColumn> columns = cf.getSortedColumns();
        assert columns.size() == 1;
    }

    @Test
    public void testTimeSort() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("StandardLong1");

        for (int i = 900; i < 1000; ++i)
        {
            RowMutation rm = new RowMutation("Keyspace1", Integer.toString(i));
            for (int j = 0; j < 8; ++j)
            {
                rm.add(new QueryPath("StandardLong1", null, getBytes(j * 2)), "a".getBytes(), j * 2);
            }
            rm.apply();
        }

        validateTimeSort(table);

        cfStore.forceBlockingFlush();
        validateTimeSort(table);

        // interleave some new data to test memtable + sstable
        String key = "900";
        RowMutation rm = new RowMutation("Keyspace1", key);
        for (int j = 0; j < 4; ++j)
        {
            rm.add(new QueryPath("StandardLong1", null, getBytes(j * 2 + 1)), "b".getBytes(), j * 2 + 1);
        }
        rm.apply();
        // and some overwrites
        rm = new RowMutation("Keyspace1", key);
        rm.add(new QueryPath("StandardLong1", null, getBytes(0)), "c".getBytes(), 100);
        rm.add(new QueryPath("StandardLong1", null, getBytes(10)), "c".getBytes(), 100);
        rm.apply();

        // verify
        ColumnFamily cf = cfStore.getColumnFamily(key, new QueryPath("StandardLong1"), getBytes(0), ArrayUtils.EMPTY_BYTE_ARRAY, false, 1000);
        Collection<IColumn> columns = cf.getSortedColumns();
        assertEquals(12, columns.size());
        Iterator<IColumn> iter = columns.iterator();
        IColumn column;
        for (int j = 0; j < 8; j++)
        {
            column = iter.next();
            assert Arrays.equals(column.name(), getBytes(j));
        }
        TreeSet<byte[]> columnNames = new TreeSet<byte[]>(new LongType());
        columnNames.add(getBytes(10));
        columnNames.add(getBytes(0));
        cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter("900", new QueryPath("StandardLong1"), columnNames));
        assert "c".equals(new String(cf.getColumn(getBytes(0)).value()));
        assert "c".equals(new String(cf.getColumn(getBytes(10)).value()));
    }

    private void validateTimeSort(Table table) throws IOException
    {
        for (int i = 900; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            for (int j = 0; j < 8; j += 3)
            {
                ColumnFamily cf = table.getColumnFamilyStore("StandardLong1").getColumnFamily(key, new QueryPath("StandardLong1"), getBytes(j * 2), ArrayUtils.EMPTY_BYTE_ARRAY, false, 1000);
                Collection<IColumn> columns = cf.getSortedColumns();
                assert columns.size() == 8 - j;
                int k = j;
                for (IColumn c : columns)
                {
                    assertEquals((k++) * 2, c.timestamp());
                }
            }
        }
    }
}
