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
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;

import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;


public class TimeSortTest extends SchemaLoader
{
    @Test
    public void testMixedSources() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("StandardLong1");
        RowMutation rm;
        DecoratedKey key = Util.dk("key0");

        rm = new RowMutation("Keyspace1", key.key);
        rm.add("StandardLong1", getBytes(100), ByteBufferUtil.bytes("a"), 100);
        rm.apply();
        cfStore.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", key.key);
        rm.add("StandardLong1", getBytes(0), ByteBufferUtil.bytes("b"), 0);
        rm.apply();

        ColumnFamily cf = cfStore.getColumnFamily(key, getBytes(10), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1000, System.currentTimeMillis());
        Collection<Column> columns = cf.getSortedColumns();
        assert columns.size() == 1;
    }

    @Test
    public void testTimeSort() throws IOException, ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("StandardLong1");

        for (int i = 900; i < 1000; ++i)
        {
            RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes(Integer.toString(i)));
            for (int j = 0; j < 8; ++j)
            {
                rm.add("StandardLong1", getBytes(j * 2), ByteBufferUtil.bytes("a"), j * 2);
            }
            rm.apply();
        }

        validateTimeSort(keyspace);

        cfStore.forceBlockingFlush();
        validateTimeSort(keyspace);

        // interleave some new data to test memtable + sstable
        DecoratedKey key = Util.dk("900");
        RowMutation rm = new RowMutation("Keyspace1", key.key);
        for (int j = 0; j < 4; ++j)
        {
            rm.add("StandardLong1", getBytes(j * 2 + 1), ByteBufferUtil.bytes("b"), j * 2 + 1);
        }
        rm.apply();
        // and some overwrites
        rm = new RowMutation("Keyspace1", key.key);
        rm.add("StandardLong1", getBytes(0), ByteBufferUtil.bytes("c"), 100);
        rm.add("StandardLong1", getBytes(10), ByteBufferUtil.bytes("c"), 100);
        rm.apply();

        // verify
        ColumnFamily cf = cfStore.getColumnFamily(key, getBytes(0), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1000, System.currentTimeMillis());
        Collection<Column> columns = cf.getSortedColumns();
        assertEquals(12, columns.size());
        Iterator<Column> iter = columns.iterator();
        Column column;
        for (int j = 0; j < 8; j++)
        {
            column = iter.next();
            assert column.name().equals(getBytes(j));
        }
        TreeSet<ByteBuffer> columnNames = new TreeSet<ByteBuffer>(LongType.instance);
        columnNames.add(getBytes(10));
        columnNames.add(getBytes(0));
        cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(Util.dk("900"), "StandardLong1", columnNames, System.currentTimeMillis()));
        assert "c".equals(ByteBufferUtil.string(cf.getColumn(getBytes(0)).value()));
        assert "c".equals(ByteBufferUtil.string(cf.getColumn(getBytes(10)).value()));
    }

    private void validateTimeSort(Keyspace keyspace) throws IOException
    {
        for (int i = 900; i < 1000; ++i)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            for (int j = 0; j < 8; j += 3)
            {
                ColumnFamily cf = keyspace.getColumnFamilyStore("StandardLong1").getColumnFamily(key,
                                                                                                 getBytes(j * 2),
                                                                                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                                                 false,
                                                                                                 1000,
                                                                                                 System.currentTimeMillis());
                Collection<Column> columns = cf.getSortedColumns();
                assert columns.size() == 8 - j;
                int k = j;
                for (Column c : columns)
                {
                    assertEquals((k++) * 2, c.timestamp());

                }
            }
        }
    }
}
