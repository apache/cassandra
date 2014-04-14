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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.SchemaLoader;
import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;

import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.ByteBufferUtil;


public class TimeSortTest extends SchemaLoader
{
    @Test
    public void testMixedSources()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("StandardLong1");
        Mutation rm;
        DecoratedKey key = Util.dk("key0");

        rm = new Mutation("Keyspace1", key.getKey());
        rm.add("StandardLong1", cellname(100), ByteBufferUtil.bytes("a"), 100);
        rm.apply();
        cfStore.forceBlockingFlush();

        rm = new Mutation("Keyspace1", key.getKey());
        rm.add("StandardLong1", cellname(0), ByteBufferUtil.bytes("b"), 0);
        rm.apply();

        ColumnFamily cf = cfStore.getColumnFamily(key, cellname(10), Composites.EMPTY, false, 1000, System.currentTimeMillis());
        Collection<Cell> cells = cf.getSortedColumns();
        assert cells.size() == 1;
    }

    @Test
    public void testTimeSort() throws IOException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfStore = keyspace.getColumnFamilyStore("StandardLong1");

        for (int i = 900; i < 1000; ++i)
        {
            Mutation rm = new Mutation("Keyspace1", ByteBufferUtil.bytes(Integer.toString(i)));
            for (int j = 0; j < 8; ++j)
            {
                rm.add("StandardLong1", cellname(j * 2), ByteBufferUtil.bytes("a"), j * 2);
            }
            rm.apply();
        }

        validateTimeSort(keyspace);

        cfStore.forceBlockingFlush();
        validateTimeSort(keyspace);

        // interleave some new data to test memtable + sstable
        DecoratedKey key = Util.dk("900");
        Mutation rm = new Mutation("Keyspace1", key.getKey());
        for (int j = 0; j < 4; ++j)
        {
            rm.add("StandardLong1", cellname(j * 2 + 1), ByteBufferUtil.bytes("b"), j * 2 + 1);
        }
        rm.apply();
        // and some overwrites
        rm = new Mutation("Keyspace1", key.getKey());
        rm.add("StandardLong1", cellname(0), ByteBufferUtil.bytes("c"), 100);
        rm.add("StandardLong1", cellname(10), ByteBufferUtil.bytes("c"), 100);
        rm.apply();

        // verify
        ColumnFamily cf = cfStore.getColumnFamily(key, cellname(0), Composites.EMPTY, false, 1000, System.currentTimeMillis());
        Collection<Cell> cells = cf.getSortedColumns();
        assertEquals(12, cells.size());
        Iterator<Cell> iter = cells.iterator();
        Cell cell;
        for (int j = 0; j < 8; j++)
        {
            cell = iter.next();
            assert cell.name().toByteBuffer().equals(getBytes(j));
        }
        TreeSet<CellName> columnNames = new TreeSet<CellName>(cfStore.getComparator());
        columnNames.add(cellname(10));
        columnNames.add(cellname(0));
        cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(Util.dk("900"), "StandardLong1", columnNames, System.currentTimeMillis()));
        assert "c".equals(ByteBufferUtil.string(cf.getColumn(cellname(0)).value()));
        assert "c".equals(ByteBufferUtil.string(cf.getColumn(cellname(10)).value()));
    }

    private void validateTimeSort(Keyspace keyspace)
    {
        for (int i = 900; i < 1000; ++i)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            for (int j = 0; j < 8; j += 3)
            {
                ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("StandardLong1");
                ColumnFamily cf = cfs.getColumnFamily(key, cellname(j * 2), Composites.EMPTY, false, 1000, System.currentTimeMillis());
                Collection<Cell> cells = cf.getSortedColumns();
                assert cells.size() == 8 - j;
                int k = j;
                for (Cell c : cells)
                {
                    assertEquals((k++) * 2, c.timestamp());

                }
            }
        }
    }
}
