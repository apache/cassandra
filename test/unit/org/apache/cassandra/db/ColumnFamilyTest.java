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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.assertEquals;

public class ColumnFamilyTest extends SchemaLoader
{
    static int version = MessagingService.current_version;

    // TODO test SuperColumns more

    @Test
    public void testSingleColumn() throws IOException
    {
        ColumnFamily cf;

        cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf.addColumn(column("C", "v", 1));
        DataOutputBuffer bufOut = new DataOutputBuffer();
        ColumnFamily.serializer.serialize(cf, bufOut, version);

        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        cf = ColumnFamily.serializer.deserialize(new DataInputStream(bufIn), version);
        assert cf != null;
        assert cf.metadata().cfName.equals("Standard1");
        assert cf.getSortedColumns().size() == 1;
    }

    @Test
    public void testManyColumns() throws IOException
    {
        ColumnFamily cf;

        TreeMap<String, String> map = new TreeMap<>();
        for (int i = 100; i < 1000; ++i)
        {
            map.put(Integer.toString(i), "Avinash Lakshman is a good man: " + i);
        }

        // write
        cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        DataOutputBuffer bufOut = new DataOutputBuffer();
        for (String cName : map.navigableKeySet())
        {
            cf.addColumn(column(cName, map.get(cName), 314));
        }
        ColumnFamily.serializer.serialize(cf, bufOut, version);

        // verify
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        cf = ColumnFamily.serializer.deserialize(new DataInputStream(bufIn), version);
        for (String cName : map.navigableKeySet())
        {
            ByteBuffer val = cf.getColumn(cellname(cName)).value();
            assert new String(val.array(),val.position(),val.remaining()).equals(map.get(cName));
        }
        assert Iterables.size(cf.getColumnNames()) == map.size();
    }

    @Test
    public void testGetColumnCount()
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");

        cf.addColumn(column("col1", "", 1));
        cf.addColumn(column("col2", "", 2));
        cf.addColumn(column("col1", "", 3));

        assert 2 == cf.getColumnCount();
        assert 2 == cf.getSortedColumns().size();
    }

    @Test
    public void testTimestamp()
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");

        cf.addColumn(column("col1", "val1", 2));
        cf.addColumn(column("col1", "val2", 2)); // same timestamp, new value
        cf.addColumn(column("col1", "val3", 1)); // older timestamp -- should be ignored

        assert ByteBufferUtil.bytes("val2").equals(cf.getColumn(cellname("col1")).value());
    }

    @Test
    public void testMergeAndAdd()
    {
        ColumnFamily cf_new = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        ColumnFamily cf_old = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        ColumnFamily cf_result = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        ByteBuffer val = ByteBufferUtil.bytes("sample value");
        ByteBuffer val2 = ByteBufferUtil.bytes("x value ");

        cf_new.addColumn(cellname("col1"), val, 3);
        cf_new.addColumn(cellname("col2"), val, 4);

        cf_old.addColumn(cellname("col2"), val2, 1);
        cf_old.addColumn(cellname("col3"), val2, 2);

        cf_result.addAll(cf_new);
        cf_result.addAll(cf_old);

        assert 3 == cf_result.getColumnCount() : "Count is " + cf_new.getColumnCount();
        //addcolumns will only add if timestamp >= old timestamp
        assert val.equals(cf_result.getColumn(cellname("col2")).value());

        // check that tombstone wins timestamp ties
        cf_result.addTombstone(cellname("col1"), 0, 3);
        assert cf_result.getColumn(cellname("col1")).isMarkedForDelete(System.currentTimeMillis());
        cf_result.addColumn(cellname("col1"), val2, 3);
        assert cf_result.getColumn(cellname("col1")).isMarkedForDelete(System.currentTimeMillis());

        // check that column value wins timestamp ties in absence of tombstone
        cf_result.addColumn(cellname("col3"), val, 2);
        assert cf_result.getColumn(cellname("col3")).value().equals(val2);
        cf_result.addColumn(cellname("col3"), ByteBufferUtil.bytes("z"), 2);
        assert cf_result.getColumn(cellname("col3")).value().equals(ByteBufferUtil.bytes("z"));
    }

    @Test
    public void testColumnStatsRecordsRowDeletesCorrectly()
    {
        long timestamp = System.currentTimeMillis();
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf.delete(new DeletionInfo(timestamp, localDeletionTime));
        ColumnStats stats = cf.getColumnStats();
        assertEquals(timestamp, stats.maxTimestamp);
    }
}
