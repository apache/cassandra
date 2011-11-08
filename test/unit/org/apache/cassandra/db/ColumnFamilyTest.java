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

import org.apache.cassandra.SchemaLoader;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.db.filter.QueryPath;
import static org.apache.cassandra.Util.column;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;


public class ColumnFamilyTest extends SchemaLoader
{
    // TODO test SuperColumns more

    @Test
    public void testSingleColumn() throws IOException
    {
        ColumnFamily cf;

        cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("C", "v", 1));
        DataOutputBuffer bufOut = new DataOutputBuffer();
        ColumnFamily.serializer().serialize(cf, bufOut);

        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        cf = ColumnFamily.serializer().deserialize(new DataInputStream(bufIn));
        assert cf != null;
        assert cf.metadata().cfName.equals("Standard1");
        assert cf.getSortedColumns().size() == 1;
    }

    @Test
    public void testManyColumns() throws IOException
    {
        ColumnFamily cf;

        TreeMap<String, String> map = new TreeMap<String, String>();
        for (int i = 100; i < 1000; ++i)
        {
            map.put(Integer.toString(i), "Avinash Lakshman is a good man: " + i);
        }

        // write
        cf = ColumnFamily.create("Keyspace1", "Standard1");
        DataOutputBuffer bufOut = new DataOutputBuffer();
        for (String cName : map.navigableKeySet())
        {
            cf.addColumn(column(cName, map.get(cName), 314));
        }
        ColumnFamily.serializer().serialize(cf, bufOut);

        // verify
        ByteArrayInputStream bufIn = new ByteArrayInputStream(bufOut.getData(), 0, bufOut.getLength());
        cf = ColumnFamily.serializer().deserialize(new DataInputStream(bufIn));
        for (String cName : map.navigableKeySet())
        {
            ByteBuffer val = cf.getColumn(ByteBufferUtil.bytes(cName)).value();
            assert new String(val.array(),val.position(),val.remaining()).equals(map.get(cName));
        }
        assert cf.getColumnNames().size() == map.size();
    }

    @Test
    public void testGetColumnCount()
    {
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");

        cf.addColumn(column("col1", "", 1));
        cf.addColumn(column("col2", "", 2));
        cf.addColumn(column("col1", "", 3));

        assert 2 == cf.getColumnCount();
        assert 2 == cf.getSortedColumns().size();
    }

    @Test
    public void testTimestamp()
    {
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");

        cf.addColumn(column("col1", "val1", 2));
        cf.addColumn(column("col1", "val2", 2)); // same timestamp, new value
        cf.addColumn(column("col1", "val3", 1)); // older timestamp -- should be ignored

        assert ByteBufferUtil.bytes("val2").equals(cf.getColumn(ByteBufferUtil.bytes("col1")).value());
    }

    @Test
    public void testMergeAndAdd()
    {
        ColumnFamily cf_new = ColumnFamily.create("Keyspace1", "Standard1");
        ColumnFamily cf_old = ColumnFamily.create("Keyspace1", "Standard1");
        ColumnFamily cf_result = ColumnFamily.create("Keyspace1", "Standard1");
        ByteBuffer val = ByteBufferUtil.bytes("sample value");
        ByteBuffer val2 = ByteBufferUtil.bytes("x value ");

        // exercise addColumn(QueryPath, ...)
        cf_new.addColumn(QueryPath.column(ByteBufferUtil.bytes("col1")), val, 3);
        cf_new.addColumn(QueryPath.column(ByteBufferUtil.bytes("col2")), val, 4);

        cf_old.addColumn(QueryPath.column(ByteBufferUtil.bytes("col2")), val2, 1);
        cf_old.addColumn(QueryPath.column(ByteBufferUtil.bytes("col3")), val2, 2);

        cf_result.addAll(cf_new, HeapAllocator.instance);
        cf_result.addAll(cf_old, HeapAllocator.instance);

        assert 3 == cf_result.getColumnCount() : "Count is " + cf_new.getColumnCount();
        //addcolumns will only add if timestamp >= old timestamp
        assert val.equals(cf_result.getColumn(ByteBufferUtil.bytes("col2")).value());

        // check that tombstone wins timestamp ties
        cf_result.addTombstone(ByteBufferUtil.bytes("col1"), 0, 3);
        assert cf_result.getColumn(ByteBufferUtil.bytes("col1")).isMarkedForDelete();
        cf_result.addColumn(QueryPath.column(ByteBufferUtil.bytes("col1")), val2, 3);
        assert cf_result.getColumn(ByteBufferUtil.bytes("col1")).isMarkedForDelete();

        // check that column value wins timestamp ties in absence of tombstone
        cf_result.addColumn(QueryPath.column(ByteBufferUtil.bytes("col3")), val, 2);
        assert cf_result.getColumn(ByteBufferUtil.bytes("col3")).value().equals(val2);
        cf_result.addColumn(QueryPath.column(ByteBufferUtil.bytes("col3")), ByteBufferUtil.bytes("z"), 2);
        assert cf_result.getColumn(ByteBufferUtil.bytes("col3")).value().equals(ByteBufferUtil.bytes("z"));
    }

    private void testSuperColumnResolution(ISortedColumns.Factory factory)
    {
        ColumnFamilyStore cfs = Table.open("Keyspace1").getColumnFamilyStore("Super1");
        ColumnFamily cf = ColumnFamily.create(cfs.metadata, factory);
        ByteBuffer superColumnName = ByteBufferUtil.bytes("sc");
        ByteBuffer subColumnName = ByteBufferUtil.bytes(1L);

        Column first = new Column(subColumnName, ByteBufferUtil.bytes("one"), 1L);
        Column second = new Column(subColumnName, ByteBufferUtil.bytes("two"), 2L);

        cf.addColumn(superColumnName, first);

        // resolve older + new
        cf.addColumn(superColumnName, second);
        assertEquals(second, cf.getColumn(superColumnName).getSubColumn(subColumnName));

        // resolve new + older
        cf.addColumn(superColumnName, first);
        assertEquals(second, cf.getColumn(superColumnName).getSubColumn(subColumnName));
    }

    @Test
    public void testSuperColumnResolution()
    {
        testSuperColumnResolution(TreeMapBackedSortedColumns.factory());
        testSuperColumnResolution(ThreadSafeSortedColumns.factory());
        // array-sorted does allow conflict resolution IF it is the last column.  Bit of an edge case.
        testSuperColumnResolution(ArrayBackedSortedColumns.factory());
    }
}
