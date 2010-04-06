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
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.cassandra.SchemaLoader;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.db.filter.QueryPath;
import static org.apache.cassandra.Util.column;

public class ColumnFamilyTest extends SchemaLoader
{
    // TODO test SuperColumns

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
        assert cf.name().equals("Standard1");
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
            assert new String(cf.getColumn(cName.getBytes()).value()).equals(map.get(cName));
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

        assert Arrays.equals("val2".getBytes(), cf.getColumn("col1".getBytes()).value());
    }

    @Test
    public void testMergeAndAdd()
    {
        ColumnFamily cf_new = ColumnFamily.create("Keyspace1", "Standard1");
        ColumnFamily cf_old = ColumnFamily.create("Keyspace1", "Standard1");
        ColumnFamily cf_result = ColumnFamily.create("Keyspace1", "Standard1");
        byte val[] = "sample value".getBytes();
        byte val2[] = "x value ".getBytes();

        // exercise addColumn(QueryPath, ...)
        cf_new.addColumn(QueryPath.column("col1".getBytes()), val, 3);
        cf_new.addColumn(QueryPath.column("col2".getBytes()), val, 4);

        cf_old.addColumn(QueryPath.column("col2".getBytes()), val2, 1);
        cf_old.addColumn(QueryPath.column("col3".getBytes()), val2, 2);

        cf_result.addAll(cf_new);
        cf_result.addAll(cf_old);

        assert 3 == cf_result.getColumnCount() : "Count is " + cf_new.getColumnCount();
        //addcolumns will only add if timestamp >= old timestamp
        assert Arrays.equals(val, cf_result.getColumn("col2".getBytes()).value());
    }
}
