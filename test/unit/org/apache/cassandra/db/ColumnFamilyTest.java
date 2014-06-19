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
import java.util.TreeMap;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertTrue;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.tombstone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
    public void testDigest()
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");

        ByteBuffer digest = ColumnFamily.digest(cf);

        cf.addColumn(column("col1", "", 1));
        cf2.addColumn(column("col1", "", 1));

        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        assert digest.equals(ColumnFamily.digest(cf2));

        cf.addColumn(column("col2", "", 2));
        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        cf.addColumn(column("col1", "", 3));
        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        cf.delete(new DeletionTime(4, 4));
        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        cf.delete(tombstone("col1", "col11", 5, 5));
        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        assert digest.equals(ColumnFamily.digest(cf));

        cf.delete(tombstone("col2", "col21", 5, 5));
        assert !digest.equals(ColumnFamily.digest(cf));

        digest = ColumnFamily.digest(cf);
        cf.delete(tombstone("col1", "col11", 5, 5)); // this does not change RangeTombstoneLList
        assert digest.equals(ColumnFamily.digest(cf));
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
        assertFalse(cf_result.getColumn(cellname("col1")).isLive());
        cf_result.addColumn(cellname("col1"), val2, 3);
        assertFalse(cf_result.getColumn(cellname("col1")).isLive());

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

        cf.delete(new RangeTombstone(cellname("col2"), cellname("col21"), timestamp, localDeletionTime));

        stats = cf.getColumnStats();
        assertEquals(ByteBufferUtil.bytes("col2"), stats.minColumnNames.get(0));
        assertEquals(ByteBufferUtil.bytes("col21"), stats.maxColumnNames.get(0));

        cf.delete(new RangeTombstone(cellname("col6"), cellname("col61"), timestamp, localDeletionTime));
        stats = cf.getColumnStats();

        assertEquals(ByteBufferUtil.bytes("col2"), stats.minColumnNames.get(0));
        assertEquals(ByteBufferUtil.bytes("col61"), stats.maxColumnNames.get(0));
    }

    @Test
    public void testCounterDeletion()
    {
        long timestamp = FBUtilities.timestampMicros();
        CellName name = cellname("counter1");

        BufferCounterCell counter = new BufferCounterCell(name,
                                                          CounterContext.instance().createGlobal(CounterId.fromInt(1), 1, 1),
                                                          timestamp);
        BufferDeletedCell tombstone = new BufferDeletedCell(name, (int) (System.currentTimeMillis() / 1000), 0L);

        // check that the tombstone won the reconcile despite the counter cell having a higher timestamp
        assertTrue(counter.reconcile(tombstone) == tombstone);

        // check that a range tombstone overrides the counter cell, even with a lower timestamp than the counter
        ColumnFamily cf0 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Counter1");
        cf0.addColumn(counter);
        cf0.delete(new RangeTombstone(cellname("counter0"), cellname("counter2"), 0L, (int) (System.currentTimeMillis() / 1000)));
        assertTrue(cf0.deletionInfo().isDeleted(counter));
        assertTrue(cf0.deletionInfo().inOrderTester(false).isDeleted(counter));

        // check that a top-level deletion info overrides the counter cell, even with a lower timestamp than the counter
        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Counter1");
        cf1.addColumn(counter);
        cf1.delete(new DeletionInfo(0L, (int) (System.currentTimeMillis() / 1000)));
        assertTrue(cf1.deletionInfo().isDeleted(counter));
    }
}
