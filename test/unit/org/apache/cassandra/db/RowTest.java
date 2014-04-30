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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.tombstone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RowTest extends SchemaLoader
{
    @Test
    public void testDiffColumnFamily()
    {
        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "onev", 0));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        DeletionInfo delInfo = new DeletionInfo(0, 0);
        cf2.delete(delInfo);

        ColumnFamily cfDiff = cf1.diff(cf2);
        assertFalse(cfDiff.hasColumns());
        assertEquals(cfDiff.deletionInfo(), delInfo);

        RangeTombstone tombstone1 = tombstone("1", "11", (long) 123, 123);
        RangeTombstone tombstone1_2 = tombstone("111", "112", (long) 1230, 123);
        RangeTombstone tombstone2_1 = tombstone("2", "22", (long) 123, 123);
        RangeTombstone tombstone2_2 = tombstone("2", "24", (long) 123, 123);
        RangeTombstone tombstone3_1 = tombstone("3", "31", (long) 123, 123);
        RangeTombstone tombstone3_2 = tombstone("3", "31", (long) 1230, 123);
        RangeTombstone tombstone4_1 = tombstone("4", "41", (long) 123, 123);
        RangeTombstone tombstone4_2 = tombstone("4", "41", (long) 123, 1230);
        RangeTombstone tombstone5_2 = tombstone("5", "51", (long) 123, 1230);
        cf1.delete(tombstone1);
        cf1.delete(tombstone2_1);
        cf1.delete(tombstone3_1);
        cf1.delete(tombstone4_1);

        cf2.delete(tombstone1);
        cf2.delete(tombstone1_2);
        cf2.delete(tombstone2_2);
        cf2.delete(tombstone3_2);
        cf2.delete(tombstone4_2);
        cf2.delete(tombstone5_2);

        cfDiff = cf1.diff(cf2);
        assertEquals(0, cfDiff.getColumnCount());

        // only tmbstones which differ in superset or have more recent timestamp to be in diff
        delInfo.add(tombstone1_2, cf1.getComparator());
        delInfo.add(tombstone2_2, cf1.getComparator());
        delInfo.add(tombstone3_2, cf1.getComparator());
        delInfo.add(tombstone5_2, cf1.getComparator());

        assertEquals(delInfo, cfDiff.deletionInfo());
    }

    @Test
    public void testResolve()
    {
        ColumnFamily cf1 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "A", 0));

        ColumnFamily cf2 = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf2.addColumn(column("one", "B", 1));
        cf2.addColumn(column("two", "C", 1));

        cf1.addAll(cf2);
        assert Arrays.equals(cf1.getColumn(CellNames.simpleDense(ByteBufferUtil.bytes("one"))).value().array(), "B".getBytes());
        assert Arrays.equals(cf1.getColumn(CellNames.simpleDense(ByteBufferUtil.bytes("two"))).value().array(), "C".getBytes());
    }

    @Test
    public void testExpiringColumnExpiration()
    {
        Cell c = new BufferExpiringCell(CellNames.simpleDense(ByteBufferUtil.bytes("one")), ByteBufferUtil.bytes("A"), 0, 1);
        assertTrue(c.isLive());

        // Because we keep the local deletion time with a precision of a
        // second, we could have to wait 2 seconds in worst case scenario.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        assert !c.isLive() && c.timestamp() == 0;
    }
}
