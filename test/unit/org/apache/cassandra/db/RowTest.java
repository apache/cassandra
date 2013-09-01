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

import org.apache.cassandra.SchemaLoader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.Util.column;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.util.concurrent.Uninterruptibles;


public class RowTest extends SchemaLoader
{
    @Test
    public void testDiffColumnFamily()
    {
        ColumnFamily cf1 = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "onev", 0));

        ColumnFamily cf2 = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        DeletionInfo delInfo = new DeletionInfo(0, 0);
        cf2.delete(delInfo);

        ColumnFamily cfDiff = cf1.diff(cf2);
        assertEquals(cfDiff.getColumnCount(), 0);
        assertEquals(cfDiff.deletionInfo(), delInfo);
    }

    @Test
    public void testResolve()
    {
        ColumnFamily cf1 = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "A", 0));

        ColumnFamily cf2 = TreeMapBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        cf2.addColumn(column("one", "B", 1));
        cf2.addColumn(column("two", "C", 1));

        cf1.resolve(cf2);
        assert Arrays.equals(cf1.getColumn(ByteBufferUtil.bytes("one")).value().array(), "B".getBytes());
        assert Arrays.equals(cf1.getColumn(ByteBufferUtil.bytes("two")).value().array(), "C".getBytes());
    }

    @Test
    public void testExpiringColumnExpiration()
    {
        Column c = new ExpiringColumn(ByteBufferUtil.bytes("one"), ByteBufferUtil.bytes("A"), 0, 1);
        assert !c.isMarkedForDelete(System.currentTimeMillis());

        // Because we keep the local deletion time with a precision of a
        // second, we could have to wait 2 seconds in worst case scenario.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        assert c.isMarkedForDelete(System.currentTimeMillis()) && c.getMarkedForDeleteAt() == 0;
    }
}
