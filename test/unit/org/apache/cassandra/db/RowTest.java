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
import java.util.Arrays;

import org.apache.cassandra.SchemaLoader;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import org.apache.cassandra.db.marshal.AsciiType;
import static org.apache.cassandra.Util.column;
import org.apache.cassandra.utils.ByteBufferUtil;


public class RowTest extends SchemaLoader
{
    @Test
    public void testDiffColumnFamily()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "onev", 0));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.delete(0, 0);

        ColumnFamily cfDiff = cf1.diff(cf2);
        assertEquals(cfDiff.getColumnCount(), 0);
        assertEquals(cfDiff.getMarkedForDeleteAt(), 0);
    }

    @Test
    public void testDiffSuperColumn()
    {
        SuperColumn sc1 = new SuperColumn(ByteBufferUtil.bytes("one"), AsciiType.instance);
        sc1.addColumn(column("subcolumn", "A", 0));

        SuperColumn sc2 = new SuperColumn(ByteBufferUtil.bytes("one"), AsciiType.instance);
        sc2.delete(0, 0);

        SuperColumn scDiff = (SuperColumn)sc1.diff(sc2);
        assertEquals(scDiff.getSubColumns().size(), 0);
        assertEquals(scDiff.getMarkedForDeleteAt(), 0);
    }

    @Test
    public void testResolve()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "A", 0));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
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
        assert !c.isMarkedForDelete();

        try
        {
            // Because we keep the local deletion time with a precision of a
            // second, we could have to wait 2 seconds in worst case scenario.
            Thread.sleep(2000);
        }
        catch (InterruptedException e)
        {
            fail("Cannot test column expiration if you wake me up too early");
        }

        assert c.isMarkedForDelete() && c.getMarkedForDeleteAt() == 0;
    }
}
