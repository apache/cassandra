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

import org.apache.cassandra.SchemaLoader;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.db.marshal.AsciiType;
import static org.apache.cassandra.Util.column;

public class RowTest extends SchemaLoader
{
    @Test
    public void testDiffColumnFamily()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "onev", new TimestampClock(0)));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.delete(0, new TimestampClock(0));

        ColumnFamily cfDiff = cf1.diff(cf2);
        assertEquals(cfDiff.getColumnsMap().size(), 0);
        assertEquals(((TimestampClock) cfDiff.getMarkedForDeleteAt()).timestamp(), 0);
    }

    @Test
    public void testDiffSuperColumn()
    {
        SuperColumn sc1 = new SuperColumn("one".getBytes(), AsciiType.instance, ClockType.Timestamp, null);
        sc1.addColumn(column("subcolumn", "A", new TimestampClock(0)));

        SuperColumn sc2 = new SuperColumn("one".getBytes(), AsciiType.instance, ClockType.Timestamp, null);
        sc2.markForDeleteAt(0, new TimestampClock(0));

        SuperColumn scDiff = (SuperColumn)sc1.diff(sc2);
        assertEquals(scDiff.getSubColumns().size(), 0);
        assertEquals(((TimestampClock)scDiff.getMarkedForDeleteAt()).timestamp(), 0);
    }

    @Test
    public void testResolve()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "A", new TimestampClock(0)));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("one", "B", new TimestampClock(1)));
        cf2.addColumn(column("two", "C", new TimestampClock(1)));

        cf1.resolve(cf2);
        assert Arrays.equals(cf1.getColumn("one".getBytes()).value(), "B".getBytes());
        assert Arrays.equals(cf1.getColumn("two".getBytes()).value(), "C".getBytes());
    }
}
