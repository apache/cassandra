package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.SuperColumn;

import static junit.framework.Assert.*;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.superColumn;
import static org.apache.cassandra.db.TableTest.*;

public class RowResolverTest extends SchemaLoader
{
    @Test
    public void testResolveSupersetNewer()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c1", "v2", 1));

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(cf1, cf2));
        assertColumns(resolved, "c1");
        assertColumns(ColumnFamily.diff(cf1, resolved), "c1");
        assertNull(ColumnFamily.diff(cf2, resolved));
    }

    @Test
    public void testResolveSupersetDisjoint()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c2", "v2", 1));

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(cf1, cf2));
        assertColumns(resolved, "c1", "c2");
        assertColumns(ColumnFamily.diff(cf1, resolved), "c2");
        assertColumns(ColumnFamily.diff(cf2, resolved), "c1");
    }

    @Test
    public void testResolveSupersetNullOne()
    {
        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("c2", "v2", 1));

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(null, cf2));
        assertColumns(resolved, "c2");
        assertColumns(ColumnFamily.diff(null, resolved), "c2");
        assertNull(ColumnFamily.diff(cf2, resolved));
    }

    @Test
    public void testResolveSupersetNullTwo()
    {
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("c1", "v1", 0));

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(cf1, null));
        assertColumns(resolved, "c1");
        assertNull(ColumnFamily.diff(cf1, resolved));
        assertColumns(ColumnFamily.diff(null, resolved), "c1");
    }

    @Test
    public void testResolveSupersetNullBoth()
    {
        assertNull(RowRepairResolver.resolveSuperset(Arrays.<ColumnFamily>asList(null, null)));
    }

    @Test
    public void testResolveDeleted()
    {
        // one CF with columns timestamped before a delete in another cf
        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.addColumn(column("one", "A", 0));

        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.delete((int) (System.currentTimeMillis() / 1000), 1);

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(cf1, cf2));
        // no columns in the cf
        assertColumns(resolved);
        assertTrue(resolved.isMarkedForDelete());
        assertEquals(1, resolved.getMarkedForDeleteAt());

        ColumnFamily scf1 = ColumnFamily.create("Keyspace1", "Super1");
        scf1.addColumn(superColumn(scf1, "super-foo", column("one", "A", 0)));

        ColumnFamily scf2 = ColumnFamily.create("Keyspace1", "Super1");
        scf2.delete((int) (System.currentTimeMillis() / 1000), 1);

        ColumnFamily superResolved = RowRepairResolver.resolveSuperset(Arrays.asList(scf1, scf2));
        // no columns in the cf
        assertColumns(superResolved);
        assertTrue(superResolved.isMarkedForDelete());
        assertEquals(1, superResolved.getMarkedForDeleteAt());
    }

    @Test
    public void testResolveDeletedSuper()
    {
        // subcolumn is newer than a tombstone on its parent, but not newer than the row deletion
        ColumnFamily scf1 = ColumnFamily.create("Keyspace1", "Super1");
        SuperColumn sc = superColumn(scf1, "super-foo", column("one", "A", 1));
        sc.delete((int) (System.currentTimeMillis() / 1000), 0);
        scf1.addColumn(sc);

        ColumnFamily scf2 = ColumnFamily.create("Keyspace1", "Super1");
        scf2.delete((int) (System.currentTimeMillis() / 1000), 2);

        ColumnFamily superResolved = RowRepairResolver.resolveSuperset(Arrays.asList(scf1, scf2));
        // no columns in the cf
        assertColumns(superResolved);
        assertTrue(superResolved.isMarkedForDelete());
        assertEquals(2, superResolved.getMarkedForDeleteAt());
    }

    @Test
    public void testResolveMultipleDeleted()
    {
        // deletes and columns with interleaved timestamp, with out of order return sequence

        ColumnFamily cf1 = ColumnFamily.create("Keyspace1", "Standard1");
        cf1.delete((int) (System.currentTimeMillis() / 1000), 0);

        // these columns created after the previous deletion
        ColumnFamily cf2 = ColumnFamily.create("Keyspace1", "Standard1");
        cf2.addColumn(column("one", "A", 1));
        cf2.addColumn(column("two", "A", 1));

        //this column created after the next delete
        ColumnFamily cf3 = ColumnFamily.create("Keyspace1", "Standard1");
        cf3.addColumn(column("two", "B", 3));

        ColumnFamily cf4 = ColumnFamily.create("Keyspace1", "Standard1");
        cf4.delete((int) (System.currentTimeMillis() / 1000), 2);

        ColumnFamily resolved = RowRepairResolver.resolveSuperset(Arrays.asList(cf1, cf2, cf3, cf4));
        // will have deleted marker and one column
        assertColumns(resolved, "two");
        assertColumn(resolved, "two", "B", 3);
        assertTrue(resolved.isMarkedForDelete());
        assertEquals(2, resolved.getMarkedForDeleteAt());


        ColumnFamily scf1 = ColumnFamily.create("Keyspace1", "Super1");
        scf1.delete((int) (System.currentTimeMillis() / 1000), 0);

        // these columns created after the previous deletion
        ColumnFamily scf2 = ColumnFamily.create("Keyspace1", "Super1");
        scf2.addColumn(superColumn(scf2, "super1", column("one", "A", 1), column("two", "A", 1)));

        //these columns created after the next delete
        ColumnFamily scf3 = ColumnFamily.create("Keyspace1", "Super1");
        scf3.addColumn(superColumn(scf3, "super1", column("two", "B", 3)));
        scf3.addColumn(superColumn(scf3, "super2", column("three", "A", 3), column("four", "A", 3)));

        ColumnFamily scf4 = ColumnFamily.create("Keyspace1", "Super1");
        scf4.delete((int) (System.currentTimeMillis() / 1000), 2);

        ColumnFamily superResolved = RowRepairResolver.resolveSuperset(Arrays.asList(scf1, scf2, scf3, scf4));
        // will have deleted marker and two super cols
        assertColumns(superResolved, "super1", "super2");

        assertSubColumns(superResolved, "super1", "two");
        assertSubColumn(superResolved, "super1", "two", "B", 3);

        assertSubColumns(superResolved, "super2", "four", "three");
        assertSubColumn(superResolved, "super2", "three", "A", 3);
        assertSubColumn(superResolved, "super2", "four", "A", 3);

        assertTrue(superResolved.isMarkedForDelete());
        assertEquals(2, superResolved.getMarkedForDeleteAt());
    }
}
