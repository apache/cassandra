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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Collection;

import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.db.filter.IdentityQueryFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import static org.apache.cassandra.Util.addMutation;
import static org.apache.cassandra.Util.getBytes;

public class RemoveSuperColumnTest
{
    @Test
    public void testRemoveSuperColumn() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Keyspace1", "key1");
        addMutation(rm, "Super1", "SC1", 1, "val1", 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", "key1");
        rm.delete(new QueryPath("Super1", "SC1".getBytes()), 1);
        rm.apply();

        validateRemoveTwoSources();

        store.forceBlockingFlush();
        validateRemoveTwoSources();

        Future<Integer> ft = MinorCompactionManager.instance().submit(store, 2, 32);
        ft.get();
        assertEquals(1, store.getSSTables().size());
        validateRemoveCompacted();
    }

    private void validateRemoveTwoSources() throws IOException
    {
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Super1");
        ColumnFamily resolved = store.getColumnFamily(new NamesQueryFilter("key1", new QueryPath("Super1"), "SC1".getBytes()));
        assert resolved.getSortedColumns().iterator().next().getMarkedForDeleteAt() == 1;
        assert resolved.getSortedColumns().iterator().next().getSubColumns().size() == 0;
        assertNull(ColumnFamilyStore.removeDeleted(resolved, Integer.MAX_VALUE));
        assertNull(ColumnFamilyStore.removeDeleted(store.getColumnFamily(new IdentityQueryFilter("key1", new QueryPath("Super1"))), Integer.MAX_VALUE));
    }

    private void validateRemoveCompacted() throws IOException
    {
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Super1");
        ColumnFamily resolved = store.getColumnFamily(new NamesQueryFilter("key1", new QueryPath("Super1"), "SC1".getBytes()));
        assert resolved.getSortedColumns().iterator().next().getMarkedForDeleteAt() == 1;
        Collection<IColumn> subColumns = resolved.getSortedColumns().iterator().next().getSubColumns();
        assert subColumns.size() == 0;
    }

    @Test
    public void testRemoveSuperColumnWithNewData() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Super2");
        RowMutation rm;

        // add data
        rm = new RowMutation("Keyspace1", "key1");
        addMutation(rm, "Super2", "SC1", 1, "val1", 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", "key1");
        rm.delete(new QueryPath("Super2", "SC1".getBytes()), 1);
        rm.apply();

        // new data
        rm = new RowMutation("Keyspace1", "key1");
        addMutation(rm, "Super2", "SC1", 2, "val2", 2);
        rm.apply();

        validateRemoveWithNewData();

        store.forceBlockingFlush();
        validateRemoveWithNewData();

        Future<Integer> ft = MinorCompactionManager.instance().submit(store, 2, 32);
        ft.get();
        assertEquals(1, store.getSSTables().size());
        validateRemoveWithNewData();
    }

    private void validateRemoveWithNewData() throws IOException
    {
        ColumnFamilyStore store = Table.open("Keyspace1").getColumnFamilyStore("Super2");
        ColumnFamily resolved = store.getColumnFamily(new NamesQueryFilter("key1", new QueryPath("Super2", "SC1".getBytes()), getBytes(2)));
        Collection<IColumn> subColumns = resolved.getSortedColumns().iterator().next().getSubColumns();
        assert subColumns.size() == 1;
        assert subColumns.iterator().next().timestamp() == 2;
    }

}
