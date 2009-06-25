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
import java.util.List;
import java.util.Collection;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;

public class RemoveSuperColumnTest
{
    @Test
    public void testRemoveSuperColumn() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = Table.open("Table1").getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super1:SC1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Super1:SC1", 1);
        rm.apply();

        validateRemoveTwoSources();

        store.forceBlockingFlush();
        validateRemoveTwoSources();

        Future<Integer> ft = MinorCompactionManager.instance().submit(store, 2);
        ft.get();
        assertEquals(1, store.getSSTables().size());
        validateRemoveCompacted();
    }

    private void validateRemoveTwoSources() throws IOException
    {
        ColumnFamilyStore store = Table.open("Table1").getColumnFamilyStore("Super1");
        List<ColumnFamily> families = store.getColumnFamilies("key1", "Super1", new IdentityFilter());
        assert families.size() == 2 : StringUtils.join(families, ", ");
        assert families.get(0).getAllColumns().first().getMarkedForDeleteAt() == 1; // delete marker, just added
        assert !families.get(1).getAllColumns().first().isMarkedForDelete(); // flushed old version
        ColumnFamily resolved = ColumnFamily.resolve(families);
        assert resolved.getAllColumns().first().getMarkedForDeleteAt() == 1;
        Collection<IColumn> subColumns = resolved.getAllColumns().first().getSubColumns();
        assert subColumns.size() == 1;
        assert subColumns.iterator().next().timestamp() == 0;
        assertNull(ColumnFamilyStore.removeDeleted(resolved, Integer.MAX_VALUE));
    }

    private void validateRemoveCompacted() throws IOException
    {
        ColumnFamilyStore store = Table.open("Table1").getColumnFamilyStore("Super1");
        List<ColumnFamily> families = store.getColumnFamilies("key1", "Super1", new IdentityFilter());
        assert families.size() == 1 : StringUtils.join(families, ", ");
        ColumnFamily resolved = families.get(0);
        assert resolved.getAllColumns().first().getMarkedForDeleteAt() == 1;
        Collection<IColumn> subColumns = resolved.getAllColumns().first().getSubColumns();
        assert subColumns.size() == 0;
    }

    @Test
    public void testRemoveSuperColumnWithNewData() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore store = Table.open("Table1").getColumnFamilyStore("Super2");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super2:SC1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Super2:SC1", 1);
        rm.apply();

        // new data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super2:SC1:Column2", "asdf".getBytes(), 2);
        rm.apply();

        validateRemoveWithNewData();

        store.forceBlockingFlush();
        validateRemoveWithNewData();

        Future<Integer> ft = MinorCompactionManager.instance().submit(store, 2);
        ft.get();
        assertEquals(1, store.getSSTables().size());
        validateRemoveWithNewData();
    }

    private void validateRemoveWithNewData() throws IOException
    {
        ColumnFamilyStore store = Table.open("Table1").getColumnFamilyStore("Super2");
        List<ColumnFamily> families;
        ColumnFamily resolved;

        families = store.getColumnFamilies("key1", "Super2:SC1", new NamesFilter(Arrays.asList("Column2")));
        resolved = ColumnFamilyStore.removeDeleted(ColumnFamily.resolve(families));
        validateNewDataFamily(resolved);

        resolved = store.getColumnFamily("key1", "Super2:SC1:Column2", new IdentityFilter());
        validateNewDataFamily(resolved);
    }

    private void validateNewDataFamily(ColumnFamily resolved)
    {
        Collection<IColumn> subColumns = resolved.getAllColumns().first().getSubColumns();
        assert subColumns.size() == 1;
        assert subColumns.iterator().next().timestamp() == 2;
    }
}
