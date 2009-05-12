package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import static junit.framework.Assert.assertNull;

public class RemoveColumnFamilyTest
{
    @Test
    public void testRemoveColumnFamily() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Standard1:Column1", "asdf".getBytes(), 0);
        rm.apply();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Standard1", 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Standard1:Column1", new IdentityFilter());
        assert retrieved.isMarkedForDelete();
        assertNull(retrieved.getColumn("Column1"));
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
    }
}