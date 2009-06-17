package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import static junit.framework.Assert.assertNull;

public class RemoveSubColumnTest
{
    @Test
    public void testRemoveSubColumn() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super1:SC1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Super1:SC1:Column1", 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Super1:SC1", new IdentityFilter());
        assert retrieved.getColumn("SC1").getSubColumn("Column1").isMarkedForDelete();
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
    }
}