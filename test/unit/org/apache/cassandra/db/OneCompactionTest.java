package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.HashSet;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class OneCompactionTest
{
    @Test
    public void testOneCompaction() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < 2; j++) {
            String key = "0";
            RowMutation rm = new RowMutation("Table1", key);
            rm.add("Standard1:0", new byte[0], j);
            rm.apply();
            inserted.add(key);
            store.forceBlockingFlush();
            assertEquals(table.getKeyRange("", "", 10000).size(), inserted.size());
        }
        store.doCompaction(2);
        assertEquals(table.getKeyRange("", "", 10000).size(), inserted.size());
    }
}
