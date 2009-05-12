package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.Set;
import java.util.HashSet;

import org.junit.Test;

import org.apache.cassandra.io.SSTable;
import static junit.framework.Assert.assertEquals;

public class CompactionsTest
{
    @Test
    public void testCompactions() throws IOException, ExecutionException, InterruptedException
    {
        // this test does enough rows to force multiple block indexes to be used
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        Set<String> inserted = new HashSet<String>();
        for (int j = 0; j < (SSTable.indexInterval() * 3) / ROWS_PER_SSTABLE; j++) {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++) {
                String key = String.valueOf(i % 2);
                RowMutation rm = new RowMutation("Table1", key);
                rm.add("Standard1:" + (i / 2), new byte[0], j * ROWS_PER_SSTABLE + i);
                rm.apply();
                inserted.add(key);
            }
            store.forceBlockingFlush();
            assertEquals(table.getKeyRange("", "", 10000).size(), inserted.size());
        }
        while (true)
        {
            Future<Integer> ft = MinorCompactionManager.instance().submit(store);
            if (ft.get() == 0)
                break;
        }
        if (store.getSSTableFilenames().size() > 1)
        {
            store.doCompaction(store.getSSTableFilenames().size());
        }
        assertEquals(table.getKeyRange("", "", 10000).size(), inserted.size());
    }
}
