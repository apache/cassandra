package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class RecoveryManager2Test extends CleanupHelper
{
    @Test
    public void testWithFlush() throws IOException, ExecutionException, InterruptedException
    {
        Table table1 = Table.open("Table1");
        Set<String> keys = new HashSet<String>();

        for (int i = 0; i < 100; i++)
        {
            String key = "key" + i;
            RowMutation rm = new RowMutation("Table1", key);
            ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
            cf.addColumn(new Column("col1", "val1".getBytes(), 1L));
            rm.add(cf);
            rm.apply();
            keys.add(key);
        }
        table1.getColumnFamilyStore("Standard1").forceBlockingFlush();

        table1.getColumnFamilyStore("Standard1").clearUnsafe();
        RecoveryManager.doRecovery();

        Set<String> foundKeys = new HashSet<String>(table1.getKeyRange("Standard1", "", "", 1000));
        assert keys.equals(foundKeys);
    }
}
