package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.SortedSet;
import java.util.Iterator;
import java.util.Collection;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

public class NameSortTest extends ColumnFamilyStoreTest
{
    @Test
    public void testNameSort1() throws IOException, ExecutionException, InterruptedException
    {
        // single key
        testNameSort(1);
    }

    @Test
    public void testNameSort10() throws IOException, ExecutionException, InterruptedException
    {
        // multiple keys, flushing concurrently w/ inserts
        testNameSort(10);
    }

    @Test
    public void testNameSort100() throws IOException, ExecutionException, InterruptedException
    {
        // enough keys to force compaction concurrently w/ inserts
        testNameSort(100);
    }


    private void testNameSort(int N) throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");

        for (int i = 0; i < N; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm;

            // standard
            for (int j = 0; j < 8; ++j)
            {
                byte[] bytes = j % 2 == 0 ? bytes1 : bytes2;
                rm = new RowMutation("Table1", key);
                rm.add("Standard1:" + "Column-" + j, bytes, j);
                rm.apply();
            }

            // super
            for (int j = 0; j < 8; ++j)
            {
                for (int k = 0; k < 4; ++k)
                {
                    byte[] bytes = (j + k) % 2 == 0 ? bytes1 : bytes2;
                    rm = new RowMutation("Table1", key);
                    rm.add("Super1:" + "SuperColumn-" + j + ":Column-" + k, bytes, k);
                    rm.apply();
                }
            }
        }

        validateNameSort(table, N);

        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        table.getColumnFamilyStore("Super1").forceBlockingFlush();
        validateNameSort(table, N);
    }

    private void validateNameSort(Table table, int N) throws IOException
    {
        for (int i = 0; i < N; ++i)
        {
            String key = Integer.toString(i);
            ColumnFamily cf;

            cf = table.get(key, "Standard1");
            Collection<IColumn> columns = cf.getAllColumns();
            for (IColumn column : columns)
            {
                int j = Integer.valueOf(column.name().split("-")[1]);
                byte[] bytes = j % 2 == 0 ? bytes1 : bytes2;
                assert Arrays.equals(bytes, column.value());
            }

            cf = table.get(key, "Super1");
            assert cf != null;
            Collection<IColumn> superColumns = cf.getAllColumns();
            assert superColumns.size() == 8;
            for (IColumn superColumn : superColumns)
            {
                int j = Integer.valueOf(superColumn.name().split("-")[1]);
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                assert subColumns.size() == 4;
                for (IColumn subColumn : subColumns)
                {
                    int k = Integer.valueOf(subColumn.name().split("-")[1]);
                    byte[] bytes = (j + k) % 2 == 0 ? bytes1 : bytes2;
                    assert Arrays.equals(bytes, subColumn.value());
                }
            }
        }
    }

}
