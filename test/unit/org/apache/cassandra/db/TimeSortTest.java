package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.SortedSet;
import java.util.Iterator;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

public class TimeSortTest extends ColumnFamilyStoreTest
{
    @Test
    public void testTimeSort() throws IOException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");

        for (int i = 900; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm;
            for (int j = 0; j < 8; ++j)
            {
                byte[] bytes = j % 2 == 0 ? bytes1 : bytes2;
                rm = new RowMutation("Table1", key);
                rm.add("StandardByTime1:" + "Column-" + j, bytes, j * 2);
                rm.apply();
            }
        }

        validateTimeSort(table);

        table.getColumnFamilyStore("StandardByTime1").forceBlockingFlush();
        validateTimeSort(table);

        // interleave some new data to test memtable + sstable
        String key = "900";
        RowMutation rm;
        for (int j = 0; j < 4; ++j)
        {
            rm = new RowMutation("Table1", key);
            rm.add("StandardByTime1:" + "Column+" + j, ArrayUtils.EMPTY_BYTE_ARRAY, j * 2 + 1);
            rm.apply();
        }
        // and some overwrites
        for (int j = 4; j < 8; ++j)
        {
            rm = new RowMutation("Table1", key);
            rm.add("StandardByTime1:" + "Column-" + j, ArrayUtils.EMPTY_BYTE_ARRAY, j * 3);
            rm.apply();
        }
        // verify
        ColumnFamily cf = table.getRow(key, "StandardByTime1", 0).getColumnFamilies().iterator().next();
        SortedSet<IColumn> columns = cf.getAllColumns();
        assert columns.size() == 12;
        Iterator<IColumn> iter = columns.iterator();
        IColumn column;
        for (int j = 7; j >= 4; j--)
        {
            column = iter.next();
            assert column.name().equals("Column-" + j);
            assert column.timestamp() == j * 3;
            assert column.value().length == 0;
        }
        for (int j = 3; j >= 0; j--)
        {
            column = iter.next();
            assert column.name().equals("Column+" + j);
            column = iter.next();
            assert column.name().equals("Column-" + j);
        }
    }

    private void validateTimeSort(Table table) throws IOException
    {
        for (int i = 900; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            for (int j = 0; j < 8; j += 3)
            {
                ColumnFamily cf = table.getRow(key, "StandardByTime1", j * 2).getColumnFamilies().iterator().next();
                SortedSet<IColumn> columns = cf.getAllColumns();
                assert columns.size() == 8 - j;
                int k = 7;
                for (IColumn c : columns)
                {
                    assert c.timestamp() == (k--) * 2;
                }
            }
        }
    }
}
