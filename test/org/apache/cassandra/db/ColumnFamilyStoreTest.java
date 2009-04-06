package org.apache.cassandra.db;

import org.apache.cassandra.ServerTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

public class ColumnFamilyStoreTest extends ServerTest
{
    static byte[] bytes1, bytes2;

    static
    {
        Random random = new Random();
        bytes1 = new byte[1024];
        bytes2 = new byte[128];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
    }

    @Test
    public void testNameSort() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
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
                rm.add("Standard1:" + "Column-" + j, bytes, j);
                rm.apply();

                for (int k = 0; k < 4; ++k)
                {
                    bytes = (j + k) % 2 == 0 ? bytes1 : bytes2;
                    rm = new RowMutation("Table1", key);
                    rm.add("Super1:" + "SuperColumn-" + j + ":Column-" + k, bytes, k);
                    rm.apply();
                }
            }
        }

        validateNameSort(table);

        table.getColumnFamilyStore("Standard1").forceFlush();
        table.getColumnFamilyStore("Super1").forceFlush();
        waitForFlush();
        validateNameSort(table);
    }

    @Test
    public void testTimeSort() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
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
                rm.add("StandardByTime1:" + "Column-" + j, bytes, j);
                rm.apply();
            }
        }

        validateTimeSort(table);

        table.getColumnFamilyStore("StandardByTime1").forceFlush();
        waitForFlush();
        validateTimeSort(table);
    }

    private void validateTimeSort(Table table) throws IOException, ColumnFamilyNotDefinedException
    {
        for (int i = 900; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            for (int j = 0; j < 8; j += 3)
            {
                ColumnFamily cf = table.getRow(key, "StandardByTime1", j).getColumnFamilies().iterator().next();
                SortedSet<IColumn> columns = cf.getAllColumns();
                assert columns.size() == 8 - j;
                int k = 7;
                for (IColumn c : columns)
                {
                    assert c.timestamp() == k--;
                }
            }
        }
    }

    private void waitForFlush()
            throws InterruptedException, ExecutionException
    {
        Future f = MemtableManager.instance().flusher_.submit(new Runnable()
        {
            public void run()
            {
            }
        });
        f.get();
    }

    private void validateNameSort(Table table)
            throws ColumnFamilyNotDefinedException, IOException
    {
        for (int i = 900; i < 1000; ++i)
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

    @Test
    public void testRemove() throws IOException, ColumnFamilyNotDefinedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Standard1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        ColumnFamily cf = new ColumnFamily("Standard1");
        cf.delete(1);
        rm.add(cf);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Standard1", new IdentityFilter());
        assert retrieved.getColumnCount() == 0;
    }

    @Test
    public void testRemoveSuperColumn() throws IOException, ColumnFamilyNotDefinedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Super1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Super1:SC1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        ColumnFamily cf = new ColumnFamily("Super1");
        SuperColumn sc = new SuperColumn("SC1");
        sc.markForDeleteAt(1);
        cf.addColumn(sc);
        rm.add(cf);
        rm.apply();

        List<ColumnFamily> families = store.getColumnFamilies("key1", "Super1", new IdentityFilter());
        assert families.get(0).getAllColumns().first().getMarkedForDeleteAt() == 1; // delete marker, just added
        assert !families.get(1).getAllColumns().first().isMarkedForDelete(); // flushed old version
        ColumnFamily resolved = ColumnFamilyStore.resolve(families);
        assert resolved.getAllColumns().first().getMarkedForDeleteAt() == 1;
        Collection<IColumn> subColumns = resolved.getAllColumns().first().getSubColumns();
        assert subColumns.size() == 1;
        assert subColumns.iterator().next().timestamp() == 0;
        assert ColumnFamilyStore.removeDeleted(resolved).getColumnCount() == 0;
    }

    @Test
    public void testGetCompactionBuckets() throws IOException
    {
        // create files 20 40 60 ... 180
        List<String> small = new ArrayList<String>();
        List<String> med = new ArrayList<String>();
        List<String> all = new ArrayList<String>();

        String fname;
        fname = createFile(20);
        small.add(fname);
        all.add(fname);
        fname = createFile(40);
        small.add(fname);
        all.add(fname);

        for (int i = 60; i <= 140; i += 20)
        {
            fname = createFile(i);
            med.add(fname);
            all.add(fname);
        }

        Set<List<String>> buckets = ColumnFamilyStore.getCompactionBuckets(all, 50);
        assert buckets.contains(small);
        assert buckets.contains(med);
    }

    private String createFile(int nBytes) throws IOException
    {
        File f = File.createTempFile("bucket_test", "");
        FileOutputStream fos = new FileOutputStream(f);
        byte[] bytes = new byte[nBytes];
        fos.write(bytes);
        fos.close();
        return f.getAbsolutePath();
    }
}
