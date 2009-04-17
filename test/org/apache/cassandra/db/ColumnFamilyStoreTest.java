package org.apache.cassandra.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.ServerTest;
import org.testng.annotations.Test;

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

        // validateNameSort(table);

        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        table.getColumnFamilyStore("Super1").forceBlockingFlush();
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

    private void validateTimeSort(Table table) throws IOException, ColumnFamilyNotDefinedException
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
    public void testRemoveColumn() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Standard1:Column1", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Standard1:Column1", 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Standard1", new IdentityFilter());
        assert retrieved.getColumnCount() == 0;
    }

    @Test
    public void testRemoveSubColumn() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
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
        assert retrieved.getColumnCount() == 0;
    }

    @Test
    public void testRemoveSuperColumn() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
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
        rm.delete("Super1:SC1", 1);
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
    public void testRemoveColumnFamily() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");
        RowMutation rm;

        // add data
        rm = new RowMutation("Table1", "key1");
        rm.add("Standard1:Column1", "asdf".getBytes(), 0);
        rm.add("Standard1:Column2", "asdf".getBytes(), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Table1", "key1");
        rm.delete("Standard1", 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Standard1", new IdentityFilter());
        assert retrieved.getColumnCount() == 0;
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
        assert buckets.size() == 2 : bucketString(buckets);
        Iterator<List<String>> iter = buckets.iterator();
        List<String> bucket1 = iter.next();
        List<String> bucket2 = iter.next();
        assert bucket1.size() + bucket2.size() == all.size() : bucketString(buckets) + " does not match [" + StringUtils.join(all, ", ") + "]";
        assert buckets.contains(small) : bucketString(buckets) + " does not contain {" + StringUtils.join(small, ", ") + "}";
        assert buckets.contains(med) : bucketString(buckets) + " does not contain {" + StringUtils.join(med, ", ") + "}";
    }

    private static String bucketString(Set<List<String>> buckets)
    {
        ArrayList<String> pieces = new ArrayList<String>();
        for (List<String> bucket : buckets)
        {
            pieces.add("[" + StringUtils.join(bucket, ", ") + "]");
        }
        return "{" + StringUtils.join(pieces, ", ") + "}";
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

    @Test
    public void testCompaction() throws IOException, ColumnFamilyNotDefinedException, ExecutionException, InterruptedException
    {
        Table table = Table.open("Table1");
        ColumnFamilyStore store = table.getColumnFamilyStore("Standard1");

        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 10; i++) {
                long epoch = System.currentTimeMillis()  /  1000;
                String key = String.format("%s.%s.%s",  epoch,  1,  i);
                RowMutation rm = new RowMutation("Table1", key);
                rm.add("Standard1:A", new byte[0], epoch);
                rm.apply();
            }
            store.forceBlockingFlush();
        }
        Future ft = MinorCompactionManager.instance().submit(store);
        ft.get();
    }
}
