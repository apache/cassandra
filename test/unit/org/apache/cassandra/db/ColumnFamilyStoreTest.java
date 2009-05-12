package org.apache.cassandra.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.service.StorageService;

public class ColumnFamilyStoreTest extends CleanupHelper
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

    @Test
    public void testRemoveColumn() throws IOException, ExecutionException, InterruptedException
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
        assert retrieved.getColumn("Column1").isMarkedForDelete();
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
    }

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

    @Test
    public void testRemoveSuperColumn() throws IOException, ExecutionException, InterruptedException
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

    @Test
    public void testRemoveColumnFamilyWithFlush1() throws IOException, ExecutionException, InterruptedException
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
        assert retrieved.isMarkedForDelete();
        assertNull(retrieved.getColumn("Column1"));
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
    }

    @Test
    public void testRemoveColumnFamilyWithFlush2() throws IOException, ExecutionException, InterruptedException
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
        store.forceBlockingFlush();

        ColumnFamily retrieved = store.getColumnFamily("key1", "Standard1:Column1", new IdentityFilter());
        assert retrieved.isMarkedForDelete();
        assertNull(retrieved.getColumn("Column1"));
        assertNull(ColumnFamilyStore.removeDeleted(retrieved, Integer.MAX_VALUE));
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
    
    @Test
    public void testGetColumnWithWrongBF() throws IOException, ExecutionException, InterruptedException
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

        List<String> ssTables = table.getAllSSTablesOnDisk();
        /* the following call can happen if BF is wrong. Should return an empty buffer. */
        IFilter filter = new IdentityFilter(); 
        SSTable ssTable = new SSTable(ssTables.get(0), StorageService.getPartitioner());
        DataInputBuffer bufIn = filter.next("key2", "Standard1:Column1", ssTable);
        assertEquals(bufIn.getLength(), 0);
    }
}
