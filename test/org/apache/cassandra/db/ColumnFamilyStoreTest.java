package org.apache.cassandra.db;

import org.apache.cassandra.ServerTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class ColumnFamilyStoreTest extends ServerTest {
    @Test
    public void testMain() throws IOException, ColumnFamilyNotDefinedException {
        Table table = Table.open("Table1");
        Random random = new Random();
        byte[] bytes1 = new byte[1024];
        byte[] bytes2 = new byte[1024];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);

        for (int i = 800; i < 1000; ++i)
        {
            String key = Integer.toString(i);
            RowMutation rm;
            for ( int j = 0; j < 8; ++j )
            {
                byte[] bytes = j % 2 == 0 ? bytes1 : bytes2;
                rm = new RowMutation("Table1", key);
                rm.add("Standard1:" + "Column-" + j, bytes, j);
                rm.apply();

                for ( int k = 0; k < 8; ++k )
                {
                    bytes = (j + k) % 2 == 0 ? bytes1 : bytes2;
                    rm = new RowMutation("Table1", key);
                    rm.add("Super1:" + "SuperColumn-" + j + ":Column-" + k, bytes, k);
                    rm.apply();
                }
            }
        }

        for ( int i = 800; i < 1000; ++i )
        {
            String key = Integer.toString(i);
            // TODO actually test results
            ColumnFamily cf = table.get(key, "Super1:SuperColumn-1");
            assert cf != null;
            Collection<IColumn> superColumns = cf.getAllColumns();
            for ( IColumn superColumn : superColumns )
            {
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                for ( IColumn subColumn : subColumns )
                {
                    //System.out.println(subColumn);
                }
            }
        }
    }

    @Test
    public void testRemove() throws IOException, ColumnFamilyNotDefinedException {
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
    public void testRemoveSuperColumn() throws IOException, ColumnFamilyNotDefinedException {
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
}
