package org.apache.cassandra.db;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.DataOutputBuffer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.TreeMap;

public class ColumnFamilyTest
{
    // TODO test SuperColumns

    @Test
    public void testSingleColumn() throws IOException {
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        ColumnFamily cf;

        cf = new ColumnFamily("Standard1", "Standard");
        cf.addColumn("C", bytes, 1);
        DataOutputBuffer bufOut = new DataOutputBuffer();
        ColumnFamily.serializer().serialize(cf, bufOut);

        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        cf = ColumnFamily.serializer().deserialize(bufIn);
        assert cf != null;
        assert cf.name().equals("Standard1");
        assert cf.getAllColumns().size() == 1;
    }

    @Test
    public void testManyColumns() throws IOException {
        ColumnFamily cf;

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        cf = new ColumnFamily("Standard1", "Standard");
        DataOutputBuffer bufOut = new DataOutputBuffer();
        for (String cName: map.navigableKeySet())
        {
            cf.addColumn(cName, map.get(cName), 314);
        }
        ColumnFamily.serializer().serialize(cf, bufOut);

        // verify
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bufOut.getData(), bufOut.getLength());
        cf = ColumnFamily.serializer().deserialize(bufIn);
        for (String cName: map.navigableKeySet())
        {
            assert Arrays.equals(cf.getColumn(cName).value(), map.get(cName));

        }
        assert new HashSet<String>(cf.getColumns().keySet()).equals(map.keySet());
    }
}
