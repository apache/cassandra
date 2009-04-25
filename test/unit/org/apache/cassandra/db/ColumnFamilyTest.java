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
    
    @Test
	public void testGetColumnCount() {
    	ColumnFamily cf = new ColumnFamily("Standard1", "Standard");
		byte val[] = "sample value".getBytes();
		
		cf.addColumn("col1", val, 1);
		cf.addColumn("col2", val, 2);
		cf.addColumn("col1", val, 3);

		assert 2 == cf.getColumnCount();
		assert 2 == cf.getAllColumns().size();
	}
    
    @Test
    public void testTimestamp() {
    	ColumnFamily cf = new ColumnFamily("Standard1", "Standard");
    	byte val1[] = "sample 1".getBytes();
        byte val2[] = "sample 2".getBytes();
        byte val3[] = "sample 3".getBytes();

    	cf.addColumn("col1", val1, 2);
        cf.addColumn("col1", val2, 2); // same timestamp, new value
        cf.addColumn("col1", val3, 1); // older timestamp -- should be ignored

        assert Arrays.equals(val2, cf.getColumn("col1").value());
    }
    
    @Test
    public void testMergeAndAdd(){
    	ColumnFamily cf_new = new ColumnFamily("Standard1", "Standard");
    	ColumnFamily cf_old = new ColumnFamily("Standard1", "Standard");
    	ColumnFamily cf_result = new ColumnFamily("Standard1", "Standard");
    	byte val[] = "sample value".getBytes();
    	byte val2[] = "x value ".getBytes();
    	
    	cf_new.addColumn("col1", val, 3);
    	cf_new.addColumn("col2", val, 4);

    	cf_old.addColumn("col2", val2, 1);
    	cf_old.addColumn("col3", val2, 2);

    	cf_result.addColumns(cf_new);
    	cf_result.addColumns(cf_old);
    	
    	assert 3 == cf_result.getColumnCount() : "Count is " + cf_new.getColumnCount();
    	//addcolumns will only add if timestamp >= old timestamp
        assert Arrays.equals(val, cf_result.getColumn("col2").value());
    }
}
