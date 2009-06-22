/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.util.SortedSet;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static junit.framework.Assert.*;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.io.SSTable;

public class TableTest extends CleanupHelper
{
    private static final String KEY2 = "key2";
    private static final String TEST_KEY = "key1";
    private static final String TABLE_NAME = "Table1";

    @Test
    public void testOpen() throws Throwable {
        Table table = Table.open("Mailbox");
        Row row = table.get("35300190:1");
        assertNotNull(row);
    }
    
    @Test
    public void testGetRowSingleColumn() throws Throwable
    {
        Table table = Table.open(TABLE_NAME);
        RowMutation rm = makeSimpleRowMutation();
        rm.apply();
        Row result = table.getRow(TEST_KEY, "Standard1:col1");
        ColumnFamily cres = result.getColumnFamily("Standard1");
        assertNotNull(cres);
        assertEquals(1, cres.getColumnCount());
        assertNotNull(cres.getColumn("col1"));
    }
    
    @Test
    public void testGetRowOffsetCount() throws Throwable
    {
        Table table = Table.open(TABLE_NAME);
        
        RowMutation rm = makeSimpleRowMutation(); //inserts col1, col2, col3

        
        rm.apply();
        Row result = table.getRow(TEST_KEY, "Standard1", 0, 2);
        ColumnFamily cres = result.getColumnFamily("Standard1");
        assertNotNull(cres);
        assertEquals(cres.getColumnCount(), 2);
        // should have col1 and col2
        assertNotNull(cres.getColumn("col1"));
        assertNotNull(cres.getColumn("col2"));

        result = table.getRow(TEST_KEY, "Standard1", 1, 2);
        cres = result.getColumnFamily("Standard1");
        assertNotNull(cres);
        assertEquals(2, cres.getColumnCount());
        // offset is 1, so we should have col2 and col3
        assertNotNull(cres.getColumn("col2"));
        assertNotNull(cres.getColumn("col3"));
    }
    
    @Test
    public void testGetRowOffsetCountFromDisk() throws Throwable
    {
        Table table = Table.open("Table1");
        
        RowMutation rm = new RowMutation(TABLE_NAME,KEY2);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        // First write 5, 6
        cf.addColumn(new Column("col5", "val5".getBytes(), 1L));
        cf.addColumn(new Column("col6", "val6".getBytes(), 1L));
        rm.add(cf);

        rm.apply();
        Row result = table.getRow(KEY2, "Standard1:col5");
        ColumnFamily cres = result.getColumnFamily("Standard1");
        assertNotNull(cres.getColumn("col5"));

        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        // Flushed memtable to disk, we're now inserting into a new memtable

        rm = new RowMutation(TABLE_NAME, KEY2);
        cf = ColumnFamily.create("Table1", "Standard1");
        // now write 7, 8, 4 into new memtable
        cf.addColumn(new Column("col7", "val7".getBytes(), 1L));
        cf.addColumn(new Column("col8", "val8".getBytes(), 1L));
        cf.addColumn(new Column("col4", "val4".getBytes(), 1L));
        rm.add(cf);
        rm.apply();

        // Check col5 is still around
        result = table.getRow(KEY2, "Standard1:col5");
        cres = result.getColumnFamily("Standard1");
        assertNotNull(cres.getColumn("col5"));

        // Read back 3 cols from start -- should see 4,5,6 NOT 4,7,8
        result = table.getRow(KEY2, "Standard1", 0, 3);
        cres = result.getColumnFamily("Standard1");
        assertNotNull(cres);
        assertEquals(cres.getColumnCount(), 3);

        // Should have col4, col5, and col6
        assertNotNull(cres.getColumn("col4"));
        assertNotNull(cres.getColumn("col5"));
        assertNotNull(cres.getColumn("col6"));
        // not 8
        assertNull(cres.getColumn("col8"));
    }
    
    @Test
    public void testGetRowSliceByRange() throws Throwable
    {
    	String key = TEST_KEY+"slicerow";
    	Table table = Table.open(TABLE_NAME);
    	RowMutation rm = new RowMutation(TABLE_NAME,key);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        // First write "a", "b", "c"
        cf.addColumn(new Column("a", "val1".getBytes(), 1L));
        cf.addColumn(new Column("b", "val2".getBytes(), 1L));
        cf.addColumn(new Column("c", "val3".getBytes(), 1L));
        rm.add(cf);
        rm.apply();
        
        Row result = table.getRow(key, "Standard1", "b", "c",-1);
        assertEquals(2, result.getColumnFamily("Standard1").getColumnCount());
        
        result = table.getRow(key, "Standard1", "b", "b", 50);
        assertEquals(1, result.getColumnFamily("Standard1").getColumnCount());
        
        result = table.getRow(key, "Standard1", "b", "c",1);
        assertEquals(1, result.getColumnFamily("Standard1").getColumnCount());
        
        result = table.getRow(key, "Standard1", "c", "b",1);
        assertEquals(0, result.getColumnFamily("Standard1").getColumnCount());
        
    }
    
    @Test
    public void testGetRowSuperColumnOffsetCount() throws Throwable
    {
        Table table = Table.open(TABLE_NAME);
        
        RowMutation rm = new RowMutation(TABLE_NAME,TEST_KEY);
        ColumnFamily cf = ColumnFamily.create("Table1", "Super1");
        SuperColumn sc1 = new SuperColumn("sc1");
        sc1.addColumn(new Column("col1","val1".getBytes(), 1L));
        sc1.addColumn(new Column("col2","val2".getBytes(), 1L));
        SuperColumn sc2 = new SuperColumn("sc2");
        sc2.addColumn(new Column("col3","val3".getBytes(), 1L));
        sc2.addColumn(new Column("col4","val4".getBytes(), 1L));
        cf.addColumn(sc1);
        cf.addColumn(sc2);
        rm.add(cf);
        
        rm.apply();
        //Slicing top level columns of a supercolumn
        Row result = table.getRow(TEST_KEY, "Super1", 0, 2);
        ColumnFamily cres = result.getColumnFamily("Super1");
        assertNotNull(cres);
        assertEquals(cres.getAllColumns().size(), 2); //2 supercolumns
        assertEquals(cres.getColumnCount(),2+4); //2 cols, 2 subcols each
        // should have col1 ... col4
        assertNotNull(cres.getColumn("sc1").getSubColumn("col1"));
        assertNotNull(cres.getColumn("sc1").getSubColumn("col2"));
        assertNotNull(cres.getColumn("sc2").getSubColumn("col3"));
        assertNotNull(cres.getColumn("sc2").getSubColumn("col4"));

        
        result = table.getRow(TEST_KEY, "Super1:sc1", 1, 2); //get at most 2, but only 1 column will be available
        cres = result.getColumnFamily("Super1");
        assertNotNull(cres);
        assertEquals(cres.getAllColumns().size(), 1); //only 1 top level column. sc1 has only 2 subcolumns
        assertEquals(cres.getColumnCount(), 2); //getObjectCount: 1 for the column, and 1 for subcolumn
        
        assertNotNull(cres.getColumn("sc1").getSubColumn("col2"));
        assertNull(cres.getColumn("sc2"));       
    }

    private RowMutation makeSimpleRowMutation()
    {
        RowMutation rm = new RowMutation(TABLE_NAME,TEST_KEY);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        cf.addColumn(new Column("col1","val1".getBytes(), 1L));
        cf.addColumn(new Column("col2","val2".getBytes(), 1L));
        cf.addColumn(new Column("col3","val3".getBytes(), 1L));
        rm.add(cf);
        return rm;
    }

    @Test
    public void testGetSliceNoMatch() throws Throwable
    {
        Table table = Table.open(TABLE_NAME);
        RowMutation rm = new RowMutation(TABLE_NAME, "row1000");
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard2");
        cf.addColumn(new Column("col1", "val1".getBytes(), 1));
        rm.add(cf);
        rm.apply();

        validateGetSliceNoMatch(table);
        table.getColumnFamilyStore("Standard2").forceBlockingFlush();
        validateGetSliceNoMatch(table);

        SortedSet<String> ssTables = table.getColumnFamilyStore("Standard2").getSSTableFilenames();
        assertEquals(1, ssTables.size());
        SSTable.forceBloomFilterFailures(ssTables.iterator().next());
        validateGetSliceNoMatch(table);
    }

    private void validateGetSliceNoMatch(Table table) throws IOException
    {
        Row result;
        ColumnFamily cf;

        // key before the rows that exists
        result = table.getSliceFrom("a", "Standard2", true, 0);
        cf = result.getColumnFamily("Standard2");
        assertColumns(cf);

        // key after the rows that exist
        result = table.getSliceFrom("z", "Standard2", true, 0);
        cf = result.getColumnFamily("Standard2");
        assertColumns(cf);
    }

    @Test
    public void testGetSliceFromBasic() throws Throwable
    {
        // tests slicing against data from one row in a memtable and then flushed to an sstable
        Table table = Table.open(TABLE_NAME);
        String ROW = "row1";
        RowMutation rm = new RowMutation(TABLE_NAME, ROW);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        cf.addColumn(new Column("col1", "val1".getBytes(), 1L));
        cf.addColumn(new Column("col3", "val3".getBytes(), 1L));
        cf.addColumn(new Column("col4", "val4".getBytes(), 1L));
        cf.addColumn(new Column("col5", "val5".getBytes(), 1L));
        cf.addColumn(new Column("col7", "val7".getBytes(), 1L));
        cf.addColumn(new Column("col9", "val9".getBytes(), 1L));
        rm.add(cf);
        rm.apply();
        
        rm = new RowMutation(TABLE_NAME, ROW);
        rm.delete("Standard1:col4", 2L);
        rm.apply();

        validateGetSliceFromBasic(table, ROW);
        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        validateGetSliceFromBasic(table, ROW);        
    }

    @Test
    public void testGetSliceFromAdvanced() throws Throwable
    {
        // tests slicing against data from one row spread across two sstables
        Table table = Table.open(TABLE_NAME);
        String ROW = "row2";
        RowMutation rm = new RowMutation(TABLE_NAME, ROW);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        cf.addColumn(new Column("col1", "val1".getBytes(), 1L));
        cf.addColumn(new Column("col2", "val2".getBytes(), 1L));
        cf.addColumn(new Column("col3", "val3".getBytes(), 1L));
        cf.addColumn(new Column("col4", "val4".getBytes(), 1L));
        cf.addColumn(new Column("col5", "val5".getBytes(), 1L));
        cf.addColumn(new Column("col6", "val6".getBytes(), 1L));
        rm.add(cf);
        rm.apply();
        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        
        rm = new RowMutation(TABLE_NAME, ROW);
        cf = ColumnFamily.create("Table1", "Standard1");
        cf.addColumn(new Column("col1", "valx".getBytes(), 2L));
        cf.addColumn(new Column("col2", "valx".getBytes(), 2L));
        cf.addColumn(new Column("col3", "valx".getBytes(), 2L));
        rm.add(cf);
        rm.apply();

        validateGetSliceFromAdvanced(table, ROW);
        table.getColumnFamilyStore("Standard1").forceBlockingFlush();
        validateGetSliceFromAdvanced(table, ROW);
    }

    @Test
    public void testGetSliceFromLarge() throws Throwable
    {
        // tests slicing against 1000 columns in an sstable
        Table table = Table.open(TABLE_NAME);
        String ROW = "row3";
        RowMutation rm = new RowMutation(TABLE_NAME, ROW);
        ColumnFamily cf = ColumnFamily.create("Table1", "Standard1");
        for (int i = 1000; i < 2000; i++)
            cf.addColumn(new Column("col" + i, ("vvvvvvvvvvvvvvvv" + i).getBytes(), 1L));
        rm.add(cf);
        rm.apply();
        table.getColumnFamilyStore("Standard1").forceBlockingFlush();

        Row result;
        ColumnFamily cfres;
        result = table.getSliceFrom(ROW, "Standard1:col1000", true, 3);
        cfres = result.getColumnFamily("Standard1");
        assertColumns(cfres, "col1000", "col1001", "col1002");
        assertEquals(new String(cfres.getColumn("col1000").value()), "vvvvvvvvvvvvvvvv1000");
        assertEquals(new String(cfres.getColumn("col1001").value()), "vvvvvvvvvvvvvvvv1001");
        assertEquals(new String(cfres.getColumn("col1002").value()), "vvvvvvvvvvvvvvvv1002");

        result = table.getSliceFrom(ROW, "Standard1:col1195", true, 3);
        cfres = result.getColumnFamily("Standard1");
        assertColumns(cfres, "col1195", "col1196", "col1197");
        assertEquals(new String(cfres.getColumn("col1195").value()), "vvvvvvvvvvvvvvvv1195");
        assertEquals(new String(cfres.getColumn("col1196").value()), "vvvvvvvvvvvvvvvv1196");
        assertEquals(new String(cfres.getColumn("col1197").value()), "vvvvvvvvvvvvvvvv1197");

        result = table.getSliceFrom(ROW, "Standard1:col1196", false, 3);
        cfres = result.getColumnFamily("Standard1");
        assertColumns(cfres, "col1194", "col1195", "col1196");
        assertEquals(new String(cfres.getColumn("col1194").value()), "vvvvvvvvvvvvvvvv1194");
        assertEquals(new String(cfres.getColumn("col1195").value()), "vvvvvvvvvvvvvvvv1195");
        assertEquals(new String(cfres.getColumn("col1196").value()), "vvvvvvvvvvvvvvvv1196");

        result = table.getSliceFrom(ROW, "Standard1:col1990", true, 3);
        cfres = result.getColumnFamily("Standard1");
        assertColumns(cfres, "col1990", "col1991", "col1992");
        assertEquals(new String(cfres.getColumn("col1990").value()), "vvvvvvvvvvvvvvvv1990");
        assertEquals(new String(cfres.getColumn("col1991").value()), "vvvvvvvvvvvvvvvv1991");
        assertEquals(new String(cfres.getColumn("col1992").value()), "vvvvvvvvvvvvvvvv1992");
    }

    public static void assertColumns(ColumnFamily columnFamily, String... columnNames)
    {
        assertNotNull(columnFamily);
        SortedSet<IColumn> columns = columnFamily.getAllColumns();
        List<String> L = new ArrayList<String>();
        for (IColumn column : columns)
        {
            L.add(column.name());
        }
        assert Arrays.equals(L.toArray(new String[columns.size()]), columnNames)
                : "Columns [" + StringUtils.join(columns, ", ") + "] is not expected [" + StringUtils.join(columnNames, ", ") + "]";
    }

    private void validateGetSliceFromAdvanced(Table table, String row) throws Throwable
    {
        Row result;
        ColumnFamily cfres;

        result = table.getSliceFrom(row, "Standard1:col2", true, 3);
        cfres = result.getColumnFamily("Standard1");
        assertColumns(cfres, "col2", "col3", "col4");
        assertEquals(new String(cfres.getColumn("col2").value()), "valx");
        assertEquals(new String(cfres.getColumn("col3").value()), "valx");
        assertEquals(new String(cfres.getColumn("col4").value()), "val4");
    }

    private void validateGetSliceFromBasic(Table table, String row) throws Throwable
    {
        Row result;
        ColumnFamily cf;

        result = table.getSliceFrom(row, "Standard1:col5", true, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf, "col5", "col7");

        result = table.getSliceFrom(row, "Standard1:col4", true, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf, "col4", "col5", "col7");

        result = table.getSliceFrom(row, "Standard1:col5", false, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf, "col3", "col4", "col5");

        result = table.getSliceFrom(row, "Standard1:col6", false, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf, "col3", "col4", "col5");

        result = table.getSliceFrom(row, "Standard1:col95", true, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf);

        result = table.getSliceFrom(row, "Standard1:col0", false, 2);
        cf = result.getColumnFamily("Standard1");
        assertColumns(cf);
    }
}
