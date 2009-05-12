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

import org.junit.Test;

import static junit.framework.Assert.*;
import org.apache.cassandra.CleanupHelper;

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
        ColumnFamily cf = new ColumnFamily("Standard1","Standard");
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
        cf = new ColumnFamily("Standard1","Standard");
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
    public void testGetRowSuperColumnOffsetCount() throws Throwable
    {
        Table table = Table.open(TABLE_NAME);
        
        RowMutation rm = new RowMutation(TABLE_NAME,TEST_KEY);
        ColumnFamily cf = new ColumnFamily("Super1","Super");
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
        ColumnFamily cf = new ColumnFamily("Standard1","Standard");
        cf.addColumn(new Column("col1","val1".getBytes(), 1L));
        cf.addColumn(new Column("col2","val2".getBytes(), 1L));
        cf.addColumn(new Column("col3","val3".getBytes(), 1L));
        rm.add(cf);
        return rm;
    }
}
