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

import java.util.*;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import static junit.framework.Assert.*;
import org.apache.cassandra.CleanupHelper;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.SSTableReader;

public class TableTest extends CleanupHelper
{
    private static final String KEY2 = "key2";
    private static final String TEST_KEY = "key1";

    interface Runner
    {
        public void run() throws Exception;
    }

    private void reTest(Runner setup, ColumnFamilyStore cfs, Runner verify) throws Exception
    {
        setup.run();
        verify.run();
        cfs.forceBlockingFlush();
        verify.run();
    }

    @Test
    public void testOpen() throws Throwable {
        Table table = Table.open("Mailbox");
        Row row = table.get("35300190:1");
        assertNotNull(row);
    }
    
    @Test
    public void testGetRowSingleColumn() throws Throwable
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");

        Runner setup = new Runner()
        {
            public void run() throws Exception
            {
                RowMutation rm = makeSimpleRowMutation();
                rm.apply();
            }
        };
        Runner verify = new Runner()
        {
            public void run() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(new NamesQueryFilter(TEST_KEY, new QueryPath("Standard1"), "col1".getBytes()));
                assertColumns(cf, "col1");

                cf = cfStore.getColumnFamily(new NamesQueryFilter(TEST_KEY, new QueryPath("Standard1"), "col3".getBytes()));
                assertColumns(cf, "col3");
            }
        };
        reTest(setup, table.getColumnFamilyStore("Standard1"), verify);
    }
            
    @Test
    public void testGetRowSliceByRange() throws Throwable
    {
    	String key = TEST_KEY+"slicerow";
    	Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
    	RowMutation rm = new RowMutation("Keyspace1", key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        // First write "a", "b", "c"
        cf.addColumn(column("a", "val1", 1L));
        cf.addColumn(column("b", "val2", 1L));
        cf.addColumn(column("c", "val3", 1L));
        rm.add(cf);
        rm.apply();
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "c".getBytes(), true, 100);
        assertEquals(2, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "b".getBytes(), true, 100);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "c".getBytes(), true, 1);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "c".getBytes(), "b".getBytes(), true, 1);
        assertNull(cf);
    }

    private RowMutation makeSimpleRowMutation()
    {
        RowMutation rm = new RowMutation("Keyspace1",TEST_KEY);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1","val1", 1L));
        cf.addColumn(column("col2","val2", 1L));
        cf.addColumn(column("col3","val3", 1L));
        rm.add(cf);
        return rm;
    }

    @Test
    public void testGetSliceNoMatch() throws Throwable
    {
        Table table = Table.open("Keyspace1");
        RowMutation rm = new RowMutation("Keyspace1", "row1000");
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard2");
        cf.addColumn(column("col1", "val1", 1));
        rm.add(cf);
        rm.apply();

        validateGetSliceNoMatch(table);
        table.getColumnFamilyStore("Standard2").forceBlockingFlush();
        validateGetSliceNoMatch(table);

        Collection<SSTableReader> ssTables = table.getColumnFamilyStore("Standard2").getSSTables();
        assertEquals(1, ssTables.size());
        ssTables.iterator().next().forceBloomFilterFailures();
        validateGetSliceNoMatch(table);
    }

    private void validateGetSliceNoMatch(Table table) throws IOException
    {
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard2");
        ColumnFamily cf;

        // key before the rows that exists
        cf = cfStore.getColumnFamily("a", new QueryPath("Standard2"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1);
        assertColumns(cf);

        // key after the rows that exist
        cf = cfStore.getColumnFamily("z", new QueryPath("Standard2"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 1);
        assertColumns(cf);
    }

    @Test
    public void testGetSliceFromBasic() throws Throwable
    {
        // tests slicing against data from one row in a memtable and then flushed to an sstable
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final String ROW = "row1";
        Runner setup = new Runner()
        {
            public void run() throws Exception
            {
                RowMutation rm = new RowMutation("Keyspace1", ROW);
                ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
                cf.addColumn(column("col1", "val1", 1L));
                cf.addColumn(column("col3", "val3", 1L));
                cf.addColumn(column("col4", "val4", 1L));
                cf.addColumn(column("col5", "val5", 1L));
                cf.addColumn(column("col7", "val7", 1L));
                cf.addColumn(column("col9", "val9", 1L));
                rm.add(cf);
                rm.apply();

                rm = new RowMutation("Keyspace1", ROW);
                rm.delete(new QueryPath("Standard1", null, "col4".getBytes()), 2L);
                rm.apply();
            }
        };

        Runner verify = new Runner()
        {
            public void run() throws Exception
            {
                Row result;
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col5".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf, "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col4".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf, "col4", "col5", "col7");
                assertColumns(ColumnFamilyStore.removeDeleted(cf, Integer.MAX_VALUE), "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col5".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col6".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col95".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf);

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col0".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf);
            }
        };

        reTest(setup, table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetSliceFromAdvanced() throws Throwable
    {
        // tests slicing against data from one row spread across two sstables
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final String ROW = "row2";
        Runner setup = new Runner()
        {
            public void run() throws Exception
            {
                RowMutation rm = new RowMutation("Keyspace1", ROW);
                ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
                cf.addColumn(column("col1", "val1", 1L));
                cf.addColumn(column("col2", "val2", 1L));
                cf.addColumn(column("col3", "val3", 1L));
                cf.addColumn(column("col4", "val4", 1L));
                cf.addColumn(column("col5", "val5", 1L));
                cf.addColumn(column("col6", "val6", 1L));
                rm.add(cf);
                rm.apply();
                cfStore.forceBlockingFlush();

                rm = new RowMutation("Keyspace1", ROW);
                cf = ColumnFamily.create("Keyspace1", "Standard1");
                cf.addColumn(column("col1", "valx", 2L));
                cf.addColumn(column("col2", "valx", 2L));
                cf.addColumn(column("col3", "valx", 2L));
                rm.add(cf);
                rm.apply();
            }
        };

        Runner verify = new Runner()
        {
            public void run() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col2".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
                assertColumns(cf, "col2", "col3", "col4");
                assertEquals(new String(cf.getColumn("col2".getBytes()).value()), "valx");
                assertEquals(new String(cf.getColumn("col3".getBytes()).value()), "valx");
                assertEquals(new String(cf.getColumn("col4".getBytes()).value()), "val4");
            }
        };

        reTest(setup, table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetSliceFromLarge() throws Throwable
    {
        // tests slicing against 1000 columns in an sstable
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        String ROW = "row3";
        RowMutation rm = new RowMutation("Keyspace1", ROW);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        for (int i = 1000; i < 2000; i++)
            cf.addColumn(column("col" + i, ("vvvvvvvvvvvvvvvv" + i), 1L));
        rm.add(cf);
        rm.apply();
        cfStore.forceBlockingFlush();

        cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col1000".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
        assertColumns(cf, "col1000", "col1001", "col1002");
        assertEquals(new String(cf.getColumn("col1000".getBytes()).value()), "vvvvvvvvvvvvvvvv1000");
        assertEquals(new String(cf.getColumn("col1001".getBytes()).value()), "vvvvvvvvvvvvvvvv1001");
        assertEquals(new String(cf.getColumn("col1002".getBytes()).value()), "vvvvvvvvvvvvvvvv1002");

        cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col1195".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
        assertColumns(cf, "col1195", "col1196", "col1197");
        assertEquals(new String(cf.getColumn("col1195".getBytes()).value()), "vvvvvvvvvvvvvvvv1195");
        assertEquals(new String(cf.getColumn("col1196".getBytes()).value()), "vvvvvvvvvvvvvvvv1196");
        assertEquals(new String(cf.getColumn("col1197".getBytes()).value()), "vvvvvvvvvvvvvvvv1197");

        cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col1196".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
        assertColumns(cf, "col1194", "col1195", "col1196");
        assertEquals(new String(cf.getColumn("col1194".getBytes()).value()), "vvvvvvvvvvvvvvvv1194");
        assertEquals(new String(cf.getColumn("col1195".getBytes()).value()), "vvvvvvvvvvvvvvvv1195");
        assertEquals(new String(cf.getColumn("col1196".getBytes()).value()), "vvvvvvvvvvvvvvvv1196");

        cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col1990".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
        assertColumns(cf, "col1990", "col1991", "col1992");
        assertEquals(new String(cf.getColumn("col1990".getBytes()).value()), "vvvvvvvvvvvvvvvv1990");
        assertEquals(new String(cf.getColumn("col1991".getBytes()).value()), "vvvvvvvvvvvvvvvv1991");
        assertEquals(new String(cf.getColumn("col1992".getBytes()).value()), "vvvvvvvvvvvvvvvv1992");
    }

    @Test
    public void testGetSliceFromSuperBasic() throws Throwable
    {
        // tests slicing against data from one row spread across two sstables
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Super1");
        final String ROW = "row2";
        Runner setup = new Runner()
        {
            public void run() throws Exception
            {
                RowMutation rm = new RowMutation("Keyspace1", ROW);
                ColumnFamily cf = ColumnFamily.create("Keyspace1", "Super1");
                SuperColumn sc = new SuperColumn("sc1".getBytes(), new LongType());
                sc.addColumn(new Column(getBytes(1), "val1".getBytes(), 1L));
                cf.addColumn(sc);
                rm.add(cf);
                rm.apply();
            }
        };

        Runner verify = new Runner()
        {
            public void run() throws Exception
            {
                ColumnFamily cf = cfStore.getColumnFamily(ROW, new QueryPath("Super1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 10);
                assertColumns(cf, "sc1");
                assertEquals(new String(cf.getColumn("sc1".getBytes()).getSubColumn(getBytes(1)).value()), "val1");
            }
        };

        reTest(setup, table.getColumnFamilyStore("Standard1"), verify);
    }

    public static void assertColumns(ColumnFamily cf, String... columnNames)
    {
        Collection<IColumn> columns = cf == null ? new TreeSet<IColumn>() : cf.getSortedColumns();
        List<String> L = new ArrayList<String>();
        for (IColumn column : columns)
        {
            L.add(new String(column.name()));
        }
        assert Arrays.equals(L.toArray(new String[columns.size()]), columnNames)
                : "Columns [" + ((cf == null) ? "" : cf.getComparator().getColumnsString(columns)) + "]"
                  + " is not expected [" + StringUtils.join(columnNames, ",") + "]";
    }
}
