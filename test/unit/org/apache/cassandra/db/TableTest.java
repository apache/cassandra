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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static junit.framework.Assert.*;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.WrappedRunnable;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;


public class TableTest extends CleanupHelper
{
    private static final DecoratedKey TEST_KEY = Util.dk("key1");
    private static final DecoratedKey TEST_SLICE_KEY = Util.dk("key1-slicerange");

    public static void reTest(ColumnFamilyStore cfs, Runnable verify) throws Exception
    {
        verify.run();
        cfs.forceBlockingFlush();
        verify.run();
    }

    @Test
    public void testGetRowNoColumns() throws Throwable
    {
        final Table table = Table.open("Keyspace2");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard3");

        RowMutation rm = new RowMutation("Keyspace2", TEST_KEY.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace2", "Standard3");
        cf.addColumn(column("col1","val1", 1L));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard3"), new TreeSet<ByteBuffer>()));
                assertColumns(cf);

                cf = cfStore.getColumnFamily(QueryFilter.getSliceFilter(TEST_KEY, new QueryPath("Standard3"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 0));
                assertColumns(cf);

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard3"), ByteBufferUtil.bytes("col99")));
                assertColumns(cf);
            }
        };
        reTest(table.getColumnFamilyStore("Standard3"), verify);
    }

    @Test
    public void testGetRowSingleColumn() throws Throwable
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");

        RowMutation rm = new RowMutation("Keyspace1", TEST_KEY.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1","val1", 1L));
        cf.addColumn(column("col2","val2", 1L));
        cf.addColumn(column("col3","val3", 1L));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard1"), ByteBufferUtil.bytes("col1")));
                assertColumns(cf, "col1");

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard1"), ByteBufferUtil.bytes("col3")));
                assertColumns(cf, "col3");
            }
        };
        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetRowSliceByRange() throws Throwable
    {
    	DecoratedKey key = TEST_SLICE_KEY;
    	Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
    	RowMutation rm = new RowMutation("Keyspace1", key.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        // First write "a", "b", "c"
        cf.addColumn(column("a", "val1", 1L));
        cf.addColumn(column("b", "val2", 1L));
        cf.addColumn(column("c", "val3", 1L));
        rm.add(cf);
        rm.apply();
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("b"), ByteBufferUtil.bytes("c"), false, 100);
        assertEquals(2, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("b"), ByteBufferUtil.bytes("b"), false, 100);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("b"), ByteBufferUtil.bytes("c"), false, 1);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("c"), ByteBufferUtil.bytes("b"), false, 1);
        assertNull(cf);
    }

    @Test
    public void testGetSliceNoMatch() throws Throwable
    {
        Table table = Table.open("Keyspace1");
        RowMutation rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("row1000"));
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard2");
        cf.addColumn(column("col1", "val1", 1));
        rm.add(cf);
        rm.apply();

        validateGetSliceNoMatch(table);
        table.getColumnFamilyStore("Standard2").forceBlockingFlush();
        validateGetSliceNoMatch(table);

        Collection<SSTableReader> ssTables = table.getColumnFamilyStore("Standard2").getSSTables();
        assertEquals(1, ssTables.size());
        ssTables.iterator().next().forceFilterFailures();
        validateGetSliceNoMatch(table);
    }

    @Test
    public void testGetSliceWithCutoff() throws Throwable
    {
        // tests slicing against data from one row in a memtable and then flushed to an sstable
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final DecoratedKey ROW = Util.dk("row4");
        final NumberFormat fmt = new DecimalFormat("000");

        RowMutation rm = new RowMutation("Keyspace1", ROW.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        // at this rate, we're getting 78-79 cos/block, assuming the blocks are set to be about 4k.
        // so if we go to 300, we'll get at least 4 blocks, which is plenty for testing.
        for (int i = 0; i < 300; i++)
            cf.addColumn(column("col" + fmt.format(i), "omg!thisisthevalue!"+i, 1L));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                // blocks are partitioned like this: 000-097, 098-193, 194-289, 290-299, assuming a 4k column index size.
                assert DatabaseDescriptor.getColumnIndexSize() == 4096 : "Unexpected column index size, block boundaries won't be where tests expect them.";

                // test forward, spanning a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col096"), ByteBufferUtil.bytes("col099"), false, 4);
                assertColumns(cf, "col096", "col097", "col098", "col099");

                // test reversed, spanning a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col099"), ByteBufferUtil.bytes("col096"), true, 4);
                assertColumns(cf, "col096", "col097", "col098", "col099");

                // test forward, within a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col100"), ByteBufferUtil.bytes("col103"), false, 4);
                assertColumns(cf, "col100", "col101", "col102", "col103");

                // test reversed, within a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col103"), ByteBufferUtil.bytes("col100"), true, 4);
                assertColumns(cf, "col100", "col101", "col102", "col103");

                // test forward from beginning, spanning a segment.
                String[] strCols = new String[100]; // col000-col099
                for (int i = 0; i < 100; i++)
                    strCols[i] = "col" + fmt.format(i);
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.bytes("col099"), false, 100);
                assertColumns(cf, strCols);

                // test reversed, from end, spanning a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.bytes("col288"), true, 12);
                assertColumns(cf, "col288", "col289", "col290", "col291", "col292", "col293", "col294", "col295", "col296", "col297", "col298", "col299");
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testReversedWithFlushing() throws IOException, ExecutionException, InterruptedException
    {
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfs = table.getColumnFamilyStore("StandardLong1");
        final DecoratedKey ROW = Util.dk("row4");

        for (int i = 0; i < 10; i++)
        {
            RowMutation rm = new RowMutation("Keyspace1", ROW.key);
            ColumnFamily cf = ColumnFamily.create("Keyspace1", "StandardLong1");
            cf.addColumn(new Column(ByteBufferUtil.bytes((long)i), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0));
            rm.add(cf);
            rm.apply();
        }

        cfs.forceBlockingFlush();

        for (int i = 10; i < 20; i++)
        {
            RowMutation rm = new RowMutation("Keyspace1", ROW.key);
            ColumnFamily cf = ColumnFamily.create("Keyspace1", "StandardLong1");
            cf.addColumn(new Column(ByteBufferUtil.bytes((long)i), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0));
            rm.add(cf);
            rm.apply();

            cf = cfs.getColumnFamily(ROW, new QueryPath("StandardLong1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 1);
            assertEquals(1, cf.getColumnNames().size());
            assertEquals(i, cf.getColumnNames().iterator().next().getLong());
        }
    }

    private void validateGetSliceNoMatch(Table table) throws IOException
    {
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard2");
        ColumnFamily cf;

        // key before the rows that exists
        cf = cfStore.getColumnFamily(Util.dk("a"), new QueryPath("Standard2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertColumns(cf);

        // key after the rows that exist
        cf = cfStore.getColumnFamily(Util.dk("z"), new QueryPath("Standard2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 1);
        assertColumns(cf);
    }

    @Test
    public void testGetSliceFromBasic() throws Throwable
    {
        // tests slicing against data from one row in a memtable and then flushed to an sstable
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final DecoratedKey ROW = Util.dk("row1");

        RowMutation rm = new RowMutation("Keyspace1", ROW.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "val1", 1L));
        cf.addColumn(column("col3", "val3", 1L));
        cf.addColumn(column("col4", "val4", 1L));
        cf.addColumn(column("col5", "val5", 1L));
        cf.addColumn(column("col7", "val7", 1L));
        cf.addColumn(column("col9", "val9", 1L));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation("Keyspace1", ROW.key);
        rm.delete(new QueryPath("Standard1", null, ByteBufferUtil.bytes("col4")), 2L);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col5"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 2);
                assertColumns(cf, "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col4"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 2);
                assertColumns(cf, "col4", "col5", "col7");
                assertColumns(ColumnFamilyStore.removeDeleted(cf, Integer.MAX_VALUE), "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col5"), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col6"), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
                assertColumns(cf, "col7", "col9");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col95"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 2);
                assertColumns(cf);

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 2);
                assertColumns(cf);
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetSliceFromAdvanced() throws Throwable
    {
        // tests slicing against data from one row spread across two sstables
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final DecoratedKey ROW = Util.dk("row2");

        RowMutation rm = new RowMutation("Keyspace1", ROW.key);
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

        rm = new RowMutation("Keyspace1", ROW.key);
        cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "valx", 2L));
        cf.addColumn(column("col2", "valx", 2L));
        cf.addColumn(column("col3", "valx", 2L));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ByteBufferUtil.bytes("col2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 3);
                assertColumns(cf, "col2", "col3", "col4");
                
                ByteBuffer col = cf.getColumn(ByteBufferUtil.bytes("col2")).value();
                assertEquals(ByteBufferUtil.string(col), "valx");
                
                col = cf.getColumn(ByteBufferUtil.bytes("col3")).value();
                assertEquals(ByteBufferUtil.string(col), "valx");
                
                col = cf.getColumn(ByteBufferUtil.bytes("col4")).value();
                assertEquals(ByteBufferUtil.string(col), "val4");
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetSliceFromLarge() throws Throwable
    {
        // tests slicing against 1000 columns in an sstable
        Table table = Table.open("Keyspace1");
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        DecoratedKey key = Util.dk("row3");
        RowMutation rm = new RowMutation("Keyspace1", key.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        for (int i = 1000; i < 2000; i++)
            cf.addColumn(column("col" + i, ("v" + i), 1L));
        rm.add(cf);
        rm.apply();
        cfStore.forceBlockingFlush();

        validateSliceLarge(cfStore);

        // compact so we have a big row with more than the minimum index count
        if (cfStore.getSSTables().size() > 1)
        {
            CompactionManager.instance.performMaximal(cfStore);
        }
        // verify that we do indeed have multiple index entries
        SSTableReader sstable = cfStore.getSSTables().iterator().next();
        long position = sstable.getPosition(key, SSTableReader.Operator.EQ);
        RandomAccessReader file = sstable.openDataReader(false);
        file.seek(position);
        assert ByteBufferUtil.readWithShortLength(file).equals(key.key);
        SSTableReader.readRowSize(file, sstable.descriptor);
        IndexHelper.skipBloomFilter(file);
        ArrayList<IndexHelper.IndexInfo> indexes = IndexHelper.deserializeIndex(file);
        assert indexes.size() > 2;

        validateSliceLarge(cfStore);
    }

    private void validateSliceLarge(ColumnFamilyStore cfStore) throws IOException
    {
        DecoratedKey key = Util.dk("row3");
        ColumnFamily cf;
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col1000"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 3);
        assertColumns(cf, "col1000", "col1001", "col1002");
        
        ByteBuffer col; 
        col = cf.getColumn(ByteBufferUtil.bytes("col1000")).value();
        assertEquals(ByteBufferUtil.string(col), "v1000");
        col = cf.getColumn(ByteBufferUtil.bytes("col1001")).value();
        assertEquals(ByteBufferUtil.string(col), "v1001");
        col = cf.getColumn(ByteBufferUtil.bytes("col1002")).value();
        assertEquals(ByteBufferUtil.string(col), "v1002");
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col1195"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 3);
        assertColumns(cf, "col1195", "col1196", "col1197");
        
        col = cf.getColumn(ByteBufferUtil.bytes("col1195")).value();
        assertEquals(ByteBufferUtil.string(col), "v1195");
        col = cf.getColumn(ByteBufferUtil.bytes("col1196")).value();
        assertEquals(ByteBufferUtil.string(col), "v1196");
        col = cf.getColumn(ByteBufferUtil.bytes("col1197")).value();
        assertEquals(ByteBufferUtil.string(col), "v1197");
        
       
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col1996"), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 1000);
        IColumn[] columns = cf.getSortedColumns().toArray(new IColumn[0]);
        for (int i = 1000; i < 1996; i++)
        {
            String expectedName = "col" + i;
            IColumn column = columns[i - 1000];
            assertEquals(ByteBufferUtil.string(column.name()), expectedName);
            assertEquals(ByteBufferUtil.string(column.value()), ("v" + i));
        }

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col1990"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 3);
        assertColumns(cf, "col1990", "col1991", "col1992");
        col = cf.getColumn(ByteBufferUtil.bytes("col1990")).value();
        assertEquals(ByteBufferUtil.string(col), "v1990");
        col = cf.getColumn(ByteBufferUtil.bytes("col1991")).value();
        assertEquals(ByteBufferUtil.string(col), "v1991");
        col = cf.getColumn(ByteBufferUtil.bytes("col1992")).value();
        assertEquals(ByteBufferUtil.string(col), "v1992");
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 3);
        assertColumns(cf, "col1997", "col1998", "col1999");
        col = cf.getColumn(ByteBufferUtil.bytes("col1997")).value();
        assertEquals(ByteBufferUtil.string(col), "v1997");
        col = cf.getColumn(ByteBufferUtil.bytes("col1998")).value();
        assertEquals(ByteBufferUtil.string(col), "v1998");
        col = cf.getColumn(ByteBufferUtil.bytes("col1999")).value();
        assertEquals(ByteBufferUtil.string(col), "v1999");
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col9000"), ByteBufferUtil.EMPTY_BYTE_BUFFER, true, 3);
        assertColumns(cf, "col1997", "col1998", "col1999");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ByteBufferUtil.bytes("col9000"), ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 3);
        assertColumns(cf);
    }

    @Test
    public void testGetSliceFromSuperBasic() throws Throwable
    {
        // tests slicing against data from one row spread across two sstables
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Super1");
        final DecoratedKey ROW = Util.dk("row2");

        RowMutation rm = new RowMutation("Keyspace1", ROW.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Super1");
        SuperColumn sc = new SuperColumn(ByteBufferUtil.bytes("sc1"), Int32Type.instance);
        sc.addColumn(new Column(getBytes(1), ByteBufferUtil.bytes("val1"), 1L));
        cf.addColumn(sc);
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf = cfStore.getColumnFamily(ROW, new QueryPath("Super1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, 10);
                assertColumns(cf, "sc1");
                
                ByteBuffer val = cf.getColumn(ByteBufferUtil.bytes("sc1")).getSubColumn(getBytes(1)).value();
                
                assertEquals(ByteBufferUtil.string(val), "val1");
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    public static void assertColumns(ColumnFamily cf, String... columnNames)
    {
        assertColumns((IColumnContainer)cf, columnNames);
    }

    public static void assertSubColumns(ColumnFamily cf, String scName, String... columnNames)
    {
        IColumnContainer sc = cf == null ? null : ((IColumnContainer)cf.getColumn(ByteBufferUtil.bytes(scName)));
        assertColumns(sc, columnNames);
    }

    public static void assertColumns(IColumnContainer container, String... columnNames)
    {
        Collection<IColumn> columns = container == null ? new TreeSet<IColumn>() : container.getSortedColumns();
        List<String> L = new ArrayList<String>();
        for (IColumn column : columns)
        {
            try
            {
                L.add(ByteBufferUtil.string(column.name()));
            }
            catch (CharacterCodingException e)
            {
                throw new AssertionError(e);
            }
        }

        List<String> names = new ArrayList<String>(columnNames.length);

        names.addAll(Arrays.asList(columnNames));

        String[] columnNames1 = names.toArray(new String[0]);
        String[] la = L.toArray(new String[columns.size()]);

        assert Arrays.equals(la, columnNames1)
                : String.format("Columns [%s])] is not expected [%s]",
                                ((container == null) ? "" : container.getComparator().getColumnsString(columns)),
                                StringUtils.join(columnNames1, ","));
    }

    public static void assertColumn(ColumnFamily cf, String name, String value, long timestamp)
    {
        assertColumn(cf.getColumn(ByteBufferUtil.bytes(name)), value, timestamp);
    }

    public static void assertSubColumn(ColumnFamily cf, String scName, String name, String value, long timestamp)
    {
        SuperColumn sc = (SuperColumn)cf.getColumn(ByteBufferUtil.bytes(scName));
        assertColumn(sc.getSubColumn(ByteBufferUtil.bytes(name)), value, timestamp);
    }

    public static void assertColumn(IColumn column, String value, long timestamp)
    {
        assertNotNull(column);
        assertEquals(0, ByteBufferUtil.compareUnsigned(column.value(), ByteBufferUtil.bytes(value)));
        assertEquals(timestamp, column.timestamp());
    }
}
