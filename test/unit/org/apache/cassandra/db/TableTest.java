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

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import static junit.framework.Assert.*;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.WrappedRunnable;
import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.getBytes;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.utils.FBUtilities;

public class TableTest extends CleanupHelper
{
    private static final DecoratedKey KEY2 = Util.dk("key2");
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
        cf.addColumn(column("col1","val1", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard3"), new TreeSet<byte[]>()));
                assertColumns(cf);

                cf = cfStore.getColumnFamily(QueryFilter.getSliceFilter(TEST_KEY, new QueryPath("Standard3"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, null, false, 0));
                assertColumns(cf);

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard3"), "col99".getBytes()));
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
        cf.addColumn(column("col1","val1", new TimestampClock(1L)));
        cf.addColumn(column("col2","val2", new TimestampClock(1L)));
        cf.addColumn(column("col3","val3", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard1"), "col1".getBytes()));
                assertColumns(cf, "col1");

                cf = cfStore.getColumnFamily(QueryFilter.getNamesFilter(TEST_KEY, new QueryPath("Standard1"), "col3".getBytes()));
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
        cf.addColumn(column("a", "val1", new TimestampClock(1L)));
        cf.addColumn(column("b", "val2", new TimestampClock(1L)));
        cf.addColumn(column("c", "val3", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "c".getBytes(), false, 100);
        assertEquals(2, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "b".getBytes(), false, 100);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "b".getBytes(), "c".getBytes(), false, 1);
        assertEquals(1, cf.getColumnCount());
        
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "c".getBytes(), "b".getBytes(), false, 1);
        assertNull(cf);
    }

    @Test
    public void testGetSliceNoMatch() throws Throwable
    {
        Table table = Table.open("Keyspace1");
        RowMutation rm = new RowMutation("Keyspace1", "row1000".getBytes());
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard2");
        cf.addColumn(column("col1", "val1", new TimestampClock(1)));
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
            cf.addColumn(column("col" + fmt.format(i), "omg!thisisthevalue!"+i, new TimestampClock(1L)));
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
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col096".getBytes(), "col099".getBytes(), false, 4);
                assertColumns(cf, "col096", "col097", "col098", "col099");

                // test reversed, spanning a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col099".getBytes(), "col096".getBytes(), true, 4);
                assertColumns(cf, "col096", "col097", "col098", "col099");

                // test forward, within a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col100".getBytes(), "col103".getBytes(), false, 4);
                assertColumns(cf, "col100", "col101", "col102", "col103");

                // test reversed, within a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col103".getBytes(), "col100".getBytes(), true, 4);
                assertColumns(cf, "col100", "col101", "col102", "col103");

                // test forward from beginning, spanning a segment.
                String[] strCols = new String[100]; // col000-col099
                for (int i = 0; i < 100; i++)
                    strCols[i] = "col" + fmt.format(i);
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "".getBytes(), "col099".getBytes(), false, 100);
                assertColumns(cf, strCols);

                // test reversed, from end, spanning a segment.
                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "".getBytes(), "col288".getBytes(), true, 12);
                assertColumns(cf, "col288", "col289", "col290", "col291", "col292", "col293", "col294", "col295", "col296", "col297", "col298", "col299");
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    @Test
    public void testGetSliceWithBitmasks() throws Throwable
    {
        // tests slicing against data from one row in a memtable and then flushed to an sstable
        final Table table = Table.open("Keyspace1");
        final ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard1");
        final DecoratedKey ROW = Util.dk("row-bitmasktest");
        final NumberFormat fmt = new DecimalFormat("000");

        RowMutation rm = new RowMutation("Keyspace1", ROW.key);
        ColumnFamily cf = ColumnFamily.create("Keyspace1", "Standard1");
        // at this rate, we're getting 78-79 cos/block, assuming the blocks are set to be about 4k.
        // so if we go to 300, we'll get at least 4 blocks, which is plenty for testing.
        for (int i = 0; i < 300; i++)
            cf.addColumn(column("col" + fmt.format(i), "omg!thisisthevalue!"+i, new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                // blocks are partitioned like this: 000-097, 098-193, 194-289, 290-299, assuming a 4k column index size.
                assert DatabaseDescriptor.getColumnIndexSize() == 4096 : "Unexpected column index size, block boundaries won't be where tests expect them.";

                for (String[] bitmaskTests: new String[][] { {}, {"test one", "test two" }, { new String(new byte[] { 0, 1, 0x20, (byte) 0xff }) } })
                {
                    ArrayList<byte[]> bitmasks = new ArrayList<byte[]>(bitmaskTests.length);

                    // test forward, spanning a segment.
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col096".getBytes(), "col099".getBytes(), bitmasks, false, 4);
                    assertBitmaskedColumns(cf, bitmasks, "col096", "col097", "col098", "col099");

                    // test reversed, spanning a segment.
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col099".getBytes(), "col096".getBytes(), bitmasks, true, 4);
                    assertBitmaskedColumns(cf, bitmasks, "col096", "col097", "col098", "col099");

                    // test forward, within a segment.
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col100".getBytes(), "col103".getBytes(), bitmasks, false, 4);
                    assertBitmaskedColumns(cf, bitmasks, "col100", "col101", "col102", "col103");

                    // test reversed, within a segment.
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col103".getBytes(), "col100".getBytes(), bitmasks, true, 4);
                    assertBitmaskedColumns(cf, bitmasks, "col100", "col101", "col102", "col103");

                    // test forward from beginning, spanning a segment.
                    String[] strCols = new String[100]; // col000-col099
                    for (int i = 0; i < 100; i++)
                        strCols[i] = "col" + fmt.format(i);
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "".getBytes(), "col099".getBytes(), bitmasks, false, 100);
                    assertBitmaskedColumns(cf, bitmasks, strCols);

                    // test reversed, from end, spanning a segment.
                    cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "".getBytes(), "col288".getBytes(), bitmasks, true, 12);
                    assertBitmaskedColumns(cf, bitmasks, "col288", "col289", "col290", "col291", "col292", "col293", "col294", "col295", "col296", "col297", "col298", "col299");
                }
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    private void validateGetSliceNoMatch(Table table) throws IOException
    {
        ColumnFamilyStore cfStore = table.getColumnFamilyStore("Standard2");
        ColumnFamily cf;

        // key before the rows that exists
        cf = cfStore.getColumnFamily(Util.dk("a"), new QueryPath("Standard2"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
        assertColumns(cf);

        // key after the rows that exist
        cf = cfStore.getColumnFamily(Util.dk("z"), new QueryPath("Standard2"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 1);
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
        cf.addColumn(column("col1", "val1", new TimestampClock(1L)));
        cf.addColumn(column("col3", "val3", new TimestampClock(1L)));
        cf.addColumn(column("col4", "val4", new TimestampClock(1L)));
        cf.addColumn(column("col5", "val5", new TimestampClock(1L)));
        cf.addColumn(column("col7", "val7", new TimestampClock(1L)));
        cf.addColumn(column("col9", "val9", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();

        rm = new RowMutation("Keyspace1", ROW.key);
        rm.delete(new QueryPath("Standard1", null, "col4".getBytes()), new TimestampClock(2L));
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col5".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf, "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col4".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf, "col4", "col5", "col7");
                assertColumns(ColumnFamilyStore.removeDeleted(cf, Integer.MAX_VALUE), "col5", "col7");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col5".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col6".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf, "col3", "col4", "col5");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
                assertColumns(cf, "col7", "col9");

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col95".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 2);
                assertColumns(cf);

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col0".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 2);
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
        cf.addColumn(column("col1", "val1", new TimestampClock(1L)));
        cf.addColumn(column("col2", "val2", new TimestampClock(1L)));
        cf.addColumn(column("col3", "val3", new TimestampClock(1L)));
        cf.addColumn(column("col4", "val4", new TimestampClock(1L)));
        cf.addColumn(column("col5", "val5", new TimestampClock(1L)));
        cf.addColumn(column("col6", "val6", new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();
        cfStore.forceBlockingFlush();

        rm = new RowMutation("Keyspace1", ROW.key);
        cf = ColumnFamily.create("Keyspace1", "Standard1");
        cf.addColumn(column("col1", "valx", new TimestampClock(2L)));
        cf.addColumn(column("col2", "valx", new TimestampClock(2L)));
        cf.addColumn(column("col3", "valx", new TimestampClock(2L)));
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf;

                cf = cfStore.getColumnFamily(ROW, new QueryPath("Standard1"), "col2".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
                assertColumns(cf, "col2", "col3", "col4");
                assertEquals(new String(cf.getColumn("col2".getBytes()).value()), "valx");
                assertEquals(new String(cf.getColumn("col3".getBytes()).value()), "valx");
                assertEquals(new String(cf.getColumn("col4".getBytes()).value()), "val4");
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
            cf.addColumn(column("col" + i, ("v" + i), new TimestampClock(1L)));
        rm.add(cf);
        rm.apply();
        cfStore.forceBlockingFlush();

        validateSliceLarge(cfStore);
        // compact so we have a big row with more than the minimum index count
        if (cfStore.getSSTables().size() > 1)
        {
            CompactionManager.instance.submitMajor(cfStore).get();
        }
        SSTableReader sstable = cfStore.getSSTables().iterator().next();
        SSTable.PositionSize info = sstable.getPosition(key);
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(sstable.getFilename(), "r");
        file.seek(info.position);
        assert Arrays.equals(FBUtilities.readShortByteArray(file), key.key);
        file.readInt();
        IndexHelper.skipBloomFilter(file);
        ArrayList<IndexHelper.IndexInfo> indexes = IndexHelper.deserializeIndex(file);
        assert indexes.size() > 2;
        validateSliceLarge(cfStore);
    }

    private void validateSliceLarge(ColumnFamilyStore cfStore) throws IOException
    {
        DecoratedKey key = Util.dk("row3");
        ColumnFamily cf;
        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col1000".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
        assertColumns(cf, "col1000", "col1001", "col1002");
        assertEquals(new String(cf.getColumn("col1000".getBytes()).value()), "v1000");
        assertEquals(new String(cf.getColumn("col1001".getBytes()).value()), "v1001");
        assertEquals(new String(cf.getColumn("col1002".getBytes()).value()), "v1002");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col1195".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
        assertColumns(cf, "col1195", "col1196", "col1197");
        assertEquals(new String(cf.getColumn("col1195".getBytes()).value()), "v1195");
        assertEquals(new String(cf.getColumn("col1196".getBytes()).value()), "v1196");
        assertEquals(new String(cf.getColumn("col1197".getBytes()).value()), "v1197");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col1996".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 1000);
        IColumn[] columns = cf.getSortedColumns().toArray(new IColumn[0]);
        for (int i = 1000; i < 1996; i++)
        {
            String expectedName = "col" + i;
            IColumn column = columns[i - 1000];
            assert Arrays.equals(column.name(), expectedName.getBytes()) : cfStore.getComparator().getString(column.name()) + " is not " + expectedName;
            assert Arrays.equals(column.value(), ("v" + i).getBytes());
        }

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col1990".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
        assertColumns(cf, "col1990", "col1991", "col1992");
        assertEquals(new String(cf.getColumn("col1990".getBytes()).value()), "v1990");
        assertEquals(new String(cf.getColumn("col1991".getBytes()).value()), "v1991");
        assertEquals(new String(cf.getColumn("col1992".getBytes()).value()), "v1992");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
        assertColumns(cf, "col1997", "col1998", "col1999");
        assertEquals(new String(cf.getColumn("col1999".getBytes()).value()), "v1999");
        assertEquals(new String(cf.getColumn("col1998".getBytes()).value()), "v1998");
        assertEquals(new String(cf.getColumn("col1997".getBytes()).value()), "v1997");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col9000".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, true, 3);
        assertColumns(cf, "col1997", "col1998", "col1999");

        cf = cfStore.getColumnFamily(key, new QueryPath("Standard1"), "col9000".getBytes(), ArrayUtils.EMPTY_BYTE_ARRAY, false, 3);
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
        SuperColumn sc = new SuperColumn("sc1".getBytes(), LongType.instance, ClockType.Timestamp);
        sc.addColumn(new Column(getBytes(1), "val1".getBytes(), new TimestampClock(1L)));
        cf.addColumn(sc);
        rm.add(cf);
        rm.apply();

        Runnable verify = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                ColumnFamily cf = cfStore.getColumnFamily(ROW, new QueryPath("Super1"), ArrayUtils.EMPTY_BYTE_ARRAY, ArrayUtils.EMPTY_BYTE_ARRAY, false, 10);
                assertColumns(cf, "sc1");
                assertEquals(new String(cf.getColumn("sc1".getBytes()).getSubColumn(getBytes(1)).value()), "val1");
            }
        };

        reTest(table.getColumnFamilyStore("Standard1"), verify);
    }

    public static void assertColumns(ColumnFamily cf, String... columnNames)
    {
        assertBitmaskedColumnsNameArray(cf, null, columnNames);
    }

    public static void assertBitmaskedColumns(ColumnFamily cf, List<byte[]> bitmasks, String... unfilteredColumnNames)
    {
        assertBitmaskedColumnsNameArray(cf, bitmasks, unfilteredColumnNames);
    }

    public static void assertBitmaskedColumnsNameArray(ColumnFamily cf, List<byte[]> bitmasks, String[] unfilteredColumnNames)
    {
        Collection<IColumn> columns = cf == null ? new TreeSet<IColumn>() : cf.getSortedColumns();
        List<String> L = new ArrayList<String>();
        for (IColumn column : columns)
        {
            L.add(new String(column.name()));
        }

        List<String> names = new ArrayList<String>(unfilteredColumnNames.length);

        names.addAll(Arrays.asList(unfilteredColumnNames));

        if (bitmasks != null && bitmasks.size() > 0)
        {
            List<Predicate> predicates = new ArrayList<Predicate>(bitmasks.size());
            for (final byte[] bitmask: bitmasks)
            {
                predicates.add(new Predicate()
                {
                    public boolean evaluate(Object o)
                    {
                        try
                        {
                            return SliceQueryFilter.matchesBitmask(bitmask, o.toString().getBytes("UTF-8"));
                        }
                        catch (UnsupportedEncodingException e)
                        {
                            return false;
                        }
                    }
                });
            }

            CollectionUtils.filter(names, PredicateUtils.anyPredicate(predicates));
        }

        String[] columnNames = names.toArray(new String[0]);
        String[] la = L.toArray(new String[columns.size()]);
        StringBuffer lasb = new StringBuffer();
        for (String l: la)
        {
            lasb.append(l);
            lasb.append(", ");
        }

        assert Arrays.equals(la, columnNames)
                : String.format("Columns [%s(as string: %s)])] is not expected [%s] (bitmasks %s)",
                                ((cf == null) ? "" : cf.getComparator().getColumnsString(columns)),
                                lasb.toString(),
                                StringUtils.join(columnNames, ","),
                                SliceFromReadCommand.getBitmaskDescription(bitmasks));
                                
    }
}
