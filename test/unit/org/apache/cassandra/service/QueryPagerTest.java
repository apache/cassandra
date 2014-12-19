/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.util.*;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.*;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.Util.range;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

@RunWith(OrderedJUnit4ClassRunner.class)
public class QueryPagerTest extends SchemaLoader
{
    private static final String KS = "Keyspace1";
    private static final String CF = "Standard1";

    private static String string(CellName name)
    {
        return string(name.toByteBuffer());
    }

    private static String string(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void addData()
    {
        cfs().clearUnsafe();

        int nbKeys = 10;
        int nbCols = 10;

        /*
         * Creates the following data:
         *   k1: c1 ... cn
         *   ...
         *   ki: c1 ... cn
         */
        for (int i = 0; i < nbKeys; i++)
        {
            Mutation rm = new Mutation(KS, bytes("k" + i));
            ColumnFamily cf = rm.addOrGet(CF);

            for (int j = 0; j < nbCols; j++)
                cf.addColumn(Util.column("c" + j, "", 0));

            rm.applyUnsafe();
        }
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(KS).getColumnFamilyStore(CF);
    }

    private static String toString(List<Row> rows)
    {
        StringBuilder sb = new StringBuilder();
        for (Row row : rows)
            sb.append(string(row.key.getKey())).append(":").append(toString(row.cf)).append("\n");
        return sb.toString();
    }

    private static String toString(ColumnFamily cf)
    {
        if (cf == null)
            return "";

        StringBuilder sb = new StringBuilder();
        for (Cell c : cf)
            sb.append(" ").append(string(c.name()));
        return sb.toString();
    }

    private static ReadCommand namesQuery(String key, String... names)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs().metadata.comparator);
        for (String name : names)
            s.add(CellNames.simpleDense(bytes(name)));
        return new SliceByNamesReadCommand(KS, bytes(key), CF, System.currentTimeMillis(), new NamesQueryFilter(s, true));
    }

    private static ReadCommand sliceQuery(String key, String start, String end, int count)
    {
        return sliceQuery(key, start, end, false, count);
    }

    private static ReadCommand sliceQuery(String key, String start, String end, boolean reversed, int count)
    {
        SliceQueryFilter filter = new SliceQueryFilter(CellNames.simpleDense(bytes(start)), CellNames.simpleDense(bytes(end)), reversed, count);
        // Note: for MultiQueryTest, we need the same timestamp/expireBefore for all queries, so we just use 0 as it doesn't matter here.
        return new SliceFromReadCommand(KS, bytes(key), CF, 0, filter);
    }

    private static RangeSliceCommand rangeNamesQuery(AbstractBounds<RowPosition> range, int count, String... names)
    {
        SortedSet<CellName> s = new TreeSet<CellName>(cfs().metadata.comparator);
        for (String name : names)
            s.add(CellNames.simpleDense(bytes(name)));
        return new RangeSliceCommand(KS, CF, System.currentTimeMillis(), new NamesQueryFilter(s, true), range, count);
    }

    private static RangeSliceCommand rangeSliceQuery(AbstractBounds<RowPosition> range, int count, String start, String end)
    {
        SliceQueryFilter filter = new SliceQueryFilter(CellNames.simpleDense(bytes(start)), CellNames.simpleDense(bytes(end)), false, Integer.MAX_VALUE);
        return new RangeSliceCommand(KS, CF, System.currentTimeMillis(), filter, range, count);
    }

    private static void assertRow(Row r, String key, String... names)
    {
        ByteBuffer[] bbs = new ByteBuffer[names.length];
        for (int i = 0; i < names.length; i++)
            bbs[i] = bytes(names[i]);
        assertRow(r, key, bbs);
    }

    private static void assertRow(Row r, String key, ByteBuffer... names)
    {
        assertEquals(key, string(r.key.getKey()));
        assertNotNull(r.cf);
        int i = 0;
        for (Cell c : r.cf)
        {
            // Ignore deleted cells if we have them
            if (!c.isLive())
                continue;

            ByteBuffer expected = names[i++];
            assertEquals("column " + i + " doesn't match: " + toString(r.cf), expected, c.name().toByteBuffer());
        }
    }

    @Test
    public void namesQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(namesQuery("k0", "c1", "c5", "c7", "c8"));

        assertFalse(pager.isExhausted());
        List<Row> page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c1", "c5", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void sliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(sliceQuery("k0", "c1", "c8", 10));

        List<Row> page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c1", "c2", "c3");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c4", "c5", "c6");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c7", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void reversedSliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(sliceQuery("k0", "c8", "c1", true, 10));

        List<Row> page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c6", "c7", "c8");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c3", "c4", "c5");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k0", "c1", "c2");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void multiQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(new Pageable.ReadCommands(new ArrayList<ReadCommand>() {{
            add(sliceQuery("k1", "c2", "c6", 10));
            add(sliceQuery("k4", "c3", "c5", 10));
        }}, 10));

        List<Row> page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k1", "c2", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(4);
        assertEquals(toString(page), 2, page.size());
        assertRow(page.get(0), "k1", "c5", "c6");
        assertRow(page.get(1), "k4", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k4", "c5");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeNamesQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(rangeNamesQuery(range("k0", "k5"), 100, "c1", "c4", "c8"));

        List<Row> page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 3, page.size());
        for (int i = 1; i <= 3; i++)
            assertRow(page.get(i-1), "k" + i, "c1", "c4", "c8");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(3);
        assertEquals(toString(page), 2, page.size());
        for (int i = 4; i <= 5; i++)
            assertRow(page.get(i-4), "k" + i, "c1", "c4", "c8");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void rangeSliceQueryTest() throws Exception
    {
        QueryPager pager = QueryPagers.localPager(rangeSliceQuery(range("k1", "k5"), 100, "c1", "c7"));

        List<Row> page;

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k2", "c1", "c2", "c3", "c4", "c5");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(4);
        assertEquals(toString(page), 2, page.size());
        assertRow(page.get(0), "k2", "c6", "c7");
        assertRow(page.get(1), "k3", "c1", "c2");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(6);
        assertEquals(toString(page), 2, page.size());
        assertRow(page.get(0), "k3", "c3", "c4", "c5", "c6", "c7");
        assertRow(page.get(1), "k4", "c1");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k4", "c2", "c3", "c4", "c5", "c6");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        assertEquals(toString(page), 2, page.size());
        assertRow(page.get(0), "k4", "c7");
        assertRow(page.get(1), "k5", "c1", "c2", "c3", "c4");

        assertFalse(pager.isExhausted());
        page = pager.fetchPage(5);
        assertEquals(toString(page), 1, page.size());
        assertRow(page.get(0), "k5", "c5", "c6", "c7");

        assertTrue(pager.isExhausted());
    }

    @Test
    public void SliceQueryWithTombstoneTest() throws Exception
    {
        // Testing for the bug of #6748
        String keyspace = "cql_keyspace";
        String table = "table2";
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        CompositeType ct = (CompositeType)cfs.metadata.comparator.asAbstractType();

        // Insert rows but with a tombstone as last cell
        for (int i = 0; i < 5; i++)
            executeInternal(String.format("INSERT INTO %s.%s (k, c, v) VALUES ('k%d', 'c%d', null)", keyspace, table, 0, i));

        SliceQueryFilter filter = new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 100);
        QueryPager pager = QueryPagers.localPager(new SliceFromReadCommand(keyspace, bytes("k0"), table, 0, filter));

        for (int i = 0; i < 5; i++)
        {
            List<Row> page = pager.fetchPage(1);
            assertEquals(toString(page), 1, page.size());
            // The only live cell we should have each time is the row marker
            assertRow(page.get(0), "k0", ct.decompose("c" + i, ""));
        }
    }
}
