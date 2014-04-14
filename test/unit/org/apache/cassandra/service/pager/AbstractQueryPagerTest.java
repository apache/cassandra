/*
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
package org.apache.cassandra.service.pager;

import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.filter.ColumnCounter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AbstractQueryPagerTest
{
    @Test
    public void discardFirstTest()
    {
        TestPager pager = new TestPager();
        List<Row> rows = Arrays.asList(createRow("r1", 1),
                                       createRow("r2", 3),
                                       createRow("r3", 2));

        assertEquals(3, rows.size());
        assertRow(rows.get(0), "r1", 0);
        assertRow(rows.get(1), "r2", 0, 1, 2);
        assertRow(rows.get(2), "r3", 0, 1);

        rows = pager.discardFirst(rows, 1);

        assertEquals(2, rows.size());
        assertRow(rows.get(0), "r2", 0, 1, 2);
        assertRow(rows.get(1), "r3", 0, 1);

        rows = pager.discardFirst(rows, 1);

        assertEquals(2, rows.size());
        assertRow(rows.get(0), "r2", 1, 2);
        assertRow(rows.get(1), "r3", 0, 1);

        rows = pager.discardFirst(rows, 3);

        assertEquals(1, rows.size());
        assertRow(rows.get(0), "r3", 1);

        rows = pager.discardFirst(rows, 1);

        assertTrue(rows.isEmpty());
    }

    @Test
    public void discardLastTest()
    {
        TestPager pager = new TestPager();
        List<Row> rows = Arrays.asList(createRow("r1", 2),
                                       createRow("r2", 3),
                                       createRow("r3", 1));

        assertEquals(3, rows.size());
        assertRow(rows.get(0), "r1", 0, 1);
        assertRow(rows.get(1), "r2", 0, 1, 2);
        assertRow(rows.get(2), "r3", 0);

        rows = pager.discardLast(rows, 1);

        assertEquals(2, rows.size());
        assertRow(rows.get(0), "r1", 0, 1);
        assertRow(rows.get(1), "r2", 0, 1, 2);

        rows = pager.discardLast(rows, 1);

        assertEquals(2, rows.size());
        assertRow(rows.get(0), "r1", 0, 1);
        assertRow(rows.get(1), "r2", 0, 1);

        rows = pager.discardLast(rows, 3);

        assertEquals(1, rows.size());
        assertRow(rows.get(0), "r1", 0);

        rows = pager.discardLast(rows, 1);

        assertTrue(rows.isEmpty());
    }

    private void assertRow(Row row, String name, int... values)
    {
        assertEquals(row.key.getKey(), ByteBufferUtil.bytes(name));
        assertEquals(values.length, row.cf.getColumnCount());

        int i = 0;
        for (Cell c : row.cf)
            assertEquals(values[i++], i(c.name().toByteBuffer()));
    }

    private Row createRow(String name, int nbCol)
    {
        return new Row(Util.dk(name), createCF(nbCol));
    }

    private ColumnFamily createCF(int nbCol)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(createMetadata());
        for (int i = 0; i < nbCol; i++)
            cf.addColumn(CellNames.simpleDense(bb(i)), bb(i), 0);
        return cf;
    }

    private CFMetaData createMetadata()
    {
        return new CFMetaData("ks", "cf", ColumnFamilyType.Standard, CellNames.fromAbstractType(Int32Type.instance, false));
    }

    private static ByteBuffer bb(int i)
    {
        return ByteBufferUtil.bytes(i);
    }

    private static int i(ByteBuffer bb)
    {
        return ByteBufferUtil.toInt(bb);
    }

    private static class TestPager extends AbstractQueryPager
    {
        public TestPager()
        {
            // We use this to test more thorougly DiscardFirst and DiscardLast (more generic pager behavior is tested in
            // QueryPagerTest). The only thing those method use is the result of the columnCounter() method. So to keep
            // it simple, we fake all actual parameters in the ctor below but just override the columnCounter() method.
            super(null, 0, false, null, null, 0);
        }

        @Override
        public ColumnCounter columnCounter()
        {
            return new ColumnCounter(0);
        }

        public PagingState state()
        {
            return null;
        }

        protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery)
        {
            return null;
        }

        protected boolean containsPreviousLast(Row first)
        {
            return false;
        }

        protected boolean recordLast(Row last)
        {
            return false;
        }

        protected boolean isReversed()
        {
            return false;
        }
    }
}
