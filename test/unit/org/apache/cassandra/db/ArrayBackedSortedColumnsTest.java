package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.*;

import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.utils.HeapAllocator;

public class ArrayBackedSortedColumnsTest
{
    @Test
    public void testAdd()
    {
        testAddInternal(false);
        testAddInternal(true);
    }

    private void testAddInternal(boolean reversed)
    {
        ISortedColumns map = ArrayBackedSortedColumns.factory().create(BytesType.instance, reversed);
        int[] values = new int[]{ 1, 2, 2, 3 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        Iterator<IColumn> iter = map.iterator();
        assertEquals("1st column", 1, iter.next().name().getInt(0));
        assertEquals("2nd column", 2, iter.next().name().getInt(0));
        assertEquals("3rd column", 3, iter.next().name().getInt(0));
    }

    @Test
    public void testAddAll()
    {
        testAddAllInternal(false);
        testAddAllInternal(true);
    }

    private void testAddAllInternal(boolean reversed)
    {
        ISortedColumns map = ArrayBackedSortedColumns.factory().create(BytesType.instance, reversed);
        ISortedColumns map2 = ArrayBackedSortedColumns.factory().create(BytesType.instance, reversed);

        int[] values1 = new int[]{ 1, 3, 5, 6 };
        int[] values2 = new int[]{ 2, 4, 5, 6 };

        for (int i = 0; i < values1.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values1[reversed ? values1.length - 1 - i : i])), HeapAllocator.instance);

        for (int i = 0; i < values2.length; ++i)
            map2.addColumn(new Column(ByteBufferUtil.bytes(values2[reversed ? values2.length - 1 - i : i])), HeapAllocator.instance);

        map2.addAll(map, HeapAllocator.instance);

        Iterator<IColumn> iter = map2.iterator();
        assertEquals("1st column", 1, iter.next().name().getInt(0));
        assertEquals("2nd column", 2, iter.next().name().getInt(0));
        assertEquals("3rd column", 3, iter.next().name().getInt(0));
        assertEquals("4st column", 4, iter.next().name().getInt(0));
        assertEquals("5st column", 5, iter.next().name().getInt(0));
        assertEquals("6st column", 6, iter.next().name().getInt(0));
    }

    @Test
    public void testGetCollection()
    {
        testGetCollectionInternal(false);
        testGetCollectionInternal(true);
    }

    private void testGetCollectionInternal(boolean reversed)
    {
        ISortedColumns map = ArrayBackedSortedColumns.factory().create(BytesType.instance, reversed);
        int[] values = new int[]{ 1, 2, 3, 5, 9 };

        List<IColumn> sorted = new ArrayList<IColumn>();
        for (int v : values)
            sorted.add(new Column(ByteBufferUtil.bytes(v)));
        List<IColumn> reverseSorted = new ArrayList<IColumn>(sorted);
        Collections.reverse(reverseSorted);

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        assertSame(sorted, map.getSortedColumns());
        assertSame(reverseSorted, map.getReverseSortedColumns());
    }

    @Test
    public void testGetNames()
    {
        testGetNamesInternal(false);
        testGetNamesInternal(true);
    }

    private void testGetNamesInternal(boolean reversed)
    {
        ISortedColumns map = ArrayBackedSortedColumns.factory().create(BytesType.instance, reversed);
        List<ByteBuffer> names = new ArrayList<ByteBuffer>();
        int[] values = new int[]{ 1, 2, 3, 5, 9 };
        for (int v : values)
            names.add(ByteBufferUtil.bytes(v));

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        assertSame(names, map.getColumnNames());
    }

    private <T> void assertSame(Collection<T> c1, Collection<T> c2)
    {
        Iterator<T> iter1 = c1.iterator();
        Iterator<T> iter2 = c2.iterator();
        while (iter1.hasNext() && iter2.hasNext())
            assertEquals(iter1.next(), iter2.next());
        if (iter1.hasNext() || iter2.hasNext())
            fail("The collection don't have the same size");
    }
}
