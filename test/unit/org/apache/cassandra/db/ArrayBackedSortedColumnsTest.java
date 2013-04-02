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


import java.util.*;

import org.junit.Test;

import static org.junit.Assert.*;

import com.google.common.base.Functions;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.HeapAllocator;

public class ArrayBackedSortedColumnsTest extends SchemaLoader
{
    @Test
    public void testAdd()
    {
        testAddInternal(false);
        testAddInternal(true);
    }

    private CFMetaData metadata()
    {
        return Schema.instance.getCFMetaData("Keyspace1", "Standard1");
    }

    private void testAddInternal(boolean reversed)
    {
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        int[] values = new int[]{ 1, 2, 2, 3 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        Iterator<Column> iter = map.iterator();
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
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        ColumnFamily map2 = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values1 = new int[]{ 1, 3, 5, 6 };
        int[] values2 = new int[]{ 2, 4, 5, 6 };

        for (int i = 0; i < values1.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values1[reversed ? values1.length - 1 - i : i])), HeapAllocator.instance);

        for (int i = 0; i < values2.length; ++i)
            map2.addColumn(new Column(ByteBufferUtil.bytes(values2[reversed ? values2.length - 1 - i : i])), HeapAllocator.instance);

        map2.addAll(map, HeapAllocator.instance, Functions.<Column>identity());

        Iterator<Column> iter = map2.iterator();
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
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        int[] values = new int[]{ 1, 2, 3, 5, 9 };

        List<Column> sorted = new ArrayList<Column>();
        for (int v : values)
            sorted.add(new Column(ByteBufferUtil.bytes(v)));
        List<Column> reverseSorted = new ArrayList<Column>(sorted);
        Collections.reverse(reverseSorted);

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        assertSame(sorted, map.getSortedColumns());
        assertSame(reverseSorted, map.getReverseSortedColumns());
    }

    @Test
    public void testIterator()
    {
        testIteratorInternal(false);
        //testIteratorInternal(true);
    }

    private void testIteratorInternal(boolean reversed)
    {
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values = new int[]{ 1, 2, 3, 5, 9 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new Column(ByteBufferUtil.bytes(values[reversed ? values.length - 1 - i : i])), HeapAllocator.instance);

        assertSame(new int[]{ 3, 2, 1 }, map.reverseIterator(new ColumnSlice[]{ new ColumnSlice(ByteBufferUtil.bytes(3), ByteBufferUtil.EMPTY_BYTE_BUFFER) }));
        assertSame(new int[]{ 3, 2, 1 }, map.reverseIterator(new ColumnSlice[]{ new ColumnSlice(ByteBufferUtil.bytes(4), ByteBufferUtil.EMPTY_BYTE_BUFFER) }));

        assertSame(map.iterator(), map.iterator(ColumnSlice.ALL_COLUMNS_ARRAY));
    }

    private <T> void assertSame(Iterable<T> c1, Iterable<T> c2)
    {
        assertSame(c1.iterator(), c2.iterator());
    }

    private <T> void assertSame(Iterator<T> iter1, Iterator<T> iter2)
    {
        while (iter1.hasNext() && iter2.hasNext())
            assertEquals(iter1.next(), iter2.next());
        if (iter1.hasNext() || iter2.hasNext())
            fail("The collection don't have the same size");
    }

    private void assertSame(int[] names, Iterator<Column> iter)
    {
        for (int name : names)
        {
            assert iter.hasNext() : "Expected " + name + " but no more result";
            int value = ByteBufferUtil.toInt(iter.next().name());
            assert name == value : "Expected " + name + " but got " + value;
        }
    }
}
