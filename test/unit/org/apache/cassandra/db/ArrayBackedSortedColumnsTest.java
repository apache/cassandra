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
package org.apache.cassandra.db;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.BatchRemoveIterator;

public class ArrayBackedSortedColumnsTest
{
    private static final String KEYSPACE1 = "ArrayBackedSortedColumnsTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testAdd()
    {
        testAddInternal(false);
        testAddInternal(true);
    }

    private CFMetaData metadata()
    {
        return Schema.instance.getCFMetaData(KEYSPACE1, CF_STANDARD1);
    }

    private void testAddInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        int[] values = new int[]{ 1, 2, 2, 3 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        Iterator<Cell> iter = map.iterator();
        assertEquals("1st column", 1, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("2nd column", 2, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("3rd column", 3, iter.next().name().toByteBuffer().getInt(0));
    }

    @Test
    public void testOutOfOrder()
    {
        testAddOutOfOrder(false);
        testAddOutOfOrder(false);
    }

    private void testAddOutOfOrder(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values = new int[]{ 1, 2, 1, 3, 4, 4, 5, 5, 1, 2, 6, 6, 6, 1, 2, 3 };
        for (int i = 0; i < values.length; ++i)
            cells.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        assertEquals(6, cells.getColumnCount());

        Iterator<Cell> iter = cells.iterator();
        assertEquals(1, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(2, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(3, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(4, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(5, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(6, iter.next().name().toByteBuffer().getInt(0));

        // Add more values
        values = new int[]{ 11, 15, 12, 12, 12, 16, 10, 8, 8, 7, 4, 4, 5 };
        for (int i = 0; i < values.length; ++i)
            cells.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        assertEquals(13, cells.getColumnCount());

        iter = cells.reverseIterator();
        assertEquals(16, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(15, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(12, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(11, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(10, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(8,  iter.next().name().toByteBuffer().getInt(0));
        assertEquals(7, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(6, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(5, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(4, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(3, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(2, iter.next().name().toByteBuffer().getInt(0));
        assertEquals(1, iter.next().name().toByteBuffer().getInt(0));
    }

    @Test
    public void testGetColumn()
    {
        testGetColumnInternal(true);
        testGetColumnInternal(false);
    }

    private void testGetColumnInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values = new int[]{ -1, 20, 44, 55, 27, 27, 17, 1, 9, 89, 33, 44, 0, 9 };
        for (int i = 0; i < values.length; ++i)
            cells.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        for (int i : values)
            assertEquals(i, cells.getColumn(type.makeCellName(i)).name().toByteBuffer().getInt(0));
    }

    @Test
    public void testAddAll()
    {
        testAddAllInternal(false);
        testAddAllInternal(true);
    }

    private void testAddAllInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        ColumnFamily map2 = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values1 = new int[]{ 1, 3, 5, 6 };
        int[] values2 = new int[]{ 2, 4, 5, 6 };

        for (int i = 0; i < values1.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values1[reversed ? values1.length - 1 - i : i])));

        for (int i = 0; i < values2.length; ++i)
            map2.addColumn(new BufferCell(type.makeCellName(values2[reversed ? values2.length - 1 - i : i])));

        map2.addAll(map);

        Iterator<Cell> iter = map2.iterator();
        assertEquals("1st column", 1, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("2nd column", 2, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("3rd column", 3, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("4st column", 4, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("5st column", 5, iter.next().name().toByteBuffer().getInt(0));
        assertEquals("6st column", 6, iter.next().name().toByteBuffer().getInt(0));
    }

    @Test
    public void testGetCollection()
    {
        testGetCollectionInternal(false);
        testGetCollectionInternal(true);
    }

    private void testGetCollectionInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        int[] values = new int[]{ 1, 2, 3, 5, 9 };

        List<Cell> sorted = new ArrayList<>();
        for (int v : values)
            sorted.add(new BufferCell(type.makeCellName(v)));
        List<Cell> reverseSorted = new ArrayList<>(sorted);
        Collections.reverse(reverseSorted);

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

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
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values = new int[]{ 1, 2, 3, 5, 9 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        assertSame(new int[]{ 3, 2, 1 }, map.reverseIterator(new ColumnSlice[]{ new ColumnSlice(type.make(3), Composites.EMPTY) }));
        assertSame(new int[]{ 3, 2, 1 }, map.reverseIterator(new ColumnSlice[]{ new ColumnSlice(type.make(4), Composites.EMPTY) }));

        assertSame(map.iterator(), map.iterator(ColumnSlice.ALL_COLUMNS_ARRAY));
    }

    @Test
    public void testSearchIterator()
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), false);

        int[] values = new int[]{ 1, 2, 3, 5, 9, 15, 21, 22 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[i])));

        SearchIterator<CellName, Cell> iter = map.searchIterator();
        for (int i = 0 ; i < values.length ; i++)
            assertSame(values[i], iter.next(type.makeCellName(values[i])));

        iter = map.searchIterator();
        for (int i = 0 ; i < values.length ; i+=2)
            assertSame(values[i], iter.next(type.makeCellName(values[i])));

        iter = map.searchIterator();
        for (int i = 0 ; i < values.length ; i+=4)
            assertSame(values[i], iter.next(type.makeCellName(values[i])));

        iter = map.searchIterator();
        for (int i = 0 ; i < values.length ; i+=1)
        {
            if (i % 2 == 0)
            {
                Cell cell = iter.next(type.makeCellName(values[i] - 1));
                if (i > 0 && values[i - 1] == values[i] - 1)
                    assertSame(values[i - 1], cell);
                else
                    assertNull(cell);
            }
        }
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

    private void assertSame(int name, Cell cell)
    {
        int value = ByteBufferUtil.toInt(cell.name().toByteBuffer());
        assert name == value : "Expected " + name + " but got " + value;
    }
    private void assertSame(int[] names, Iterator<Cell> iter)
    {
        for (int name : names)
        {
            assert iter.hasNext() : "Expected " + name + " but no more result";
            int value = ByteBufferUtil.toInt(iter.next().name().toByteBuffer());
            assert name == value : "Expected " + name + " but got " + value;
        }
    }

    @Test
    public void testRemove()
    {
        testRemoveInternal(false);
        testRemoveInternal(true);
    }

    private void testRemoveInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);

        int[] values = new int[]{ 1, 2, 2, 3 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        Iterator<Cell> iter = map.getReverseSortedColumns().iterator();
        assertTrue(iter.hasNext());
        iter.next();
        iter.remove();
        assertTrue(iter.hasNext());
        iter.next();
        iter.remove();
        assertTrue(iter.hasNext());
        iter.next();
        iter.remove();
        assertTrue(!iter.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testBatchRemoveTwice()
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), false);
        map.addColumn(new BufferCell(type.makeCellName(1)));
        map.addColumn(new BufferCell(type.makeCellName(2)));

        BatchRemoveIterator<Cell> batchIter = map.batchRemoveIterator();
        batchIter.next();
        batchIter.remove();
        batchIter.remove();
    }

    @Test(expected = IllegalStateException.class)
    public void testBatchCommitTwice()
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), false);
        map.addColumn(new BufferCell(type.makeCellName(1)));
        map.addColumn(new BufferCell(type.makeCellName(2)));

        BatchRemoveIterator<Cell> batchIter = map.batchRemoveIterator();
        batchIter.next();
        batchIter.remove();
        batchIter.commit();
        batchIter.commit();
    }

    @Test
    public void testBatchRemove()
    {
        testBatchRemoveInternal(false);
        testBatchRemoveInternal(true);
    }

    public void testBatchRemoveInternal(boolean reversed)
    {
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), reversed);
        int[] values = new int[]{ 1, 2, 3, 5 };

        for (int i = 0; i < values.length; ++i)
            map.addColumn(new BufferCell(type.makeCellName(values[reversed ? values.length - 1 - i : i])));

        BatchRemoveIterator<Cell> batchIter = map.batchRemoveIterator();
        batchIter.next();
        batchIter.remove();
        batchIter.next();
        batchIter.remove();

        assertEquals("1st column before commit", 1, map.iterator().next().name().toByteBuffer().getInt(0));

        batchIter.commit();

        assertEquals("1st column after commit", 3, map.iterator().next().name().toByteBuffer().getInt(0));
    }

    @Test
    public void testBatchRemoveCopy()
    {
        // Test delete some random columns and check the result
        CellNameType type = new SimpleDenseCellNameType(Int32Type.instance);
        ColumnFamily map = ArrayBackedSortedColumns.factory.create(metadata(), false);
        int n = 127;
        int[] values = new int[n];
        for (int i = 0; i < n; i++)
            values[i] = i;
        Set<Integer> toRemove = Sets.newHashSet(3, 12, 13, 15, 58, 103, 112);

        for (int value : values)
            map.addColumn(new BufferCell(type.makeCellName(value)));

        BatchRemoveIterator<Cell> batchIter = map.batchRemoveIterator();
        while (batchIter.hasNext())
            if (toRemove.contains(batchIter.next().name().toByteBuffer().getInt(0)))
                batchIter.remove();

        batchIter.commit();

        int expected = 0;
        while (toRemove.contains(expected))
            expected++;

        for (Cell column : map)
        {
            assertEquals(expected, column.name().toByteBuffer().getInt(0));
            expected++;
            while (toRemove.contains(expected))
                expected++;
        }
        assertEquals(expected, n);
    }
}
