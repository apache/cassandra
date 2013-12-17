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
package org.apache.cassandra.db;

import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.Allocator;

/**
 * A ColumnFamily backed by an ArrayList.
 * This implementation is not synchronized and should only be used when
 * thread-safety is not required. This implementation makes sense when the
 * main operations performed are iterating over the map and adding cells
 * (especially if insertion is in sorted order).
 */
public class ArrayBackedSortedColumns extends AbstractThreadUnsafeSortedColumns
{
    private final boolean reversed;
    private final ArrayList<Cell> cells;

    public static final ColumnFamily.Factory<ArrayBackedSortedColumns> factory = new Factory<ArrayBackedSortedColumns>()
    {
        public ArrayBackedSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            return new ArrayBackedSortedColumns(metadata, insertReversed);
        }
    };

    private ArrayBackedSortedColumns(CFMetaData metadata, boolean reversed)
    {
        super(metadata);
        this.reversed = reversed;
        this.cells = new ArrayList<Cell>();
    }

    private ArrayBackedSortedColumns(Collection<Cell> cells, CFMetaData metadata, boolean reversed)
    {
        super(metadata);
        this.reversed = reversed;
        this.cells = new ArrayList<Cell>(cells);
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new ArrayBackedSortedColumns(cells, metadata, reversed);
    }

    public boolean isInsertReversed()
    {
        return reversed;
    }

    private Comparator<Composite> internalComparator()
    {
        return reversed ? getComparator().reverseComparator() : getComparator();
    }

    public Cell getColumn(CellName name)
    {
        int pos = binarySearch(name);
        return pos >= 0 ? cells.get(pos) : null;
    }

    /**
     * AddColumn throws an exception if the cell added does not sort after
     * the last cell in the map.
     * The reasoning is that this implementation can get slower if too much
     * insertions are done in unsorted order and right now we only use it when
     * *all* insertion (with this method) are done in sorted order. The
     * assertion throwing is thus a protection against performance regression
     * without knowing about (we can revisit that decision later if we have
     * use cases where most insert are in sorted order but a few are not).
     */
    public void addColumn(Cell cell, Allocator allocator)
    {
        if (cells.isEmpty())
        {
            cells.add(cell);
            return;
        }

        // Fast path if inserting at the tail
        int c = internalComparator().compare(cells.get(getColumnCount() - 1).name(), cell.name());
        // note that we want an assertion here (see addColumn javadoc), but we also want that if
        // assertion are disabled, addColumn works correctly with unsorted input
        assert c <= 0 : "Added cell does not sort as the " + (reversed ? "first" : "last") + " cell";

        if (c < 0)
        {
            // Insert as last
            cells.add(cell);
        }
        else if (c == 0)
        {
            // Resolve against last
            resolveAgainst(getColumnCount() - 1, cell, allocator);
        }
        else
        {
            int pos = binarySearch(cell.name());
            if (pos >= 0)
                resolveAgainst(pos, cell, allocator);
            else
                cells.add(-pos-1, cell);
        }
    }

    /**
     * Resolve against element at position i.
     * Assume that i is a valid position.
     */
    private void resolveAgainst(int i, Cell cell, Allocator allocator)
    {
        Cell oldCell = cells.get(i);

        // calculate reconciled col from old (existing) col and new col
        Cell reconciledCell = cell.reconcile(oldCell, allocator);
        cells.set(i, reconciledCell);
    }

    private int binarySearch(CellName name)
    {
        return binarySearch(cells, internalComparator(), name, 0);
    }

    /**
     * Simple binary search for a given column name.
     * The return value has the exact same meaning that the one of Collections.binarySearch().
     * (We don't use Collections.binarySearch() directly because it would require us to create
     * a fake Cell (as well as an Cell comparator) to do the search, which is ugly.
     */
    private static int binarySearch(List<Cell> cells, Comparator<Composite> comparator, Composite name, int start)
    {
        int low = start;
        int mid = cells.size();
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = comparator.compare(name, cells.get(mid).name())) > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation)
    {
        delete(cm.deletionInfo());
        if (cm.getColumnCount() == 0)
            return;

        Cell[] copy = cells.toArray(new Cell[getColumnCount()]);
        int idx = 0;
        Iterator<Cell> other = reversed ? cm.reverseIterator(ColumnSlice.ALL_COLUMNS_ARRAY) : cm.iterator();
        Cell otherCell = other.next();

        cells.clear();

        while (idx < copy.length && otherCell != null)
        {
            int c = internalComparator().compare(copy[idx].name(), otherCell.name());
            if (c < 0)
            {
                cells.add(copy[idx]);
                idx++;
            }
            else if (c > 0)
            {
                cells.add(transformation.apply(otherCell));
                otherCell = other.hasNext() ? other.next() : null;
            }
            else // c == 0
            {
                cells.add(copy[idx]);
                resolveAgainst(getColumnCount() - 1, transformation.apply(otherCell), allocator);
                idx++;
                otherCell = other.hasNext() ? other.next() : null;
            }
        }
        while (idx < copy.length)
        {
            cells.add(copy[idx++]);
        }
        while (otherCell != null)
        {
            cells.add(transformation.apply(otherCell));
            otherCell = other.hasNext() ? other.next() : null;
        }
    }

    public boolean replace(Cell oldCell, Cell newCell)
    {
        if (!oldCell.name().equals(newCell.name()))
            throw new IllegalArgumentException();

        int pos = binarySearch(oldCell.name());
        if (pos >= 0)
        {
            cells.set(pos, newCell);
        }

        return pos >= 0;
    }

    public Collection<Cell> getSortedColumns()
    {
        return reversed ? new ReverseSortedCollection() : cells;
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        // If reversed, the element are sorted reversely, so we could expect
        // to return *this*, but *this* redefine the iterator to be in sorted
        // order, so we need a collection that uses the super constructor
        return reversed ? new ForwardSortedCollection() : new ReverseSortedCollection();
    }

    public int getColumnCount()
    {
        return cells.size();
    }

    public void clear()
    {
        setDeletionInfo(DeletionInfo.live());
        cells.clear();
    }

    public Iterable<CellName> getColumnNames()
    {
        return Iterables.transform(cells, new Function<Cell, CellName>()
        {
            public CellName apply(Cell cell)
            {
                return cell.name;
            }
        });
    }

    public Iterator<Cell> iterator()
    {
        return reversed ? Lists.reverse(cells).iterator() : cells.iterator();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(cells, getComparator(), slices, reversed);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(cells, getComparator(), slices, !reversed);
    }

    private static class SlicesIterator extends AbstractIterator<Cell>
    {
        private final List<Cell> list;
        private final ColumnSlice[] slices;
        private final Comparator<Composite> comparator;

        private int idx = 0;
        private int previousSliceEnd = 0;
        private Iterator<Cell> currentSlice;

        public SlicesIterator(List<Cell> list, CellNameType comparator, ColumnSlice[] slices, boolean reversed)
        {
            this.list = reversed ? Lists.reverse(list) : list;
            this.slices = slices;
            this.comparator = reversed ? comparator.reverseComparator() : comparator;
        }

        protected Cell computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // The first idx to include
                int startIdx = slice.start.isEmpty() ? 0 : binarySearch(list, comparator, slice.start, previousSliceEnd);
                if (startIdx < 0)
                    startIdx = -startIdx - 1;

                // The first idx to exclude
                int finishIdx = slice.finish.isEmpty() ? list.size() - 1 : binarySearch(list, comparator, slice.finish, previousSliceEnd);
                if (finishIdx >= 0)
                    finishIdx++;
                else
                    finishIdx = -finishIdx - 1;

                if (startIdx == 0 && finishIdx == list.size())
                    currentSlice = list.iterator();
                else
                    currentSlice = list.subList(startIdx, finishIdx).iterator();

                previousSliceEnd = finishIdx > 0 ? finishIdx - 1 : 0;
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }
    }

    private class ReverseSortedCollection extends AbstractCollection<Cell>
    {
        public int size()
        {
            return cells.size();
        }

        public Iterator<Cell> iterator()
        {
            return new Iterator<Cell>()
            {
                int idx = size() - 1;

                public boolean hasNext()
                {
                    return idx >= 0;
                }

                public Cell next()
                {
                    return cells.get(idx--);
                }

                public void remove()
                {
                    cells.remove(idx--);
                }
            };
        }
    }

    private class ForwardSortedCollection extends AbstractCollection<Cell>
    {
        public int size()
        {
            return cells.size();
        }

        public Iterator<Cell> iterator()
        {
            return cells.iterator();
        }
    }
}
