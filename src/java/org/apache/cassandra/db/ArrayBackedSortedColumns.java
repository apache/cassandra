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

/**
 * A ColumnFamily backed by an array.
 * This implementation is not synchronized and should only be used when
 * thread-safety is not required. This implementation makes sense when the
 * main operations performed are iterating over the cells and adding cells
 * (especially if insertion is in sorted order).
 */
public class ArrayBackedSortedColumns extends ColumnFamily
{
    private static final Cell[] EMPTY_ARRAY = new Cell[0];
    private static final int MINIMAL_CAPACITY = 10;

    private final boolean reversed;

    private DeletionInfo deletionInfo;
    private Cell[] cells;
    private int size;
    private int sortedSize;
    private volatile boolean isSorted;

    public static final ColumnFamily.Factory<ArrayBackedSortedColumns> factory = new Factory<ArrayBackedSortedColumns>()
    {
        public ArrayBackedSortedColumns create(CFMetaData metadata, boolean insertReversed, int initialCapacity)
        {
            return new ArrayBackedSortedColumns(metadata, insertReversed, initialCapacity);
        }
    };

    private ArrayBackedSortedColumns(CFMetaData metadata, boolean reversed, int initialCapacity)
    {
        super(metadata);
        this.reversed = reversed;
        this.deletionInfo = DeletionInfo.live();
        this.cells = initialCapacity == 0 ? EMPTY_ARRAY : new Cell[initialCapacity];
        this.size = 0;
        this.sortedSize = 0;
        this.isSorted = true;
    }

    private ArrayBackedSortedColumns(ArrayBackedSortedColumns original)
    {
        super(original.metadata);
        this.reversed = original.reversed;
        this.deletionInfo = DeletionInfo.live(); // this is INTENTIONALLY not set to original.deletionInfo.
        this.cells = Arrays.copyOf(original.cells, original.size);
        this.size = original.size;
        this.sortedSize = original.sortedSize;
        this.isSorted = original.isSorted;
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new ArrayBackedSortedColumns(this);
    }

    public boolean isInsertReversed()
    {
        return reversed;
    }

    private Comparator<Composite> internalComparator()
    {
        return reversed ? getComparator().reverseComparator() : getComparator();
    }

    private void maybeSortCells()
    {
        if (!isSorted)
            sortCells();
    }

    /**
     * synchronized so that concurrent (read-only) accessors don't mess the internal state.
     */
    private synchronized void sortCells()
    {
        if (isSorted)
            return; // Just sorted by a previous call

        Comparator<Cell> comparator = reversed
                                    ? getComparator().columnReverseComparator()
                                    : getComparator().columnComparator();

        // Sort the unsorted segment - will still potentially contain duplicate (non-reconciled) cells
        Arrays.sort(cells, sortedSize, size, comparator);

        // Determine the merge start position for that segment
        int pos = binarySearch(0, sortedSize, cells[sortedSize].name, internalComparator());
        if (pos < 0)
            pos = -pos - 1;

        // Copy [pos, lastSortedCellIndex] cells into a separate array
        Cell[] leftCopy = pos == sortedSize
                        ? EMPTY_ARRAY
                        : Arrays.copyOfRange(cells, pos, sortedSize);

        // Store the beginning (inclusive) and the end (exclusive) indexes of the right segment
        int rightStart = sortedSize;
        int rightEnd = size;

        // 'Trim' the sizes to what's left without the leftCopy
        size = sortedSize = pos;

        // Merge the cells from both segments. When adding from the left segment we can rely on it not having any
        // duplicate cells, and thus omit the comparison with the previously entered cell - we'll never need to reconcile.
        int l = 0, r = rightStart;
        while (l < leftCopy.length && r < rightEnd)
        {
            int cmp = comparator.compare(leftCopy[l], cells[r]);
            if (cmp < 0)
                append(leftCopy[l++]);
            else if (cmp == 0)
                append(leftCopy[l++].reconcile(cells[r++]));
            else
                appendOrReconcile(cells[r++]);
        }
        while (l < leftCopy.length)
            append(leftCopy[l++]);
        while (r < rightEnd)
            appendOrReconcile(cells[r++]);

        // Nullify the remainder of the array (in case we had duplicate cells that got reconciled)
        for (int i = size; i < rightEnd; i++)
            cells[i] = null;

        // Fully sorted at this point
        isSorted = true;
    }

    private void appendOrReconcile(Cell cell)
    {
        if (size > 0 && cells[size - 1].name().equals(cell.name()))
            reconcileWith(size - 1, cell);
        else
            append(cell);
    }

    private void append(Cell cell)
    {
        cells[size] = cell;
        size++;
        sortedSize++;
    }

    public Cell getColumn(CellName name)
    {
        maybeSortCells();
        int pos = binarySearch(name);
        return pos >= 0 ? cells[pos] : null;
    }

    public void addColumn(Cell cell)
    {
        if (size == 0)
        {
            internalAdd(cell);
            sortedSize++;
            return;
        }

        if (size != sortedSize)
        {
            internalAdd(cell);
            return;
        }

        int c = internalComparator().compare(cells[size - 1].name(), cell.name());
        if (c < 0)
        {
            // Append to the end
            internalAdd(cell);
            sortedSize++;
        }
        else if (c == 0)
        {
            // Resolve against the last cell
            reconcileWith(size - 1, cell);
        }
        else
        {
            int pos = binarySearch(cell.name());
            if (pos >= 0) // Reconcile with an existing cell
            {
                reconcileWith(pos, cell);
            }
            else
            {
                internalAdd(cell); // Append to the end, making cells unsorted from now on
                isSorted = false;
            }
        }
    }

    public void addAll(ColumnFamily other)
    {
        delete(other.deletionInfo());

        if (other.getColumnCount() == 0)
            return;

        // In reality, with ABSC being the only remaining container (aside from ABTC), other will aways be ABSC.
        if (size == 0 && other instanceof ArrayBackedSortedColumns)
        {
            fastAddAll((ArrayBackedSortedColumns) other);
        }
        else
        {
            Iterator<Cell> iterator = reversed ? other.reverseIterator() : other.iterator();
            while (iterator.hasNext())
                addColumn(iterator.next());
        }
    }

    // Fast path, when this ABSC is empty.
    private void fastAddAll(ArrayBackedSortedColumns other)
    {
        if (other.isInsertReversed() == isInsertReversed())
        {
            cells = Arrays.copyOf(other.cells, other.cells.length);
            size = other.size;
            sortedSize = other.sortedSize;
            isSorted = other.isSorted;
        }
        else
        {
            if (cells.length < other.getColumnCount())
                cells = new Cell[Math.max(MINIMAL_CAPACITY, other.getColumnCount())];
            Iterator<Cell> iterator = reversed ? other.reverseIterator() : other.iterator();
            while (iterator.hasNext())
                cells[size++] = iterator.next();
            sortedSize = size;
            isSorted = true;
        }
    }

    /**
     * Add a cell to the array, 'resizing' it first if necessary (if it doesn't fit).
     */
    private void internalAdd(Cell cell)
    {
        if (cells.length == size)
            cells = Arrays.copyOf(cells, Math.max(MINIMAL_CAPACITY, size * 3 / 2 + 1));
        cells[size++] = cell;
    }

    /**
     * Remove the cell at a given index, shifting the rest of the array to the left if needed.
     * Please note that we mostly remove from the end, so the shifting should be rare.
     */
    private void internalRemove(int index)
    {
        int moving = size - index - 1;
        if (moving > 0)
            System.arraycopy(cells, index + 1, cells, index, moving);
        cells[--size] = null;
    }

    /**
     * Reconcile with a cell at position i.
     * Assume that i is a valid position.
     */
    private void reconcileWith(int i, Cell cell)
    {
        cells[i] = cell.reconcile(cells[i]);
    }

    private int binarySearch(CellName name)
    {
        return binarySearch(0, size, name, internalComparator());
    }

    /**
     * Simple binary search for a given cell name.
     * The return value has the exact same meaning that the one of Collections.binarySearch().
     * (We don't use Collections.binarySearch() directly because it would require us to create
     * a fake Cell (as well as an Cell comparator) to do the search, which is ugly.
     */
    private int binarySearch(int fromIndex, int toIndex, Composite name, Comparator<Composite> comparator)
    {
        int low = fromIndex;
        int mid = toIndex;
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = comparator.compare(name, cells[mid].name())) > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    public Collection<Cell> getSortedColumns()
    {
        maybeSortCells();
        return reversed ? new ReverseSortedCollection() : new ForwardSortedCollection();
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        maybeSortCells();
        return reversed ? new ForwardSortedCollection() : new ReverseSortedCollection();
    }

    public int getColumnCount()
    {
        maybeSortCells();
        return size;
    }

    public void clear()
    {
        setDeletionInfo(DeletionInfo.live());
        for (int i = 0; i < size; i++)
            cells[i] = null;
        size = sortedSize = 0;
        isSorted = true;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        deletionInfo.add(delTime);
    }

    public void delete(DeletionInfo newInfo)
    {
        deletionInfo.add(newInfo);
    }

    protected void delete(RangeTombstone tombstone)
    {
        deletionInfo.add(tombstone, getComparator());
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        deletionInfo = newInfo;
    }

    /**
     * Purges any tombstones with a local deletion time before gcBefore.
     * @param gcBefore a timestamp (in seconds) before which tombstones should be purged
     */
    public void purgeTombstones(int gcBefore)
    {
        deletionInfo.purge(gcBefore);
    }

    public Iterable<CellName> getColumnNames()
    {
        maybeSortCells();
        return Iterables.transform(new ForwardSortedCollection(), new Function<Cell, CellName>()
        {
            public CellName apply(Cell cell)
            {
                return cell.name;
            }
        });
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        maybeSortCells();
        return new SlicesIterator(Arrays.asList(cells).subList(0, size), getComparator(), slices, reversed);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        maybeSortCells();
        return new SlicesIterator(Arrays.asList(cells).subList(0, size), getComparator(), slices, !reversed);
    }

    private static class SlicesIterator extends AbstractIterator<Cell>
    {
        private final List<Cell> cells;
        private final ColumnSlice[] slices;
        private final Comparator<Composite> comparator;

        private int idx = 0;
        private int previousSliceEnd = 0;
        private Iterator<Cell> currentSlice;

        public SlicesIterator(List<Cell> cells, CellNameType comparator, ColumnSlice[] slices, boolean reversed)
        {
            this.cells = reversed ? Lists.reverse(cells) : cells;
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
                int startIdx = slice.start.isEmpty() ? 0 : binarySearch(previousSliceEnd, slice.start);
                if (startIdx < 0)
                    startIdx = -startIdx - 1;

                // The first idx to exclude
                int finishIdx = slice.finish.isEmpty() ? cells.size() - 1 : binarySearch(previousSliceEnd, slice.finish);
                if (finishIdx >= 0)
                    finishIdx++;
                else
                    finishIdx = -finishIdx - 1;

                if (startIdx == 0 && finishIdx == cells.size())
                    currentSlice = cells.iterator();
                else
                    currentSlice = cells.subList(startIdx, finishIdx).iterator();

                previousSliceEnd = finishIdx > 0 ? finishIdx - 1 : 0;
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }

        // Copy of ABSC.binarySearch() that takes lists
        private int binarySearch(int fromIndex, Composite name)
        {
            int low = fromIndex;
            int mid = cells.size();
            int high = mid - 1;
            int result = -1;
            while (low <= high)
            {
                mid = (low + high) >> 1;
                if ((result = comparator.compare(name, cells.get(mid).name())) > 0)
                    low = mid + 1;
                else if (result == 0)
                    return mid;
                else
                    high = mid - 1;
            }
            return -mid - (result < 0 ? 1 : 2);
        }
    }

    private class ReverseSortedCollection extends AbstractCollection<Cell>
    {
        public int size()
        {
            return size;
        }

        public Iterator<Cell> iterator()
        {
            return new Iterator<Cell>()
            {
                int idx = size - 1;
                boolean shouldCallNext = true;

                public boolean hasNext()
                {
                    return idx >= 0;
                }

                public Cell next()
                {
                    shouldCallNext = false;
                    return cells[idx--];
                }

                public void remove()
                {
                    if (shouldCallNext)
                        throw new IllegalStateException();
                    internalRemove(idx + 1);
                    shouldCallNext = true;
                    sortedSize--;
                }
            };
        }
    }

    private class ForwardSortedCollection extends AbstractCollection<Cell>
    {
        public int size()
        {
            return size;
        }

        public Iterator<Cell> iterator()
        {
            return new Iterator<Cell>()
            {
                int idx = 0;
                boolean shouldCallNext = true;

                public boolean hasNext()
                {
                    return idx < size;
                }

                public Cell next()
                {
                    shouldCallNext = false;
                    return cells[idx++];
                }

                public void remove()
                {
                    if (shouldCallNext)
                        throw new IllegalStateException();
                    internalRemove(--idx);
                    shouldCallNext = true;
                    sortedSize--;
                }
            };
        }
    }
}
