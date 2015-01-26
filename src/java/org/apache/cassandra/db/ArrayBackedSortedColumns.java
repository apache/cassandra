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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.BatchRemoveIterator;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.SearchIterator;

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
            return new ArrayBackedSortedColumns(metadata, insertReversed, initialCapacity == 0 ? EMPTY_ARRAY : new Cell[initialCapacity], 0, 0);
        }
    };

    private ArrayBackedSortedColumns(CFMetaData metadata, boolean reversed, Cell[] cells, int size, int sortedSize)
    {
        super(metadata);
        this.reversed = reversed;
        this.deletionInfo = DeletionInfo.live();
        this.cells = cells;
        this.size = size;
        this.sortedSize = sortedSize;
        this.isSorted = size == sortedSize;
    }

    protected ArrayBackedSortedColumns(CFMetaData metadata, boolean reversed)
    {
        this(metadata, reversed, EMPTY_ARRAY, 0, 0);
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

    public static ArrayBackedSortedColumns localCopy(ColumnFamily original, AbstractAllocator allocator)
    {
        ArrayBackedSortedColumns copy = new ArrayBackedSortedColumns(original.metadata, false, new Cell[original.getColumnCount()], 0, 0);
        for (Cell cell : original)
            copy.internalAdd(cell.localCopy(original.metadata, allocator));
        copy.sortedSize = copy.size; // internalAdd doesn't update sortedSize.
        copy.delete(original);
        return copy;
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

    public BatchRemoveIterator<Cell> batchRemoveIterator()
    {
        maybeSortCells();

        return new BatchRemoveIterator<Cell>()
        {
            private final Iterator<Cell> iter = iterator();
            private BitSet removedIndexes = new BitSet(size);
            private int idx = -1;
            private boolean shouldCallNext = false;
            private boolean isCommitted = false;
            private boolean removedAnything = false;

            public void commit()
            {
                if (isCommitted)
                    throw new IllegalStateException();
                isCommitted = true;

                if (!removedAnything)
                    return;

                int retainedCount = 0;
                int clearIdx, setIdx = -1;

                // shift all [clearIdx, setIdx) segments to the left, skipping any removed columns
                while (true)
                {
                    clearIdx = removedIndexes.nextClearBit(setIdx + 1);
                    if (clearIdx >= size)
                        break; // nothing left to retain

                    setIdx = removedIndexes.nextSetBit(clearIdx + 1);
                    if (setIdx < 0)
                        setIdx = size; // no removals past retainIdx - copy all remaining cells

                    if (retainedCount != clearIdx)
                        System.arraycopy(cells, clearIdx, cells, retainedCount, setIdx - clearIdx);

                    retainedCount += (setIdx - clearIdx);
                }

                for (int i = retainedCount; i < size; i++)
                    cells[i] = null;

                size = sortedSize = retainedCount;
            }

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Cell next()
            {
                idx++;
                shouldCallNext = false;
                return iter.next();
            }

            public void remove()
            {
                if (shouldCallNext)
                    throw new IllegalStateException();

                removedIndexes.set(reversed ? size - idx - 1 : idx);
                removedAnything = true;
                shouldCallNext = true;
            }
        };
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
                                    : getComparator().columnComparator(false);

        // Sort the unsorted segment - will still potentially contain duplicate (non-reconciled) cells
        Arrays.sort(cells, sortedSize, size, comparator);

        // Determine the merge start position for that segment
        int pos = binarySearch(0, sortedSize, cells[sortedSize].name(), internalComparator());
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

    /**
      * Adds a cell, assuming that:
      * - it's non-gc-able (if a tombstone) or not a tombstone
      * - it has a more recent timestamp than any partition/range tombstone shadowing it
      * - it sorts *strictly after* the current-last cell in the array.
      */
    public void maybeAppendColumn(Cell cell, DeletionInfo.InOrderTester tester, int gcBefore)
    {
        if (cell.getLocalDeletionTime() >= gcBefore && !tester.isDeleted(cell))
        {
            internalAdd(cell);
            sortedSize++;
        }
    }

    public void addColumn(Cell cell)
    {
        if (size == 0)
        {
            internalAdd(cell);
            sortedSize++;
            return;
        }

        if (!isSorted)
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

        if (!other.hasColumns())
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
        return new CellCollection(reversed);
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return new CellCollection(!reversed);
    }

    public int getColumnCount()
    {
        maybeSortCells();
        return size;
    }

    public boolean hasColumns()
    {
        return size > 0;
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
        return Iterables.transform(new CellCollection(false), new Function<Cell, CellName>()
        {
            public CellName apply(Cell cell)
            {
                return cell.name();
            }
        });
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        maybeSortCells();
        return slices.length == 1
             ? slice(slices[0], reversed, null)
             : new SlicesIterator(slices, reversed);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        maybeSortCells();
        return slices.length == 1
             ? slice(slices[0], !reversed, null)
             : new SlicesIterator(slices, !reversed);
    }

    public SearchIterator<CellName, Cell> searchIterator()
    {
        maybeSortCells();

        return new SearchIterator<CellName, Cell>()
        {
            // the first index that we could find the next key at, i.e. one larger
            // than the last key's location
            private int i = 0;

            // We assume a uniform distribution of keys,
            // so we keep track of how many keys were skipped to satisfy last lookup, and only look at twice that
            // many keys for next lookup initially, extending to whole range only if we couldn't find it in that subrange
            private int range = size / 2;

            public boolean hasNext()
            {
                return i < size;
            }

            public Cell next(CellName name)
            {
                if (!isSorted || !hasNext())
                    throw new IllegalStateException();

                // optimize for runs of sequential matches, as in CollationController
                // checking to see if we've found the desired cells yet (CASSANDRA-6933)
                int c = metadata.comparator.compare(name, cells[i].name());
                if (c <= 0)
                    return c < 0 ? null : cells[i++];

                // use range to manually force a better bsearch "pivot" by breaking it into two calls:
                // first for i..i+range, then i+range..size if necessary.
                // https://issues.apache.org/jira/browse/CASSANDRA-6933?focusedCommentId=13958264&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13958264
                int limit = Math.min(size, i + range);
                int i2 = binarySearch(i + 1, limit, name, internalComparator());
                if (-1 - i2 == limit)
                    i2 = binarySearch(limit, size, name, internalComparator());
                // i2 can't be zero since we already checked cells[i] above
                if (i2 > 0)
                {
                    range = i2 - i;
                    i = i2 + 1;
                    return cells[i2];
                }
                i2 = -1 - i2;
                range = i2 - i;
                i = i2;
                return null;
            }
        };
    }

    private class SlicesIterator extends AbstractIterator<Cell>
    {
        private final ColumnSlice[] slices;
        private final boolean invert;

        private int idx = 0;
        private int previousSliceEnd;
        private Iterator<Cell> currentSlice;

        public SlicesIterator(ColumnSlice[] slices, boolean invert)
        {
            this.slices = slices;
            this.invert = invert;
            previousSliceEnd = invert ? size : 0;
        }

        protected Cell computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();
                currentSlice = slice(slices[idx++], invert, this);
            }

            if (currentSlice.hasNext())
                return currentSlice.next();

            currentSlice = null;
            return computeNext();
        }
    }

    /**
     * @return a sub-range of our cells as an Iterator, between the provided composites (inclusive)
     *
     * @param slice  The slice with the inclusive start and finish bounds
     * @param invert If the sort order of our collection is opposite to the desired sort order of the result;
     *               this results in swapping the start/finish (since they are provided based on the desired
     *               sort order, not our sort order), to normalise to our sort order, and a backwards iterator is returned
     * @param iter   If this slice is part of a multi-slice, the iterator will be updated to ensure cells are visited only once
     */
    private Iterator<Cell> slice(ColumnSlice slice, boolean invert, SlicesIterator iter)
    {
        Composite start = invert ? slice.finish : slice.start;
        Composite finish = invert ? slice.start : slice.finish;

        int lowerBound = 0, upperBound = size;
        if (iter != null)
        {
            if (invert)
                upperBound = iter.previousSliceEnd;
            else
                lowerBound = iter.previousSliceEnd;
        }

        if (!start.isEmpty())
        {
            lowerBound = binarySearch(lowerBound, upperBound, start, internalComparator());
            if (lowerBound < 0)
                lowerBound = -lowerBound - 1;
        }

        if (!finish.isEmpty())
        {
            upperBound = binarySearch(lowerBound, upperBound, finish, internalComparator());
            upperBound = upperBound < 0
                       ? -upperBound - 1
                       : upperBound + 1; // upperBound is exclusive for the iterators
        }

        // If we're going backwards (wrt our sort order) we store the startIdx and use it as our upper bound next round
        if (iter != null)
            iter.previousSliceEnd = invert ? lowerBound : upperBound;

        return invert
             ? new BackwardsCellIterator(lowerBound, upperBound)
             : new ForwardsCellIterator(lowerBound, upperBound);
    }

    private final class BackwardsCellIterator implements Iterator<Cell>
    {
        private int idx, end;
        private boolean shouldCallNext = true;

        // lowerBound inclusive, upperBound exclusive
        private BackwardsCellIterator(int lowerBound, int upperBound)
        {
            idx = upperBound - 1;
            end = lowerBound - 1;
        }

        public boolean hasNext()
        {
            return idx > end;
        }

        public Cell next()
        {
            try
            {
                shouldCallNext = false;
                return cells[idx--];
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                NoSuchElementException ne = new NoSuchElementException(e.getMessage());
                ne.initCause(e);
                throw ne;
            }
        }

        public void remove()
        {
            if (shouldCallNext)
                throw new IllegalStateException();
            shouldCallNext = true;
            internalRemove(idx + 1);
            sortedSize--;
        }
    }

    private final class ForwardsCellIterator implements Iterator<Cell>
    {
        private int idx, end;
        private boolean shouldCallNext = true;

        // lowerBound inclusive, upperBound exclusive
        private ForwardsCellIterator(int lowerBound, int upperBound)
        {
            idx = lowerBound;
            end = upperBound;
        }

        public boolean hasNext()
        {
            return idx < end;
        }

        public Cell next()
        {
            try
            {
                shouldCallNext = false;
                return cells[idx++];
            }
            catch (ArrayIndexOutOfBoundsException e)
            {
                NoSuchElementException ne = new NoSuchElementException(e.getMessage());
                ne.initCause(e);
                throw ne;
            }
        }

        public void remove()
        {
            if (shouldCallNext)
                throw new IllegalStateException();
            shouldCallNext = true;
            internalRemove(--idx);
            sortedSize--;
            end--;
        }
    }

    private final class CellCollection extends AbstractCollection<Cell>
    {
        private final boolean invert;

        private CellCollection(boolean invert)
        {
            this.invert = invert;
        }

        public int size()
        {
            return getColumnCount();
        }

        public Iterator<Cell> iterator()
        {
            maybeSortCells();
            return invert
                 ? new BackwardsCellIterator(0, size)
                 : new ForwardsCellIterator(0, size);
        }
    }
}
