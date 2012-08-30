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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

/**
 * A ISortedColumns backed by an ArrayList.
 * This implementation is not synchronized and should only be used when
 * thread-safety is not required. This implementation makes sense when the
 * main operations performed are iterating over the map and adding columns
 * (especially if insertion is in sorted order).
 */
public class ArrayBackedSortedColumns extends AbstractThreadUnsafeSortedColumns implements ISortedColumns
{
    private final AbstractType<?> comparator;
    private final boolean reversed;
    private final ArrayList<IColumn> columns;

    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new ArrayBackedSortedColumns(comparator, insertReversed);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sortedMap, boolean insertReversed)
        {
            return new ArrayBackedSortedColumns(sortedMap.values(), (AbstractType<?>)sortedMap.comparator(), insertReversed);
        }
    };

    public static ISortedColumns.Factory factory()
    {
        return factory;
    }

    private ArrayBackedSortedColumns(AbstractType<?> comparator, boolean reversed)
    {
        super();
        this.comparator = comparator;
        this.reversed = reversed;
        this.columns = new ArrayList<IColumn>();
    }

    private ArrayBackedSortedColumns(Collection<IColumn> columns, AbstractType<?> comparator, boolean reversed)
    {
        this.comparator = comparator;
        this.reversed = reversed;
        this.columns = new ArrayList<IColumn>(columns);
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory();
    }

    public AbstractType<?> getComparator()
    {
        return comparator;
    }

    public ISortedColumns cloneMe()
    {
        return new ArrayBackedSortedColumns(columns, comparator, reversed);
    }

    public boolean isInsertReversed()
    {
        return reversed;
    }

    private Comparator<ByteBuffer> internalComparator()
    {
        return reversed ? comparator.reverseComparator : comparator;
    }

    public IColumn getColumn(ByteBuffer name)
    {
        int pos = binarySearch(name);
        return pos >= 0 ? columns.get(pos) : null;
    }

    /**
     * AddColumn throws an exception if the column added does not sort after
     * the last column in the map.
     * The reasoning is that this implementation can get slower if too much
     * insertions are done in unsorted order and right now we only use it when
     * *all* insertion (with this method) are done in sorted order. The
     * assertion throwing is thus a protection against performance regression
     * without knowing about (we can revisit that decision later if we have
     * use cases where most insert are in sorted order but a few are not).
     */
    public void addColumn(IColumn column, Allocator allocator)
    {
        if (columns.isEmpty())
        {
            columns.add(column);
            return;
        }

        // Fast path if inserting at the tail
        int c = internalComparator().compare(columns.get(size() - 1).name(), column.name());
        // note that we want an assertion here (see addColumn javadoc), but we also want that if
        // assertion are disabled, addColumn works correctly with unsorted input
        assert c <= 0 : "Added column does not sort as the " + (reversed ? "first" : "last") + " column";

        if (c < 0)
        {
            // Insert as last
            columns.add(column);
        }
        else if (c == 0)
        {
            // Resolve against last
            resolveAgainst(size() - 1, column, allocator);
        }
        else
        {
            int pos = binarySearch(column.name());
            if (pos >= 0)
                resolveAgainst(pos, column, allocator);
            else
                columns.add(-pos-1, column);
        }
    }

    /**
     * Resolve against element at position i.
     * Assume that i is a valid position.
     */
    private void resolveAgainst(int i, IColumn column, Allocator allocator)
    {
        IColumn oldColumn = columns.get(i);
        if (oldColumn instanceof SuperColumn)
        {
            // Delegated to SuperColumn
            assert column instanceof SuperColumn;
            ((SuperColumn) oldColumn).putColumn((SuperColumn)column, allocator);
        }
        else
        {
            // calculate reconciled col from old (existing) col and new col
            IColumn reconciledColumn = column.reconcile(oldColumn, allocator);
            columns.set(i, reconciledColumn);
        }
    }

    private int binarySearch(ByteBuffer name)
    {
        return binarySearch(columns, internalComparator(), name, 0);
    }

    /**
     * Simple binary search for a given column name.
     * The return value has the exact same meaning that the one of Collections.binarySearch().
     * (We don't use Collections.binarySearch() directly because it would require us to create
     * a fake IColumn (as well as an IColumn comparator) to do the search, which is ugly.
     */
    private static int binarySearch(List<IColumn> columns, Comparator<ByteBuffer> comparator, ByteBuffer name, int start)
    {
        int low = start;
        int mid = columns.size();
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = comparator.compare(name, columns.get(mid).name())) > 0)
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

    public long addAllWithSizeDelta(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation, SecondaryIndexManager.Updater indexer)
    {
        throw new UnsupportedOperationException();
    }

    public void addAll(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation)
    {
        delete(cm.getDeletionInfo());
        if (cm.isEmpty())
            return;

        IColumn[] copy = columns.toArray(new IColumn[size()]);
        int idx = 0;
        Iterator<IColumn> other = reversed ? cm.reverseIterator(ColumnSlice.ALL_COLUMNS_ARRAY) : cm.iterator();
        IColumn otherColumn = other.next();

        columns.clear();

        while (idx < copy.length && otherColumn != null)
        {
            int c = internalComparator().compare(copy[idx].name(), otherColumn.name());
            if (c < 0)
            {
                columns.add(copy[idx]);
                idx++;
            }
            else if (c > 0)
            {
                columns.add(transformation.apply(otherColumn));
                otherColumn = other.hasNext() ? other.next() : null;
            }
            else // c == 0
            {
                columns.add(copy[idx]);
                resolveAgainst(size() - 1, transformation.apply(otherColumn), allocator);
                idx++;
                otherColumn = other.hasNext() ? other.next() : null;
            }
        }
        while (idx < copy.length)
        {
            columns.add(copy[idx++]);
        }
        while (otherColumn != null)
        {
            columns.add(transformation.apply(otherColumn));
            otherColumn = other.hasNext() ? other.next() : null;
        }
    }

    public boolean replace(IColumn oldColumn, IColumn newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        int pos = binarySearch(oldColumn.name());
        if (pos >= 0)
        {
            columns.set(pos, newColumn);
        }

        return pos >= 0;
    }

    public Collection<IColumn> getSortedColumns()
    {
        return reversed ? new ReverseSortedCollection() : columns;
    }

    public Collection<IColumn> getReverseSortedColumns()
    {
        // If reversed, the element are sorted reversely, so we could expect
        // to return *this*, but *this* redefine the iterator to be in sorted
        // order, so we need a collection that uses the super constructor
        return reversed ? new ForwardSortedCollection() : new ReverseSortedCollection();
    }

    public void removeColumn(ByteBuffer name)
    {
        int pos = binarySearch(name);
        if (pos >= 0)
            columns.remove(pos);
    }

    public int size()
    {
        return columns.size();
    }

    public void clear()
    {
        columns.clear();
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        // we could memoize the created set but it's unlikely we'll call this method a lot on the same object anyway
        return new ColumnNamesSet();
    }

    public Iterator<IColumn> iterator()
    {
        return reversed ? Lists.reverse(columns).iterator() : columns.iterator();
    }

    public Iterator<IColumn> iterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(columns, comparator, slices, reversed);
    }

    public Iterator<IColumn> reverseIterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(columns, comparator, slices, !reversed);
    }

    private static class SlicesIterator extends AbstractIterator<IColumn>
    {
        private final List<IColumn> list;
        private final ColumnSlice[] slices;
        private final Comparator<ByteBuffer> comparator;

        private int idx = 0;
        private int previousSliceEnd = 0;
        private Iterator<IColumn> currentSlice;

        public SlicesIterator(List<IColumn> list, AbstractType<?> comparator, ColumnSlice[] slices, boolean reversed)
        {
            this.list = reversed ? Lists.reverse(list) : list;
            this.slices = slices;
            this.comparator = reversed ? comparator.reverseComparator : comparator;
        }

        protected IColumn computeNext()
        {
            if (currentSlice == null)
            {
                if (idx >= slices.length)
                    return endOfData();

                ColumnSlice slice = slices[idx++];
                // The first idx to include
                int startIdx = slice.start.remaining() == 0 ? 0 : binarySearch(list, comparator, slice.start, previousSliceEnd);
                if (startIdx < 0)
                    startIdx = -startIdx - 1;

                // The first idx to exclude
                int finishIdx = slice.finish.remaining() == 0 ? list.size() - 1 : binarySearch(list, comparator, slice.finish, previousSliceEnd);
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

    private class ReverseSortedCollection extends AbstractCollection<IColumn>
    {
        public int size()
        {
            return columns.size();
        }

        public Iterator<IColumn> iterator()
        {
            return new Iterator<IColumn>()
            {
                int idx = size() - 1;

                public boolean hasNext()
                {
                    return idx >= 0;
                }

                public IColumn next()
                {
                    return columns.get(idx--);
                }

                public void remove()
                {
                    columns.remove(idx--);
                }
            };
        }
    }

    private class ForwardSortedCollection extends AbstractCollection<IColumn>
    {
        public int size()
        {
            return columns.size();
        }

        public Iterator<IColumn> iterator()
        {
            return columns.iterator();
        }
    }

    private class ColumnNamesSet extends AbstractSet<ByteBuffer> implements SortedSet<ByteBuffer>
    {
        public int size()
        {
            return columns.size();
        }

        public Iterator<ByteBuffer> iterator()
        {
            final Iterator<IColumn> outerIterator = ArrayBackedSortedColumns.this.iterator(); // handles reversed
            return new Iterator<ByteBuffer>()
            {
                public boolean hasNext()
                {
                    return outerIterator.hasNext();
                }

                public ByteBuffer next()
                {
                    return outerIterator.next().name();
                }

                public void remove()
                {
                    outerIterator.remove();
                }
            };
        }

        public Comparator<ByteBuffer> comparator()
        {
            return getComparator();
        }

        public ByteBuffer first()
        {
            final ArrayBackedSortedColumns outerList = ArrayBackedSortedColumns.this;
            if (outerList.isEmpty())
                throw new NoSuchElementException();
            return outerList.columns.get(outerList.reversed ? size() - 1 : 0).name();
        }

        public ByteBuffer last()
        {
            final ArrayBackedSortedColumns outerList = ArrayBackedSortedColumns.this;
            if (outerList.isEmpty())
                throw new NoSuchElementException();
            return outerList.columns.get(outerList.reversed ? 0 : size() - 1).name();
        }

        /*
         * It is fairly hard to implement headSet, tailSet and subSet so that they respect their specification.
         * Namely, the part "The returned set is backed by this set, so changes in the returned set are reflected
         * in this set, and vice-versa". Simply keeping a lower and upper index in the backing arrayList wouldn't
         * ensure those property. Since we do not use those function so far, we prefer returning UnsupportedOperationException
         * for now and revisit this when and if the need arise.
         */
        public SortedSet<ByteBuffer> headSet(ByteBuffer fromElement)
        {
            throw new UnsupportedOperationException();
        }

        // see headSet
        public SortedSet<ByteBuffer> tailSet(ByteBuffer toElement)
        {
            throw new UnsupportedOperationException();
        }

        // see headSet
        public SortedSet<ByteBuffer> subSet(ByteBuffer fromElement, ByteBuffer toElement)
        {
            throw new UnsupportedOperationException();
        }
    }
}
