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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

/**
 * A ColumnFamily backed by an ArrayList.
 * This implementation is not synchronized and should only be used when
 * thread-safety is not required. This implementation makes sense when the
 * main operations performed are iterating over the map and adding columns
 * (especially if insertion is in sorted order).
 */
public class ArrayBackedSortedColumns extends AbstractThreadUnsafeSortedColumns
{
    private final boolean reversed;
    private final ArrayList<Column> columns;

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
        this.columns = new ArrayList<Column>();
    }

    private ArrayBackedSortedColumns(Collection<Column> columns, CFMetaData metadata, boolean reversed)
    {
        super(metadata);
        this.reversed = reversed;
        this.columns = new ArrayList<Column>(columns);
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new ArrayBackedSortedColumns(columns, metadata, reversed);
    }

    public boolean isInsertReversed()
    {
        return reversed;
    }

    private Comparator<ByteBuffer> internalComparator()
    {
        return reversed ? getComparator().reverseComparator : getComparator();
    }

    public Column getColumn(ByteBuffer name)
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
    public void addColumn(Column column, Allocator allocator)
    {
        if (columns.isEmpty())
        {
            columns.add(column);
            return;
        }

        // Fast path if inserting at the tail
        int c = internalComparator().compare(columns.get(getColumnCount() - 1).name(), column.name());
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
            resolveAgainst(getColumnCount() - 1, column, allocator);
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
    private void resolveAgainst(int i, Column column, Allocator allocator)
    {
        Column oldColumn = columns.get(i);

        // calculate reconciled col from old (existing) col and new col
        Column reconciledColumn = column.reconcile(oldColumn, allocator);
        columns.set(i, reconciledColumn);
    }

    private int binarySearch(ByteBuffer name)
    {
        return binarySearch(columns, internalComparator(), name, 0);
    }

    /**
     * Simple binary search for a given column name.
     * The return value has the exact same meaning that the one of Collections.binarySearch().
     * (We don't use Collections.binarySearch() directly because it would require us to create
     * a fake Column (as well as an Column comparator) to do the search, which is ugly.
     */
    private static int binarySearch(List<Column> columns, Comparator<ByteBuffer> comparator, ByteBuffer name, int start)
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

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        delete(cm.deletionInfo());
        if (cm.getColumnCount() == 0)
            return;

        Column[] copy = columns.toArray(new Column[getColumnCount()]);
        int idx = 0;
        Iterator<Column> other = reversed ? cm.reverseIterator(ColumnSlice.ALL_COLUMNS_ARRAY) : cm.iterator();
        Column otherColumn = other.next();

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
                resolveAgainst(getColumnCount() - 1, transformation.apply(otherColumn), allocator);
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

    public boolean replace(Column oldColumn, Column newColumn)
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

    public Collection<Column> getSortedColumns()
    {
        return reversed ? new ReverseSortedCollection() : columns;
    }

    public Collection<Column> getReverseSortedColumns()
    {
        // If reversed, the element are sorted reversely, so we could expect
        // to return *this*, but *this* redefine the iterator to be in sorted
        // order, so we need a collection that uses the super constructor
        return reversed ? new ForwardSortedCollection() : new ReverseSortedCollection();
    }

    public int getColumnCount()
    {
        return columns.size();
    }

    public void clear()
    {
        setDeletionInfo(DeletionInfo.live());
        columns.clear();
    }

    public Iterable<ByteBuffer> getColumnNames()
    {
        return Iterables.transform(columns, new Function<Column, ByteBuffer>()
        {
            public ByteBuffer apply(Column column)
            {
                return column.name;
            }
        });
    }

    public Iterator<Column> iterator()
    {
        return reversed ? Lists.reverse(columns).iterator() : columns.iterator();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(columns, getComparator(), slices, reversed);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new SlicesIterator(columns, getComparator(), slices, !reversed);
    }

    private static class SlicesIterator extends AbstractIterator<Column>
    {
        private final List<Column> list;
        private final ColumnSlice[] slices;
        private final Comparator<ByteBuffer> comparator;

        private int idx = 0;
        private int previousSliceEnd = 0;
        private Iterator<Column> currentSlice;

        public SlicesIterator(List<Column> list, AbstractType<?> comparator, ColumnSlice[] slices, boolean reversed)
        {
            this.list = reversed ? Lists.reverse(list) : list;
            this.slices = slices;
            this.comparator = reversed ? comparator.reverseComparator : comparator;
        }

        protected Column computeNext()
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

    private class ReverseSortedCollection extends AbstractCollection<Column>
    {
        public int size()
        {
            return columns.size();
        }

        public Iterator<Column> iterator()
        {
            return new Iterator<Column>()
            {
                int idx = size() - 1;

                public boolean hasNext()
                {
                    return idx >= 0;
                }

                public Column next()
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

    private class ForwardSortedCollection extends AbstractCollection<Column>
    {
        public int size()
        {
            return columns.size();
        }

        public Iterator<Column> iterator()
        {
            return columns.iterator();
        }
    }
}
