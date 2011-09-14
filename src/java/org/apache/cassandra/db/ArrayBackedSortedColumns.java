/**
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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;

/**
 * A ISortedColumns backed by an ArrayList.
 * This implementation is not synchronized and should only be used when
 * thread-safety is not required. This implementation makes sense when the
 * main operations performed are iterating over the map and adding columns
 * (especially if insertion is in sorted order).
 */
public class ArrayBackedSortedColumns extends ArrayList<IColumn> implements ISortedColumns
{
    private final AbstractType<?> comparator;
    private final boolean reversed;

    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new ArrayBackedSortedColumns(comparator, insertReversed);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sortedMap, boolean insertReversed)
        {
            return new ArrayBackedSortedColumns(sortedMap.values(), (AbstractType)sortedMap.comparator(), insertReversed);
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
    }

    private ArrayBackedSortedColumns(Collection<IColumn> columns, AbstractType<?> comparator, boolean reversed)
    {
        super(columns);
        this.comparator = comparator;
        this.reversed = reversed;
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
        return new ArrayBackedSortedColumns(this, comparator, reversed);
    }

    public boolean isInsertReversed()
    {
        return reversed;
    }

    private int compare(ByteBuffer name1, ByteBuffer name2)
    {
        if (reversed)
            return comparator.reverseComparator.compare(name1, name2);
        else
            return comparator.compare(name1, name2);
    }

    public IColumn getColumn(ByteBuffer name)
    {
        int pos = binarySearch(name);
        return pos >= 0 ? get(pos) : null;
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
        if (isEmpty())
        {
            add(column);
            return;
        }

        // Fast path if inserting at the tail
        int c = compare(get(size() - 1).name(), column.name());
        // note that we want an assertion here (see addColumn javadoc), but we also want that if
        // assertion are disabled, addColumn works correctly with unsorted input
        assert c <= 0 : "Added column does not sort as the " + (reversed ? "first" : "last") + " column";

        if (c < 0)
        {
            // Insert as last
            add(column);
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
                add(-pos-1, column);
        }
    }

    /**
     * Resolve against element at position i.
     * Assume that i is a valid position.
     */
    private void resolveAgainst(int i, IColumn column, Allocator allocator)
    {
        IColumn oldColumn = get(i);
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
            set(i, reconciledColumn);
        }
    }

    /**
     * Simple binary search for a given column name.
     * The return value has the exact same meaning that the one of Collections.binarySearch().
     * (We don't use Collections.binarySearch() directly because it would require us to create
     * a fake IColumn (as well an IColumn comparator) to do the search, which is ugly.
     */
    private int binarySearch(ByteBuffer name)
    {
        int low = 0;
        int mid = size();
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = -compare(get(mid).name(), name)) > 0)
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

    public void addAll(ISortedColumns cm, Allocator allocator)
    {
        if (cm.isEmpty())
            return;

        IColumn[] copy = toArray(new IColumn[size()]);
        int idx = 0;
        Iterator<IColumn> other = reversed ? cm.reverseIterator() : cm.iterator();
        IColumn otherColumn = other.next();

        clear();

        while (idx < copy.length && otherColumn != null)
        {
            int c = compare(copy[idx].name(), otherColumn.name());
            if (c < 0)
            {
                add(copy[idx]);
                idx++;
            }
            else if (c > 0)
            {
                add(otherColumn);
                otherColumn = other.hasNext() ? other.next() : null;
            }
            else // c == 0
            {
                add(copy[idx]);
                resolveAgainst(size() - 1, otherColumn, allocator);
                idx++;
                otherColumn = other.hasNext() ? other.next() : null;
            }
        }
        while (idx < copy.length)
        {
            add(copy[idx++]);
        }
        while (otherColumn != null)
        {
            add(otherColumn);
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
            set(pos, newColumn);
        }

        return pos >= 0;
    }

    public Collection<IColumn> getSortedColumns()
    {
        return reversed ? new ReverseSortedCollection() : this;
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
            remove(pos);
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        // we could memoize the created set but it's unlikely we'll call this method a lot on the same object anyway
        return new ColumnNamesSet();
    }

    public int getEstimatedColumnCount()
    {
        return size();
    }

    @Override
    public Iterator<IColumn> iterator()
    {
        return reversed ? reverseInternalIterator() : super.iterator();
    }

    public Iterator<IColumn> reverseIterator()
    {
        return reversed ? super.iterator() : reverseInternalIterator();
    }

    private Iterator<IColumn> reverseInternalIterator()
    {
        final ListIterator<IColumn> iter = listIterator(size());
        return new Iterator<IColumn>()
        {
            public boolean hasNext()
            {
                return iter.hasPrevious();
            }

            public IColumn next()
            {
                return iter.previous();
            }

            public void remove()
            {
                iter.remove();
            }
        };
    }

    private class ReverseSortedCollection extends AbstractCollection<IColumn>
    {
        public int size()
        {
            return ArrayBackedSortedColumns.this.size();
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
                    return ArrayBackedSortedColumns.this.get(idx--);
                }

                public void remove()
                {
                    ArrayBackedSortedColumns.this.remove(idx--);
                }
            };
        }
    }

    private class ForwardSortedCollection extends AbstractCollection<IColumn>
    {
        public int size()
        {
            return ArrayBackedSortedColumns.this.size();
        }

        public Iterator<IColumn> iterator()
        {
            return ArrayBackedSortedColumns.super.iterator();
        }
    }

    private class ColumnNamesSet extends AbstractSet<ByteBuffer> implements SortedSet<ByteBuffer>
    {
        public int size()
        {
            return ArrayBackedSortedColumns.this.size();
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
            return ArrayBackedSortedColumns.this.getComparator();
        }

        public ByteBuffer first()
        {
            final ArrayBackedSortedColumns outerList = ArrayBackedSortedColumns.this;
            if (outerList.isEmpty())
                throw new NoSuchElementException();
            return outerList.get(outerList.reversed ? size() - 1 : 0).name();
        }

        public ByteBuffer last()
        {
            final ArrayBackedSortedColumns outerList = ArrayBackedSortedColumns.this;
            if (outerList.isEmpty())
                throw new NoSuchElementException();
            return outerList.get(outerList.reversed ? 0 : size() - 1).name();
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
