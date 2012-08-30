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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import edu.stanford.ppl.concurrent.SnapTreeMap;

import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Allocator;


/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 *
 * The implementation uses snaptree (https://github.com/nbronson/snaptree),
 * and in particular it's copy-on-write clone operation to achieve its
 * atomicity guarantee.
 *
 * WARNING: removing element through getSortedColumns().iterator() is *not*
 * isolated of other operations and could actually be fully ignored in the
 * face of a concurrent. Don't use it unless in a non-concurrent context.
 *
 * TODO: check the snaptree license make it ok to use
 */
public class AtomicSortedColumns implements ISortedColumns
{
    private final AtomicReference<Holder> ref;

    public static final ISortedColumns.Factory factory = new Factory()
    {
        public ISortedColumns create(AbstractType<?> comparator, boolean insertReversed)
        {
            return new AtomicSortedColumns(comparator);
        }

        public ISortedColumns fromSorted(SortedMap<ByteBuffer, IColumn> sortedMap, boolean insertReversed)
        {
            return new AtomicSortedColumns(sortedMap);
        }
    };

    public static ISortedColumns.Factory factory()
    {
        return factory;
    }

    private AtomicSortedColumns(AbstractType<?> comparator)
    {
        this(new Holder(comparator));
    }

    private AtomicSortedColumns(SortedMap<ByteBuffer, IColumn> columns)
    {
        this(new Holder(columns));
    }

    private AtomicSortedColumns(Holder holder)
    {
        this.ref = new AtomicReference<Holder>(holder);
    }

    public AbstractType<?> getComparator()
    {
        return (AbstractType<?>)ref.get().map.comparator();
    }

    public ISortedColumns.Factory getFactory()
    {
        return factory;
    }

    public ISortedColumns cloneMe()
    {
        return new AtomicSortedColumns(ref.get().cloneMe());
    }

    public DeletionInfo getDeletionInfo()
    {
        return ref.get().deletionInfo;
    }

    public void delete(DeletionInfo info)
    {
        // Keeping deletion info for max markedForDeleteAt value
        while (true)
        {
            Holder current = ref.get();
            DeletionInfo newDelInfo = current.deletionInfo.add(info);
            if (newDelInfo == current.deletionInfo || ref.compareAndSet(current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref.set(ref.get().with(newInfo));
    }

    public void maybeResetDeletionTimes(int gcBefore)
    {
        while (true)
        {
            Holder current = ref.get();
            DeletionInfo purgedInfo = current.deletionInfo.purge(gcBefore);
            if (purgedInfo == current.deletionInfo || ref.compareAndSet(current, current.with(DeletionInfo.LIVE)))
                break;
        }
    }

    public void retainAll(ISortedColumns columns)
    {
        Holder current, modified;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            modified.retainAll(columns);
        }
        while (!ref.compareAndSet(current, modified));
    }

    public void addColumn(IColumn column, Allocator allocator)
    {
        Holder current, modified;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            modified.addColumn(column, allocator, SecondaryIndexManager.nullUpdater);
        }
        while (!ref.compareAndSet(current, modified));
    }

    public void addAll(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    public long addAllWithSizeDelta(ISortedColumns cm, Allocator allocator, Function<IColumn, IColumn> transformation, SecondaryIndexManager.Updater indexer)
    {
        /*
         * This operation needs to atomicity and isolation. To that end, we
         * add the new column to a copy of the map (a cheap O(1) snapTree
         * clone) and atomically compare and swap when everything has been
         * added. Of course, we must not forget to update the deletion times
         * too.
         * In case we are adding a lot of columns, failing the final compare
         * and swap could be expensive. To mitigate, we check we haven't been
         * beaten by another thread after every column addition. If we have,
         * we bail early, avoiding unnecessary work if possible.
         */
        Holder current, modified;
        long sizeDelta;

        main_loop:
        do
        {
            sizeDelta = 0;
            current = ref.get();
            DeletionInfo newDelInfo = current.deletionInfo.add(cm.getDeletionInfo());
            modified = new Holder(current.map.clone(), newDelInfo);

            for (IColumn column : cm.getSortedColumns())
            {
                sizeDelta += modified.addColumn(transformation.apply(column), allocator, indexer);
                // bail early if we know we've been beaten
                if (ref.get() != current)
                    continue main_loop;
            }
        }
        while (!ref.compareAndSet(current, modified));

        return sizeDelta;
    }

    public boolean replace(IColumn oldColumn, IColumn newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        Holder current, modified;
        boolean replaced;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            replaced = modified.map.replace(oldColumn.name(), oldColumn, newColumn);
        }
        while (!ref.compareAndSet(current, modified));
        return replaced;
    }

    public void removeColumn(ByteBuffer name)
    {
        Holder current, modified;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            modified.map.remove(name);
        }
        while (!ref.compareAndSet(current, modified));
    }

    public void clear()
    {
        Holder current, modified;
        do
        {
            current = ref.get();
            modified = current.clear();
        }
        while (!ref.compareAndSet(current, modified));
    }

    public IColumn getColumn(ByteBuffer name)
    {
        return ref.get().map.get(name);
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return ref.get().map.keySet();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return ref.get().map.values();
    }

    public Collection<IColumn> getReverseSortedColumns()
    {
        return ref.get().map.descendingMap().values();
    }

    public int size()
    {
        return ref.get().map.size();
    }

    public int getEstimatedColumnCount()
    {
        return size();
    }

    public boolean isEmpty()
    {
        return ref.get().map.isEmpty();
    }

    public Iterator<IColumn> iterator()
    {
        return getSortedColumns().iterator();
    }

    public Iterator<IColumn> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.get().map, slices);
    }

    public Iterator<IColumn> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.get().map.descendingMap(), slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {
        final SnapTreeMap<ByteBuffer, IColumn> map;
        final DeletionInfo deletionInfo;

        Holder(AbstractType<?> comparator)
        {
            this(new SnapTreeMap<ByteBuffer, IColumn>(comparator), DeletionInfo.LIVE);
        }

        Holder(SortedMap<ByteBuffer, IColumn> columns)
        {
            this(new SnapTreeMap<ByteBuffer, IColumn>(columns), DeletionInfo.LIVE);
        }

        Holder(SnapTreeMap<ByteBuffer, IColumn> map, DeletionInfo deletionInfo)
        {
            this.map = map;
            this.deletionInfo = deletionInfo;
        }

        Holder cloneMe()
        {
            return with(map.clone());
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(map, info);
        }

        Holder with(SnapTreeMap<ByteBuffer, IColumn> newMap)
        {
            return new Holder(newMap, deletionInfo);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return new Holder(new SnapTreeMap<ByteBuffer, IColumn>(map.comparator()), deletionInfo);
        }

        long addColumn(IColumn column, Allocator allocator, SecondaryIndexManager.Updater indexer)
        {
            ByteBuffer name = column.name();
            while (true)
            {
                IColumn oldColumn = map.putIfAbsent(name, column);
                if (oldColumn == null)
                {
                    indexer.insert(column);
                    return column.dataSize();
                }

                if (oldColumn instanceof SuperColumn)
                {
                    assert column instanceof SuperColumn;
                    long previousSize = oldColumn.dataSize();
                    ((SuperColumn) oldColumn).putColumn((SuperColumn)column, allocator);
                    return oldColumn.dataSize() - previousSize;
                }
                else
                {
                    IColumn reconciledColumn = column.reconcile(oldColumn, allocator);
                    if (map.replace(name, oldColumn, reconciledColumn))
                    {
                        // for memtable updates we only care about oldcolumn, reconciledcolumn, but when compacting
                        // we need to make sure we update indexes no matter the order we merge
                        if (reconciledColumn == column)
                            indexer.update(oldColumn, reconciledColumn);
                        else
                            indexer.update(column, reconciledColumn);
                        return reconciledColumn.dataSize() - oldColumn.dataSize();
                    }
                    // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
                    // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
                }
            }
        }

        void retainAll(ISortedColumns columns)
        {
            Iterator<IColumn> iter = map.values().iterator();
            Iterator<IColumn> toRetain = columns.iterator();
            IColumn current = iter.hasNext() ? iter.next() : null;
            IColumn retain = toRetain.hasNext() ? toRetain.next() : null;
            Comparator<? super ByteBuffer> comparator = map.comparator();
            while (current != null && retain != null)
            {
                int c = comparator.compare(current.name(), retain.name());
                if (c == 0)
                {
                    if (current instanceof SuperColumn)
                    {
                        assert retain instanceof SuperColumn;
                        ((SuperColumn)current).retainAll((SuperColumn)retain);
                    }
                    current = iter.hasNext() ? iter.next() : null;
                    retain = toRetain.hasNext() ? toRetain.next() : null;
                }
                else if (c < 0)
                {
                    iter.remove();
                    current = iter.hasNext() ? iter.next() : null;
                }
                else // c > 0
                {
                    retain = toRetain.hasNext() ? toRetain.next() : null;
                }
            }
            while (current != null)
            {
                iter.remove();
                current = iter.hasNext() ? iter.next() : null;
            }
        }
    }
}
