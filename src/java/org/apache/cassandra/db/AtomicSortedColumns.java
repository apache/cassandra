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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.*;

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
 */
public class AtomicSortedColumns extends ColumnFamily
{
    private final AtomicReference<Holder> ref;

    private static final boolean enableColUpdateTimeDelta = Boolean.parseBoolean(System.getProperty("cassandra.enableColUpdateTimeDelta", "false"));

    public static final ColumnFamily.Factory<AtomicSortedColumns> factory = new Factory<AtomicSortedColumns>()
    {
        public AtomicSortedColumns create(CFMetaData metadata, boolean insertReversed)
        {
            return new AtomicSortedColumns(metadata);
        }
    };

    private AtomicSortedColumns(CFMetaData metadata)
    {
        this(metadata, new Holder(metadata.comparator));
    }

    private AtomicSortedColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = new AtomicReference<>(holder);
    }

    public AbstractType<?> getComparator()
    {
        return (AbstractType<?>)ref.get().map.comparator();
    }

    public ColumnFamily.Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicSortedColumns(metadata, ref.get().cloneMe());
    }

    public DeletionInfo deletionInfo()
    {
        return ref.get().deletionInfo;
    }

    public void delete(DeletionTime delTime)
    {
        delete(new DeletionInfo(delTime));
    }

    protected void delete(RangeTombstone tombstone)
    {
        delete(new DeletionInfo(tombstone, getComparator()));
    }

    public void delete(DeletionInfo info)
    {
        if (info.isLive())
            return;

        // Keeping deletion info for max markedForDeleteAt value
        while (true)
        {
            Holder current = ref.get();
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(info);
            if (ref.compareAndSet(current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref.set(ref.get().with(newInfo));
    }

    public void purgeTombstones(int gcBefore)
    {
        while (true)
        {
            Holder current = ref.get();
            if (!current.deletionInfo.hasPurgeableTombstones(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (ref.compareAndSet(current, current.with(purgedInfo)))
                break;
        }
    }

    public void addColumn(Column column, Allocator allocator)
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

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    /**
     *  This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     *  @return the difference in size seen after merging the given columns and minimum time delta between timestamps of two reconciled columns.
     */
    public Pair<Long,Long> addAllWithSizeDelta(ColumnFamily cm, Allocator allocator, Function<Column, Column> transformation, SecondaryIndexManager.Updater indexer)
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
        long timeDelta;

        main_loop:
        do
        {
            sizeDelta = 0;
            timeDelta = Long.MAX_VALUE;
            current = ref.get();
            DeletionInfo newDelInfo = current.deletionInfo;
            if (cm.deletionInfo().mayModify(newDelInfo))
            {
                newDelInfo = current.deletionInfo.copy().add(cm.deletionInfo());
                sizeDelta += newDelInfo.dataSize() - current.deletionInfo.dataSize();
            }
            modified = new Holder(current.map.clone(), newDelInfo);

            for (Column column : cm)
            {
                final Pair<Integer, Long> pair = modified.addColumn(transformation.apply(column), allocator, indexer);
                sizeDelta += pair.left;

                //We will store the minimum delta for all columns if enabled
                if(enableColUpdateTimeDelta)
                    timeDelta = Math.min(pair.right, timeDelta);

                // bail early if we know we've been beaten
                if (ref.get() != current)
                    continue main_loop;
            }
        }
        while (!ref.compareAndSet(current, modified));

        indexer.updateRowLevelIndexes();

        return Pair.create(sizeDelta, timeDelta);
    }

    public boolean replace(Column oldColumn, Column newColumn)
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

    public Column getColumn(ByteBuffer name)
    {
        return ref.get().map.get(name);
    }

    public SortedSet<ByteBuffer> getColumnNames()
    {
        return ref.get().map.keySet();
    }

    public Collection<Column> getSortedColumns()
    {
        return ref.get().map.values();
    }

    public Collection<Column> getReverseSortedColumns()
    {
        return ref.get().map.descendingMap().values();
    }

    public int getColumnCount()
    {
        return ref.get().map.size();
    }

    public Iterator<Column> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.get().map, slices);
    }

    public Iterator<Column> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.get().map.descendingMap(), slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {
        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        private static final DeletionInfo LIVE = DeletionInfo.live();

        final SnapTreeMap<ByteBuffer, Column> map;
        final DeletionInfo deletionInfo;

        Holder(AbstractType<?> comparator)
        {
            this(new SnapTreeMap<ByteBuffer, Column>(comparator), LIVE);
        }

        Holder(SnapTreeMap<ByteBuffer, Column> map, DeletionInfo deletionInfo)
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

        Holder with(SnapTreeMap<ByteBuffer, Column> newMap)
        {
            return new Holder(newMap, deletionInfo);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return new Holder(new SnapTreeMap<ByteBuffer, Column>(map.comparator()), LIVE);
        }

        Pair<Integer, Long> addColumn(Column column, Allocator allocator, SecondaryIndexManager.Updater indexer)
        {
            ByteBuffer name = column.name();
            while (true)
            {
                Column oldColumn = map.putIfAbsent(name, column);
                if (oldColumn == null)
                {
                    indexer.insert(column);
                    return Pair.create(column.dataSize(), Long.MAX_VALUE);
                }

                Column reconciledColumn = column.reconcile(oldColumn, allocator);
                if (map.replace(name, oldColumn, reconciledColumn))
                {
                    indexer.update(oldColumn, reconciledColumn);
                    return Pair.create(reconciledColumn.dataSize() - oldColumn.dataSize(),
                            enableColUpdateTimeDelta?  Math.abs(oldColumn.timestamp - column.timestamp): Long.MAX_VALUE );
                }
                // We failed to replace column due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
            }
        }
    }
}
