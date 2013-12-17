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
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.stanford.ppl.concurrent.SnapTreeMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
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
 */
public class AtomicSortedColumns extends ColumnFamily
{
    private final AtomicReference<Holder> ref;

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

    public CellNameType getComparator()
    {
        return (CellNameType)ref.get().map.comparator();
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

    public void addColumn(Cell cell, Allocator allocator)
    {
        Holder current, modified;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            modified.addColumn(cell, allocator, SecondaryIndexManager.nullUpdater);
        }
        while (!ref.compareAndSet(current, modified));
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    /**
     *  This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     *  @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation, SecondaryIndexManager.Updater indexer)
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
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(cm.deletionInfo());
            modified = new Holder(current.map.clone(), newDelInfo);

            if (cm.deletionInfo().hasRanges())
            {
                for (Cell currentCell : Iterables.concat(current.map.values(), cm))
                {
                    if (cm.deletionInfo().isDeleted(currentCell))
                        indexer.remove(currentCell);
                }
            }

            for (Cell cell : cm)
            {
                sizeDelta += modified.addColumn(transformation.apply(cell), allocator, indexer);
                // bail early if we know we've been beaten
                if (ref.get() != current)
                    continue main_loop;
            }
        }
        while (!ref.compareAndSet(current, modified));

        indexer.updateRowLevelIndexes();

        return sizeDelta;
    }

    public boolean replace(Cell oldCell, Cell newCell)
    {
        if (!oldCell.name().equals(newCell.name()))
            throw new IllegalArgumentException();

        Holder current, modified;
        boolean replaced;
        do
        {
            current = ref.get();
            modified = current.cloneMe();
            replaced = modified.map.replace(oldCell.name(), oldCell, newCell);
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

    public Cell getColumn(CellName name)
    {
        return ref.get().map.get(name);
    }

    public SortedSet<CellName> getColumnNames()
    {
        return ref.get().map.keySet();
    }

    public Collection<Cell> getSortedColumns()
    {
        return ref.get().map.values();
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return ref.get().map.descendingMap().values();
    }

    public int getColumnCount()
    {
        return ref.get().map.size();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableMapIterator(ref.get().map, slices);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
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

        final SnapTreeMap<CellName, Cell> map;
        final DeletionInfo deletionInfo;

        Holder(CellNameType comparator)
        {
            this(new SnapTreeMap<CellName, Cell>(comparator), LIVE);
        }

        Holder(SnapTreeMap<CellName, Cell> map, DeletionInfo deletionInfo)
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

        Holder with(SnapTreeMap<CellName, Cell> newMap)
        {
            return new Holder(newMap, deletionInfo);
        }

        // There is no point in cloning the underlying map to clear it
        // afterwards.
        Holder clear()
        {
            return new Holder(new SnapTreeMap<CellName, Cell>(map.comparator()), LIVE);
        }

        long addColumn(Cell cell, Allocator allocator, SecondaryIndexManager.Updater indexer)
        {
            CellName name = cell.name();
            while (true)
            {
                Cell oldCell = map.putIfAbsent(name, cell);
                if (oldCell == null)
                {
                    indexer.insert(cell);
                    return cell.dataSize();
                }

                Cell reconciledCell = cell.reconcile(oldCell, allocator);
                if (map.replace(name, oldCell, reconciledCell))
                {
                    // for memtable updates we only care about oldcolumn, reconciledcolumn, but when compacting
                    // we need to make sure we update indexes no matter the order we merge
                    if (reconciledCell == cell)
                        indexer.update(oldCell, reconciledCell);
                    else
                        indexer.update(cell, reconciledCell);
                    return reconciledCell.dataSize() - oldCell.dataSize();
                }
                // We failed to replace cell due to a concurrent update or a concurrent removal. Keep trying.
                // (Currently, concurrent removal should not happen (only updates), but let us support that anyway.)
            }
        }
    }
}
