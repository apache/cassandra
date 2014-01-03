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

import com.google.common.base.*;
import com.google.common.collect.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.ReplaceFunction;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.cassandra.db.index.SecondaryIndexManager.Updater;

/**
 * A thread-safe and atomic ISortedColumns implementation.
 * Operations (in particular addAll) on this implemenation are atomic and
 * isolated (in the sense of ACID). Typically a addAll is guaranteed that no
 * other thread can see the state where only parts but not all columns have
 * been added.
 * <p/>
 * WARNING: removing element through getSortedColumns().iterator() is *not* supported
 */
public class AtomicBTreeColumns extends ColumnFamily
{
    private static final Function<Cell, CellName> NAME = new Function<Cell, CellName>()
    {
        public CellName apply(Cell column)
        {
            return column.name;
        }
    };

    public static final Factory<AtomicBTreeColumns> factory = new Factory<AtomicBTreeColumns>()
    {
        public AtomicBTreeColumns create(CFMetaData metadata, boolean insertReversed)
        {
            if (insertReversed)
                throw new IllegalArgumentException();
            return new AtomicBTreeColumns(metadata);
        }
    };

    private static final DeletionInfo LIVE = DeletionInfo.live();
    private static final Holder EMPTY = new Holder(BTree.empty(), LIVE);

    private volatile Holder ref;

    private static final AtomicReferenceFieldUpdater<AtomicBTreeColumns, Holder> refUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreeColumns.class, Holder.class, "ref");

    private AtomicBTreeColumns(CFMetaData metadata)
    {
        this(metadata, EMPTY);
    }

    private AtomicBTreeColumns(CFMetaData metadata, Holder holder)
    {
        super(metadata);
        this.ref = holder;
    }

    public CellNameType getComparator()
    {
        return metadata.comparator;
    }

    public Factory getFactory()
    {
        return factory;
    }

    public ColumnFamily cloneMe()
    {
        return new AtomicBTreeColumns(metadata, ref);
    }

    public DeletionInfo deletionInfo()
    {
        return ref.deletionInfo;
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
            Holder current = ref;
            DeletionInfo newDelInfo = current.deletionInfo.copy().add(info);
            if (refUpdater.compareAndSet(this, current, current.with(newDelInfo)))
                break;
        }
    }

    public void setDeletionInfo(DeletionInfo newInfo)
    {
        ref = ref.with(newInfo);
    }

    public void purgeTombstones(int gcBefore)
    {
        while (true)
        {
            Holder current = ref;
            if (!current.deletionInfo.hasPurgeableTombstones(gcBefore))
                break;

            DeletionInfo purgedInfo = current.deletionInfo.copy();
            purgedInfo.purge(gcBefore);
            if (refUpdater.compareAndSet(this, current, current.with(purgedInfo)))
                break;
        }
    }

    public void addColumn(Cell column, Allocator allocator)
    {
        while (true)
        {
            Holder current = ref;
            Holder update = ref.update(this, current.deletionInfo, metadata.comparator.columnComparator(), Arrays.asList(column), null);
            if (refUpdater.compareAndSet(this, current, update))
                return;
        }
    }

    public void addAll(ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation)
    {
        addAllWithSizeDelta(cm, allocator, transformation, SecondaryIndexManager.nullUpdater);
    }

    // the function we provide to the btree utilities to perform any column replacements
    private static final class ColumnUpdater implements ReplaceFunction<Cell>
    {
        final Allocator allocator;
        final Function<Cell, Cell> transform;
        final Updater indexer;
        long delta;

        private ColumnUpdater(Allocator allocator, Function<Cell, Cell> transform, Updater indexer)
        {
            this.allocator = allocator;
            this.transform = transform;
            this.indexer = indexer;
        }

        public Cell apply(Cell replaced, Cell update)
        {
            if (replaced == null)
            {
                indexer.insert(update);
                delta += update.dataSize();
            }
            else
            {
                Cell reconciled = update.reconcile(replaced, allocator);
                if (reconciled == update)
                    indexer.update(replaced, reconciled);
                else
                    indexer.update(update, reconciled);
                delta += reconciled.dataSize() - replaced.dataSize();
            }

            return transform.apply(update);
        }
    }

    private static Collection<Cell> transform(Comparator<Cell> cmp, ColumnFamily cf, Function<Cell, Cell> transformation, boolean sort)
    {
        Cell[] tmp = new Cell[cf.getColumnCount()];

        int i = 0;
        for (Cell c : cf)
            tmp[i++] = transformation.apply(c);

        if (sort)
            Arrays.sort(tmp, cmp);

        return Arrays.asList(tmp);
    }

    /**
     * This is only called by Memtable.resolve, so only AtomicSortedColumns needs to implement it.
     *
     * @return the difference in size seen after merging the given columns
     */
    public long addAllWithSizeDelta(final ColumnFamily cm, Allocator allocator, Function<Cell, Cell> transformation, Updater indexer)
    {
        boolean transformed = false;
        Collection<Cell> insert;
        if (cm instanceof UnsortedColumns)
        {
            insert = transform(metadata.comparator.columnComparator(), cm, transformation, true);
            transformed = true;
        }
        else
            insert = cm.getSortedColumns();

        while (true)
        {
            Holder current = ref;

            DeletionInfo deletionInfo = cm.deletionInfo();
            if (deletionInfo.hasRanges())
            {
                for (Iterator<Cell> iter : new Iterator[] { insert.iterator(), BTree.<Cell>slice(current.tree, true) })
                {
                    while (iter.hasNext())
                    {
                        Cell col = iter.next();
                        if (deletionInfo.isDeleted(col))
                            indexer.remove(col);
                    }
                }
            }
            deletionInfo = current.deletionInfo.copy().add(deletionInfo);

            ColumnUpdater updater = new ColumnUpdater(allocator, transformation, indexer);
            Holder h = current.update(this, deletionInfo, metadata.comparator.columnComparator(), insert, updater);
            if (h != null && refUpdater.compareAndSet(this, current, h))
            {
                indexer.updateRowLevelIndexes();
                return updater.delta;
            }

            if (!transformed)
            {
                // After failing once, transform Columns into a new collection to avoid repeatedly allocating Slab space
                insert = transform(metadata.comparator.columnComparator(), cm, transformation, false);
                transformed = true;
            }
        }

    }

    public boolean replace(Cell oldColumn, Cell newColumn)
    {
        if (!oldColumn.name().equals(newColumn.name()))
            throw new IllegalArgumentException();

        while (true)
        {
            Holder current = ref;
            Holder modified = current.update(this, current.deletionInfo, metadata.comparator.columnComparator(), Arrays.asList(newColumn), null);
            if (modified == current)
                return false;
            if (refUpdater.compareAndSet(this, current, modified))
                return true;
        }
    }

    public void clear()
    {
        // no particular reason not to implement this, we just haven't needed it yet
        throw new UnsupportedOperationException();
    }

    public Cell getColumn(CellName name)
    {
        return (Cell) BTree.find(ref.tree, asymmetricComparator(), name);
    }

    private Comparator<Object> asymmetricComparator()
    {
        final Comparator<? super CellName> cmp = metadata.comparator;
        return new Comparator<Object>()
        {
            public int compare(Object o1, Object o2)
            {
                return cmp.compare((CellName) o1, ((Cell) o2).name);
            }
        };
    }

    public Iterable<CellName> getColumnNames()
    {
        return collection(false, NAME);
    }

    public Collection<Cell> getSortedColumns()
    {
        return collection(true, Functions.<Cell>identity());
    }

    public Collection<Cell> getReverseSortedColumns()
    {
        return collection(false, Functions.<Cell>identity());
    }

    private <V> Collection<V> collection(final boolean forwards, final Function<Cell, V> f)
    {
        final Holder ref = this.ref;
        return new AbstractCollection<V>()
        {
            public Iterator<V> iterator()
            {
                return Iterators.transform(BTree.<Cell>slice(ref.tree, forwards), f);
            }

            public int size()
            {
                return BTree.slice(ref.tree, true).count();
            }
        };
    }

    public int getColumnCount()
    {
        return BTree.slice(ref.tree, true).count();
    }

    public Iterator<Cell> iterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableSetIterator(new BTreeSet<>(ref.tree, getComparator().columnComparator()), slices);
    }

    public Iterator<Cell> reverseIterator(ColumnSlice[] slices)
    {
        return new ColumnSlice.NavigableSetIterator(new BTreeSet<>(ref.tree, getComparator().columnComparator()).descendingSet(), slices);
    }

    public boolean isInsertReversed()
    {
        return false;
    }

    private static class Holder
    {
        // This is a small optimization: DeletionInfo is mutable, but we know that we will always copy it in that class,
        // so we can safely alias one DeletionInfo.live() reference and avoid some allocations.
        final DeletionInfo deletionInfo;
        // the btree of columns
        final Object[] tree;

        Holder(Object[] tree, DeletionInfo deletionInfo)
        {
            this.tree = tree;
            this.deletionInfo = deletionInfo;
        }

        Holder with(DeletionInfo info)
        {
            return new Holder(this.tree, info);
        }

        Holder update(AtomicBTreeColumns container, DeletionInfo deletionInfo, Comparator<Cell> cmp, Collection<Cell> update, ReplaceFunction<Cell> replaceF)
        {
            Object[] r = BTree.update(tree, cmp, update, true, replaceF, new TerminateEarly(container, this));
            // result can be null if terminate early kicks in, in which case we need to propagate the early failure so we can retry
            if (r == null)
                return null;
            return new Holder(r, deletionInfo);
        }
    }

    // a function provided to the btree functions that aborts the modification
    // if we already know the final cas will fail
    private static final class TerminateEarly implements Function<Object, Boolean>
    {
        final AtomicBTreeColumns columns;
        final Holder ref;

        private TerminateEarly(AtomicBTreeColumns columns, Holder ref)
        {
            this.columns = columns;
            this.ref = ref;
        }

        public Boolean apply(Object o)
        {
            if (ref != columns.ref)
                return Boolean.TRUE;
            return Boolean.FALSE;
        }
    }
}
