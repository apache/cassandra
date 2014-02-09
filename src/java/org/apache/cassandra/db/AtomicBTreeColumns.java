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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.AbstractAllocator;

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
    static final long HEAP_SIZE = ObjectSizes.measure(new AtomicBTreeColumns(CFMetaData.IndexCf, null))
            + ObjectSizes.measure(new Holder(null, null));

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

    // the function we provide to the btree utilities to perform any column replacements
    private static final class ColumnUpdater implements UpdateFunction<Cell>
    {
        final AtomicBTreeColumns updating;
        final Holder ref;
        final AbstractAllocator allocator;
        final Function<Cell, Cell> transform;
        final Updater indexer;
        final Delta delta;

        private ColumnUpdater(AtomicBTreeColumns updating, Holder ref, AbstractAllocator allocator, Function<Cell, Cell> transform, Updater indexer, Delta delta)
        {
            this.updating = updating;
            this.ref = ref;
            this.allocator = allocator;
            this.transform = transform;
            this.indexer = indexer;
            this.delta = delta;
        }

        public Cell apply(Cell inserted)
        {
            indexer.insert(inserted);
            delta.insert(inserted);
            return transform.apply(inserted);
        }

        public Cell apply(Cell existing, Cell update)
        {
            Cell reconciled = update.reconcile(existing, allocator);
            indexer.update(existing, reconciled);
            if (existing != reconciled)
                delta.swap(existing, reconciled);
            else
                delta.abort(update);
            return transform.apply(reconciled);
        }

        public boolean abortEarly()
        {
            return updating.ref != ref;
        }

        public void allocated(long heapSize)
        {
            delta.addHeapSize(heapSize);
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
     * This is only called by Memtable.resolve, so only AtomicBTreeColumns needs to implement it.
     *
     * @return the difference in size seen after merging the given columns
     */
    public Delta addAllWithSizeDelta(final ColumnFamily cm, AbstractAllocator allocator, Function<Cell, Cell> transformation, Updater indexer, Delta delta)
    {
        boolean transformed = false;
        Collection<Cell> insert = cm.getSortedColumns();

        while (true)
        {
            Holder current = ref;

            delta.reset();
            DeletionInfo deletionInfo = cm.deletionInfo();
            if (deletionInfo.mayModify(current.deletionInfo))
            {
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
                delta.addHeapSize(deletionInfo.unsharedHeapSize() - current.deletionInfo.unsharedHeapSize());
            }

            ColumnUpdater updater = new ColumnUpdater(this, current, allocator, transformation, indexer, delta);
            Object[] tree = BTree.update(current.tree, metadata.comparator.columnComparator(), insert, true, updater);

            if (tree != null && refUpdater.compareAndSet(this, current, new Holder(tree, deletionInfo)))
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

    // no particular reason not to implement these next methods, we just haven't needed them yet

    public void addColumn(Cell column)
    {
        throw new UnsupportedOperationException();
    }

    public void addAll(ColumnFamily cf)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
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
    }

    // TODO: create a stack-allocation-friendly list to help optimise garbage for updates to rows with few columns

    /**
     * tracks the size changes made while merging a new group of cells in
     */
    public static final class Delta
    {
        private long dataSize;
        private long heapSize;

        // we track the discarded cells (cells that were in the btree, but replaced by new ones)
        // separately from aborted ones (were part of an update but older than existing cells)
        // since we need to reset the former when we race on the btree update, but not the latter
        private List<Cell> discarded = new ArrayList<>();
        private List<Cell> aborted;

        protected void reset()
        {
            this.dataSize = 0;
            this.heapSize = 0;
            discarded.clear();
        }

        protected void addHeapSize(long heapSize)
        {
            this.heapSize += heapSize;
        }

        protected void swap(Cell old, Cell updated)
        {
            dataSize += updated.dataSize() - old.dataSize();
            heapSize += updated.excessHeapSizeExcludingData() - old.excessHeapSizeExcludingData();
            discarded.add(old);
        }

        protected void insert(Cell insert)
        {
            this.dataSize += insert.dataSize();
            this.heapSize += insert.excessHeapSizeExcludingData();
        }

        private void abort(Cell neverUsed)
        {
            if (aborted == null)
                aborted = new ArrayList<>();
            aborted.add(neverUsed);
        }

        public long dataSize()
        {
            return dataSize;
        }

        public long excessHeapSize()
        {
            return heapSize;
        }

        public Iterable<Cell> reclaimed()
        {
            if (aborted == null)
                return discarded;
            return Iterables.concat(discarded, aborted);
        }
    }
}
