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

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.SimpleAbstractColumnIterator;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.HeapAllocator;

public class CollationController
{
    private static final Logger logger = LoggerFactory.getLogger(CollationController.class);

    private final ColumnFamilyStore cfs;
    private final QueryFilter filter;
    private final ISortedColumns.Factory factory;
    private final int gcBefore;

    private int sstablesIterated = 0;

    public CollationController(ColumnFamilyStore cfs, boolean mutableColumns, QueryFilter filter, int gcBefore)
    {
        this.cfs = cfs;
        this.filter = filter;
        this.gcBefore = gcBefore;

        // AtomicSortedColumns doesn't work for super columns (see #3821)
        this.factory = mutableColumns
                     ? cfs.metadata.cfType == ColumnFamilyType.Super ? ThreadSafeSortedColumns.factory() : AtomicSortedColumns.factory()
                     : ArrayBackedSortedColumns.factory();
    }

    public ColumnFamily getTopLevelColumns()
    {
        return filter.filter instanceof NamesQueryFilter
               && (cfs.metadata.cfType == ColumnFamilyType.Standard || filter.path.superColumnName != null)
               && cfs.metadata.getDefaultValidator() != CounterColumnType.instance
               ? collectTimeOrderedData()
               : collectAllData();
    }

    /**
     * Collects data in order of recency, using the sstable maxtimestamp data.
     * Once we have data for all requests columns that is newer than the newest remaining maxtimestamp,
     * we stop.
     */
    private ColumnFamily collectTimeOrderedData()
    {
        logger.trace("collectTimeOrderedData");
        ColumnFamily container = ColumnFamily.create(cfs.metadata, factory, filter.filter.isReversed());
        List<OnDiskAtomIterator> iterators = new ArrayList<OnDiskAtomIterator>();
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.markReferenced(filter.key);

        // We use a temporary CF object per memtable or sstable source so we can accomodate this.factory being ABSC,
        // which requires addAtom to happen in sorted order.  Then we use addAll to merge into the final collection,
        // which allows a (sorted) set of columns to be merged even if they are not uniformly sorted after the existing
        // ones.
        ColumnFamily temp = ColumnFamily.create(cfs.metadata, ArrayBackedSortedColumns.factory(), filter.filter.isReversed());

        try
        {
            Tracing.trace("Merging memtable contents");
            for (Memtable memtable : view.memtables)
            {
                OnDiskAtomIterator iter = filter.getMemtableColumnIterator(memtable);
                if (iter != null)
                {
                    iterators.add(iter);
                    temp.delete(iter.getColumnFamily());
                    while (iter.hasNext())
                        temp.addAtom(iter.next());
                }

                container.addAll(temp, HeapAllocator.instance);
                temp.clear();
            }

            // avoid changing the filter columns of the original filter
            // (reduceNameFilter removes columns that are known to be irrelevant)
            NamesQueryFilter namesFilter = (NamesQueryFilter) filter.filter;
            TreeSet<ByteBuffer> filterColumns = new TreeSet<ByteBuffer>(namesFilter.columns);
            QueryFilter reducedFilter = new QueryFilter(filter.key, filter.path, namesFilter.withUpdatedColumns(filterColumns));

            /* add the SSTables on disk */
            Collections.sort(view.sstables, SSTable.maxTimestampComparator);

            // read sorted sstables
            long mostRecentRowTombstone = Long.MIN_VALUE;
            for (SSTableReader sstable : view.sstables)
            {
                // if we've already seen a row tombstone with a timestamp greater
                // than the most recent update to this sstable, we're done, since the rest of the sstables
                // will also be older
                if (sstable.getMaxTimestamp() < mostRecentRowTombstone)
                    break;

                long currentMaxTs = sstable.getMaxTimestamp();
                reduceNameFilter(reducedFilter, container, currentMaxTs);
                if (((NamesQueryFilter) reducedFilter.filter).columns.isEmpty())
                    break;

                OnDiskAtomIterator iter = reducedFilter.getSSTableColumnIterator(sstable);
                iterators.add(iter);
                if (iter.getColumnFamily() != null)
                {
                    ColumnFamily cf = iter.getColumnFamily();
                    if (cf.isMarkedForDelete())
                    {
                        // track the most recent row level tombstone we encounter
                        mostRecentRowTombstone = cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt;
                    }

                    temp.delete(cf);
                    sstablesIterated++;
                    Tracing.trace("Merging data from sstable {}", sstable.descriptor.generation);
                    while (iter.hasNext())
                        temp.addAtom(iter.next());
                }

                container.addAll(temp, HeapAllocator.instance);
                temp.clear();
            }

            // we need to distinguish between "there is no data at all for this row" (BF will let us rebuild that efficiently)
            // and "there used to be data, but it's gone now" (we should cache the empty CF so we don't need to rebuild that slower)
            if (iterators.isEmpty())
                return null;

            // do a final collate.  toCollate is boilerplate required to provide a CloseableIterator
            final ColumnFamily c2 = container;
            CloseableIterator<OnDiskAtom> toCollate = new SimpleAbstractColumnIterator()
            {
                final Iterator<IColumn> iter = c2.iterator();

                protected OnDiskAtom computeNext()
                {
                    return iter.hasNext() ? iter.next() : endOfData();
                }

                public ColumnFamily getColumnFamily()
                {
                    return c2;
                }

                public DecoratedKey getKey()
                {
                    return filter.key;
                }
            };
            ColumnFamily returnCF = container.cloneMeShallow();
            Tracing.trace("Collating all results");
            filter.collateOnDiskAtom(returnCF, Collections.singletonList(toCollate), gcBefore);

            // "hoist up" the requested data into a more recent sstable
            if (sstablesIterated > cfs.getMinimumCompactionThreshold()
                && !cfs.isCompactionDisabled()
                && cfs.getCompactionStrategy() instanceof SizeTieredCompactionStrategy)
            {
                Tracing.trace("Defragmenting requested data");
                RowMutation rm = new RowMutation(cfs.table.name, new Row(filter.key, returnCF.cloneMe()));
                // skipping commitlog and index updates is fine since we're just de-fragmenting existing data
                Table.open(rm.getTable()).apply(rm, false, false);
            }

            // Caller is responsible for final removeDeletedCF.  This is important for cacheRow to work correctly:
            return returnCF;
        }
        finally
        {
            for (OnDiskAtomIterator iter : iterators)
                FileUtils.closeQuietly(iter);
            SSTableReader.releaseReferences(view.sstables);
        }
    }

    /**
     * remove columns from @param filter where we already have data in @param returnCF newer than @param sstableTimestamp
     */
    private void reduceNameFilter(QueryFilter filter, ColumnFamily returnCF, long sstableTimestamp)
    {
        AbstractColumnContainer container = filter.path.superColumnName == null
                                          ? returnCF
                                          : (SuperColumn) returnCF.getColumn(filter.path.superColumnName);
        if (container == null)
            return;

        for (Iterator<ByteBuffer> iterator = ((NamesQueryFilter) filter.filter).columns.iterator(); iterator.hasNext(); )
        {
            ByteBuffer filterColumn = iterator.next();
            IColumn column = container.getColumn(filterColumn);
            if (column != null && column.timestamp() > sstableTimestamp)
                iterator.remove();
        }
    }

    /**
     * Collects data the brute-force way: gets an iterator for the filter in question
     * from every memtable and sstable, then merges them together.
     */
    private ColumnFamily collectAllData()
    {
        logger.trace("collectAllData");
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.markReferenced(filter.key);
        List<OnDiskAtomIterator> iterators = new ArrayList<OnDiskAtomIterator>(Iterables.size(view.memtables) + view.sstables.size());
        ColumnFamily returnCF = ColumnFamily.create(cfs.metadata, factory, filter.filter.isReversed());

        try
        {
            Tracing.trace("Merging memtable contents");
            for (Memtable memtable : view.memtables)
            {
                OnDiskAtomIterator iter = filter.getMemtableColumnIterator(memtable);
                if (iter != null)
                {
                    returnCF.delete(iter.getColumnFamily());
                    iterators.add(iter);
                }
            }

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 > maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In othere words, iterating in maxTimestamp order allow to do our mostRecentTombstone elimination
             * in one pass, and minimize the number of sstables for which we read a rowTombstone.
             */
            Collections.sort(view.sstables, SSTable.maxTimestampComparator);

            long mostRecentRowTombstone = Long.MIN_VALUE;
            for (SSTableReader sstable : view.sstables)
            {
                // if we've already seen a row tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                if (sstable.getMaxTimestamp() < mostRecentRowTombstone)
                    break;

                OnDiskAtomIterator iter = filter.getSSTableColumnIterator(sstable);
                iterators.add(iter);
                if (iter.getColumnFamily() != null)
                {
                    ColumnFamily cf = iter.getColumnFamily();
                    if (cf.isMarkedForDelete())
                        mostRecentRowTombstone = cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt;

                    returnCF.delete(cf);
                    sstablesIterated++;
                }
            }

            // we need to distinguish between "there is no data at all for this row" (BF will let us rebuild that efficiently)
            // and "there used to be data, but it's gone now" (we should cache the empty CF so we don't need to rebuild that slower)
            if (iterators.isEmpty())
                return null;

            Tracing.trace("Merging data from memtables and {} sstables", sstablesIterated);
            filter.collateOnDiskAtom(returnCF, iterators, gcBefore);

            // Caller is responsible for final removeDeletedCF.  This is important for cacheRow to work correctly:
            return returnCF;
        }
        finally
        {
            for (OnDiskAtomIterator iter : iterators)
                FileUtils.closeQuietly(iter);
            SSTableReader.releaseReferences(view.sstables);
        }
    }

    public int getSstablesIterated()
    {
        return sstablesIterated;
    }
}
