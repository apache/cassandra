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

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.columniterator.IColumnIteratorFactory;
import org.apache.cassandra.db.columniterator.LazyColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class RowIteratorFactory
{

    private static final Comparator<OnDiskAtomIterator> COMPARE_BY_KEY = new Comparator<OnDiskAtomIterator>()
    {
        public int compare(OnDiskAtomIterator o1, OnDiskAtomIterator o2)
        {
            return DecoratedKey.comparator.compare(o1.getKey(), o2.getKey());
        }
    };


    /**
     * Get a row iterator over the provided memtables and sstables, between the provided keys
     * and filtered by the queryfilter.
     * @param memtables Memtables pending flush.
     * @param sstables SStables to scan through.
     * @param range The data range to fetch
     * @param cfs
     * @return A row iterator following all the given restrictions
     */
    public static CloseableIterator<Row> getIterator(final Iterable<Memtable> memtables,
                                                     final Collection<SSTableReader> sstables,
                                                     final DataRange range,
                                                     final ColumnFamilyStore cfs,
                                                     final long now)
    {
        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<CloseableIterator<OnDiskAtomIterator>> iterators = new ArrayList<>(Iterables.size(memtables) + sstables.size());

        for (Memtable memtable : memtables)
            iterators.add(new ConvertToColumnIterator(range, memtable.getEntryIterator(range.startKey(), range.stopKey())));

        for (SSTableReader sstable : sstables)
            iterators.add(sstable.getScanner(range));

        // reduce rows from all sources into a single row
        return MergeIterator.get(iterators, COMPARE_BY_KEY, new MergeIterator.Reducer<OnDiskAtomIterator, Row>()
        {
            private final int gcBefore = cfs.gcBefore(now);
            private final List<OnDiskAtomIterator> colIters = new ArrayList<>();
            private DecoratedKey key;
            private ColumnFamily returnCF;

            @Override
            protected void onKeyChange()
            {
                this.returnCF = ArrayBackedSortedColumns.factory.create(cfs.metadata, range.columnFilter.isReversed());
            }

            public void reduce(OnDiskAtomIterator current)
            {
                this.colIters.add(current);
                this.key = current.getKey();
                this.returnCF.delete(current.getColumnFamily());
            }

            protected Row getReduced()
            {
                // First check if this row is in the rowCache. If it is and it covers our filter, we can skip the rest
                ColumnFamily cached = cfs.getRawCachedRow(key);
                IDiskAtomFilter filter = range.columnFilter(key.getKey());

                if (cached == null || !cfs.isFilterFullyCoveredBy(filter, cached, now))
                {
                    // not cached: collate
                    QueryFilter.collateOnDiskAtom(returnCF, colIters, filter, key, gcBefore, now);
                }
                else
                {
                    QueryFilter keyFilter = new QueryFilter(key, cfs.name, filter, now);
                    returnCF = cfs.filterColumnFamily(cached, keyFilter);
                }

                Row rv = new Row(key, returnCF);
                colIters.clear();
                key = null;
                return rv;
            }
        });
    }

    /**
     * Get a ColumnIterator for a specific key in the memtable.
     */
    private static class ConvertToColumnIterator implements CloseableIterator<OnDiskAtomIterator>
    {
        private final DataRange range;
        private final Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter;

        public ConvertToColumnIterator(DataRange range, Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter)
        {
            this.range = range;
            this.iter = iter;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        /*
         * Note that when doing get_paged_slice, we reset the start of the queryFilter after we've fetched the
         * first row. This means that this iterator should not use in any way the filter to fetch a row before
         * we call next(). Which prevents us for using guava AbstractIterator.
         * This is obviously rather fragile and we should consider refactoring that code, but such refactor will go
         * deep into the storage engine code so this will have to do until then.
         */
        public OnDiskAtomIterator next()
        {
            final Map.Entry<DecoratedKey, ColumnFamily> entry = iter.next();
            return new LazyColumnIterator(entry.getKey(), new IColumnIteratorFactory()
            {
                public OnDiskAtomIterator create()
                {
                    return range.columnFilter(entry.getKey().getKey()).getColumnIterator(entry.getKey(), entry.getValue());
                }
            });
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public void close()
        {
            // pass
        }
    }
}
