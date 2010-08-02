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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ReducingIterator;
import org.apache.commons.collections.IteratorUtils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class RowIteratorFactory
{

    private static final int RANGE_FILE_BUFFER_SIZE = 256 * 1024;

    private static final Comparator<IColumnIterator> COMPARE_BY_KEY = new Comparator<IColumnIterator>()
    {
        public int compare(IColumnIterator o1, IColumnIterator o2)
        {
            return DecoratedKey.comparator.compare(o1.getKey(), o2.getKey());
        }
    };

    
    /**
     * Get a row iterator over the provided memtables and sstables, between the provided keys
     * and filtered by the queryfilter.
     * @param memtables Memtables pending flush.
     * @param sstables SStables to scan through.
     * @param startWith Start at this key
     * @param stopAt Stop and this key
     * @param filter Used to decide which columns to pull out
     * @param comparator
     * @return A row iterator following all the given restrictions
     */
    public static RowIterator getIterator(final Collection<Memtable> memtables,
                                          final Collection<SSTableReader> sstables,
                                          final DecoratedKey startWith,
                                          final DecoratedKey stopAt,
                                          final QueryFilter filter,
                                          final AbstractType comparator,
                                          final ColumnFamilyStore cfs
    )
    {
        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<Iterator<IColumnIterator>> iterators = new ArrayList<Iterator<IColumnIterator>>();
        // we iterate through memtables with a priority queue to avoid more sorting than necessary.
        // this predicate throws out the rows before the start of our range.
        Predicate<IColumnIterator> p = new Predicate<IColumnIterator>()
        {
            public boolean apply(IColumnIterator row)
            {
                return startWith.compareTo(row.getKey()) <= 0
                       && (stopAt.isEmpty() || row.getKey().compareTo(stopAt) <= 0);
            }
        };

        // memtables
        for (Memtable memtable : memtables)
        {
            iterators.add(Iterators.filter(Iterators.transform(memtable.getEntryIterator(startWith),
                                                               new ConvertToColumnIterator(filter, comparator)), p));
        }

        // sstables
        for (SSTableReader sstable : sstables)
        {
            final SSTableScanner scanner = sstable.getScanner(RANGE_FILE_BUFFER_SIZE, filter);
            scanner.seekTo(startWith);
            assert scanner instanceof Closeable; // otherwise we leak FDs
            iterators.add(scanner);
        }

        Iterator<IColumnIterator> collated = IteratorUtils.collatedIterator(COMPARE_BY_KEY, iterators);
        final Memtable firstMemtable = memtables.iterator().next();

        // reduce rows from all sources into a single row
        ReducingIterator<IColumnIterator, Row> reduced = new ReducingIterator<IColumnIterator, Row>(collated)
        {
            private final int gcBefore = (int) (System.currentTimeMillis() / 1000) - cfs.metadata.gcGraceSeconds;
            private final List<IColumnIterator> colIters = new ArrayList<IColumnIterator>();
            private DecoratedKey key;

            public void reduce(IColumnIterator current)
            {
                this.colIters.add(current);
                this.key = current.getKey();
            }

            @Override
            protected boolean isEqual(IColumnIterator o1, IColumnIterator o2)
            {
                return COMPARE_BY_KEY.compare(o1, o2) == 0;
            }

            protected Row getReduced()
            {
                Comparator<IColumn> colComparator = QueryFilter.getColumnComparator(comparator);
                Iterator<IColumn> colCollated = IteratorUtils.collatedIterator(colComparator, colIters);

                ColumnFamily returnCF = null;
                
                // First check if this row is in the rowCache. If it is we can skip the rest
                ColumnFamily cached = cfs.getRawCachedRow(key);
                if (cached != null)
                {
                    QueryFilter keyFilter = new QueryFilter(key, filter.path, filter.filter);
                    returnCF = cfs.filterColumnFamily(cached, keyFilter, cfs.metadata.gcGraceSeconds);
                }
                else
                {
                    returnCF = firstMemtable.getColumnFamily(key);            
                    // TODO this is a little subtle: the Memtable ColumnIterator has to be a shallow clone of the source CF,
                    // with deletion times set correctly, so we can use it as the "base" CF to add query results to.
                    // (for sstable ColumnIterators we do not care if it is a shallow clone or not.)
                    returnCF = returnCF == null ? ColumnFamily.create(firstMemtable.getTableName(), filter.getColumnFamilyName())
                            : returnCF.cloneMeShallow();

                    if (colCollated.hasNext())
                    {
                        filter.collectCollatedColumns(returnCF, colCollated, gcBefore);
                    }
                    else
                    {
                        returnCF = null;
                    }
                }

                Row rv = new Row(key, returnCF);
                colIters.clear();
                key = null;
                return rv;
            }
        };

        return new RowIterator(reduced, iterators);
    }

    /** 
     * Used when locks are required before getting the entry iterator.
     * @param memtable Memtable to get iterator from
     * @param startWith Start at this key position
     * @return entry iterator for the current memtable
     */
    private static Iterator<Map.Entry<DecoratedKey, ColumnFamily>> memtableEntryIterator(Memtable memtable, DecoratedKey startWith)
    {
        Table.flusherLock.readLock().lock();
        try
        {
            return memtable.getEntryIterator(startWith);
        }
        finally
        {
            Table.flusherLock.readLock().unlock();
        }
    }

    /**
     * Get a ColumnIterator for a specific key in the memtable.
     */
    private static class ConvertToColumnIterator implements Function<Map.Entry<DecoratedKey, ColumnFamily>, IColumnIterator>
    {
        private QueryFilter filter;
        private AbstractType comparator;

        public ConvertToColumnIterator(QueryFilter filter, AbstractType comparator)
        {
            this.filter = filter;
            this.comparator = comparator;
        }

        public IColumnIterator apply(final Entry<DecoratedKey, ColumnFamily> entry)
        {
            return filter.getMemtableColumnIterator(entry.getValue(), entry.getKey(), comparator);
        }
    }

}
