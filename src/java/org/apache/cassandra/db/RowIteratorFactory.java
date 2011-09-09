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
import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

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
    public static CloseableIterator<Row> getIterator(final Iterable<Memtable> memtables,
                                          final Collection<SSTableReader> sstables,
                                          final DecoratedKey startWith,
                                          final DecoratedKey stopAt,
                                          final QueryFilter filter,
                                          final AbstractType comparator,
                                          final ColumnFamilyStore cfs)
    {
        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<CloseableIterator<IColumnIterator>> iterators = new ArrayList<CloseableIterator<IColumnIterator>>();
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
            iterators.add(new ConvertToColumnIterator(filter, comparator, p, memtable.getEntryIterator(startWith)));
        }

        for (SSTableReader sstable : sstables)
        {
            final SSTableScanner scanner = sstable.getScanner(filter);
            scanner.seekTo(startWith);
            assert scanner instanceof Closeable; // otherwise we leak FDs
            iterators.add(scanner);
        }

        final Memtable firstMemtable = memtables.iterator().next();
        // reduce rows from all sources into a single row
        return MergeIterator.get(iterators, COMPARE_BY_KEY, new MergeIterator.Reducer<IColumnIterator, Row>()
        {
            private final int gcBefore = (int) (System.currentTimeMillis() / 1000) - cfs.metadata.getGcGraceSeconds();
            private final List<IColumnIterator> colIters = new ArrayList<IColumnIterator>();
            private DecoratedKey key;
            private ColumnFamily returnCF;

            @Override
            protected void onKeyChange()
            {
                this.returnCF = ColumnFamily.create(cfs.metadata);
            }

            public void reduce(IColumnIterator current)
            {
                this.colIters.add(current);
                this.key = current.getKey();
                this.returnCF.delete(current.getColumnFamily());
            }

            protected Row getReduced()
            {

                // First check if this row is in the rowCache. If it is we can skip the rest
                ColumnFamily cached = cfs.getRawCachedRow(key);
                if (cached == null)
                    // not cached: collate
                    filter.collateColumns(returnCF, colIters, comparator, gcBefore);
                else
                {
                    QueryFilter keyFilter = new QueryFilter(key, filter.path, filter.filter);
                    returnCF = cfs.filterColumnFamily(cached, keyFilter, gcBefore);
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
    private static class ConvertToColumnIterator extends AbstractIterator<IColumnIterator> implements CloseableIterator<IColumnIterator>
    {
        private final QueryFilter filter;
        private final AbstractType comparator;
        private final Predicate<IColumnIterator> pred;
        private final Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter;

        public ConvertToColumnIterator(QueryFilter filter, AbstractType comparator, Predicate<IColumnIterator> pred, Iterator<Map.Entry<DecoratedKey, ColumnFamily>> iter)
        {
            this.filter = filter;
            this.comparator = comparator;
            this.pred = pred;
            this.iter = iter;
        }

        public IColumnIterator computeNext()
        {
            while (iter.hasNext())
            {
                Map.Entry<DecoratedKey, ColumnFamily> entry = iter.next();
                IColumnIterator ici = filter.getMemtableColumnIterator(entry.getValue(), entry.getKey(), comparator);
                if (pred.apply(ici))
                    return ici;
            }
            return endOfData();
        }

        public void close()
        {
            // pass
        }
    }
}
