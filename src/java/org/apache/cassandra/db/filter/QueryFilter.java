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
package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.MergeIterator;

public class QueryFilter
{
    public final DecoratedKey key;
    public final String cfName;
    public final IDiskAtomFilter filter;
    public final long timestamp;

    public QueryFilter(DecoratedKey key, String cfName, IDiskAtomFilter filter, long timestamp)
    {
        this.key = key;
        this.cfName = cfName;
        this.filter = filter;
        this.timestamp = timestamp;
    }

    public Iterator<Cell> getIterator(ColumnFamily cf)
    {
        assert cf != null;
        return filter.getColumnIterator(cf);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable)
    {
        return filter.getSSTableColumnIterator(sstable, key);
    }

    public void collateOnDiskAtom(ColumnFamily returnCF,
                                  List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                  int gcBefore)
    {
        collateOnDiskAtom(returnCF, toCollate, filter, gcBefore, timestamp);
    }

    public static void collateOnDiskAtom(ColumnFamily returnCF,
                                         List<? extends Iterator<? extends OnDiskAtom>> toCollate,
                                         IDiskAtomFilter filter,
                                         int gcBefore,
                                         long timestamp)
    {
        List<Iterator<Cell>> filteredIterators = new ArrayList<>(toCollate.size());
        for (Iterator<? extends OnDiskAtom> iter : toCollate)
            filteredIterators.add(gatherTombstones(returnCF, iter));
        collateColumns(returnCF, filteredIterators, filter, gcBefore, timestamp);
    }

    // When there is only a single source of atoms, we can skip the collate step
    public void collateOnDiskAtom(ColumnFamily returnCF, Iterator<? extends OnDiskAtom> toCollate, int gcBefore)
    {
        filter.collectReducedColumns(returnCF, gatherTombstones(returnCF, toCollate), gcBefore, timestamp);
    }

    public void collateColumns(ColumnFamily returnCF, List<? extends Iterator<Cell>> toCollate, int gcBefore)
    {
        collateColumns(returnCF, toCollate, filter, gcBefore, timestamp);
    }

    public static void collateColumns(ColumnFamily returnCF,
                                      List<? extends Iterator<Cell>> toCollate,
                                      IDiskAtomFilter filter,
                                      int gcBefore,
                                      long timestamp)
    {
        Comparator<Cell> comparator = filter.getColumnComparator(returnCF.getComparator());

        Iterator<Cell> reduced = toCollate.size() == 1
                               ? toCollate.get(0)
                               : MergeIterator.get(toCollate, comparator, getReducer(comparator));

        filter.collectReducedColumns(returnCF, reduced, gcBefore, timestamp);
    }

    private static MergeIterator.Reducer<Cell, Cell> getReducer(final Comparator<Cell> comparator)
    {
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        return new MergeIterator.Reducer<Cell, Cell>()
        {
            Cell current;

            public void reduce(Cell next)
            {
                assert current == null || comparator.compare(current, next) == 0;
                current = current == null ? next : current.reconcile(next);
            }

            protected Cell getReduced()
            {
                assert current != null;
                Cell toReturn = current;
                current = null;
                return toReturn;
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                return true;
            }
        };
    }

    /**
     * Given an iterator of on disk atom, returns an iterator that filters the tombstone range
     * markers adding them to {@code returnCF} and returns the normal column.
     */
    public static Iterator<Cell> gatherTombstones(final ColumnFamily returnCF, final Iterator<? extends OnDiskAtom> iter)
    {
        return new Iterator<Cell>()
        {
            private Cell next;

            public boolean hasNext()
            {
                if (next != null)
                    return true;

                getNext();
                return next != null;
            }

            public Cell next()
            {
                if (next == null)
                    getNext();

                assert next != null;
                Cell toReturn = next;
                next = null;
                return toReturn;
            }

            private void getNext()
            {
                while (iter.hasNext())
                {
                    OnDiskAtom atom = iter.next();

                    if (atom instanceof Cell)
                    {
                        next = (Cell)atom;
                        break;
                    }
                    else
                    {
                        returnCF.addAtom(atom);
                    }
                }
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public String getColumnFamilyName()
    {
        return cfName;
    }

    /**
     * @return a QueryFilter object to satisfy the given slice criteria:
     * @param key the row to slice
     * @param cfName column family to query
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     * @param limit maximum number of non-deleted columns to return
     * @param timestamp time to use for determining expiring columns' state
     */
    public static QueryFilter getSliceFilter(DecoratedKey key,
                                             String cfName,
                                             Composite start,
                                             Composite finish,
                                             boolean reversed,
                                             int limit,
                                             long timestamp)
    {
        return new QueryFilter(key, cfName, new SliceQueryFilter(start, finish, reversed, limit), timestamp);
    }

    /**
     * return a QueryFilter object that includes every column in the row.
     * This is dangerous on large rows; avoid except for test code.
     */
    public static QueryFilter getIdentityFilter(DecoratedKey key, String cfName, long timestamp)
    {
        return new QueryFilter(key, cfName, new IdentityQueryFilter(), timestamp);
    }

    /**
     * @return a QueryFilter object that will return columns matching the given names
     * @param key the row to slice
     * @param cfName column family to query
     * @param columns the column names to restrict the results to, sorted in comparator order
     */
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, SortedSet<CellName> columns, long timestamp)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(columns), timestamp);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(key=" + key + ", cfName=" + cfName + (filter == null ? "" : ", filter=" + filter) + ")";
    }

    public boolean shouldInclude(SSTableReader sstable)
    {
        return filter.shouldInclude(sstable);
    }

    public void delete(DeletionInfo target, ColumnFamily source)
    {
        target.add(source.deletionInfo().getTopLevelDeletion());
        // source is the CF currently in the memtable, and it can be large compared to what the filter selects,
        // so only consider those range tombstones that the filter do select.
        for (Iterator<RangeTombstone> iter = filter.getRangeTombstoneIterator(source); iter.hasNext(); )
            target.add(iter.next(), source.getComparator());
    }
}
