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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.HeapAllocator;
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

    public OnDiskAtomIterator getMemtableColumnIterator(Memtable memtable)
    {
        ColumnFamily cf = memtable.getColumnFamily(key);
        if (cf == null)
            return null;
        return getColumnFamilyIterator(cf);
    }

    public OnDiskAtomIterator getColumnFamilyIterator(ColumnFamily cf)
    {
        assert cf != null;
        return filter.getColumnFamilyIterator(key, cf);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable)
    {
        return filter.getSSTableColumnIterator(sstable, key);
    }

    public OnDiskAtomIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return filter.getSSTableColumnIterator(sstable, file, key, indexEntry);
    }

    public void collateOnDiskAtom(final ColumnFamily returnCF, List<? extends Iterator<? extends OnDiskAtom>> toCollate, final int gcBefore)
    {
        collateOnDiskAtom(returnCF, toCollate, filter, gcBefore, timestamp);
    }

    public static void collateOnDiskAtom(final ColumnFamily returnCF, List<? extends Iterator<? extends OnDiskAtom>> toCollate, IDiskAtomFilter filter, int gcBefore, long timestamp)
    {
        List<Iterator<Column>> filteredIterators = new ArrayList<Iterator<Column>>(toCollate.size());
        for (Iterator<? extends OnDiskAtom> iter : toCollate)
            filteredIterators.add(gatherTombstones(returnCF, iter));
        collateColumns(returnCF, filteredIterators, filter, gcBefore, timestamp);
    }

    /**
     * When there is only a single source of atoms, we can skip the collate step
     */
    public void collateOnDiskAtom(ColumnFamily returnCF, Iterator<? extends OnDiskAtom> toCollate, int gcBefore)
    {
        Iterator<Column> columns = gatherTombstones(returnCF, toCollate);
        filter.collectReducedColumns(returnCF, columns, gcBefore, timestamp);
    }

    public void collateColumns(final ColumnFamily returnCF, List<? extends Iterator<Column>> toCollate, int gcBefore)
    {
        collateColumns(returnCF, toCollate, filter, gcBefore, timestamp);
    }

    public static void collateColumns(final ColumnFamily returnCF, List<? extends Iterator<Column>> toCollate, IDiskAtomFilter filter, int gcBefore, long timestamp)
    {
        final Comparator<Column> fcomp = filter.getColumnComparator(returnCF.getComparator());
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        MergeIterator.Reducer<Column, Column> reducer = new MergeIterator.Reducer<Column, Column>()
        {
            Column current;

            public void reduce(Column next)
            {
                assert current == null || fcomp.compare(current, next) == 0;
                current = current == null ? next : current.reconcile(next, HeapAllocator.instance);
            }

            protected Column getReduced()
            {
                assert current != null;
                Column toReturn = current;
                current = null;
                return toReturn;
            }
        };
        Iterator<Column> reduced = MergeIterator.get(toCollate, fcomp, reducer);

        filter.collectReducedColumns(returnCF, reduced, gcBefore, timestamp);
    }

    /**
     * Given an iterator of on disk atom, returns an iterator that filters the tombstone range
     * markers adding them to {@code returnCF} and returns the normal column.
     */
    public static Iterator<Column> gatherTombstones(final ColumnFamily returnCF, final Iterator<? extends OnDiskAtom> iter)
    {
        return new Iterator<Column>()
        {
            private Column next;

            public boolean hasNext()
            {
                if (next != null)
                    return true;

                getNext();
                return next != null;
            }

            public Column next()
            {
                if (next == null)
                    getNext();

                assert next != null;
                Column toReturn = next;
                next = null;
                return toReturn;
            }

            private void getNext()
            {
                while (iter.hasNext())
                {
                    OnDiskAtom atom = iter.next();

                    if (atom instanceof Column)
                    {
                        next = (Column)atom;
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
                                             ByteBuffer start,
                                             ByteBuffer finish,
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
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, SortedSet<ByteBuffer> columns, long timestamp)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(columns), timestamp);
    }

    /**
     * convenience method for creating a name filter matching a single column
     */
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, ByteBuffer column, long timestamp)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(column), timestamp);
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
}
