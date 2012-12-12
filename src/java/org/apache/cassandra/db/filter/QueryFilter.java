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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ISSTableColumnIterator;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class QueryFilter
{
    public final DecoratedKey key;
    public final String cfName;
    public final IDiskAtomFilter filter;

    public QueryFilter(DecoratedKey key, String cfName, IDiskAtomFilter filter)
    {
        this.key = key;
        this.cfName = cfName;
        this.filter = filter;
    }

    public OnDiskAtomIterator getMemtableColumnIterator(Memtable memtable)
    {
        ColumnFamily cf = memtable.getColumnFamily(key);
        if (cf == null)
            return null;
        return getMemtableColumnIterator(cf, key);
    }

    public OnDiskAtomIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey key)
    {
        assert cf != null;
        return filter.getMemtableColumnIterator(cf, key);
    }

    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable)
    {
        return filter.getSSTableColumnIterator(sstable, key);
    }

    public ISSTableColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey key, RowIndexEntry indexEntry)
    {
        return filter.getSSTableColumnIterator(sstable, file, key, indexEntry);
    }

    public void collateOnDiskAtom(final ColumnFamily returnCF, List<? extends CloseableIterator<OnDiskAtom>> toCollate, final int gcBefore)
    {
        List<CloseableIterator<Column>> filteredIterators = new ArrayList<CloseableIterator<Column>>(toCollate.size());
        for (CloseableIterator<OnDiskAtom> iter : toCollate)
            filteredIterators.add(gatherTombstones(returnCF, iter));
        collateColumns(returnCF, filteredIterators, gcBefore);
    }

    public void collateColumns(final ColumnFamily returnCF, List<? extends CloseableIterator<Column>> toCollate, final int gcBefore)
    {
        Comparator<Column> fcomp = filter.getColumnComparator(returnCF.getComparator());
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        MergeIterator.Reducer<Column, Column> reducer = new MergeIterator.Reducer<Column, Column>()
        {
            ColumnFamily curCF = returnCF.cloneMeShallow();

            public void reduce(Column current)
            {
                curCF.addColumn(current);
            }

            protected Column getReduced()
            {
                Column c = curCF.iterator().next();
                curCF.clear();
                return c;
            }
        };
        Iterator<Column> reduced = MergeIterator.get(toCollate, fcomp, reducer);

        filter.collectReducedColumns(returnCF, reduced, gcBefore);
    }

    /**
     * Given an iterator of on disk atom, returns an iterator that filters the tombstone range
     * markers adding them to {@code returnCF} and returns the normal column.
     */
    public static CloseableIterator<Column> gatherTombstones(final ColumnFamily returnCF, final CloseableIterator<OnDiskAtom> iter)
    {
        return new CloseableIterator<Column>()
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

            public void close() throws IOException
            {
                iter.close();
            }
        };
    }

    public String getColumnFamilyName()
    {
        return cfName;
    }

    public static boolean isRelevant(Column column, ColumnFamily container, int gcBefore)
    {
        // the column itself must be not gc-able (it is live, or a still relevant tombstone, or has live subcolumns), (1)
        // and if its container is deleted, the column must be changed more recently than the container tombstone (2)
        // (since otherwise, the only thing repair cares about is the container tombstone)
        long maxChange = column.mostRecentNonGCableChangeAt(gcBefore);
        return (column.getLocalDeletionTime() >= gcBefore || maxChange > column.getMarkedForDeleteAt()) // (1)
               && (!container.deletionInfo().isDeleted(column.name(), maxChange)); // (2)
    }

    /**
     * @return a QueryFilter object to satisfy the given slice criteria:
     * @param key the row to slice
     * @param cfName column family to query
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     * @param limit maximum number of non-deleted columns to return
     */
    public static QueryFilter getSliceFilter(DecoratedKey key, String cfName, ByteBuffer start, ByteBuffer finish, boolean reversed, int limit)
    {
        return new QueryFilter(key, cfName, new SliceQueryFilter(start, finish, reversed, limit));
    }

    /**
     * return a QueryFilter object that includes every column in the row.
     * This is dangerous on large rows; avoid except for test code.
     */
    public static QueryFilter getIdentityFilter(DecoratedKey key, String cfName)
    {
        return new QueryFilter(key, cfName, new IdentityQueryFilter());
    }

    /**
     * @return a QueryFilter object that will return columns matching the given names
     * @param key the row to slice
     * @param cfName column family to query
     * @param columns the column names to restrict the results to, sorted in comparator order
     */
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, SortedSet<ByteBuffer> columns)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(columns));
    }

    /**
     * convenience method for creating a name filter matching a single column
     */
    public static QueryFilter getNamesFilter(DecoratedKey key, String cfName, ByteBuffer column)
    {
        return new QueryFilter(key, cfName, new NamesQueryFilter(column));
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(key=" + key + ", cfName=" + cfName + (filter == null ? "" : ", filter=" + filter) + ")";
    }
}
