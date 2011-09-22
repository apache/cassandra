package org.apache.cassandra.db.filter;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;

public class QueryFilter
{
    private static final Logger logger = LoggerFactory.getLogger(QueryFilter.class);

    public final DecoratedKey<?> key;
    public final QueryPath path;
    public final IFilter filter;
    private final IFilter superFilter;

    public QueryFilter(DecoratedKey<?> key, QueryPath path, IFilter filter)
    {
        this.key = key;
        this.path = path;
        this.filter = filter;
        superFilter = path.superColumnName == null ? null : new NamesQueryFilter(path.superColumnName);
    }

    public IColumnIterator getMemtableColumnIterator(Memtable memtable, AbstractType comparator)
    {
        ColumnFamily cf = memtable.getColumnFamily(key);
        if (cf == null)
            return null;
        return getMemtableColumnIterator(cf, key, comparator);
    }

    public IColumnIterator getMemtableColumnIterator(ColumnFamily cf, DecoratedKey<?> key, AbstractType comparator)
    {
        assert cf != null;
        if (path.superColumnName == null)
            return filter.getMemtableColumnIterator(cf, key, comparator);
        return superFilter.getMemtableColumnIterator(cf, key, comparator);
    }

    // TODO move gcBefore into a field
    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable)
    {
        if (path.superColumnName == null)
            return filter.getSSTableColumnIterator(sstable, key);
        return superFilter.getSSTableColumnIterator(sstable, key);
    }

    public IColumnIterator getSSTableColumnIterator(SSTableReader sstable, FileDataInput file, DecoratedKey<?> key)
    {
        if (path.superColumnName == null)
            return filter.getSSTableColumnIterator(sstable, file, key);
        return superFilter.getSSTableColumnIterator(sstable, file, key);
    }

    // TODO move gcBefore into a field
    public void collateColumns(final ColumnFamily returnCF, List<? extends CloseableIterator<IColumn>> toCollate, AbstractType comparator, final int gcBefore)
    {
        IFilter topLevelFilter = (superFilter == null ? filter : superFilter);
        Comparator<IColumn> fcomp = topLevelFilter.getColumnComparator(comparator);
        // define a 'reduced' iterator that merges columns w/ the same name, which
        // greatly simplifies computing liveColumns in the presence of tombstones.
        MergeIterator.Reducer<IColumn, IColumn> reducer = new MergeIterator.Reducer<IColumn, IColumn>()
        {
            ColumnFamily curCF = returnCF.cloneMeShallow();

            protected boolean isEqual(IColumn o1, IColumn o2)
            {
                return o1.name().equals(o2.name());
            }

            public void reduce(IColumn current)
            {
                if (curCF.isSuper() && curCF.isEmpty())
                {
                    // If it is the first super column we add, we must clone it since other super column may modify
                    // it otherwise and it could be aliased in a memtable somewhere. We'll also don't have to care about what
                    // consumers make of the result (for instance CFS.getColumnFamily() call removeDeleted() on the
                    // result which removes column; which shouldn't be done on the original super column).
                    assert current instanceof SuperColumn;
                    curCF.addColumn(((SuperColumn) current).cloneMe());
                }
                else
                {
                    curCF.addColumn(current);
                }
            }

            protected IColumn getReduced()
            {
                IColumn c = curCF.iterator().next();
                if (superFilter != null)
                {
                    // filterSuperColumn only looks at immediate parent (the supercolumn) when determining if a subcolumn
                    // is still live, i.e., not shadowed by the parent's tombstone.  so, bump it up temporarily to the tombstone
                    // time of the cf, if that is greater.
                    long deletedAt = c.getMarkedForDeleteAt();
                    if (returnCF.getMarkedForDeleteAt() > deletedAt)
                        ((SuperColumn) c).delete(c.getLocalDeletionTime(), returnCF.getMarkedForDeleteAt());

                    c = filter.filterSuperColumn((SuperColumn) c, gcBefore);
                    ((SuperColumn) c).delete(c.getLocalDeletionTime(), deletedAt); // reset sc tombstone time to what it should be
                }
                curCF.clear();

                return c;
            }
        };
        Iterator<IColumn> reduced = MergeIterator.get(toCollate, fcomp, reducer);

        topLevelFilter.collectReducedColumns(returnCF, reduced, gcBefore);
    }

    public String getColumnFamilyName()
    {
        return path.columnFamilyName;
    }

    public static boolean isRelevant(IColumn column, IColumnContainer container, int gcBefore)
    {
        // the column itself must be not gc-able (it is live, or a still relevant tombstone, or has live subcolumns), (1)
        // and if its container is deleted, the column must be changed more recently than the container tombstone (2)
        // (since otherwise, the only thing repair cares about is the container tombstone)
        long maxChange = column.mostRecentLiveChangeAt();
        return (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore || maxChange > column.getMarkedForDeleteAt()) // (1)
               && (!container.isMarkedForDelete() || maxChange > container.getMarkedForDeleteAt()); // (2)
    }

    /**
     * @return a QueryFilter object to satisfy the given slice criteria:  @param key the row to slice
     * @param path path to the level to slice at (CF or SuperColumn)
     * @param start column to start slice at, inclusive; empty for "the first column"
     * @param finish column to stop slice at, inclusive; empty for "the last column"
     * @param reversed true to start with the largest column (as determined by configured sort order) instead of smallest
     * @param limit maximum number of non-deleted columns to return
     */
    public static QueryFilter getSliceFilter(DecoratedKey<?> key, QueryPath path, ByteBuffer start, ByteBuffer finish, boolean reversed, int limit)
    {
        return new QueryFilter(key, path, new SliceQueryFilter(start, finish, reversed, limit));
    }

    /**
     * return a QueryFilter object that includes every column in the row.
     * This is dangerous on large rows; avoid except for test code.
     */
    public static QueryFilter getIdentityFilter(DecoratedKey<?> key, QueryPath path)
    {
        return new QueryFilter(key, path, new IdentityQueryFilter());
    }

    /**
     * @return a QueryFilter object that will return columns matching the given names
     * @param key the row to slice
     * @param path path to the level to slice at (CF or SuperColumn)
     * @param columns the column names to restrict the results to
     */
    public static QueryFilter getNamesFilter(DecoratedKey<?> key, QueryPath path, SortedSet<ByteBuffer> columns)
    {
        return new QueryFilter(key, path, new NamesQueryFilter(columns));
    }

    public static IFilter getFilter(SlicePredicate predicate, AbstractType comparator)
    {
        if (predicate.column_names != null)
        {
            final SortedSet<ByteBuffer> columnNameSet = new TreeSet<ByteBuffer>(comparator);
            columnNameSet.addAll(predicate.column_names);
            return new NamesQueryFilter(columnNameSet);
        }

        SliceRange range = predicate.slice_range;
        return new SliceQueryFilter(range.start, range.finish, range.reversed, range.count);
    }

    /**
     * convenience method for creating a name filter matching a single column
     */
    public static QueryFilter getNamesFilter(DecoratedKey<?> key, QueryPath path, ByteBuffer column)
    {
        return new QueryFilter(key, path, new NamesQueryFilter(column));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(key=" + key +
               ", path=" + path +
               (filter == null ? "" : ", filter=" + filter) +
               (superFilter == null ? "" : ", superFilter=" + superFilter) +
               ")";
    }
}
