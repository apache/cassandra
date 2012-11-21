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
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Extends a column filter (IFilter) to include a number of IndexExpression.
 */
public abstract class ExtendedFilter
{
    private static final Logger logger = LoggerFactory.getLogger(ExtendedFilter.class);

    public final ColumnFamilyStore cfs;
    protected final IDiskAtomFilter originalFilter;
    private final int maxResults;
    private final boolean countCQL3Rows;
    private final boolean isPaging;

    public static ExtendedFilter create(ColumnFamilyStore cfs, IDiskAtomFilter filter, List<IndexExpression> clause, int maxResults, boolean countCQL3Rows, boolean isPaging)
    {
        if (clause == null || clause.isEmpty())
        {
            return new EmptyClauseFilter(cfs, filter, maxResults, countCQL3Rows, isPaging);
        }
        else
        {
            if (isPaging)
                throw new IllegalArgumentException("Cross-row paging is not supported along with index clauses");
            return cfs.getComparator() instanceof CompositeType
                 ? new FilterWithCompositeClauses(cfs, filter, clause, maxResults, countCQL3Rows)
                 : new FilterWithClauses(cfs, filter, clause, maxResults, countCQL3Rows);
        }
    }

    protected ExtendedFilter(ColumnFamilyStore cfs, IDiskAtomFilter filter, int maxResults, boolean countCQL3Rows, boolean isPaging)
    {
        assert cfs != null;
        assert filter != null;
        this.cfs = cfs;
        this.originalFilter = filter;
        this.maxResults = maxResults;
        this.countCQL3Rows = countCQL3Rows;
        this.isPaging = isPaging;
        if (countCQL3Rows)
            originalFilter.updateColumnsLimit(maxResults);
        if (isPaging && (!(originalFilter instanceof SliceQueryFilter) || ((SliceQueryFilter)originalFilter).finish().remaining() != 0))
            throw new IllegalArgumentException("Cross-row paging is only supported for SliceQueryFilter having an empty finish column");
    }

    public int maxRows()
    {
        return countCQL3Rows ? Integer.MAX_VALUE : maxResults;
    }

    public int maxColumns()
    {
        return countCQL3Rows ? maxResults : Integer.MAX_VALUE;
    }

    /**
     * Update the filter if necessary given the number of column already
     * fetched.
     */
    public void updateFilter(int currentColumnsCount)
    {
        // As soon as we'd done our first call, we want to reset the start column if we're paging
        if (isPaging)
            ((SliceQueryFilter)initialFilter()).setStart(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        if (!countCQL3Rows)
            return;

        int remaining = maxResults - currentColumnsCount;
        initialFilter().updateColumnsLimit(remaining);
    }

    public int lastCounted(ColumnFamily data)
    {
        if (initialFilter() instanceof SliceQueryFilter)
            return ((SliceQueryFilter)initialFilter()).lastCounted();
        else
            return initialFilter().getLiveCount(data);
    }

    /** The initial filter we'll do our first slice with (either the original or a superset of it) */
    public abstract IDiskAtomFilter initialFilter();

    public IDiskAtomFilter originalFilter()
    {
        return originalFilter;
    }

    public abstract List<IndexExpression> getClause();

    /**
     * Returns a filter to query the columns from the clause that the initial slice filter may not have caught.
     * @param data the data retrieve by the initial filter
     * @return a filter or null if there can't be any columns we missed with our initial filter (typically if it was a names query, or a slice of the entire row)
     */
    public abstract IDiskAtomFilter getExtraFilter(ColumnFamily data);

    /**
     * @return data pruned down to the columns originally asked for
     */
    public abstract ColumnFamily prune(ColumnFamily data);

    /**
     * @return true if the provided data satisfies all the expressions from
     * the clause of this filter.
     */
    public abstract boolean isSatisfiedBy(ColumnFamily data, ColumnNameBuilder builder);

    public static boolean satisfies(int comparison, IndexOperator op)
    {
        switch (op)
        {
            case EQ:
                return comparison == 0;
            case GTE:
                return comparison >= 0;
            case GT:
                return comparison > 0;
            case LTE:
                return comparison <= 0;
            case LT:
                return comparison < 0;
            default:
                throw new IllegalStateException();
        }
    }

    private static class FilterWithClauses extends ExtendedFilter
    {
        protected final List<IndexExpression> clause;
        protected final IDiskAtomFilter initialFilter;

        public FilterWithClauses(ColumnFamilyStore cfs, IDiskAtomFilter filter, List<IndexExpression> clause, int maxResults, boolean countCQL3Rows)
        {
            super(cfs, filter, maxResults, countCQL3Rows, false);
            assert clause != null;
            this.clause = clause;
            this.initialFilter = computeInitialFilter();
        }

        /** Sets up the initial filter. */
        protected IDiskAtomFilter computeInitialFilter()
        {
            if (originalFilter instanceof SliceQueryFilter)
            {
                // if we have a high chance of getting all the columns in a single index slice (and it's not too costly), do that.
                // otherwise, the extraFilter (lazily created) will fetch by name the columns referenced by the additional expressions.
                if (cfs.getMaxRowSize() < DatabaseDescriptor.getColumnIndexSize())
                {
                    logger.trace("Expanding slice filter to entire row to cover additional expressions");
                    return new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                ((SliceQueryFilter) originalFilter).reversed,
                                                Integer.MAX_VALUE);
                }
            }
            else
            {
                logger.trace("adding columns to original Filter to cover additional expressions");
                assert originalFilter instanceof NamesQueryFilter;
                if (!clause.isEmpty())
                {
                    SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
                    for (IndexExpression expr : clause)
                    {
                        columns.add(expr.column_name);
                    }
                    columns.addAll(((NamesQueryFilter) originalFilter).columns);
                    return ((NamesQueryFilter)originalFilter).withUpdatedColumns(columns);
                }
            }
            return originalFilter;
        }

        public IDiskAtomFilter initialFilter()
        {
            return initialFilter;
        }

        public List<IndexExpression> getClause()
        {
            return clause;
        }

        /*
         * We may need an extra query only if the original was a slice query (and thus may have miss the expression for the clause).
         * Even then, there is no point in doing an extra query if the original filter grabbed the whole row.
         * Lastly, we only need the extra query if we haven't yet got all the expressions from the clause.
         */
        private boolean needsExtraQuery(ColumnFamily data)
        {
            if (!(originalFilter instanceof SliceQueryFilter))
                return false;

            SliceQueryFilter filter = (SliceQueryFilter)originalFilter;
            // Check if we've fetch the whole row
            if (filter.slices.length == 1
             && filter.start().equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
             && filter.finish().equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
             && filter.count == Integer.MAX_VALUE)
                return false;

            for (IndexExpression expr : clause)
            {
                if (data.getColumn(expr.column_name) == null)
                {
                    logger.debug("adding extraFilter to cover additional expressions");
                    return true;
                }
            }
            return false;
        }

        public IDiskAtomFilter getExtraFilter(ColumnFamily data)
        {
            if (!needsExtraQuery(data))
                return null;

            // Note: for counters we must be careful to not add a column that was already there (to avoid overcount). That is
            // why we do the dance of avoiding to query any column we already have (it's also more efficient anyway)
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
            for (IndexExpression expr : clause)
            {
                if (data.getColumn(expr.column_name) == null)
                    columns.add(expr.column_name);
            }
            assert !columns.isEmpty();
            return new NamesQueryFilter(columns);
        }

        public ColumnFamily prune(ColumnFamily data)
        {
            if (initialFilter == originalFilter)
                return data;
            ColumnFamily pruned = data.cloneMeShallow();
            OnDiskAtomIterator iter = originalFilter.getMemtableColumnIterator(data, null);
            originalFilter.collectReducedColumns(pruned, QueryFilter.gatherTombstones(pruned, iter), cfs.gcBefore());
            return pruned;
        }

        public boolean isSatisfiedBy(ColumnFamily data, ColumnNameBuilder builder)
        {
            // We enforces even the primary clause because reads are not synchronized with writes and it is thus possible to have a race
            // where the index returned a row which doesn't have the primary column when we actually read it
            for (IndexExpression expression : clause)
            {
                // check column data vs expression
                ByteBuffer colName = builder == null ? expression.column_name : builder.copy().add(expression.column_name).build();
                IColumn column = data.getColumn(colName);
                if (column == null)
                    return false;
                int v = data.metadata().getValueValidator(expression.column_name).compare(column.value(), expression.value);
                if (!satisfies(v, expression.op))
                    return false;
            }
            return true;
        }
    }

    private static class FilterWithCompositeClauses extends FilterWithClauses
    {
        public FilterWithCompositeClauses(ColumnFamilyStore cfs, IDiskAtomFilter filter, List<IndexExpression> clause, int maxResults, boolean countCQL3Rows)
        {
            super(cfs, filter, clause, maxResults, countCQL3Rows);
        }

        /*
         * For composites, the index name is not a valid column name (it's only
         * one of the component), which means we should not do the
         * NamesQueryFilter part of FilterWithClauses in particular.
         * Besides, CompositesSearcher doesn't really use the initial filter
         * expect to know the limit set by the user, so create a fake filter
         * with only the count information.
         */
        protected IDiskAtomFilter computeInitialFilter()
        {
            int limit = originalFilter instanceof SliceQueryFilter
                      ? ((SliceQueryFilter)originalFilter).count
                      : Integer.MAX_VALUE;
            return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, limit);
        }
    }

    private static class EmptyClauseFilter extends ExtendedFilter
    {
        public EmptyClauseFilter(ColumnFamilyStore cfs, IDiskAtomFilter filter, int maxResults, boolean countCQL3Rows, boolean isPaging)
        {
            super(cfs, filter, maxResults, countCQL3Rows, isPaging);
        }

        public IDiskAtomFilter initialFilter()
        {
            return originalFilter;
        }

        public List<IndexExpression> getClause()
        {
            throw new UnsupportedOperationException();
        }

        public IDiskAtomFilter getExtraFilter(ColumnFamily data)
        {
            return null;
        }

        public ColumnFamily prune(ColumnFamily data)
        {
            return data;
        }

        public boolean isSatisfiedBy(ColumnFamily data, ColumnNameBuilder builder)
        {
            return true;
        }
    }
}
