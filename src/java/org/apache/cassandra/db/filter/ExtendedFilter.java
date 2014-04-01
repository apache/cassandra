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
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;

/**
 * Extends a column filter (IFilter) to include a number of IndexExpression.
 */
public abstract class ExtendedFilter
{
    private static final Logger logger = LoggerFactory.getLogger(ExtendedFilter.class);

    public final ColumnFamilyStore cfs;
    public final long timestamp;
    public final DataRange dataRange;
    private final int maxResults;
    private final boolean countCQL3Rows;
    private volatile int currentLimit;

    public static ExtendedFilter create(ColumnFamilyStore cfs,
                                        DataRange dataRange,
                                        List<IndexExpression> clause,
                                        int maxResults,
                                        boolean countCQL3Rows,
                                        long timestamp)
    {
        if (clause == null || clause.isEmpty())
            return new EmptyClauseFilter(cfs, dataRange, maxResults, countCQL3Rows, timestamp);

        return new WithClauses(cfs, dataRange, clause, maxResults, countCQL3Rows, timestamp);
    }

    protected ExtendedFilter(ColumnFamilyStore cfs, DataRange dataRange, int maxResults, boolean countCQL3Rows, long timestamp)
    {
        assert cfs != null;
        assert dataRange != null;
        this.cfs = cfs;
        this.dataRange = dataRange;
        this.maxResults = maxResults;
        this.timestamp = timestamp;
        this.countCQL3Rows = countCQL3Rows;
        this.currentLimit = maxResults;
        if (countCQL3Rows)
            dataRange.updateColumnsLimit(maxResults);
    }

    public int maxRows()
    {
        return countCQL3Rows ? Integer.MAX_VALUE : maxResults;
    }

    public int maxColumns()
    {
        return countCQL3Rows ? maxResults : Integer.MAX_VALUE;
    }

    public int currentLimit()
    {
        return currentLimit;
    }

    public IDiskAtomFilter columnFilter(ByteBuffer key)
    {
        return dataRange.columnFilter(key);
    }

    public int lastCounted(ColumnFamily data)
    {
        return dataRange.getLiveCount(data, timestamp);
    }

    public void updateFilter(int currentColumnsCount)
    {
        if (!countCQL3Rows)
            return;

        currentLimit = maxResults - currentColumnsCount;
        // We propagate that limit to the underlying filter so each internal query don't
        // fetch more than we needs it to.
        dataRange.updateColumnsLimit(currentLimit);
    }

    public abstract List<IndexExpression> getClause();

    /**
     * Returns a filter to query the columns from the clause that the initial slice filter may not have caught.
     * @param data the data retrieve by the initial filter
     * @return a filter or null if there can't be any columns we missed with our initial filter (typically if it was a names query, or a slice of the entire row)
     */
    public abstract IDiskAtomFilter getExtraFilter(DecoratedKey key, ColumnFamily data);

    /**
     * @return data pruned down to the columns originally asked for
     */
    public abstract ColumnFamily prune(DecoratedKey key, ColumnFamily data);

    /**
     * @return true if the provided data satisfies all the expressions from
     * the clause of this filter.
     */
    public abstract boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, ColumnNameBuilder builder);

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

    public static class WithClauses extends ExtendedFilter
    {
        private final List<IndexExpression> clause;
        private final IDiskAtomFilter optimizedFilter;

        public WithClauses(ColumnFamilyStore cfs,
                           DataRange range,
                           List<IndexExpression> clause,
                           int maxResults,
                           boolean countCQL3Rows,
                           long timestamp)
        {
            super(cfs, range, maxResults, countCQL3Rows, timestamp);
            assert clause != null;
            this.clause = clause;
            this.optimizedFilter = computeOptimizedFilter();
        }

        /*
         * Potentially optimize the column filter if we have a change to make it catch all clauses
         * right away.
         */
        private IDiskAtomFilter computeOptimizedFilter()
        {
            /*
             * We shouldn't do the "optimization" for composites as the index names are not valid column names 
             * (which the rest of the method assumes). Said optimization is not useful for composites anyway.
             * We also don't want to do for paging ranges as the actual filter depends on the row key (it would
             * probably be possible to make it work but we won't really use it so we don't bother).
             */
            if (cfs.getComparator() instanceof CompositeType || dataRange instanceof DataRange.Paging)
                return null;

            IDiskAtomFilter filter = dataRange.columnFilter(null); // ok since not a paging range
            if (filter instanceof SliceQueryFilter)
            {
                // if we have a high chance of getting all the columns in a single index slice (and it's not too costly), do that.
                // otherwise, the extraFilter (lazily created) will fetch by name the columns referenced by the additional expressions.
                if (cfs.getMaxRowSize() < DatabaseDescriptor.getColumnIndexSize())
                {
                    logger.trace("Expanding slice filter to entire row to cover additional expressions");
                    return new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, ((SliceQueryFilter)filter).reversed, Integer.MAX_VALUE);
                }
            }
            else
            {
                logger.trace("adding columns to original Filter to cover additional expressions");
                assert filter instanceof NamesQueryFilter;
                if (!clause.isEmpty())
                {
                    SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(cfs.getComparator());
                    for (IndexExpression expr : clause)
                    {
                        columns.add(expr.column_name);
                    }
                    columns.addAll(((NamesQueryFilter) filter).columns);
                    return ((NamesQueryFilter) filter).withUpdatedColumns(columns);
                }
            }
            return null;
        }

        @Override
        public IDiskAtomFilter columnFilter(ByteBuffer key)
        {
            return optimizedFilter == null ? dataRange.columnFilter(key) : optimizedFilter;
        }

        public List<IndexExpression> getClause()
        {
            return clause;
        }

        /*
         * We may need an extra query only if the original query wasn't selecting the row entirely.
         * Furthermore, we only need the extra query if we haven't yet got all the expressions from the clause.
         */
        private boolean needsExtraQuery(ByteBuffer rowKey, ColumnFamily data)
        {
            IDiskAtomFilter filter = columnFilter(rowKey);
            if (filter instanceof SliceQueryFilter && DataRange.isFullRowSlice((SliceQueryFilter)filter))
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

        public IDiskAtomFilter getExtraFilter(DecoratedKey rowKey, ColumnFamily data)
        {
            /*
             * This method assumes the IndexExpression names are valid column names, which is not the
             * case with composites. This is ok for now however since:
             * 1) CompositeSearcher doesn't use it.
             * 2) We don't yet allow non-indexed range slice with filters in CQL3 (i.e. this will never be
             * called by CFS.filter() for composites).
             */
            assert !(cfs.getComparator() instanceof CompositeType) : "Sequential scan with filters is not supported (if you just created an index, you "
                                                                     + "need to wait for the creation to be propagated to all nodes before querying it)";

            if (!needsExtraQuery(rowKey.key, data))
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

        public ColumnFamily prune(DecoratedKey rowKey, ColumnFamily data)
        {
            if (optimizedFilter == null)
                return data;

            ColumnFamily pruned = data.cloneMeShallow();
            IDiskAtomFilter filter = dataRange.columnFilter(rowKey.key);
            OnDiskAtomIterator iter = filter.getColumnFamilyIterator(rowKey, data);
            filter.collectReducedColumns(pruned, QueryFilter.gatherTombstones(pruned, iter), cfs.gcBefore(timestamp), timestamp);
            return pruned;
        }

        public boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, ColumnNameBuilder builder)
        {
            // We enforces even the primary clause because reads are not synchronized with writes and it is thus possible to have a race
            // where the index returned a row which doesn't have the primary column when we actually read it
            for (IndexExpression expression : clause)
            {
                ColumnDefinition def = data.metadata().getColumnDefinition(expression.column_name);
                ByteBuffer dataValue = null;
                AbstractType<?> validator = null;
                if (def == null)
                {
                    // This can't happen with CQL3 as this should be rejected upfront. For thrift however,
                    // column name are not predefined. But that means the column name correspond to an internal one.
                    Column column = data.getColumn(expression.column_name);
                    if (column != null)
                    {
                        dataValue = column.value();
                        validator = data.metadata().getDefaultValidator();
                    }
                }
                else
                {
                    dataValue = extractDataValue(def, rowKey.key, data, builder);
                    validator = def.getValidator();
                }

                if (dataValue == null)
                    return false;

                int v = validator.compare(dataValue, expression.value);
                if (!satisfies(v, expression.op))
                    return false;
            }
            return true;
        }

        private ByteBuffer extractDataValue(ColumnDefinition def, ByteBuffer rowKey, ColumnFamily data, ColumnNameBuilder builder)
        {
            switch (def.type)
            {
                case PARTITION_KEY:
                    return def.componentIndex == null
                         ? rowKey
                         : ((CompositeType)data.metadata().getKeyValidator()).split(rowKey)[def.componentIndex];
                case CLUSTERING_KEY:
                    return builder.get(def.componentIndex);
                case REGULAR:
                    ByteBuffer colName = builder == null ? def.name : builder.copy().add(def.name).build();
                    Column column = data.getColumn(colName);
                    return column == null ? null : column.value();
                case COMPACT_VALUE:
                    assert data.getColumnCount() == 1;
                    return data.getSortedColumns().iterator().next().value();
            }
            throw new AssertionError();
        }
    }

    private static class EmptyClauseFilter extends ExtendedFilter
    {
        public EmptyClauseFilter(ColumnFamilyStore cfs, DataRange range, int maxResults, boolean countCQL3Rows, long timestamp)
        {
            super(cfs, range, maxResults, countCQL3Rows, timestamp);
        }

        public List<IndexExpression> getClause()
        {
            return Collections.<IndexExpression>emptyList();
        }

        public IDiskAtomFilter getExtraFilter(DecoratedKey key, ColumnFamily data)
        {
            return null;
        }

        public ColumnFamily prune(DecoratedKey rowKey, ColumnFamily data)
        {
            return data;
        }

        public boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, ColumnNameBuilder builder)
        {
            return true;
        }
    }
}
