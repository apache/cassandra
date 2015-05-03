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
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.*;

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

    /** Returns true if tombstoned partitions should not be included in results or count towards the limit, false otherwise. */
    public boolean ignoreTombstonedPartitions()
    {
        return dataRange.ignoredTombstonedPartitions();
    }

    /**
     * @return true if the provided data satisfies all the expressions from
     * the clause of this filter.
     */
    public abstract boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, Composite prefix, ByteBuffer collectionElement);

    public static boolean satisfies(int comparison, Operator op)
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

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("dataRange", dataRange)
                      .add("maxResults", maxResults)
                      .add("currentLimit", currentLimit)
                      .add("timestamp", timestamp)
                      .add("countCQL3Rows", countCQL3Rows)
                      .toString();
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
            if (cfs.getComparator().isCompound() || dataRange instanceof DataRange.Paging)
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
                    SortedSet<CellName> columns = new TreeSet<CellName>(cfs.getComparator());
                    for (IndexExpression expr : clause)
                        columns.add(cfs.getComparator().cellFromByteBuffer(expr.column));
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
                if (data.getColumn(data.getComparator().cellFromByteBuffer(expr.column)) == null)
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
            assert !(cfs.getComparator().isCompound()) : "Sequential scan with filters is not supported (if you just created an index, you "
                                                         + "need to wait for the creation to be propagated to all nodes before querying it)";

            if (!needsExtraQuery(rowKey.getKey(), data))
                return null;

            // Note: for counters we must be careful to not add a column that was already there (to avoid overcount). That is
            // why we do the dance of avoiding to query any column we already have (it's also more efficient anyway)
            SortedSet<CellName> columns = new TreeSet<CellName>(cfs.getComparator());
            for (IndexExpression expr : clause)
            {
                CellName name = data.getComparator().cellFromByteBuffer(expr.column);
                if (data.getColumn(name) == null)
                    columns.add(name);
            }
            assert !columns.isEmpty();
            return new NamesQueryFilter(columns);
        }

        public ColumnFamily prune(DecoratedKey rowKey, ColumnFamily data)
        {
            if (optimizedFilter == null)
                return data;

            ColumnFamily pruned = data.cloneMeShallow();
            IDiskAtomFilter filter = dataRange.columnFilter(rowKey.getKey());
            Iterator<Cell> iter = filter.getColumnIterator(data);
            filter.collectReducedColumns(pruned, QueryFilter.gatherTombstones(pruned, iter), rowKey, cfs.gcBefore(timestamp), timestamp);
            return pruned;
        }

        public boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, Composite prefix, ByteBuffer collectionElement)
        {
            for (IndexExpression expression : clause)
            {
                ColumnDefinition def = data.metadata().getColumnDefinition(expression.column);
                ByteBuffer dataValue = null;
                AbstractType<?> validator = null;
                if (def == null)
                {
                    // This can't happen with CQL3 as this should be rejected upfront. For thrift however,
                    // cell name are not predefined. But that means the cell name correspond to an internal one.
                    Cell cell = data.getColumn(data.getComparator().cellFromByteBuffer(expression.column));
                    if (cell != null)
                    {
                        dataValue = cell.value();
                        validator = data.metadata().getDefaultValidator();
                    }
                }
                else
                {
                    if (def.type.isCollection() && def.type.isMultiCell())
                    {
                        if (!collectionSatisfies(def, data, prefix, expression, collectionElement))
                            return false;
                        continue;
                    }

                    dataValue = extractDataValue(def, rowKey.getKey(), data, prefix);
                    validator = def.type;
                }

                if (dataValue == null)
                    return false;

                if (expression.operator == Operator.CONTAINS)
                {
                    assert def != null && def.type.isCollection() && !def.type.isMultiCell();
                    CollectionType type = (CollectionType)def.type;
                    switch (type.kind)
                    {
                        case LIST:
                            ListType<?> listType = (ListType)def.type;
                            if (!listType.getSerializer().deserialize(dataValue).contains(listType.getElementsType().getSerializer().deserialize(expression.value)))
                                return false;
                            break;
                        case SET:
                            SetType<?> setType = (SetType)def.type;
                            if (!setType.getSerializer().deserialize(dataValue).contains(setType.getElementsType().getSerializer().deserialize(expression.value)))
                                return false;
                            break;
                        case MAP:
                            MapType<?,?> mapType = (MapType)def.type;
                            if (!mapType.getSerializer().deserialize(dataValue).containsValue(mapType.getValuesType().getSerializer().deserialize(expression.value)))
                                return false;
                            break;
                    }
                }
                else if (expression.operator == Operator.CONTAINS_KEY)
                {
                    assert def != null && def.type.isCollection() && !def.type.isMultiCell() && def.type instanceof MapType;
                    MapType<?,?> mapType = (MapType)def.type;
                    if (mapType.getSerializer().getSerializedValue(dataValue, expression.value, mapType.getKeysType()) == null)
                        return false;
                }
                else
                {
                    int v = validator.compare(dataValue, expression.value);
                    if (!satisfies(v, expression.operator))
                        return false;
                }
            }
            return true;
        }

        private static boolean collectionSatisfies(ColumnDefinition def, ColumnFamily data, Composite prefix, IndexExpression expr, ByteBuffer collectionElement)
        {
            assert def.type.isCollection() && def.type.isMultiCell();
            CollectionType type = (CollectionType)def.type;

            if (expr.isContains())
            {
                // get a slice of the collection cells
                Iterator<Cell> iter = data.iterator(new ColumnSlice[]{ data.getComparator().create(prefix, def).slice() });
                while (iter.hasNext())
                {
                    Cell cell = iter.next();
                    if (type.kind == CollectionType.Kind.SET)
                    {
                        if (type.nameComparator().compare(cell.name().collectionElement(), expr.value) == 0)
                            return true;
                    }
                    else
                    {
                        if (type.valueComparator().compare(cell.value(), expr.value) == 0)
                            return true;
                    }
                }

                return false;
            }

            switch (type.kind)
            {
                case LIST:
                    assert collectionElement != null;
                    return type.valueComparator().compare(data.getColumn(data.getComparator().create(prefix, def, collectionElement)).value(), expr.value) == 0;
                case SET:
                    return data.getColumn(data.getComparator().create(prefix, def, expr.value)) != null;
                case MAP:
                    if (expr.isContainsKey())
                    {
                        return data.getColumn(data.getComparator().create(prefix, def, expr.value)) != null;
                    }

                    assert collectionElement != null;
                    return type.valueComparator().compare(data.getColumn(data.getComparator().create(prefix, def, collectionElement)).value(), expr.value) == 0;
            }
            throw new AssertionError();
        }

        private ByteBuffer extractDataValue(ColumnDefinition def, ByteBuffer rowKey, ColumnFamily data, Composite prefix)
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    return def.isOnAllComponents()
                         ? rowKey
                         : ((CompositeType)data.metadata().getKeyValidator()).split(rowKey)[def.position()];
                case CLUSTERING_COLUMN:
                    return prefix.get(def.position());
                case REGULAR:
                    CellName cname = prefix == null
                                   ? data.getComparator().cellFromByteBuffer(def.name.bytes)
                                   : data.getComparator().create(prefix, def);

                    Cell cell = data.getColumn(cname);
                    return cell == null ? null : cell.value();
                case COMPACT_VALUE:
                    assert data.getColumnCount() == 1;
                    return data.getSortedColumns().iterator().next().value();
            }
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                          .add("dataRange", dataRange)
                          .add("timestamp", timestamp)
                          .add("clause", clause)
                          .toString();
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

        public boolean isSatisfiedBy(DecoratedKey rowKey, ColumnFamily data, Composite prefix, ByteBuffer collectionElement)
        {
            return true;
        }
    }
}
