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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);
    
    public KeysSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }
    
    private IndexExpression highestSelectivityPredicate(IndexClause clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        for (IndexExpression expression : clause.expressions)
        {
            //skip columns belonging to a different index type
            if(!columns.contains(expression.column_name))
                continue;
            
            SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
            if (index == null || (expression.op != IndexOperator.EQ))
                continue;
            int columns = index.getIndexCfs().getMeanColumns();
            if (columns < bestMeanCount)
            {
                best = expression;
                bestMeanCount = columns;
            }
        }
        return best;
    }

    private String expressionString(IndexExpression expr)
    {
        return String.format("'%s.%s %s %s'",
                             baseCfs.columnFamily,
                             baseCfs.getComparator().getString(expr.column_name),
                             expr.op,
                             baseCfs.metadata.getColumn_metadata().get(expr.column_name).getValidator().getString(expr.value));
    }

    private static boolean isIdentityFilter(SliceQueryFilter filter)
    {
        return filter.start.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
            && filter.finish.equals(ByteBufferUtil.EMPTY_BYTE_BUFFER)
            && filter.count == Integer.MAX_VALUE;
    }
    
    @Override
    public List<Row> search(IndexClause clause, AbstractBounds range, IFilter dataFilter)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        IndexExpression primary = highestSelectivityPredicate(clause);
        SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);
        if (logger.isDebugEnabled())
            logger.debug("Primary scan clause is " + baseCfs.getComparator().getString(primary.column_name));
        assert index != null;
        DecoratedKey indexKey = indexManager.getIndexKeyFor(primary.column_name, primary.value);

        // if the slicepredicate doesn't contain all the columns for which we have expressions to evaluate,
        // it needs to be expanded to include those too
        IFilter firstFilter = dataFilter;
        if (dataFilter instanceof SliceQueryFilter)
        {
            // if we have a high chance of getting all the columns in a single index slice, do that.
            // otherwise, we'll create an extraFilter (lazily) to fetch by name the columns referenced by the additional expressions.
            if (baseCfs.getMaxRowSize() < DatabaseDescriptor.getColumnIndexSize())
            {
                logger.debug("Expanding slice filter to entire row to cover additional expressions");
                firstFilter = new SliceQueryFilter(ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        ByteBufferUtil.EMPTY_BYTE_BUFFER,
                        ((SliceQueryFilter) dataFilter).reversed,
                        Integer.MAX_VALUE);
            }
        }
        else
        {
            logger.debug("adding columns to firstFilter to cover additional expressions");
            // just add in columns that are not part of the resultset
            assert dataFilter instanceof NamesQueryFilter;
            SortedSet<ByteBuffer> columns = new TreeSet<ByteBuffer>(baseCfs.getComparator());
            for (IndexExpression expr : clause.expressions)
            {
                columns.add(expr.column_name);
            }
            if (columns.size() > 0)
            {
                columns.addAll(((NamesQueryFilter) dataFilter).columns);
                firstFilter = new NamesQueryFilter(columns);
            }
        }

        List<Row> rows = new ArrayList<Row>();
        ByteBuffer startKey = clause.start_key;
        QueryPath path = new QueryPath(baseCfs.columnFamily);

        // we need to store last data key accessed to avoid duplicate results
        // because in the while loop new iteration we can access the same column if start_key was not set
        ByteBuffer lastDataKey = null;

        // fetch row keys matching the primary expression, fetch the slice predicate for each
        // and filter by remaining expressions.  repeat until finished w/ assigned range or index row is exhausted.
        outer:
        while (true)
        {
            /* we don't have a way to get the key back from the DK -- we just have a token --
             * so, we need to loop after starting with start_key, until we get to keys in the given `range`.
             * But, if the calling StorageProxy is doing a good job estimating data from each range, the range
             * should be pretty close to `start_key`. */
            if (logger.isDebugEnabled())
                logger.debug(String.format("Scanning index %s starting with %s",
                                           expressionString(primary), index.getBaseCfs().metadata.getKeyValidator().getString(startKey)));

            // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
            int count = Math.max(clause.count, 2);
            QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                 new QueryPath(index.getIndexCfs().getColumnFamilyName()),
                                                                 startKey,
                                                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                                 false,
                                                                 count);
            ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
            logger.debug("fetched {}", indexRow);
            if (indexRow == null)
                break;

            ByteBuffer dataKey = null;
            int n = 0;
            for (IColumn column : indexRow.getSortedColumns())
            {
                if (column.isMarkedForDelete())
                {
                    logger.debug("skipping {}",column.name());
                    continue;
                }
                
                dataKey = column.name();
                n++;
                
                if(logger.isDebugEnabled())
                    logger.debug("fetching {}",column.name());

                DecoratedKey dk = baseCfs.partitioner.decorateKey(dataKey);
                if (!range.right.equals(baseCfs.partitioner.getMinimumToken()) && range.right.compareTo(dk.token) < 0)
                    break outer;
                if (!range.contains(dk.token) || dataKey.equals(lastDataKey))
                    continue;

                // get the row columns requested, and additional columns for the expressions if necessary
                ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, path, firstFilter));
                // While we the column family we'll get in the end should contains the primary clause column, the firstFilter may not have found it.
                if (data == null)
                    data = ColumnFamily.create(baseCfs.metadata);
                logger.debug("fetched data row {}", data);
                NamesQueryFilter extraFilter = null;
                if (dataFilter instanceof SliceQueryFilter && !isIdentityFilter((SliceQueryFilter)dataFilter))
                {
                    // we might have gotten the expression columns in with the main data slice, but
                    // we can't know for sure until that slice is done.  So, we'll do the extra query
                    // if we go through and any expression columns are not present.
                    boolean needExtraFilter = false;
                    for (IndexExpression expr : clause.expressions)
                    {
                        if (data.getColumn(expr.column_name) == null)
                        {
                            logger.debug("adding extraFilter to cover additional expressions");
                            // Lazily creating extra filter
                            needExtraFilter = true;
                            break;
                        }
                    }
                    if (needExtraFilter)
                    {
                        // Note: for counters we must be careful to not add a column that was already there (to avoid overcount). That is
                        // why we do the dance of avoiding to query any column we already have (it's also more efficient anyway)
                        extraFilter = getExtraFilter(clause);
                        for (IndexExpression expr : clause.expressions)
                        {
                            if (data.getColumn(expr.column_name) != null)
                                extraFilter.columns.remove(expr.column_name);
                        }
                        assert !extraFilter.columns.isEmpty();
                        ColumnFamily cf = baseCfs.getColumnFamily(new QueryFilter(dk, path, extraFilter));
                        if (cf != null)
                            data.addAll(cf, HeapAllocator.instance);
                    }

                }

                if (SecondaryIndexSearcher.satisfies(data, clause, primary))
                {
                    logger.debug("row {} satisfies all clauses", data);
                    // cut the resultset back to what was requested, if necessary
                    if (firstFilter != dataFilter || extraFilter != null)
                    {
                        ColumnFamily expandedData = data;
                        data = expandedData.cloneMeShallow();
                        IColumnIterator iter = dataFilter.getMemtableColumnIterator(expandedData, dk, baseCfs.getComparator());
                        new QueryFilter(dk, path, dataFilter).collateColumns(data, Collections.singletonList(iter), baseCfs.getComparator(), baseCfs.gcBefore());
                    }

                    rows.add(new Row(dk, data));
                }

                if (rows.size() == clause.count)
                    break outer;
            }
            if (n < clause.count || startKey.equals(dataKey))
                break;

            lastDataKey = startKey = dataKey;
        }

        return rows;
    }

}
