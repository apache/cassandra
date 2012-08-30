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
package org.apache.cassandra.db.index.composites;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositesSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(CompositesSearcher.class);

    private final int prefixSize;

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns, int prefixSize)
    {
        super(indexManager, columns);
        this.prefixSize = prefixSize;
    }

    private IndexExpression highestSelectivityPredicate(List<IndexExpression> clause)
    {
        IndexExpression best = null;
        int bestMeanCount = Integer.MAX_VALUE;
        for (IndexExpression expression : clause)
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

    public boolean isIndexing(List<IndexExpression> clause)
    {
        return highestSelectivityPredicate(clause) != null;
    }

    @Override
    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IFilter dataFilter, boolean maxIsColumns)
    {
        assert clause != null && !clause.isEmpty();
        ExtendedFilter filter = ExtendedFilter.create(baseCfs, dataFilter, clause, maxResults, maxIsColumns, false);
        return baseCfs.filter(getIndexedIterator(range, filter), filter);
    }

    public ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final AbstractBounds<RowPosition> range, final ExtendedFilter filter)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);
        assert index != null;
        final DecoratedKey indexKey = index.getIndexKeyFor(primary.value);

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the small
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        ByteBuffer startKey = range.left instanceof DecoratedKey ? ((DecoratedKey)range.left).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer endKey = range.right instanceof DecoratedKey ? ((DecoratedKey)range.right).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        final CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        final CompositeType indexComparator = (CompositeType)index.getIndexCfs().getComparator();

        CompositeType.Builder builder = null;
        if (startKey.remaining() > 0)
        {
            builder = indexComparator.builder().add(startKey);
            // For names filter, we have no choice but to query from the beginning of the key. This can be highly inefficient however.
            if (filter.originalFilter() instanceof SliceQueryFilter)
            {
                ByteBuffer[] components = baseComparator.split(((SliceQueryFilter)filter.originalFilter()).start());
                for (int i = 0; i < Math.min(prefixSize, components.length); ++i)
                    builder.add(components[i]);
            }
        }
        final ByteBuffer startPrefix = startKey.remaining() == 0 ? ByteBufferUtil.EMPTY_BYTE_BUFFER : builder.build();

        if (endKey.remaining() > 0)
        {
            builder = indexComparator.builder().add(endKey);
            // For names filter, we have no choice but to query until the end of the key. This can be highly inefficient however.
            if (filter.originalFilter() instanceof SliceQueryFilter)
            {
                ByteBuffer[] components = baseComparator.split(((SliceQueryFilter)filter.originalFilter()).finish());
                for (int i = 0; i < Math.min(prefixSize, components.length); ++i)
                    builder.add(components[i]);
            }
        }
        final ByteBuffer endPrefix = endKey.remaining() == 0 ? ByteBufferUtil.EMPTY_BYTE_BUFFER : builder.buildAsEndOfRange();

        // We will need to filter clustering keys based on the user filter. If
        // it is a names filter, we are really interested on the clustering
        // part, not the actual column name (NOTE: this is a hack that assumes CQL3).
        final SliceQueryFilter originalFilter;
        if (filter.originalFilter() instanceof SliceQueryFilter)
        {
            originalFilter = (SliceQueryFilter)filter.originalFilter();
        }
        else
        {
            ByteBuffer first = ((NamesQueryFilter)filter.originalFilter()).columns.iterator().next();
            ByteBuffer[] components = baseComparator.split(first);
            builder = baseComparator.builder();
            // All all except the last component, since it's the column name
            for (int i = 0; i < components.length - 1; i++)
                builder.add(components[i]);
            originalFilter = new SliceQueryFilter(builder.copy().build(), builder.copy().buildAsEndOfRange(), false, Integer.MAX_VALUE);
        }

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private ByteBuffer lastSeenPrefix = startPrefix;
            private Deque<IColumn> indexColumns;
            private final QueryPath path = new QueryPath(baseCfs.columnFamily);
            private int columnsRead = Integer.MAX_VALUE;

            private final int meanColumns = Math.max(index.getIndexCfs().getMeanColumns(), 1);
            // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
            private final int rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);

            public boolean needsFiltering()
            {
                return false;
            }

            private Row makeReturn(DecoratedKey key, ColumnFamily data)
            {
                if (data == null)
                {
                    return endOfData();
                }
                else
                {
                    assert key != null;
                    return new Row(key, data);
                }
            }

            protected Row computeNext()
            {
                /*
                 * Our internal index code is wired toward internal rows. So we need to acumulate all results for a given
                 * row before returning from this method. Which unfortunately means that this method has to do what
                 * CFS.filter does for KeysIndex.
                 */
                DecoratedKey currentKey = null;
                ColumnFamily data = null;
                int columnsCount = 0;
                int limit = ((SliceQueryFilter)filter.initialFilter()).count;

                while (true)
                {
                    // Did we got more columns that needed to respect the user limit?
                    // (but we still need to return was fetch already)
                    if (columnsCount > limit)
                        return makeReturn(currentKey, data);

                    if (indexColumns == null || indexColumns.isEmpty())
                    {
                        if (columnsRead < rowsPerQuery)
                        {
                            logger.debug("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                            return makeReturn(currentKey, data);
                        }

                        // TODO: broken because we need to extract the component comparator rather than the whole name comparator
                        // if (logger.isDebugEnabled())
                        //     logger.debug("Scanning index {} starting with {}",
                        //                  expressionString(primary), indexComparator.getString(startPrefix));

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                             new QueryPath(index.getIndexCfs().getColumnFamilyName()),
                                                                             lastSeenPrefix,
                                                                             endPrefix,
                                                                             false,
                                                                             rowsPerQuery);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        if (indexRow == null)
                            return makeReturn(currentKey, data);

                        Collection<IColumn> sortedColumns = indexRow.getSortedColumns();
                        columnsRead = sortedColumns.size();
                        indexColumns = new ArrayDeque(sortedColumns);
                        IColumn firstColumn = sortedColumns.iterator().next();

                        // Paging is racy, so it is possible the first column of a page is not the last seen one.
                        if (lastSeenPrefix != startPrefix && lastSeenPrefix.equals(firstColumn.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            indexColumns.poll();
                            logger.debug("Skipping {}", indexComparator.getString(firstColumn.name()));
                        }
                        else if (range instanceof Range && !indexColumns.isEmpty() && firstColumn.name().equals(startPrefix))
                        {
                            // skip key excluded by range
                            indexColumns.poll();
                            logger.debug("Skipping first key as range excludes it");
                        }
                    }

                    while (!indexColumns.isEmpty() && columnsCount <= limit)
                    {
                        IColumn column = indexColumns.poll();
                        lastSeenPrefix = column.name();
                        if (column.isMarkedForDelete())
                        {
                            logger.debug("skipping {}", column.name());
                            continue;
                        }

                        ByteBuffer[] components = indexComparator.split(lastSeenPrefix);
                        DecoratedKey dk = baseCfs.partitioner.decorateKey(components[0]);

                        // Are we done for this row?
                        if (currentKey == null)
                        {
                            currentKey = dk;
                        }
                        else if (!currentKey.equals(dk))
                        {
                            DecoratedKey previousKey = currentKey;
                            currentKey = dk;

                            // We're done with the previous row, return it if it had data, continue otherwise
                            indexColumns.addFirst(column);
                            if (data == null)
                                continue;
                            else
                                return makeReturn(previousKey, data);
                        }

                        if (!range.right.isMinimum(baseCfs.partitioner) && range.right.compareTo(dk) < 0)
                        {
                            logger.debug("Reached end of assigned scan range");
                            return endOfData();
                        }
                        if (!range.contains(dk))
                        {
                            logger.debug("Skipping entry {} outside of assigned scan range", dk.token);
                            continue;
                        }

                        logger.debug("Adding index hit to current row for {}", indexComparator.getString(lastSeenPrefix));
                        // For sparse composites, we're good querying the whole logical row
                        // Obviously if this index is used for other usage, that might be inefficient
                        CompositeType.Builder builder = baseComparator.builder();
                        for (int i = 0; i < prefixSize; i++)
                            builder.add(components[i + 1]);

                        // Does this "row" match the user original filter
                        ByteBuffer start = builder.copy().build();
                        if (!originalFilter.includes(baseComparator, start))
                            continue;

                        SliceQueryFilter dataFilter = new SliceQueryFilter(start, builder.copy().buildAsEndOfRange(), false, Integer.MAX_VALUE);
                        ColumnFamily newData = baseCfs.getColumnFamily(new QueryFilter(dk, path, dataFilter));
                        if (newData != null)
                        {
                            ByteBuffer baseColumnName = builder.copy().add(primary.column_name).build();
                            ByteBuffer indexedValue = indexKey.key;

                            if (isIndexValueStale(newData, baseColumnName, indexedValue))
                            {
                                // delete the index entry w/ its own timestamp
                                IColumn dummyColumn = new Column(baseColumnName, indexedValue, column.timestamp());
                                ((PerColumnSecondaryIndex) index).delete(dk.key, dummyColumn);
                                continue;
                            }

                            if (!filter.isSatisfiedBy(newData, builder))
                                continue;

                            if (data == null)
                                data = ColumnFamily.create(baseCfs.metadata);
                            data.resolve(newData);
                            columnsCount += newData.getLiveColumnCount();
                        }
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
