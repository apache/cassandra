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

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
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

    public CompositesSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    @Override
    public List<Row> search(List<IndexExpression> clause, AbstractBounds<RowPosition> range, int maxResults, IDiskAtomFilter dataFilter, boolean countCQL3Rows)
    {
        assert clause != null && !clause.isEmpty();
        ExtendedFilter filter = ExtendedFilter.create(baseCfs, dataFilter, clause, maxResults, countCQL3Rows, false);
        return baseCfs.filter(getIndexedIterator(range, filter), filter);
    }

    private ByteBuffer makePrefix(CompositesIndex index, ByteBuffer key, ExtendedFilter filter, boolean isStart)
    {
        if (key.remaining() == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        ColumnNameBuilder builder;
        if (filter.originalFilter() instanceof SliceQueryFilter)
        {
            SliceQueryFilter originalFilter = (SliceQueryFilter)filter.originalFilter();
            builder = index.makeIndexColumnNameBuilder(key, isStart ? originalFilter.start() : originalFilter.finish());
        }
        else
        {
            builder = index.getIndexComparator().builder().add(key);
        }
        return isStart ? builder.build() : builder.buildAsEndOfRange();
    }

    public ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final AbstractBounds<RowPosition> range, final ExtendedFilter filter)
    {
        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final CompositesIndex index = (CompositesIndex)indexManager.getIndexForColumn(primary.column_name);
        assert index != null;
        final DecoratedKey indexKey = index.getIndexKeyFor(primary.value);

        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}",
                         ((AbstractSimplePerColumnSecondaryIndex) index).expressionString(primary));

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the smallest
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        ByteBuffer startKey = range.left instanceof DecoratedKey ? ((DecoratedKey)range.left).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        ByteBuffer endKey = range.right instanceof DecoratedKey ? ((DecoratedKey)range.right).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        final CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        final CompositeType indexComparator = (CompositeType)index.getIndexCfs().getComparator();

        CompositeType.Builder builder = null;
        final ByteBuffer startPrefix = makePrefix(index, startKey, filter, true);
        final ByteBuffer endPrefix = makePrefix(index, endKey, filter, false);

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private ByteBuffer lastSeenPrefix = startPrefix;
            private Deque<Column> indexColumns;
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
                            logger.trace("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                            return makeReturn(currentKey, data);
                        }

                        if (logger.isTraceEnabled())
                            logger.trace("Scanning index {} starting with {}",
                                         ((AbstractSimplePerColumnSecondaryIndex)index).expressionString(primary), indexComparator.getString(startPrefix));

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                             index.getIndexCfs().name,
                                                                             lastSeenPrefix,
                                                                             endPrefix,
                                                                             false,
                                                                             rowsPerQuery);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        if (indexRow == null || indexRow.isEmpty())
                            return makeReturn(currentKey, data);

                        Collection<Column> sortedColumns = indexRow.getSortedColumns();
                        columnsRead = sortedColumns.size();
                        indexColumns = new ArrayDeque<Column>(sortedColumns);
                        Column firstColumn = sortedColumns.iterator().next();

                        // Paging is racy, so it is possible the first column of a page is not the last seen one.
                        if (lastSeenPrefix != startPrefix && lastSeenPrefix.equals(firstColumn.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            indexColumns.poll();
                            logger.trace("Skipping {}", indexComparator.getString(firstColumn.name()));
                        }
                    }

                    while (!indexColumns.isEmpty() && columnsCount <= limit)
                    {
                        Column column = indexColumns.poll();
                        lastSeenPrefix = column.name();
                        if (column.isMarkedForDelete())
                        {
                            logger.trace("skipping {}", column.name());
                            continue;
                        }

                        CompositesIndex.IndexedEntry entry = index.decodeEntry(indexKey, column);
                        DecoratedKey dk = baseCfs.partitioner.decorateKey(entry.indexedKey);

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

                        if (!range.contains(dk))
                        {
                            // Either we're not yet in the range cause the range is start excluding, or we're
                            // past it.
                            if (!range.right.isMinimum(baseCfs.partitioner) && range.right.compareTo(dk) < 0)
                            {
                                logger.trace("Reached end of assigned scan range");
                                return endOfData();
                            }
                            else
                            {
                                logger.debug("Skipping entry {} before assigned scan range", dk.token);
                                continue;
                            }
                        }

                        // Check if this entry cannot be a hit due to the original column filter
                        ByteBuffer start = entry.indexedEntryStart();
                        if (!filter.originalFilter().maySelectPrefix(baseComparator, start))
                            continue;

                        logger.trace("Adding index hit to current row for {}", indexComparator.getString(column.name()));

                        // We always query the whole CQL3 row. In the case where the original filter was a name filter this might be
                        // slightly wasteful, but this probably doesn't matter in practice and it simplify things.
                        SliceQueryFilter dataFilter = new SliceQueryFilter(start,
                                                                           entry.indexedEntryEnd(),
                                                                           false,
                                                                           Integer.MAX_VALUE,
                                                                           baseCfs.metadata.clusteringKeyColumns().size());
                        ColumnFamily newData = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, dataFilter));
                        if (index.isStale(entry, newData))
                        {
                            index.delete(entry);
                            continue;
                        }

                        assert newData != null : "An entry with not data should have been considered stale";

                        if (!filter.isSatisfiedBy(dk.key, newData, entry.indexedEntryNameBuilder))
                            continue;

                        if (data == null)
                            data = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);
                        data.resolve(newData);
                        columnsCount += dataFilter.lastCounted();
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
