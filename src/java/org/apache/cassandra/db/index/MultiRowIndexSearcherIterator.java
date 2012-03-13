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
package org.apache.cassandra.db.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;


/**
 * This class is a general searcher that visits rows returned by nextIndexKey();
 */
public abstract class MultiRowIndexSearcherIterator extends ColumnFamilyStore.AbstractScanIterator
{
    private static final Logger logger = LoggerFactory.getLogger(MultiRowIndexSearcherIterator.class);
    private static final Iterator<IColumn> EMPTY_ITERATOR = Collections.<IColumn>emptyList().iterator();

    /* keys within a row */
    protected final AbstractBounds<RowPosition> range;
    private ByteBuffer lastSeenKey;
    private final ByteBuffer startKey;
    private final ByteBuffer endKey;

    private Iterator<IColumn> currentIndexKeyData = null;
    private final QueryPath path;

    private final IndexExpression expression;
    private final ExtendedFilter filter;
    protected final ColumnFamilyStore indexCfs;
    private final ColumnFamilyStore baseCfs;
    private final boolean rightRangeIsNotMinimum;
    protected DecoratedKey curIndexKey;

    private final int rowsPerQuery;
    private int columnsRead;


    public MultiRowIndexSearcherIterator(IndexExpression expression,
                                         ColumnFamilyStore baseCfs,
                                         ColumnFamilyStore indexCfs,
                                         ExtendedFilter filter,
                                         AbstractBounds<RowPosition> range)
    {
        this.expression = expression;
        this.baseCfs = baseCfs;
        this.range = range;
        this.filter = filter;
        this.indexCfs = indexCfs;

        /*
        * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
        * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the small
        * possible key having a given token. A fix would be to actually store the token along the key in the
        * indexed row.
        */
        startKey = range.left instanceof DecoratedKey ? ((DecoratedKey) range.left).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        endKey = range.right instanceof DecoratedKey ? ((DecoratedKey) range.right).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        int meanColumns = Math.max(indexCfs.getMeanColumns(), 1);

        // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
        rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);
        rightRangeIsNotMinimum = !range.right.isMinimum(baseCfs.partitioner);
        path = new QueryPath(baseCfs.columnFamily);

    }

    /**
     * This function should return indexCfs keys in order they would be scanned by searcher
     * @return next key for scanning of null if endOfData
     */
    protected abstract DecoratedKey nextIndexKey();

    /**
     * resets internal state preparing for next indexCfs row scan.
     */
    protected void resetState()
    {
        curIndexKey = nextIndexKey();
        currentIndexKeyData = EMPTY_ITERATOR;
        lastSeenKey = startKey;
        columnsRead = Integer.MAX_VALUE;
    }

    protected Row computeNext()
    {
        if (currentIndexKeyData == null) // this is first call. Initialize
            resetState();

        Row result = null;
        while (result == null && curIndexKey != null) // curIndexKey would be null when endOfData is reached
        {
            if (!currentIndexKeyData.hasNext()) // we've finished scanning row page
            {
                if (columnsRead < rowsPerQuery) // previously we've read less then we queried. No more pages to read within this row
                {
                    logger.debug("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                    resetState();
                }
                else
                {
                    if (logger.isDebugEnabled())
                        logger.debug(String.format("Scanning index %s starting with %s",
                                                   expressionString(expression), baseCfs.metadata.getKeyValidator().getString(startKey)));

                    QueryFilter indexFilter = QueryFilter.getSliceFilter(curIndexKey,
                                                                         new QueryPath(indexCfs.getColumnFamilyName()),
                                                                         lastSeenKey,
                                                                         endKey,
                                                                         false,
                                                                         rowsPerQuery);

                    ColumnFamily indexRow = indexCfs.getColumnFamily(indexFilter); //get next row page

                    if (indexRow != null)
                    {
                        Collection<IColumn> sortedColumns = indexRow.getSortedColumns();
                        columnsRead = sortedColumns.size();
                        currentIndexKeyData = sortedColumns.iterator();
                        IColumn firstColumn = sortedColumns.iterator().next();

                        // Paging is racy, so it is possible the first column_name of a page is not the last seen one.
                        if (lastSeenKey != startKey && lastSeenKey.equals(firstColumn.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            currentIndexKeyData.next();
                            logger.debug("Skipping {}", baseCfs.metadata.getKeyValidator().getString(firstColumn.name()));
                        }
                        else if (range instanceof Range && currentIndexKeyData.hasNext() && firstColumn.name().equals(startKey))
                        {
                            // skip key excluded by range
                            currentIndexKeyData.next();
                            logger.debug("Skipping first key as range excludes it {}", baseCfs.metadata.getKeyValidator().getString(firstColumn.name()));
                        }
                    }
                    else // page is empty, nothing to scan within this row
                    {
                        columnsRead = 0;
                        currentIndexKeyData = EMPTY_ITERATOR;
                    }
                }
            }


            while (result == null && currentIndexKeyData.hasNext()) // rolling through columns in page
            {
                IColumn column = currentIndexKeyData.next();
                lastSeenKey = column.name();

                if (column.isMarkedForDelete())
                {
                    logger.debug("Skipping {}", column);
                    continue;
                }

                DecoratedKey dk = baseCfs.partitioner.decorateKey(lastSeenKey);

                if (rightRangeIsNotMinimum && range.right.compareTo(dk) < 0) // rightRangeIsNotMinimum is required to serve ring cycles
                {
                    logger.debug("Reached end of assigned scan range");
                    resetState();
                }
                else if (range.contains(dk))
                {
                    logger.debug("Returning index hit for {}", dk);
                    ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, path, filter.initialFilter()));

                    // While the column family we'll get in the end should contains the primary clause column_name,
                    // the initialFilter may not have found it and can thus be null
                    if (data == null)
                        data = ColumnFamily.create(baseCfs.metadata);

                    result = new Row(dk, data);
                }
                else
                {
                    logger.debug("Skipping entry {} outside of assigned scan range", dk.token);
                }
            }
        }

        return result == null ? endOfData() : result;
    }

    private String expressionString(IndexExpression expr)
    {
        return String.format("'%s.%s %s %s'",
                             baseCfs.columnFamily,
                             baseCfs.getComparator().getString(expr.column_name),
                             expr.op,
                             baseCfs.metadata.getColumn_metadata().get(expr.column_name).getValidator().getString(expr.value));
    }

    public void close() throws IOException
    {}
}
