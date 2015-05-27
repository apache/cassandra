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
package org.apache.cassandra.service.pager;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnCounter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    private final ConsistencyLevel consistencyLevel;
    private final boolean localQuery;

    protected final CFMetaData cfm;
    protected final IDiskAtomFilter columnFilter;
    private final long timestamp;

    private int remaining;
    private boolean exhausted;
    private boolean shouldFetchExtraRow;

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 int toFetch,
                                 boolean localQuery,
                                 String keyspace,
                                 String columnFamily,
                                 IDiskAtomFilter columnFilter,
                                 long timestamp)
    {
        this(consistencyLevel, toFetch, localQuery, Schema.instance.getCFMetaData(keyspace, columnFamily), columnFilter, timestamp);
    }

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 int toFetch,
                                 boolean localQuery,
                                 CFMetaData cfm,
                                 IDiskAtomFilter columnFilter,
                                 long timestamp)
    {
        this.consistencyLevel = consistencyLevel;
        this.localQuery = localQuery;

        this.cfm = cfm;
        this.columnFilter = columnFilter;
        this.timestamp = timestamp;

        this.remaining = toFetch;
    }


    public List<Row> fetchPage(int pageSize) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return Collections.emptyList();

        int currentPageSize = nextPageSize(pageSize);
        List<Row> rows = filterEmpty(queryNextPage(currentPageSize, consistencyLevel, localQuery));

        if (rows.isEmpty())
        {
            logger.debug("Got empty set of rows, considering pager exhausted");
            exhausted = true;
            return Collections.emptyList();
        }

        int liveCount = getPageLiveCount(rows);
        logger.debug("Fetched {} live rows", liveCount);

        // Because SP.getRangeSlice doesn't trim the result (see SP.trim()), liveCount may be greater than what asked
        // (currentPageSize). This would throw off the paging logic so we trim the excess. It's not extremely efficient
        // but most of the time there should be nothing or very little to trim.
        if (liveCount > currentPageSize)
        {
            rows = discardLast(rows, liveCount - currentPageSize);
            liveCount = currentPageSize;
        }

        remaining -= liveCount;

        // If we've got less than requested, there is no more query to do (but
        // we still need to return the current page)
        if (liveCount < currentPageSize)
        {
            logger.debug("Got result ({}) smaller than page size ({}), considering pager exhausted", liveCount, currentPageSize);
            exhausted = true;
        }

        // If it's not the first query and the first column is the last one returned (likely
        // but not certain since paging can race with deletes/expiration), then remove the
        // first column.
        if (containsPreviousLast(rows.get(0)))
        {
            rows = discardFirst(rows);
            remaining++;
        }
        // Otherwise, if 'shouldFetchExtraRow' was set, we queried for one more than the page size,
        // so if the page is full, trim the last entry
        else if (shouldFetchExtraRow && !exhausted)
        {
            // We've asked for one more than necessary
            rows = discardLast(rows);
            remaining++;
        }

        logger.debug("Remaining rows to page: {}", remaining);

        if (!isExhausted())
            shouldFetchExtraRow = recordLast(rows.get(rows.size() - 1));

        return rows;
    }

    private List<Row> filterEmpty(List<Row> result)
    {
        for (Row row : result)
        {
            if (row.cf == null || !row.cf.hasColumns())
            {
                List<Row> newResult = new ArrayList<Row>(result.size() - 1);
                for (Row row2 : result)
                {
                    if (row2.cf == null || !row2.cf.hasColumns())
                        continue;

                    newResult.add(row2);
                }
                return newResult;
            }
        }
        return result;
    }

    protected void restoreState(int remaining, boolean shouldFetchExtraRow)
    {
        this.remaining = remaining;
        this.shouldFetchExtraRow = shouldFetchExtraRow;
    }

    public boolean isExhausted()
    {
        return exhausted || remaining == 0;
    }

    public int maxRemaining()
    {
        return remaining;
    }

    public long timestamp()
    {
        return timestamp;
    }

    private int nextPageSize(int pageSize)
    {
        return Math.min(remaining, pageSize) + (shouldFetchExtraRow ? 1 : 0);
    }

    public ColumnCounter columnCounter()
    {
        return columnFilter.columnCounter(cfm.comparator, timestamp);
    }

    protected abstract List<Row> queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery) throws RequestValidationException, RequestExecutionException;

    /**
     * Checks to see if the first row of a new page contains the last row from the previous page.
     * @param first the first row of the new page
     * @return true if <code>first</code> contains the last from from the previous page and it is live, false otherwise
     */
    protected abstract boolean containsPreviousLast(Row first);

    /**
     * Saves the paging state by recording the last seen partition key and cell name (where applicable).
     * @param last the last row in the current page
     * @return true if an extra row should be fetched in the next page,false otherwise
     */
    protected abstract boolean recordLast(Row last);

    protected abstract boolean isReversed();

    private List<Row> discardFirst(List<Row> rows)
    {
        return discardFirst(rows, 1);
    }

    @VisibleForTesting
    List<Row> discardFirst(List<Row> rows, int toDiscard)
    {
        if (toDiscard == 0 || rows.isEmpty())
            return rows;

        int i = 0;
        DecoratedKey firstKey = null;
        ColumnFamily firstCf = null;
        while (toDiscard > 0 && i < rows.size())
        {
            Row first = rows.get(i++);
            firstKey = first.key;
            firstCf = first.cf.cloneMeShallow(isReversed());
            toDiscard -= isReversed()
                       ? discardLast(first.cf, toDiscard, firstCf)
                       : discardFirst(first.cf, toDiscard, firstCf);
        }

        // If there is less live data than to discard, all is discarded
        if (toDiscard > 0)
            return Collections.<Row>emptyList();

        // i is the index of the first row that we are sure to keep. On top of that,
        // we also keep firstCf is it hasn't been fully emptied by the last iteration above.
        int count = firstCf.getColumnCount();
        int newSize = rows.size() - (count == 0 ? i : i - 1);
        List<Row> newRows = new ArrayList<Row>(newSize);
        if (count != 0)
            newRows.add(new Row(firstKey, firstCf));
        newRows.addAll(rows.subList(i, rows.size()));

        return newRows;
    }

    private List<Row> discardLast(List<Row> rows)
    {
        return discardLast(rows, 1);
    }

    @VisibleForTesting
    List<Row> discardLast(List<Row> rows, int toDiscard)
    {
        if (toDiscard == 0 || rows.isEmpty())
            return rows;

        int i = rows.size()-1;
        DecoratedKey lastKey = null;
        ColumnFamily lastCf = null;
        while (toDiscard > 0 && i >= 0)
        {
            Row last = rows.get(i--);
            lastKey = last.key;
            lastCf = last.cf.cloneMeShallow(isReversed());
            toDiscard -= isReversed()
                       ? discardFirst(last.cf, toDiscard, lastCf)
                       : discardLast(last.cf, toDiscard, lastCf);
        }

        // If there is less live data than to discard, all is discarded
        if (toDiscard > 0)
            return Collections.<Row>emptyList();

        // i is the index of the last row that we are sure to keep. On top of that,
        // we also keep lastCf is it hasn't been fully emptied by the last iteration above.
        int count = lastCf.getColumnCount();
        int newSize = count == 0 ? i+1 : i+2;
        List<Row> newRows = new ArrayList<Row>(newSize);
        newRows.addAll(rows.subList(0, i+1));
        if (count != 0)
            newRows.add(new Row(lastKey, lastCf));

        return newRows;
    }

    private int getPageLiveCount(List<Row> page)
    {
        int count = 0;
        for (Row row : page)
            count += columnCounter().countAll(row.cf).live();
        return count;
    }

    private int discardFirst(ColumnFamily cf, int toDiscard, ColumnFamily newCf)
    {
        boolean isReversed = isReversed();
        DeletionInfo.InOrderTester tester = cf.deletionInfo().inOrderTester(isReversed);
        return isReversed
             ? discardTail(cf, toDiscard, newCf, cf.reverseIterator(), tester)
             : discardHead(cf, toDiscard, newCf, cf.iterator(), tester);
    }

    private int discardLast(ColumnFamily cf, int toDiscard, ColumnFamily newCf)
    {
        boolean isReversed = isReversed();
        DeletionInfo.InOrderTester tester = cf.deletionInfo().inOrderTester(isReversed);
        return isReversed
             ? discardHead(cf, toDiscard, newCf, cf.reverseIterator(), tester)
             : discardTail(cf, toDiscard, newCf, cf.iterator(), tester);
    }

    private int discardHead(ColumnFamily cf, int toDiscard, ColumnFamily copy, Iterator<Cell> iter, DeletionInfo.InOrderTester tester)
    {
        ColumnCounter counter = columnCounter();

        List<Cell> staticCells = new ArrayList<>(cfm.staticColumns().size());

        // Discard the first 'toDiscard' live, non-static cells
        while (iter.hasNext())
        {
            Cell c = iter.next();

            // if it's a static column, don't count it and save it to add to the trimmed results
            ColumnDefinition columnDef = cfm.getColumnDefinition(c.name());
            if (columnDef != null && columnDef.kind == ColumnDefinition.Kind.STATIC)
            {
                staticCells.add(c);
                continue;
            }

            counter.count(c, tester);

            // once we've discarded the required amount, add the rest
            if (counter.live() > toDiscard)
            {
                for (Cell staticCell : staticCells)
                    copy.addColumn(staticCell);

                copy.addColumn(c);
                while (iter.hasNext())
                    copy.addColumn(iter.next());
            }
        }
        return Math.min(counter.live(), toDiscard);
    }

    private int discardTail(ColumnFamily cf, int toDiscard, ColumnFamily copy, Iterator<Cell> iter, DeletionInfo.InOrderTester tester)
    {
        // Redoing the counting like that is not extremely efficient.
        // This is called only for reversed slices or in the case of a race between
        // paging and a deletion (pretty unlikely), so this is probably acceptable.
        int liveCount = columnCounter().countAll(cf).live();

        ColumnCounter counter = columnCounter();
        // Discard the last 'toDiscard' live (so stop adding as sound as we're past 'liveCount - toDiscard')
        while (iter.hasNext())
        {
            Cell c = iter.next();
            counter.count(c, tester);
            if (counter.live() > liveCount - toDiscard)
                break;

            copy.addColumn(c);
        }
        return Math.min(liveCount, toDiscard);
    }

    /**
     * Returns the first non-static cell in the ColumnFamily.  This is necessary to avoid recording a static column
     * as the "last" cell seen in a reversed query.  Because we will always query static columns alongside the normal
     * data for a page, they are not a good indicator of where paging should resume.  When we begin the next page, we
     * need to start from the last non-static cell.
     */
    protected Cell firstNonStaticCell(ColumnFamily cf)
    {
        for (Cell cell : cf)
        {
            ColumnDefinition def = cfm.getColumnDefinition(cell.name());
            if (def == null || def.kind != ColumnDefinition.Kind.STATIC)
                return cell;
        }
        return null;
    }

    protected static Cell lastCell(ColumnFamily cf)
    {
        return cf.getReverseSortedColumns().iterator().next();
    }
}
