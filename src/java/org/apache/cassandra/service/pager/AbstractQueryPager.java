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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnCounter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;

abstract class AbstractQueryPager implements QueryPager
{
    private final ConsistencyLevel consistencyLevel;
    private final boolean localQuery;

    protected final CFMetaData cfm;
    protected final IDiskAtomFilter columnFilter;
    private final long timestamp;

    private volatile int remaining;
    private volatile boolean exhausted;
    private volatile boolean lastWasRecorded;

    protected AbstractQueryPager(ConsistencyLevel consistencyLevel,
                                 int toFetch,
                                 boolean localQuery,
                                 String keyspace,
                                 String columnFamily,
                                 IDiskAtomFilter columnFilter,
                                 long timestamp)
    {
        this.consistencyLevel = consistencyLevel;
        this.localQuery = localQuery;

        this.cfm = Schema.instance.getCFMetaData(keyspace, columnFamily);
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
            exhausted = true;
            return Collections.emptyList();
        }

        int liveCount = getPageLiveCount(rows);
        remaining -= liveCount;

        // If we've got less than requested, there is no more query to do (but
        // we still need to return the current page)
        if (liveCount < currentPageSize)
            exhausted = true;

        // If it's not the first query and the first column is the last one returned (likely
        // but not certain since paging can race with deletes/expiration), then remove the
        // first column.
        if (containsPreviousLast(rows.get(0)))
        {
            rows = discardFirst(rows);
            remaining++;
        }
        // Otherwise, if 'lastWasRecorded', we queried for one more than the page size,
        // so if the page was is full, trim the last entry
        else if (lastWasRecorded && !exhausted)
        {
            // We've asked for one more than necessary
            rows = discardLast(rows);
            remaining++;
        }

        if (!isExhausted())
            lastWasRecorded = recordLast(rows.get(rows.size() - 1));

        return rows;
    }

    private List<Row> filterEmpty(List<Row> result)
    {
        for (Row row : result)
        {
            if (row.cf == null || row.cf.getColumnCount() == 0)
            {
                List<Row> newResult = new ArrayList<Row>(result.size() - 1);
                for (Row row2 : result)
                {
                    if (row2.cf == null || row2.cf.getColumnCount() == 0)
                        continue;

                    newResult.add(row2);
                }
                return newResult;
            }
        }
        return result;
    }

    protected void restoreState(int remaining, boolean lastWasRecorded)
    {
        this.remaining = remaining;
        this.lastWasRecorded = lastWasRecorded;
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
        return Math.min(remaining, pageSize) + (lastWasRecorded ? 1 : 0);
    }

    public ColumnCounter columnCounter()
    {
        return columnFilter.columnCounter(cfm.comparator, timestamp);
    }

    protected abstract List<Row> queryNextPage(int pageSize, ConsistencyLevel consistency, boolean localQuery) throws RequestValidationException, RequestExecutionException;
    protected abstract boolean containsPreviousLast(Row first);
    protected abstract boolean recordLast(Row last);

    private List<Row> discardFirst(List<Row> rows)
    {
        Row first = rows.get(0);
        ColumnFamily newCf = discardFirst(first.cf);

        int count = newCf.getColumnCount();
        List<Row> newRows = new ArrayList<Row>(count == 0 ? rows.size() - 1 : rows.size());
        if (count != 0)
            newRows.add(new Row(first.key, newCf));
        newRows.addAll(rows.subList(1, rows.size()));

        return newRows;
    }

    private List<Row> discardLast(List<Row> rows)
    {
        Row last = rows.get(rows.size() - 1);
        ColumnFamily newCf = discardLast(last.cf);

        int count = newCf.getColumnCount();
        List<Row> newRows = new ArrayList<Row>(count == 0 ? rows.size() - 1 : rows.size());
        newRows.addAll(rows.subList(0, rows.size() - 1));
        if (count != 0)
            newRows.add(new Row(last.key, newCf));

        return newRows;
    }

    private int getPageLiveCount(List<Row> page)
    {
        int count = 0;
        for (Row row : page)
            count += columnCounter().countAll(row.cf).live();
        return count;
    }

    private ColumnFamily discardFirst(ColumnFamily cf)
    {
        ColumnFamily copy = cf.cloneMeShallow();
        ColumnCounter counter = columnCounter();

        Iterator<Column> iter = cf.iterator();
        DeletionInfo.InOrderTester tester = cf.inOrderDeletionTester();
        // Discard the first live
        while (iter.hasNext())
        {
            Column c = iter.next();
            counter.count(c, tester);
            if (counter.live() > 1)
            {
                copy.addColumn(c);
                while (iter.hasNext())
                    copy.addColumn(iter.next());
            }
        }
        return copy;
    }

    private ColumnFamily discardLast(ColumnFamily cf)
    {
        ColumnFamily copy = cf.cloneMeShallow();
        // Redoing the counting like that is not extremely efficient, but
        // discardLast is only called in case of a race between paging and
        // a deletion, which is pretty unlikely, so probably not a big deal
        int liveCount = columnCounter().countAll(cf).live();

        ColumnCounter counter = columnCounter();
        DeletionInfo.InOrderTester tester = cf.inOrderDeletionTester();
        // Discard the first live
        for (Column c : cf)
        {
            counter.count(c, tester);
            if (counter.live() < liveCount)
                copy.addColumn(c);
        }
        return copy;
    }

    protected static ByteBuffer firstName(ColumnFamily cf)
    {
        return cf.iterator().next().name();
    }

    protected static ByteBuffer lastName(ColumnFamily cf)
    {
        return cf.getReverseSortedColumns().iterator().next().name();
    }
}
