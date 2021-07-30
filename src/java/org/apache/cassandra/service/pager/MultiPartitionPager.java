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

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractIterator;

import java.util.Arrays;
import java.util.StringJoiner;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;

/**
 * Pager over a list of SinglePartitionReadQuery.
 *
 * Note that this is not easy to make efficient. Indeed, we need to page the first query fully before
 * returning results from the next one, but if the result returned by each query is small (compared to pageSize),
 * paging the queries one at a time under-performs compared to parallelizing. On the other, if we parallelize
 * and each query raised pageSize results, we'll end up with queries.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 *
 * For now, we keep it simple (somewhat) and just do one query at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though if we later want to get fancy, we could use the
 * cfs meanPartitionSize to decide if parallelizing some of the query might be worth it while being confident we don't
 * blow out memory.
 */
public class MultiPartitionPager<T extends SinglePartitionReadQuery> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionPager.class);

    private static final SinglePartitionPager[] NO_PAGERS = new SinglePartitionPager[0];

    // a pager per queried partition
    @Nonnull
    private final SinglePartitionPager[] pagers;

    // user limit
    private final DataLimits limit;

    private final int nowInSec;

    // the number of rows left to be returned according to the user limits (those provided in query)
    // when remaining reaches 0, the pager is considered exhausted
    private int remaining;

    // the index of the current single partition pager
    private int current;

    public MultiPartitionPager(SinglePartitionReadQuery.Group<T> group, PagingState state, ProtocolVersion protocolVersion)
    {
        this.limit = group.limits();
        this.nowInSec = group.nowInSec();

        int i = 0;
        // If it's not the beginning (state != null), we need to find where we were and skip previous queries
        // since they are done.
        if (state != null)
            for (; i < group.queries.size(); i++)
                if (group.queries.get(i).partitionKey().getKey().equals(state.partitionKey))
                    break;

        if (i >= group.queries.size())
        {
            pagers = NO_PAGERS;
            return;
        }

        pagers = new SinglePartitionPager[group.queries.size() - i];
        // 'i' is on the first non exhausted pager for the previous page (or the first one)
        T query = group.queries.get(i);
        pagers[0] = query.getPager(state, protocolVersion);

        // Following ones haven't been started yet
        for (int j = i + 1; j < group.queries.size(); j++)
            pagers[j - i] = group.queries.get(j).getPager(null, protocolVersion);

        remaining = state == null ? limit.count() : state.remaining;
    }

    private MultiPartitionPager(SinglePartitionPager[] pagers,
                                DataLimits limit,
                                int nowInSec,
                                int remaining,
                                int current)
    {
        this.pagers = pagers;
        this.limit = limit;
        this.nowInSec = nowInSec;
        this.remaining = remaining;
        this.current = current;
    }

    public QueryPager withUpdatedLimit(DataLimits newLimits)
    {
        SinglePartitionPager[] newPagers = Arrays.copyOf(pagers, pagers.length);
        newPagers[current] = newPagers[current].withUpdatedLimit(newLimits);

        return new MultiPartitionPager<T>(newPagers,
                                          newLimits,
                                          nowInSec,
                                          remaining,
                                          current);
    }

    public PagingState state()
    {
        // Sets current to the first non-exhausted pager
        if (isExhausted())
            return null;

        PagingState state = pagers[current].state();
        return new PagingState(pagers[current].key(), state == null ? null : state.rowMark, remaining, pagers[current].remainingInPartition());
    }

    public boolean isExhausted()
    {
        assert remaining >= 0;
        if (remaining == 0)
            return true;

        while (current < pagers.length)
        {
            if (!pagers[current].isExhausted())
                return false;

            current++;
        }
        return true;
    }

    public ReadExecutionController executionController()
    {
        // Note that for all pagers, the only difference is the partition key to which it applies, so in practice we
        // can use any of the sub-pager ReadOrderGroup group to protect the whole pager
        for (int i = current; i < pagers.length; i++)
        {
            if (pagers[i] != null)
                return pagers[i].executionController();
        }
        throw new AssertionError("Shouldn't be called on an exhausted pager");
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPage(PageSize pageSize, ConsistencyLevel consistency, QueryState queryState, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, consistency, queryState, null, queryStartNanoTime);
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, null, null, executionController, System.nanoTime());
    }

    /**
     * This is an iterator over RowIterators (subsequent partitions). It starts from {@link #pagers} at {@link #current}
     * and make sure that the overall amount of data does not exceed the provided {@link PagersIterator#pageSize}.
     * This means that it can cut the row iteration in the first partition or return multiple partitions and cut the
     * row iterator in n-th partition. It will update the {@link #current} index and {@link #remaining} as it goes.
     */
    private class PagersIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final PageSize pageSize;
        private PartitionIterator partitionIterator;
        private boolean closed;
        private final long queryStartNanoTime;

        // For "normal" queries
        private final ConsistencyLevel consistency;
        private final QueryState queryState;

        // For internal queries
        private final ReadExecutionController executionController;

        private int countedRows;
        private int countedBytes;
        private int counted;

        public PagersIterator(PageSize pageSize, ConsistencyLevel consistency, QueryState queryState, ReadExecutionController executionController, long queryStartNanoTime)
        {
            this.pageSize = pageSize;
            this.consistency = consistency;
            this.queryState = queryState;
            this.executionController = executionController;
            this.queryStartNanoTime = queryStartNanoTime;

            if (logger.isTraceEnabled())
                logger.trace("Fetching a new page - created {}", this);
        }

        protected RowIterator computeNext()
        {
            while (partitionIterator == null || !partitionIterator.hasNext())
            {
                DataLimits.Counter lastPageCounter = null;
                if (partitionIterator != null)
                {
                    // we've just reached the end of partition,
                    // let's close the row iterator and update the global counters
                    partitionIterator.close();

                    lastPageCounter = pagers[current].getLastCounter();
                    countedRows += lastPageCounter.rowsCounted();
                    countedBytes += lastPageCounter.bytesCounted();
                    counted += lastPageCounter.counted();
                    remaining -= lastPageCounter.counted();
                }

                // We are done if:
                // - we have reached the page size,
                // - or in the case of GROUP BY if the current pager is not exhausted - which means that we read all the rows withing the limit before exhausting the pager
                boolean isDone = pageSize.isCompleted(countedRows, PageSize.PageUnit.ROWS)
                                 || pageSize.isCompleted(countedBytes, PageSize.PageUnit.BYTES)
                                 || limit.count() <= counted
                                 || limit.bytes() <= countedBytes
                                 || (partitionIterator != null && limit.isGroupByLimit() && !pagers[current].isExhausted());

                // isExhausted() will sets us on the first non-exhausted pager
                if (isDone || isExhausted())
                {
                    closed = true;
                    return endOfData();
                }

                // we will update the limits for the current pager before using it so that we can be sure we don't fetch
                // more than remaining or more than what was left to be fetched according to the recently set limits
                // (for example in case of groups paging) - that later limit is just the limit which was set minus what
                // we counted so far
                int newCountedLimit = Math.max(0, Math.min(remaining, limit.count() - counted));
                // this works exactly the same way as above - it is required for the limits imposed by Guardrails,
                // whihc are set on the query
                int newBytesLimit = Math.max(0, limit.bytes() - countedBytes);

                DataLimits updatedLimit = pagers[current].limits.withCountedLimit(newCountedLimit).withBytesLimit(newBytesLimit);
                pagers[current] = pagers[current].withUpdatedLimit(updatedLimit);

                PageSize remainingPagePart = pageSize.withDecreasedRows(countedRows)
                                                     .withDecreasedBytes(countedBytes);

                partitionIterator = consistency == null
                                    ? pagers[current].fetchPageInternal(remainingPagePart, executionController)
                                    : pagers[current].fetchPage(remainingPagePart, consistency, queryState, queryStartNanoTime);
            }
            return partitionIterator.next();
        }

        public void close()
        {
            if (partitionIterator != null && !closed)
                partitionIterator.close();
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", PagersIterator.class.getSimpleName() + "[", "]")
                   .add("pageSize=" + pageSize)
                   .add("closed=" + closed)
                   .add("countedRows=" + countedRows)
                   .add("countedBytes=" + countedBytes)
                   .add("counted=" + counted)
                   .toString();
        }
    }

    public int maxRemaining()
    {
        return remaining;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", MultiPartitionPager.class.getSimpleName() + "[", "]")
               .add("current=" + current)
               .add("pagers.length=" + pagers.length)
               .add("limit=" + limit)
               .add("remaining=" + remaining)
               .toString();
    }
}