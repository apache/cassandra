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

import java.util.StringJoiner;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractIterator;

/**
 * Pager over a list of SinglePartitionReadQuery.
 * <p/>
 * Note that this is not easy to make efficient. Indeed, we need to page the first query fully before
 * returning results from the next one, but if the result returned by each query is small (compared to pageSize),
 * paging the queries one at a time under-performs compared to parallelizing. On the other hand, if we parallelize
 * and each query raised pageSize results, we'll end up with queries.size() * pageSize results in memory, which
 * defeats the purpose of paging.
 * <p/>
 * For now, we keep it simple (somewhat) and just do one query at a time. Provided that we make sure to not
 * create a pager unless we need to, this is probably fine. Though, if we later want to get fancy, we could use the
 * cfs meanPartitionSize to decide if parallelizing some query might be worth it while being confident we don't
 * blow out memory.
 */

public class MultiPartitionPager<T extends SinglePartitionReadQuery> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionPager.class);

    private static final SinglePartitionReadQuery[] NO_QUERIES = new SinglePartitionReadQuery[0];

    @Nonnull
    private final SinglePartitionReadQuery[] queries;

    private SinglePartitionPager curPager;
    /**
     * The limits provided as a part of the query (rows limit, per partition rows limit)
     */
    private final DataLimits limits;

    private final long nowInSec;

    private final ProtocolVersion protocolVersion;

    /**
     * Initially set to the user limits provided in the query (via the LIMIT clause - that value is obtained
     * from {@link #limits}) or from a {@link PagingState} object if it was provided. When a page is fetched,
     * iterated and closed, this value is updated with the number of items counted on that recently fetched page.
     */
    private int remaining;

    /**
     * The index of the current single partition pager
     */
    private int curQueryIdx;

    public MultiPartitionPager(SinglePartitionReadQuery.Group<T> group, PagingState state, ProtocolVersion protocolVersion)
    {
        this.limits = group.limits();
        this.nowInSec = group.nowInSec();
        this.protocolVersion = protocolVersion;

        int firstNotExhaustedQueryIdx = 0;

        // If it's not the beginning (state != null), we need to find where we were and skip previous queries
        // since they are done.
        if (state != null)
            for (; firstNotExhaustedQueryIdx < group.queries.size(); firstNotExhaustedQueryIdx++)
                if (group.queries.get(firstNotExhaustedQueryIdx).partitionKey().getKey().equals(state.partitionKey))
                    break;

        if (firstNotExhaustedQueryIdx >= group.queries.size())
        {
            queries = NO_QUERIES;
            return;
        }

        queries = group.queries.stream().toArray(SinglePartitionReadQuery[]::new);

        remaining = state == null ? limits.count() : Math.min(state.remaining, limits.count());

        curQueryIdx = firstNotExhaustedQueryIdx;
        SinglePartitionReadQuery query = queries[curQueryIdx];
        curPager = query.withUpdatedLimit(limits.withCountedLimit(remaining)).getPager(state, protocolVersion);
    }

    private MultiPartitionPager(SinglePartitionReadQuery[] queries,
                                DataLimits limits,
                                long nowInSec,
                                int remaining,
                                SinglePartitionPager curPager,
                                int curQueryIdx,
                                ProtocolVersion protocolVersion)
    {
        this.queries = queries;
        this.limits = limits;
        this.nowInSec = nowInSec;
        this.remaining = remaining;
        this.curPager = curPager;
        this.curQueryIdx = curQueryIdx;
        this.protocolVersion = protocolVersion;
    }

    @Override
    public QueryPager withUpdatedLimit(DataLimits newLimits)
    {
        if (moveToNextNonEmptyPager())
            newLimits = newLimits.withoutState();

        return new MultiPartitionPager<T>(queries,
                                          newLimits,
                                          nowInSec,
                                          remaining,
                                          curPager,
                                          curQueryIdx,
                                          protocolVersion);
    }

    @Override
    public DataLimits limits()
    {
        return limits;
    }

    @Override
    public PagingState state()
    {
        // Sets current to the first non-exhausted pager
        if (isExhausted())
            return null;

        PagingState state = curPager.state();
        PagingState.RowMark rowMark = state == null ? null : state.rowMark;
        return new PagingState(curPager.key(), rowMark, remaining, curPager.remainingInPartition());
    }

    private boolean isLastQuery()
    {
        assert queries.length > 0;
        return curQueryIdx == (queries.length - 1);
    }

    @Override
    public boolean isExhausted()
    {
        if (remaining == 0 || curPager == null)
            return true;

        if (!curPager.isExhausted())
            return false;

        return isLastQuery();
    }

    private boolean moveToNextNonEmptyPager()
    {
        if (isExhausted())
            return false;

        int oldIdx = curQueryIdx;
        while (curPager.isExhausted())
        {
            if (isLastQuery())
                break;

            curQueryIdx++;
            curPager = queries[curQueryIdx].withUpdatedLimit(limits.withCountedLimit(remaining)).getPager(null, protocolVersion);
        }
        return curQueryIdx > oldIdx;
    }

    public ReadExecutionController executionController()
    {
        if (remaining > 0 && curPager != null)
            return curPager.executionController();

        throw new AssertionError("Shouldn't be called on an exhausted pager");
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPage(PageSize pageSize, ConsistencyLevel consistency, ClientState clientState, Dispatcher.RequestTime requestTime) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, consistency, clientState, null, requestTime);
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, null, null, executionController, Dispatcher.RequestTime.forImmediateExecution());
    }

    /**
     * Iterates child pagers from {@link #curQueryIdx} until a page or query limit is reached.
     * The final row and associated static data can exceed the byte threshold.
     * Updates {@link #curQueryIdx} and {@link #remaining} as partitions are consumed.
     */
    private class PagersIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final PageSize pageSize;
        private PartitionIterator partitionIterator;
        private boolean closed;
        private final Dispatcher.RequestTime requestTime;

        // For "normal" queries
        private final ConsistencyLevel consistency;
        private final ClientState clientState;

        // For internal queries
        private final ReadExecutionController executionController;

        /**
         * The limits adjusted for the current page - the initial query limits with count limit reduced by what has been
         * counted so far on the previously fetched pages. In other words, the upper limit of items that can be
         * fetched on this page (before actually applying paging).
         */
        private final DataLimits curPageLimits;
        private final DataLimits pageLimits;

        /**
         * Sums the counters of child pagers used on this page.
         */
        private final DataLimits.Counter curPageCounter;


        public PagersIterator(PageSize pageSize, ConsistencyLevel consistency, ClientState clientState, ReadExecutionController executionController, Dispatcher.RequestTime requestTime)
        {
            this.pageSize = pageSize;
            this.consistency = consistency;
            this.clientState = clientState;
            this.executionController = executionController;
            this.requestTime = requestTime;
            this.curPageLimits = limits.withCountedLimit(Math.min(limits.count(), remaining));
            // Child pagers account for unfinished groups; this counter only sums their counts.
            this.pageLimits = curPageLimits.withoutState().forPaging(pageSize);
            this.curPageCounter = pageLimits.newCounter(nowInSec, true, true, false);

            if (logger.isTraceEnabled())
                logger.trace("Fetching a new page - created {}", this);
        }

        protected RowIterator computeNext()
        {
            while (partitionIterator == null || !partitionIterator.hasNext())
            {
                DataLimits.Counter lastPartitionCounter = null;
                if (partitionIterator != null)
                {
                    partitionIterator.close();

                    lastPartitionCounter = curPager.getLastCounter();

                    curPageCounter.add(lastPartitionCounter);
                    // counted() measures rows or groups, depending on the query limits.
                    remaining -= lastPartitionCounter.counted();
                }

                // Stop at a page limit, or when a GROUP BY child pager stops before exhausting its partition.
                boolean isDone = !pageLimits.isExhausted(curPageCounter)
                                 || (partitionIterator != null && limits.isGroupByLimit() && !curPager.isExhausted());

                // move to the next non-empty partition (pager)
                boolean isNewPartition = false;
                if (!isDone)
                    isNewPartition = moveToNextNonEmptyPager();

                if (isDone || isExhausted())
                {
                    closed = true;
                    return endOfData();
                }

                if (isNewPartition)
                    // Reset the outer per-partition counter. Child counters already include static data.
                    curPageCounter.applyToPartition(curPager.query.partitionKey(), Rows.EMPTY_STATIC_ROW);

                // Limit the next child pager to the unused portion of this page.
                DataLimits partitionLimits = curPageLimits.reducedBy(curPageCounter);
                curPager = curPager.withUpdatedLimit(isNewPartition ? partitionLimits.withoutState() : partitionLimits);

                PageSize remainingPagePart = curPageCounter.getDecreasedPageSize(pageSize);

                partitionIterator = consistency == null
                                    ? curPager.fetchPageInternal(remainingPagePart, executionController)
                                    : curPager.fetchPage(remainingPagePart, consistency, clientState, requestTime);
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
                   .add("counter=" + curPageCounter)
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
               .add("current=" + curQueryIdx)
               .add("queries.length=" + queries.length)
               .add("limit=" + limits)
               .add("remaining=" + remaining)
               .toString();
    }
}
