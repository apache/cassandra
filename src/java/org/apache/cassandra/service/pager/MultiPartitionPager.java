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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.AbstractIterator;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

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

    private final int nowInSec;

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
                                int nowInSec,
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
        moveToNextNonEmptyPager();

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
    public PartitionIterator fetchPage(PageSize pageSize, ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, consistency, clientState, null, queryStartNanoTime);
    }

    @SuppressWarnings("resource") // iter closed via countingIter
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
    {
        return new PagersIterator(pageSize, null, null, executionController, nanoTime());
    }

    /**
     * This is an iterator over RowIterators (subsequent partitions). It starts from {@link #pagers}
     * at {@link #curQueryIdx} and make sure that the overall amount of data does not exceed
     * the provided {@link PagersIterator#pageSize} and user-defined data limits. This means that it can cut
     * the row iteration in the first partition or return multiple partitions and cut the row iterator
     * in n-th partition. It will update the {@link #curQueryIdx} index and {@link #remaining} as it goes.
     */
    private class PagersIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final PageSize pageSize;
        private PartitionIterator partitionIterator;
        private boolean closed;
        private final long queryStartNanoTime;

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

        /**
         * The counter used to count data on the current page across all the traversed internal pagers. In contains
         * a sum of what has been counted by the internal pagers on this page.
         */
        private final DataLimits.Counter curPageCounter;


        public PagersIterator(PageSize pageSize, ConsistencyLevel consistency, ClientState clientState, ReadExecutionController executionController, long queryStartNanoTime)
        {
            this.pageSize = pageSize;
            this.consistency = consistency;
            this.clientState = clientState;
            this.executionController = executionController;
            this.queryStartNanoTime = queryStartNanoTime;
            this.curPageLimits = limits.withCountedLimit(remaining);
            this.curPageCounter = curPageLimits.forPaging(pageSize)
                                               .newCounter(nowInSec, true, true, false);

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
                    // we've just reached the end of partition - let's close the row iterator and update the global counters
                    partitionIterator.close();

                    lastPartitionCounter = curPager.getLastCounter();

                    // the counts of data measured by the lastly iterated partition are added to the outer query counter
                    curPageCounter.add(lastPartitionCounter);
                    // the remaining limit needs to be decreased as well by the number of counted rows
                    remaining -= lastPartitionCounter.counted();
                }

                // We are done if:
                // - we have reached the page size,
                // - or in the case of GROUP BY if the current pager is not exhausted - which means that we read all the rows withing the limit before exhausting the pager
                boolean isDone = curPageCounter.isDone() || (partitionIterator != null && limits.isGroupByLimit() && !curPager.isExhausted());

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
                    // since a new partition is started... it is for resetting per partition count in the outer counter
                    // EMPTY_STATIC_ROW is passed to just not pass null - we simply don't care here because if a static
                    // row is encountered, it will be handled by inner counter of a partition query - that is, its data
                    // will be counted by inner counter and then added to the outer counter as above
                    curPageCounter.applyToPartition(curPager.query.partitionKey(), Rows.EMPTY_STATIC_ROW);

                // initially individual queries have their limits set to the initial value passed in the constructor
                // the limits for subsequent queries have to be adjusted to the current page limits reduced by what has
                // been counted so far on this page
                curPager = curPager.withUpdatedLimit(curPageLimits.reducedBy(curPageCounter));

                // a single page may span multiple partitions, so we may be in a middle of a page when switching
                // to the next partition; therefore, a full page should not be requested from the next partition query,
                // and a remaining part of the page has to be calculated
                PageSize remainingPagePart = curPageCounter.getDecreasedPageSize(pageSize);

                partitionIterator = consistency == null
                                    ? curPager.fetchPageInternal(remainingPagePart, executionController)
                                    : curPager.fetchPage(remainingPagePart, consistency, clientState, queryStartNanoTime);
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
