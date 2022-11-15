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
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.ProtocolVersion;

@NotThreadSafe
abstract class AbstractQueryPager<T extends ReadQuery> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    protected final T query;

    protected final ProtocolVersion protocolVersion;

    private final boolean enforceStrictLiveness;

    /**
     * This is the counter which was used for the last page we fetched.
     * It can be used to obtain the number of fetched rows or bytes.
     */
    private DataLimits.Counter lastCounter;

    /**
     * The {@link #remaining} and {@link #remainingInPartition} are initially set to the user limits provided
     * in the query (via the LIMIT and PER PARTITION LIMIT clauses - those values are obtained from {@link #limits}).
     * When a page is fetched, iterated and closed, those values are updated with the number of items counted
     * on that recently fetched page.
     */
    private int remaining;
    private int remainingInPartition;

    /**
     * This is the current key at which the data is being read. This is the key for which {@link #remainingInPartition}
     * makes sense: when a next partition (key) is started, {@link #remainingInPartition} should be reset to the value
     * provided in {@link #limits}. This can be null (before a first data item is read).
     */
    private DecoratedKey lastKey;

    /**
     * Whether the pager is exhausted or not. The pager gets exhausted if the recently fetched, iterated and closed
     * page had fewer items than the requested page size.
     */
    private boolean exhausted;

    /**
     * The paging transformation which is used for the recently requested page. It is set when a new page is requested
     * and then cleaned (nulled), when the page is closed. It is used to prevent fetching a new page until the previous
     * one is still being iterated.
     */
    private PagerTransformation<?> currentPagerTransformation;

    protected AbstractQueryPager(T query, ProtocolVersion protocolVersion)
    {
        this.query = query;
        this.protocolVersion = protocolVersion;
        this.enforceStrictLiveness = query.metadata().enforceStrictLiveness();

        this.remaining = query.limits().count();
        this.remainingInPartition = query.limits().perPartitionCount();
    }

    @Override
    public ReadExecutionController executionController()
    {
        return query.executionController();
    }

    private ReadQuery startNextPage(PageSize pageSize, DataLimits limits)
    {
        assert currentPagerTransformation == null;

        if (isExhausted())
            return null;

        ReadQuery readQuery = nextPageReadQuery(pageSize, limits);
        if (readQuery == null)
            exhausted = true;

        return readQuery;
    }

    @Override
    public PartitionIterator fetchPage(PageSize pageSize, ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime)
    {
        DataLimits nextPageLimits = nextPageLimits();
        ReadQuery readQuery = startNextPage(pageSize, nextPageLimits);
        if (readQuery == null)
            return EmptyIterators.partition();

        RowPagerTransformation pagerTransformation = new RowPagerTransformation(nextPageLimits.forPaging(pageSize), query.nowInSec());
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.execute(consistency, clientState, queryStartNanoTime), pagerTransformation);
    }

    @Override
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController)
    {
        DataLimits nextPageLimits = nextPageLimits();
        ReadQuery readQuery = startNextPage(pageSize, nextPageLimits);
        if (readQuery == null)
            return EmptyIterators.partition();

        RowPagerTransformation pagerTransformation = new RowPagerTransformation(nextPageLimits.forPaging(pageSize), query.nowInSec());
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.executeInternal(executionController), pagerTransformation);
    }

    public UnfilteredPartitionIterator fetchPageUnfiltered(TableMetadata metadata, PageSize pageSize, ReadExecutionController executionController)
    {
        DataLimits nextPageLimits = nextPageLimits();
        ReadQuery readQuery = startNextPage(pageSize, nextPageLimits);
        if (readQuery == null)
            return EmptyIterators.unfilteredPartition(metadata);

        UnfilteredPagerTransformation pagerTransformation = new UnfilteredPagerTransformation(nextPageLimits.forPaging(pageSize), query.nowInSec());
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.executeLocally(executionController), pagerTransformation);
    }

    /**
     * Limits for the next page are basically the initial CQL limits reduced by the counts of data fetches so far on
     * previous pages, that is - remaining and remainingInPartition. The fact that we use the minimum of the actual
     * limits and remaining amounts is that, the remaining amounts are computed against the initial CQL limits, while
     * the actual limits might have been updated by {@link #withUpdatedLimit(DataLimits)} and we want that none of those
     * can be exceeded.
     * <p/>
     * TL;DR - this situation takes place when we do aggregation with grouping, while paging results by groups. When
     * the next page of groups is fetched, the remaining counters denote the CQL limits ({@link #remaining}
     * {@link #remainingInPartition}) while the actual limits are updated with the groups page size ({@link #limits()}).
     * Since both limits need to be obeyed, and we can only specify one limit, we simply use the minimum of both for
     * the next page.
     */
    protected DataLimits nextPageLimits()
    {
        return limits().withCountedLimit(Math.min(limits().count(), remaining))
                       .withCountedPerPartitionLimit(Math.min(limits().perPartitionCount(), remainingInPartition));
    }

    private class UnfilteredPagerTransformation extends PagerTransformation<Unfiltered>
    {
        private UnfilteredPagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        @Override
        protected BaseRowIterator<Unfiltered> apply(BaseRowIterator<Unfiltered> partition)
        {
            return Transformation.apply(pageCounter.applyTo((UnfilteredRowIterator) partition), this);
        }
    }

    private class RowPagerTransformation extends PagerTransformation<Row>
    {
        private RowPagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        @Override
        protected BaseRowIterator<Row> apply(BaseRowIterator<Row> partition)
        {
            return Transformation.apply(pageCounter.applyTo((RowIterator) partition), this);
        }
    }

    private abstract class PagerTransformation<T extends Unfiltered> extends Transformation<BaseRowIterator<T>>
    {
        /**
         * The limits obtained by applying page size limit on the limits provided in a query.
         */
        private final DataLimits pageLimits;
        protected final DataLimits.Counter pageCounter;
        private DecoratedKey currentKey;
        private Row lastRow;
        private boolean isFirstPartition = true;

        private PagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            this.pageCounter = pageLimits.newCounter(nowInSec, true, query.selectsFullPartition(), enforceStrictLiveness);
            AbstractQueryPager.this.lastCounter = this.pageCounter;

            // Page limits are passed to the transformation for two reasons
            // - to create a counter,
            // - to determine if the query pager is exhausted when we reach the end of data (if the end of data
            //   is reached before we reach the page limit, we conclude that the query pager is exhausted and there
            //   are no more pages to fetch)
            this.pageLimits = pageLimits;

            if (logger.isTraceEnabled())
                logger.trace("Fetching new page - created {}", this);
        }

        @Override
        public BaseRowIterator<T> applyToPartition(BaseRowIterator<T> partition)
        {
            currentKey = partition.partitionKey();

            // If this is the first partition on this page, and it was started on some previous page, and it is empty,
            // there is no point in returning it.
            // TL;DR
            // When the partition has no more regular rows, and it has a static row it will look like an empty partition
            // with a static row for the upper layer. The upper layer may treat such partitions in a special way - it is
            // undesired in this case because this is just an empty fragment of non-empty partition. That is why
            // skipping this partition is not just a slight optimization but also a requirement for correctness.
            // When the partition is indeed empty, it must have been returned as such on the previous page.
            // TODO would be good to have a unit test case for that
            if (isFirstPartition)
            {
                isFirstPartition = false;
                if (isPreviouslyReturnedPartition(currentKey) && !partition.hasNext())
                {
                    partition.close();
                    return null;
                }
            }

            return apply(partition);
        }

        protected abstract BaseRowIterator<T> apply(BaseRowIterator<T> partition);

        @Override
        public void onClose()
        {
            assert lastCounter == pageCounter;

            // In some case like GROUP BY a counter need to know when the processing is completed.
            pageCounter.onClose();

            recordLast(lastKey, lastRow);

            remaining -= pageCounter.counted();

            // If the clustering of the last row returned is a static one, it means that the partition was only
            // containing data within the static columns. If the clustering of the last row returned is empty
            // it means that there is only one row per partition. Therefore, in both cases there are no data remaining
            // within the partition.
            if (lastRow != null &&
                (lastRow.clustering() == Clustering.STATIC_CLUSTERING || lastRow.clustering().isEmpty()))
            {
                remainingInPartition = 0;
            }
            else
            {
                remainingInPartition -= pageCounter.countedInCurrentPartition();
            }
            // if the counter did not count up to the page limits, then the iteration must have reached the end,
            // so the iterator is exhausted
            exhausted = pageLimits.isExhausted(pageCounter);
            currentPagerTransformation = null;
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                return applyToRow(row);
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            if (!currentKey.equals(lastKey))
                remainingInPartition = limits().perPartitionCount();
            lastKey = currentKey;
            lastRow = row;
            return row;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", PagerTransformation.class.getSimpleName() + "[", "]")
                   .add("pageLimits=" + pageLimits)
                   .add("counter=" + pageCounter)
                   .add("currentKey=" + currentKey)
                   .add("lastRow=" + lastRow)
                   .add("isFirstPartition=" + isFirstPartition)
                   .toString();
        }
    }

    protected void restoreState(DecoratedKey lastKey, int remaining, int remainingInPartition)
    {
        this.lastKey = lastKey;
        this.remaining = remaining;
        this.remainingInPartition = remainingInPartition;
    }

    @Override
    public boolean isExhausted()
    {
        return exhausted || remaining == 0;
    }

    @Override
    public int maxRemaining()
    {
        return remaining;
    }

    @Override
    public DataLimits limits()
    {
        return query.limits();
    }

    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    /**
     * Returns the {@link DataLimits.Counter} of the last returned and traversed row iterator; the iterator must be
     * closed in order for this method to return a proper counter
     */
    public DataLimits.Counter getLastCounter()
    {
        return lastCounter;
    }

    protected abstract T nextPageReadQuery(PageSize pageSize, DataLimits limits);

    protected abstract void recordLast(DecoratedKey key, Row row);

    protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey key);

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AbstractQueryPager.class.getSimpleName() + "[", "]")
               .add("limits=" + limits())
               .add("remaining=" + remaining)
               .add("lastCounter=" + lastCounter)
               .add("lastKey=" + lastKey)
               .add("remainingInPartition=" + remainingInPartition)
               .add("exhausted=" + exhausted)
               .toString();
    }
}
