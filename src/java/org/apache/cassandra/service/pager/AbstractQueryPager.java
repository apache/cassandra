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
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;

@NotThreadSafe
abstract class AbstractQueryPager<T extends ReadQuery> implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractQueryPager.class);

    protected final T query;

    // the limits provided as a part of the query
    protected final DataLimits limits;
    protected final ProtocolVersion protocolVersion;
    private final boolean enforceStrictLiveness;

    // This is the counter which was used for the last page we fetched. It can be used to obtain the number of
    // fetched rows or bytes.
    private DataLimits.Counter lastCounter;

    // This is the last key we've been reading from (or can still be reading within). This is the key for
    // which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
    // (and this is done in PagerIterator). This can be null (when we start).
    private DecoratedKey lastKey;

    // The remaining and remainingInPartition are initially set to the user limits provided in the query (via the
    // LIMIT and PER PARTITION LIMIT clauses). When a page is fetched, iterated and closed, those values are updated
    // with the number of items counted on that recently fetched page.
    private int remaining;
    private int remainingInPartition;

    // Whether the pager is exhausted or not - the pager gets exhausted if the recently fetched, iterated and closed
    // page has less items than the requested page size
    private boolean exhausted;

    // The paging transformation which is used for the recently requested page. It is set when we request the new page
    // and then cleaned when the page is closed. We use it to prevent fetching a new page until the previous one is
    // closed.
    private PagerTransformation<?> currentPagerTransformation;

    protected AbstractQueryPager(T query, ProtocolVersion protocolVersion)
    {
        this.query = query;
        this.protocolVersion = protocolVersion;
        this.limits = query.limits();
        this.enforceStrictLiveness = query.metadata().enforceStrictLiveness();

        this.remaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();
    }

    public ReadExecutionController executionController()
    {
        return query.executionController();
    }

    public PartitionIterator fetchPage(PageSize pageSize, ConsistencyLevel consistency, QueryState queryState, long queryStartNanoTime)
    {
        assert currentPagerTransformation == null;

        if (isExhausted())
            return EmptyIterators.partition();

        DataLimits updatedQueryLimits = nextPageLimits();
        RowPagerTransformation pagerTransformation = new RowPagerTransformation(updatedQueryLimits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize, updatedQueryLimits);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.partition();
        }
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.execute(consistency, queryState, queryStartNanoTime), pagerTransformation);
    }

    @Override
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController)
    {
        assert currentPagerTransformation == null;

        if (isExhausted())
            return EmptyIterators.partition();

        DataLimits updatedQueryLimits = nextPageLimits();
        RowPagerTransformation pagerTransformation = new RowPagerTransformation(updatedQueryLimits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize, updatedQueryLimits);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.partition();
        }
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.executeInternal(executionController), pagerTransformation);
    }

    public UnfilteredPartitionIterator fetchPageUnfiltered(TableMetadata metadata, PageSize pageSize, ReadExecutionController executionController)
    {
        assert currentPagerTransformation == null;

        if (isExhausted())
            return EmptyIterators.unfilteredPartition(metadata);

        DataLimits updatedQueryLimits = nextPageLimits();
        UnfilteredPagerTransformation pagerTransformation = new UnfilteredPagerTransformation(updatedQueryLimits.forPaging(pageSize), query.nowInSec());
        ReadQuery readQuery = nextPageReadQuery(pageSize, updatedQueryLimits);
        if (readQuery == null)
        {
            exhausted = true;
            return EmptyIterators.unfilteredPartition(metadata);
        }
        currentPagerTransformation = pagerTransformation;
        return Transformation.apply(readQuery.executeLocally(executionController), pagerTransformation);
    }

    /**
     * For subsequent pages we want to limit the number of rows to the minimum of the currently set limit in the query
     * and the number of remaining rows in page. Note that paging itself will be applied separately.
     */
    protected DataLimits nextPageLimits()
    {
        return limits.withCountedLimit(Math.min(limits.count(), remaining));
    }

    private class UnfilteredPagerTransformation extends PagerTransformation<Unfiltered>
    {

        private UnfilteredPagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        protected BaseRowIterator<Unfiltered> apply(BaseRowIterator<Unfiltered> partition)
        {
            return Transformation.apply(counter.applyTo((UnfilteredRowIterator) partition), this);
        }
    }

    private class RowPagerTransformation extends PagerTransformation<Row>
    {

        private RowPagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            super(pageLimits, nowInSec);
        }

        protected BaseRowIterator<Row> apply(BaseRowIterator<Row> partition)
        {
            return Transformation.apply(counter.applyTo((RowIterator) partition), this);
        }
    }

    private abstract class PagerTransformation<T extends Unfiltered> extends Transformation<BaseRowIterator<T>>
    {
        private final DataLimits pageLimits;
        protected final DataLimits.Counter counter;
        private DecoratedKey currentKey;
        private Row lastRow;
        private boolean isFirstPartition = true;

        private PagerTransformation(DataLimits pageLimits, int nowInSec)
        {
            this.counter = pageLimits.newCounter(nowInSec, true, query.selectsFullPartition(), enforceStrictLiveness);
            lastCounter = this.counter;

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

            // If this is the first partition of this page, this could be the continuation of a partition we've started
            // on the previous page. In which case, we could have the problem that the partition has no more "regular"
            // rows (but the page size is such we didn't knew before) but it does has a static row. We should then skip
            // the partition as returning it would means to the upper layer that the partition has "only" static columns,
            // which is not the case (and we know the static results have been sent on the previous page).
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
            assert lastCounter == counter;
            // In some case like GROUP BY a counter need to know when the processing is completed.
            counter.onClose();

            recordLast(lastKey, lastRow);

            remaining -= counter.counted();
            // If the clustering of the last row returned is a static one, it means that the partition was only
            // containing data within the static columns. If the clustering of the last row returned is empty
            // it means that there is only one row per partition. Therefore, in both cases there are no data remaining
            // within the partition.
            if (lastRow != null && (lastRow.clustering() == Clustering.STATIC_CLUSTERING
                    || lastRow.clustering().isEmpty()))
            {
                remainingInPartition = 0;
            }
            else
            {
                remainingInPartition -= counter.countedInCurrentPartition();
            }
            // if the counter did not count up to the page limits, then the iteration must have reached the end
            exhausted = pageLimits.isCounterBelowLimits(counter);
            currentPagerTransformation = null;
        }

        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
            {
                if (!currentKey.equals(lastKey))
                    remainingInPartition = limits.perPartitionCount();
                lastKey = currentKey;
                lastRow = row;
            }
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            if (!currentKey.equals(lastKey))
            {
                remainingInPartition = limits.perPartitionCount();
                lastKey = currentKey;
            }
            lastRow = row;
            return row;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", PagerTransformation.class.getSimpleName() + "[", "]")
                   .add("pageLimits=" + pageLimits)
                   .add("counter=" + counter)
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

    public boolean isExhausted()
    {
        return exhausted || remaining == 0;
    }

    public int maxRemaining()
    {
        return remaining;
    }

    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    /**
     * Returns the {@link DataLimits.Counter} for the page which was last fetched (the last page in the meaning
     * the last returned and traversed row iterator, the iterator must be closed in order for this method to return
     * proper counter)
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
               .add("limits=" + limits)
               .add("remaining=" + remaining)
               .add("lastCounter=" + lastCounter)
               .add("lastKey=" + lastKey)
               .add("remainingInPartition=" + remainingInPartition)
               .add("exhausted=" + exhausted)
               .toString();
    }
}
