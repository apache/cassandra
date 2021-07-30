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
import java.util.NoSuchElementException;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.exceptions.OperationExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.QueryState;

/**
 * {@code QueryPager} that takes care of fetching the pages for aggregation queries.
 * <p>
 * For aggregation/group by queries, the user page size is in number of groups. But each group could be composed of very
 * many rows so to avoid running into OOMs, this pager will page internal queries into sub-pages. So each call to
 * {@link #fetchPage(PageSize, ConsistencyLevel, QueryState, long)} may (transparently) yield multiple internal queries
 * (sub-pages).
 */
public final class AggregationQueryPager implements QueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(AggregationQueryPager.class);

    private final DataLimits limits;

    private final PageSize subPageSize;

    // The sub-pager, used to retrieve the next sub-page.
    private QueryPager subPager;

    public AggregationQueryPager(QueryPager subPager, PageSize subPageSize, DataLimits limits)
    {
        this.subPager = subPager;
        this.limits = limits;
        this.subPageSize = subPageSize;
    }

    /**
     * This will return the iterator over the partitions. The iterator is limited by the provided page size and the user
     * specified limit (in the query). Both the limit and the page size are applied to the number of groups covered by
     * the returned data.
     * <p/>
     * In case of group-by queries the page size can be provided only in rows unit ({@link OperationExecutionException}
     * is thrown otherwise). In case of 'aggregate everything' queries, the provided page size and the limits are
     * ignored as we always return a single row.
     *
     * @param pageSize    the maximum number of elements to return in the next page (groups)
     * @param consistency the consistency level to achieve for the query
     * @param queryState  the {@code QueryState} for the query. In practice, this can be null unless
     *                    {@code consistency} is a serial consistency
     */
    @Override
    public PartitionIterator fetchPage(PageSize pageSize,
                                       ConsistencyLevel consistency,
                                       QueryState queryState,
                                       long queryStartNanoTime) throws OperationExecutionException
    {
        if (pageSize.isDefined() && pageSize.getUnit() != PageSize.PageUnit.ROWS)
            throw new OperationExecutionException("Paging in bytes is not supported for aggregation queries. Please specify the page size in rows.");

        if (limits.isGroupByLimit())
            return new GroupByPartitionIterator(pageSize, subPageSize, consistency, queryState, queryStartNanoTime);

        return new AggregationPartitionIterator(subPageSize, consistency, queryState, queryStartNanoTime);
    }

    @Override
    public ReadExecutionController executionController()
    {
        return subPager.executionController();
    }

    /**
     * {@see #fetchPage}
     *
     * @param pageSize the maximum number of elements to return in the next page
     * @param executionController the {@code ReadExecutionController} protecting the read
     */
    @Override
    public PartitionIterator fetchPageInternal(PageSize pageSize, ReadExecutionController executionController)
    {
        if (pageSize.isDefined() && pageSize.getUnit() != PageSize.PageUnit.ROWS)
            throw new OperationExecutionException("Paging in bytes is not supported for aggregation queries. Please specify the page size in rows.");

        if (limits.isGroupByLimit())
            return new GroupByPartitionIterator(pageSize, subPageSize, executionController, System.nanoTime());

        return new AggregationPartitionIterator(subPageSize, executionController, System.nanoTime());
    }

    @Override
    public boolean isExhausted()
    {
        return subPager.isExhausted();
    }

    @Override
    public int maxRemaining()
    {
        return subPager.maxRemaining();
    }

    @Override
    public PagingState state()
    {
        return subPager.state();
    }

    @Override
    public QueryPager withUpdatedLimit(DataLimits newLimits)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * <code>PartitionIterator</code> that automatically fetch a new sub-page of data if needed when the current iterator is
     * exhausted.
     */
    public class GroupByPartitionIterator implements PartitionIterator
    {
        /**
         * The top-level page size in number of groups.
         */
        private final PageSize groupsPageSize;

        /**
         * Page size for internal paging
         */
        private final PageSize subPageSize;

        // For "normal" queries
        private final ConsistencyLevel consistency;
        private final QueryState queryState;

        // For internal queries
        private final ReadExecutionController executionController;

        /**
         * The <code>PartitionIterator</code> over the last page retrieved.
         */
        private PartitionIterator partitionIterator;

        /**
         * The next <code>RowIterator</code> to be returned.
         */
        private RowIterator next;

        /**
         * Specify if all the data have been returned.
         */
        private boolean endOfData;

        /**
         * Keeps track if the partitionIterator has been closed or not.
         */
        private boolean closed;

        /**
         * The key of the last partition processed.
         */
        private ByteBuffer lastPartitionKey;

        /**
         * The clustering of the last row processed
         */
        private Clustering<?> lastClustering;

        private long queryStartNanoTime;

        protected int initialMaxRemaining;

        public GroupByPartitionIterator(PageSize groupsPageSize,
                                        PageSize subPageSize,
                                        ConsistencyLevel consistency,
                                        QueryState queryState,
                                        long queryStartNanoTime)
        {
            this(groupsPageSize, subPageSize, consistency, queryState, null, queryStartNanoTime);
        }

        public GroupByPartitionIterator(PageSize groupsPageSize,
                                        PageSize subPageSize,
                                        ReadExecutionController executionController,
                                        long queryStartNanoTime)
       {
           this(groupsPageSize, subPageSize, null, null, executionController, queryStartNanoTime);
       }

        private GroupByPartitionIterator(PageSize groupsPageSize,
                                         PageSize subPageSize,
                                         ConsistencyLevel consistency,
                                         QueryState queryState,
                                         ReadExecutionController executionController,
                                         long queryStartNanoTime)
        {
            this.groupsPageSize = groupsPageSize;
            this.subPageSize = subPageSize;
            this.consistency = consistency;
            this.queryState = queryState;
            this.executionController = executionController;
            this.queryStartNanoTime = queryStartNanoTime;
            subPager = subPager.withUpdatedLimit(limits.withCountedLimit(groupsPageSize.minRowsCount(maxRemaining())));

            if (logger.isTraceEnabled())
                logger.trace("Fetching a new page - created {}", this);
        }

        public final void close()
        {
            if (!closed)
            {
                closed = true;
                partitionIterator.close();
            }
        }

        public final boolean hasNext()
        {
            if (endOfData)
                return false;

            if (next != null)
                return true;

            fetchNextRowIterator();

            return next != null;
        }

        /**
         * Loads the next <code>RowIterator</code> to be returned. The iteration finishes when we reach either the
         * user groups limit or the groups page size. The user provided limit is initially set in subPager.maxRemaining().
         */
        private void fetchNextRowIterator()
        {
            // we haven't started yet, fetch the first sub page (partition iterator with sub-page limit)
            if (partitionIterator == null)
            {
                initialMaxRemaining = subPager.maxRemaining();
                partitionIterator = fetchSubPage(subPageSize);
            }

            while (!partitionIterator.hasNext())
            {
                partitionIterator.close();

                int remaining = getRemaining();
                assert remaining >= 0;
                if (remaining == 0 || subPager.isExhausted())
                {
                    endOfData = true;
                    closed = true;
                    return;
                }

                subPager = updatePagerLimit(subPager, limits.withCountedLimit(remaining), lastPartitionKey, lastClustering);
                partitionIterator = fetchSubPage(subPageSize);
            }

            next = partitionIterator.next();
        }

        protected int getRemaining()
        {
            int counted = initialMaxRemaining - subPager.maxRemaining();
            return groupsPageSize.withDecreasedRows(counted).rows();
        }

        /**
         * Updates the pager with the new limits if needed.
         *
         * @param pager the pager previoulsy used
         * @param limits the DataLimits
         * @param lastPartitionKey the partition key of the last row returned
         * @param lastClustering the clustering of the last row returned
         * @return the pager to use to query the next page of data
         */
        protected QueryPager updatePagerLimit(QueryPager pager,
                                              DataLimits limits,
                                              ByteBuffer lastPartitionKey,
                                              Clustering<?> lastClustering)
        {
            GroupingState state = new GroupingState(lastPartitionKey, lastClustering);
            DataLimits newLimits = limits.forGroupByInternalPaging(state);
            return pager.withUpdatedLimit(newLimits);
        }

        /**
         * Fetchs the next sub-page.
         *
         * @param subPageSize the sub-page size in number of groups
         * @return the next sub-page
         */
        private final PartitionIterator fetchSubPage(PageSize subPageSize)
        {
            return consistency != null ? subPager.fetchPage(subPageSize, consistency, queryState, queryStartNanoTime)
                                       : subPager.fetchPageInternal(subPageSize, executionController);
        }

        public final RowIterator next()
        {
            if (!hasNext())
                throw new NoSuchElementException();

            RowIterator iterator = new GroupByRowIterator(next);
            lastPartitionKey = iterator.partitionKey().getKey();
            next = null;
            return iterator;
        }

        private class GroupByRowIterator implements RowIterator
        {
            /**
             * The decorated <code>RowIterator</code>.
             */
            private RowIterator rowIterator;

            /**
             * Keeps track if the decorated iterator has been closed or not.
             */
            private boolean closed;

            public GroupByRowIterator(RowIterator delegate)
            {
                this.rowIterator = delegate;
            }

            public TableMetadata metadata()
            {
                return rowIterator.metadata();
            }

            public boolean isReverseOrder()
            {
                return rowIterator.isReverseOrder();
            }

            public RegularAndStaticColumns columns()
            {
                return rowIterator.columns();
            }

            public DecoratedKey partitionKey()
            {
                return rowIterator.partitionKey();
            }

            public Row staticRow()
            {
                Row row = rowIterator.staticRow();
                lastClustering = null;
                return row;
            }

            public boolean isEmpty()
            {
                return this.rowIterator.isEmpty() && !hasNext();
            }

            public void close()
            {
                if (!closed)
                    rowIterator.close();
            }

            public boolean hasNext()
            {
                if (rowIterator.hasNext())
                    return true;

                DecoratedKey partitionKey = rowIterator.partitionKey();

                rowIterator.close();

                // Fetch the next RowIterator
                GroupByPartitionIterator.this.hasNext();

                // if the previous page was ending within the partition the
                // next RowIterator is the continuation of this one
                if (next != null && partitionKey.equals(next.partitionKey()))
                {
                    rowIterator = next;
                    next = null;
                    return rowIterator.hasNext();
                }

                closed = true;
                return false;
            }

            public Row next()
            {
                // we need to check this because this.rowIterator may exhaust if the sub-page is done and in such a case
                // #hasNext switches this.rowIterator to the new one, which is obtained for the next page
                if (!hasNext())
                    throw new NoSuchElementException();

                Row row = this.rowIterator.next();
                lastClustering = row.clustering();
                return row;
            }
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", GroupByPartitionIterator.class.getSimpleName() + "[", "]")
                   .add("groupsPageSize=" + groupsPageSize)
                   .add("subPageSize=" + subPageSize)
                   .add("endOfData=" + endOfData)
                   .add("closed=" + closed)
                   .add("limits=" + limits)
                   .add("lastPartitionKey=" + lastPartitionKey)
                   .add("lastClustering=" + ((lastClustering != null && subPager.executionController() != null) ? lastClustering.toString(subPager.executionController().metadata()): String.valueOf(lastClustering)))
                   .add("initialMaxRemaining=" + initialMaxRemaining)
                   .add("sub-pager=" + subPager.toString())
                   .toString();
        }
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AggregationQueryPager.class.getSimpleName() + "[", "]")
               .add("limits=" + limits)
               .add("subPageSize=" + subPageSize)
               .add("subPager=" + subPager)
               .toString();
    }

    /**
     * <code>PartitionIterator</code> for queries without Group By but with aggregates.
     * <p>For maintaining backward compatibility we are forced to use the {@link org.apache.cassandra.db.filter.DataLimits.CQLLimits} instead of the
     * {@link org.apache.cassandra.db.filter.DataLimits.CQLGroupByLimits}. Due to that pages need to be fetched in a different way.</p>
     */
    public final class AggregationPartitionIterator extends GroupByPartitionIterator
    {
        public AggregationPartitionIterator(PageSize subPageSize,
                                            ConsistencyLevel consistency,
                                            QueryState queryState,
                                            long queryStartNanoTime)
        {
            super(PageSize.NONE, subPageSize, consistency, queryState, queryStartNanoTime);
        }

        public AggregationPartitionIterator(PageSize subPageSize,
                                            ReadExecutionController executionController,
                                            long queryStartNanoTime)
        {
            super(PageSize.NONE, subPageSize, executionController, queryStartNanoTime);
        }

        @Override
        protected QueryPager updatePagerLimit(QueryPager pager,
                                              DataLimits limits,
                                              ByteBuffer lastPartitionKey,
                                              Clustering<?> lastClustering)
        {
            return pager;
        }

        @Override
        protected int getRemaining()
        {
            return initialMaxRemaining;
        }
    }
}
