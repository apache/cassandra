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
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Generic abstraction for read queries.
 */
public interface ReadQuery
{
    public static ReadQuery empty(final TableMetadata metadata)
    {
        return new ReadQuery()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public ReadExecutionController executionController()
            {
                return ReadExecutionController.empty();
            }

            public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
            {
                return EmptyIterators.partition();
            }

            public PartitionIterator executeInternal(ReadExecutionController controller)
            {
                return EmptyIterators.partition();
            }

            public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
            {
                return EmptyIterators.unfilteredPartition(executionController.metadata());
            }

            public DataLimits limits()
            {
                // What we return here doesn't matter much in practice. However, returning DataLimits.NONE means
                // "no particular limit", which makes SelectStatement.execute() take the slightly more complex "paging"
                // path. Not a big deal but it's easy enough to return a limit of 0 rows which avoids this.
                return DataLimits.cqlLimits(0);
            }

            public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion)
            {
                return QueryPager.EMPTY;
            }

            public boolean selectsKey(DecoratedKey key)
            {
                return false;
            }

            public boolean selectsClustering(DecoratedKey key, Clustering<?> clustering)
            {
                return false;
            }

            @Override
            public long nowInSec()
            {
                return FBUtilities.nowInSeconds();
            }

            @Override
            public boolean selectsFullPartition()
            {
                return false;
            }

            @Override
            public boolean isEmpty()
            {
                return true;
            }

            @Override
            public RowFilter rowFilter()
            {
                return RowFilter.none();
            }

            @Override
            public ColumnFilter columnFilter()
            {
                return ColumnFilter.NONE;
            }
        };
    }

    /**
     * The metadata for the table this is a query on.
     *
     * @return the metadata for the table this is a query on.
     */
    public TableMetadata metadata();

    /**
     * Starts a new read operation.
     * <p>
     * This must be called before {@link executeInternal} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-ressource construction.
     *
     * @return a newly started execution controller for this {@code ReadQuery}.
     */
    public ReadExecutionController executionController();

    /**
     * Executes the query at the provided consistency level.
     *
     * @param consistency the consistency level to achieve for the query.
     * @param state client state
     * @return the result of the query.
     */
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException;

    /**
     * Execute the query for internal queries (that is, it basically executes the query locally).
     *
     * @param controller the {@code ReadExecutionController} protecting the read.
     * @return the result of the query.
     */
    public PartitionIterator executeInternal(ReadExecutionController controller);

    /**
     * Execute the query locally. This is similar to {@link ReadQuery#executeInternal(ReadExecutionController)}
     * but it returns an unfiltered partition iterator that can be merged later on.
     *
     * @param executionController the {@code ReadExecutionController} protecting the read.
     * @return the result of the read query.
     */
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController);

    /**
     * Returns a pager for the query.
     *
     * @param pagingState the {@code PagingState} to start from if this is a paging continuation. This can be
     * {@code null} if this is the start of paging.
     * @param protocolVersion the protocol version to use for the paging state of that pager.
     *
     * @return a pager for the query.
     */
    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion);

    /**
     * The limits for the query.
     *
     * @return The limits for the query.
     */
    public DataLimits limits();

    /**
     * @return true if the read query would select the given key, including checks against the row filter, if
     * checkRowFilter is true
     */
    public boolean selectsKey(DecoratedKey key);

    /**
     * @return true if the read query would select the given clustering, including checks against the row filter, if
     * checkRowFilter is true
     */
    public boolean selectsClustering(DecoratedKey key, Clustering<?> clustering);

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public long nowInSec();

    /**
     * Checks if this {@code ReadQuery} selects full partitions, that is it has no filtering on clustering or regular columns.
     * @return {@code true} if this {@code ReadQuery} selects full partitions, {@code false} otherwise.
     */
    public boolean selectsFullPartition();

    /**
     * Filters/Resrictions on CQL rows.
     * <p>
     * This contains the restrictions that are not directly handled by the
     * {@code ClusteringIndexFilter}. More specifically, this includes any non-PK column
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirely by the clustering index filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the filter holding the expression that rows must satisfy.
     */
    public RowFilter rowFilter();

    /**
     * A filter on which (non-PK) columns must be returned by the query.
     *
     * @return which columns must be fetched by this query.
     */
    public ColumnFilter columnFilter();

    /**
     * Whether this query is known to return nothing upfront.
     * <p>
     * This is overridden by the {@code ReadQuery} created through {@link #empty(TableMetadata)}, and that's probably the
     * only place that should override it.
     *
     * @return if this method is guaranteed to return no results whatsoever.
     */
    public default boolean isEmpty()
    {
        return false;
    }

    /**
     * If the index manager for the table determines that there's an applicable
     * 2i that can be used to execute this query, call its (optional)
     * validation method to check that nothing in this query's parameters
     * violates the implementation specific validation rules.
     */
    default void maybeValidateIndex()
    {
    }

    default void trackWarnings()
    {
    }

    /**
     * The query is a top-k query if the query has an {@link org.apache.cassandra.index.Index.QueryPlan} that
     * supports top-k ordering.
     *
     * @return {@code true} if this is a top-k query
     */
    default boolean isTopK()
    {
        return false;
    }
}
