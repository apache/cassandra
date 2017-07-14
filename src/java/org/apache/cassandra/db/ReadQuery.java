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

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Generic abstraction for read queries.
 * <p>
 * The main implementation of this is {@link ReadCommand} but we have this interface because
 * {@link SinglePartitionReadCommand.Group} is also consider as a "read query" but is not a
 * {@code ReadCommand}.
 */
public interface ReadQuery
{
    ReadQuery EMPTY = new ReadQuery()
    {
        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
        {
            return EmptyIterators.partition();
        }

        public PartitionIterator executeInternal(ReadExecutionController controller)
        {
            return EmptyIterators.partition();
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return EmptyIterators.unfilteredPartition(executionController.metaData(), false);
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

        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return false;
        }

        @Override
        public boolean selectsFullPartition()
        {
            return false;
        }
    };

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
     * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
     * {@code consistency} is a serial consistency.
     *
     * @return the result of the query.
     */
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException;

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
     * @param controller the {@code ReadExecutionController} protecting the read.
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
    public boolean selectsClustering(DecoratedKey key, Clustering clustering);

    /**
     * Checks if this {@code ReadQuery} selects full partitions, that is it has no filtering on clustering or regular columns.
     * @return {@code true} if this {@code ReadQuery} selects full partitions, {@code false} otherwise.
     */
    public boolean selectsFullPartition();
}
