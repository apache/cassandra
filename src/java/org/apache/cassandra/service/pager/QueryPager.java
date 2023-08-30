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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

/**
 * Perform a query, paging it by page of a given size.
 *
 * This is essentially an iterator of pages. Each call to fetchPage() will
 * return the next page (i.e. the next list of rows) and isExhausted()
 * indicates whether there is more page to fetch. The pageSize will
 * either be in term of cells or in term of CQL3 row, depending on the
 * parameters of the command we page.
 *
 * Please note that the pager might page within rows, so there is no guarantee
 * that successive pages won't return the same row (though with different
 * columns every time).
 *
 * Also, there is no guarantee that fetchPage() won't return an empty list,
 * even if isExhausted() return false (but it is guaranteed to return an empty
 * list *if* isExhausted() return true). Indeed, isExhausted() does *not*
 * trigger a query so in some (fairly rare) case we might not know the paging
 * is done even though it is.
 */
public interface QueryPager
{
    QueryPager EMPTY = new QueryPager()
    {
        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException
        {
            return EmptyIterators.partition();
        }

        public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
        {
            return EmptyIterators.partition();
        }

        public boolean isExhausted()
        {
            return true;
        }

        public int maxRemaining()
        {
            return 0;
        }

        public PagingState state()
        {
            return null;
        }

        public QueryPager withUpdatedLimit(DataLimits newLimits)
        {
            throw new UnsupportedOperationException();
        }
    };

    /**
     * Fetches the next page.
     *
     * @param pageSize the maximum number of elements to return in the next page.
     * @param consistency the consistency level to achieve for the query.
     * @param clientState the {@code ClientState} for the query. In practice, this can be null unless
     * {@code consistency} is a serial consistency.
     * @return the page of result.
     */
    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestValidationException, RequestExecutionException;

    /**
     * Starts a new read operation.
     * <p>
     * This must be called before {@link fetchPageInternal} and passed to it to protect the read.
     * The returned object <b>must</b> be closed on all path and it is thus strongly advised to
     * use it in a try-with-ressource construction.
     *
     * @return a newly started order group for this {@code QueryPager}.
     */
    public ReadExecutionController executionController();

    /**
     * Fetches the next page internally (in other, this does a local query).
     *
     * @param pageSize the maximum number of elements to return in the next page.
     * @param executionController the {@code ReadExecutionController} protecting the read.
     * @return the page of result.
     */
    public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException;

    /**
     * Whether or not this pager is exhausted, i.e. whether or not a call to
     * fetchPage may return more result.
     *
     * @return whether the pager is exhausted.
     */
    public boolean isExhausted();

    /**
     * The maximum number of cells/CQL3 row that we may still have to return.
     * In other words, that's the initial user limit minus what we've already
     * returned (note that it's not how many we *will* return, just the upper
     * limit on it).
     */
    public int maxRemaining();

    /**
     * Get the current state of the pager. The state can allow to restart the
     * paging on another host from where we are at this point.
     *
     * @return the current paging state. Will return null if paging is at the
     * beginning. If the pager is exhausted, the result is undefined.
     */
    public PagingState state();

    /**
     * Creates a new <code>QueryPager</code> that use the new limits.
     *
     * @param newLimits the new limits
     * @return a new <code>QueryPager</code> that use the new limits
     */
    public QueryPager withUpdatedLimit(DataLimits newLimits);


    /**
     * @return true given read query is a top-k request
     */
    default boolean isTopK()
    {
        return false;
    }
}
