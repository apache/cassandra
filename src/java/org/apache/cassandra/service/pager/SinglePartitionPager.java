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
import java.util.StringJoiner;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Common interface to single partition queries (by slice and by name).
 * <p>
 * For use by MultiPartitionPager.
 */
public class SinglePartitionPager extends AbstractQueryPager<SinglePartitionReadQuery>
{
    private volatile PagingState.RowMark lastReturned;

    public SinglePartitionPager(SinglePartitionReadQuery query, PagingState state, ProtocolVersion protocolVersion)
    {
        super(query, protocolVersion);

        if (state != null)
        {
            lastReturned = state.rowMark;
            restoreState(query.partitionKey(), state.remaining, state.remainingInPartition);
        }
    }

    private SinglePartitionPager(SinglePartitionReadQuery query,
                                 ProtocolVersion protocolVersion,
                                 PagingState.RowMark rowMark,
                                 int remaining,
                                 int remainingInPartition)
    {
        super(query, protocolVersion);
        this.lastReturned = rowMark;
        restoreState(query.partitionKey(), remaining, remainingInPartition);
    }

    @Override
    public SinglePartitionPager withUpdatedLimit(DataLimits newLimits)
    {
        return new SinglePartitionPager(query.withUpdatedLimit(newLimits),
                                        protocolVersion,
                                        lastReturned,
                                        maxRemaining(),
                                        remainingInPartition());
    }

    public ByteBuffer key()
    {
        return query.partitionKey().getKey();
    }

    public DataLimits limits()
    {
        return query.limits();
    }

    public PagingState state()
    {
        return lastReturned == null
               ? null
               : new PagingState(null, lastReturned, maxRemaining(), remainingInPartition());
    }

    @Override
    protected SinglePartitionReadQuery nextPageReadQuery(PageSize pageSize, DataLimits limits)
    {
        Clustering<?> clustering = lastReturned == null ? null : lastReturned.clustering(query.metadata());
        limits = lastReturned == null
                 ? limits.forPaging(pageSize)
                 : limits.forPaging(pageSize, key(), remainingInPartition());

        return query.forPaging(clustering, limits);
    }

    @Override
    public boolean isExhausted()
    {
        return super.isExhausted() || remainingInPartition() == 0;
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null && last.clustering() != Clustering.STATIC_CLUSTERING)
            lastReturned = PagingState.RowMark.create(query.metadata(), last, protocolVersion);
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        return lastReturned != null;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SinglePartitionPager.class.getSimpleName() + "[", "]")
               .add("super=" + super.toString())
               .add("lastReturned=" + (lastReturned != null ? lastReturned.clustering(query.metadata()).toString(query.metadata()) : null))
               .toString();
    }
}
