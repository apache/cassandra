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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;

/**
 * Common interface to single partition queries (by slice and by name).
 *
 * For use by MultiPartitionPager.
 */
public class SinglePartitionPager extends AbstractQueryPager
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionPager.class);

    private final SinglePartitionReadCommand command;

    private volatile PagingState.RowMark lastReturned;

    public SinglePartitionPager(SinglePartitionReadCommand command, PagingState state, int protocolVersion)
    {
        super(command, protocolVersion);
        this.command = command;

        if (state != null)
        {
            lastReturned = state.rowMark;
            restoreState(command.partitionKey(), state.remaining, state.remainingInPartition);
        }
    }

    public ByteBuffer key()
    {
        return command.partitionKey().getKey();
    }

    public DataLimits limits()
    {
        return command.limits();
    }

    public PagingState state()
    {
        return lastReturned == null
             ? null
             : new PagingState(null, lastReturned, maxRemaining(), remainingInPartition());
    }

    protected ReadCommand nextPageReadCommand(int pageSize)
    {
        return command.forPaging(lastReturned == null ? null : lastReturned.clustering(command.metadata()), pageSize);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null && last.clustering() != Clustering.STATIC_CLUSTERING)
            lastReturned = PagingState.RowMark.create(command.metadata(), last, protocolVersion);
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        return lastReturned != null;
    }
}
