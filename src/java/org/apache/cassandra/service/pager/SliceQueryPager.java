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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageProxy;

/**
 * Pager over a SliceFromReadCommand.
 */
public class SliceQueryPager extends AbstractQueryPager implements SinglePartitionPager
{
    private final SliceFromReadCommand command;

    private volatile CellName lastReturned;

    // Don't use directly, use QueryPagers method instead
    SliceQueryPager(SliceFromReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery)
    {
        super(consistencyLevel, command.filter.count, localQuery, command.ksName, command.cfName, command.filter, command.timestamp);
        this.command = command;
    }

    SliceQueryPager(SliceFromReadCommand command, ConsistencyLevel consistencyLevel, boolean localQuery, PagingState state)
    {
        this(command, consistencyLevel, localQuery);

        if (state != null)
        {
            lastReturned = cfm.comparator.cellFromByteBuffer(state.cellName);
            restoreState(state.remaining, true);
        }
    }

    public ByteBuffer key()
    {
        return command.key;
    }

    public PagingState state()
    {
        return lastReturned == null
             ? null
             : new PagingState(null, lastReturned.toByteBuffer(), maxRemaining());
    }

    protected List<Row> queryNextPage(int pageSize, ConsistencyLevel consistencyLevel, boolean localQuery)
    throws RequestValidationException, RequestExecutionException
    {
        SliceQueryFilter filter = command.filter.withUpdatedCount(pageSize);
        if (lastReturned != null)
            filter = filter.withUpdatedStart(lastReturned, cfm.comparator);

        ReadCommand pageCmd = command.withUpdatedFilter(filter);
        return localQuery
             ? Collections.singletonList(pageCmd.getRow(Keyspace.open(command.ksName)))
             : StorageProxy.read(Collections.singletonList(pageCmd), consistencyLevel);
    }

    protected boolean containsPreviousLast(Row first)
    {
        if (lastReturned == null)
            return false;

        Cell firstCell = isReversed() ? lastCell(first.cf) : firstCell(first.cf);
        // Note: we only return true if the column is the lastReturned *and* it is live. If it is deleted, it is ignored by the
        // rest of the paging code (it hasn't been counted as live in particular) and we want to act as if it wasn't there.
        return !first.cf.deletionInfo().isDeleted(firstCell)
            && firstCell.isLive(timestamp())
            && lastReturned.equals(firstCell.name());
    }

    protected boolean recordLast(Row last)
    {
        Cell lastCell = isReversed() ? firstCell(last.cf) : lastCell(last.cf);
        lastReturned = lastCell.name();
        return true;
    }

    protected boolean isReversed()
    {
        return command.filter.reversed;
    }
}
