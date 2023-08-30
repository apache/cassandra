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

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Pages a PartitionRangeReadQuery.
 */
public class PartitionRangeQueryPager extends AbstractQueryPager<PartitionRangeReadQuery>
{
    private volatile DecoratedKey lastReturnedKey;
    private volatile PagingState.RowMark lastReturnedRow;

    public PartitionRangeQueryPager(PartitionRangeReadQuery query, PagingState state, ProtocolVersion protocolVersion)
    {
        super(query, protocolVersion);

        if (state != null)
        {
            lastReturnedKey = query.metadata().partitioner.decorateKey(state.partitionKey);
            lastReturnedRow = state.rowMark;
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }
    }

    public PartitionRangeQueryPager(PartitionRangeReadQuery query,
                                    ProtocolVersion protocolVersion,
                                    DecoratedKey lastReturnedKey,
                                    PagingState.RowMark lastReturnedRow,
                                    int remaining,
                                    int remainingInPartition)
    {
        super(query, protocolVersion);
        this.lastReturnedKey = lastReturnedKey;
        this.lastReturnedRow = lastReturnedRow;
        restoreState(lastReturnedKey, remaining, remainingInPartition);
    }

    public PartitionRangeQueryPager withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeQueryPager(query.withUpdatedLimit(newLimits),
                                            protocolVersion,
                                            lastReturnedKey,
                                            lastReturnedRow,
                                            maxRemaining(),
                                            remainingInPartition());
    }

    public PagingState state()
    {
        return lastReturnedKey == null
             ? null
             : new PagingState(lastReturnedKey.getKey(), lastReturnedRow, maxRemaining(), remainingInPartition());
    }

    @Override
    protected PartitionRangeReadQuery nextPageReadQuery(int pageSize)
    {
        DataLimits limits;
        DataRange fullRange = query.dataRange();
        DataRange pageRange;
        if (lastReturnedKey == null)
        {
            pageRange = fullRange;
            limits = query.limits().forPaging(pageSize);
        }
        // if the last key was the one of the end of the range we know that we are done
        else if (lastReturnedKey.equals(fullRange.keyRange().right) && remainingInPartition() == 0 && lastReturnedRow == null)
        {
            return null;
        }
        else
        {
            // We want to include the last returned key only if we haven't achieved our per-partition limit, otherwise, don't bother.
            boolean includeLastKey = remainingInPartition() > 0 && lastReturnedRow != null;
            AbstractBounds<PartitionPosition> bounds = makeKeyBounds(lastReturnedKey, includeLastKey);
            if (includeLastKey)
            {
                pageRange = fullRange.forPaging(bounds, query.metadata().comparator, lastReturnedRow.clustering(query.metadata()), false);
                limits = query.limits().forPaging(pageSize, lastReturnedKey.getKey(), remainingInPartition());
            }
            else
            {
                pageRange = fullRange.forSubRange(bounds);
                limits = query.limits().forPaging(pageSize);
            }
        }

        return query.withUpdatedLimitsAndDataRange(limits, pageRange);
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null)
        {
            lastReturnedKey = key;
            if (last.clustering() != Clustering.STATIC_CLUSTERING)
                lastReturnedRow = PagingState.RowMark.create(query.metadata(), last, protocolVersion);
        }
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        // Note that lastReturnedKey can be null, but key cannot.
        return key.equals(lastReturnedKey);
    }

    private AbstractBounds<PartitionPosition> makeKeyBounds(PartitionPosition lastReturnedKey, boolean includeLastKey)
    {
        AbstractBounds<PartitionPosition> bounds = query.dataRange().keyRange();
        if (bounds instanceof Range || bounds instanceof Bounds)
        {
            return includeLastKey
                 ? new Bounds<>(lastReturnedKey, bounds.right)
                 : new Range<>(lastReturnedKey, bounds.right);
        }

        return includeLastKey
             ? new IncludingExcludingBounds<>(lastReturnedKey, bounds.right)
             : new ExcludingBounds<>(lastReturnedKey, bounds.right);
    }

    @Override
    public boolean isTopK()
    {
        return query.isTopK();
    }
}
