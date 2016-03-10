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
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

abstract class AbstractQueryPager implements QueryPager
{
    protected final ReadCommand command;
    protected final DataLimits limits;
    protected final int protocolVersion;

    private int remaining;

    // This is the last key we've been reading from (or can still be reading within). This the key for
    // which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
    // (and this is done in PagerIterator). This can be null (when we start).
    private DecoratedKey lastKey;
    private int remainingInPartition;

    private boolean exhausted;

    protected AbstractQueryPager(ReadCommand command, int protocolVersion)
    {
        this.command = command;
        this.protocolVersion = protocolVersion;
        this.limits = command.limits();

        this.remaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();
    }

    public ReadExecutionController executionController()
    {
        return command.executionController();
    }

    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return EmptyIterators.partition();

        pageSize = Math.min(pageSize, remaining);
        Pager pager = new Pager(limits.forPaging(pageSize), command.nowInSec());
        return Transformation.apply(nextPageReadCommand(pageSize).execute(consistency, clientState), pager);
    }

    public PartitionIterator fetchPageInternal(int pageSize, ReadExecutionController executionController) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return EmptyIterators.partition();

        pageSize = Math.min(pageSize, remaining);
        Pager pager = new Pager(limits.forPaging(pageSize), command.nowInSec());
        return Transformation.apply(nextPageReadCommand(pageSize).executeInternal(executionController), pager);
    }

    private class Pager extends Transformation<RowIterator>
    {
        private final DataLimits pageLimits;
        private final DataLimits.Counter counter;
        private Row lastRow;
        private boolean isFirstPartition = true;

        private Pager(DataLimits pageLimits, int nowInSec)
        {
            this.counter = pageLimits.newCounter(nowInSec, true);
            this.pageLimits = pageLimits;
        }

        @Override
        public RowIterator applyToPartition(RowIterator partition)
        {
            DecoratedKey key = partition.partitionKey();
            if (lastKey == null || !lastKey.equals(key))
                remainingInPartition = limits.perPartitionCount();
            lastKey = key;

            // If this is the first partition of this page, this could be the continuation of a partition we've started
            // on the previous page. In which case, we could have the problem that the partition has no more "regular"
            // rows (but the page size is such we didn't knew before) but it does has a static row. We should then skip
            // the partition as returning it would means to the upper layer that the partition has "only" static columns,
            // which is not the case (and we know the static results have been sent on the previous page).
            if (isFirstPartition)
            {
                isFirstPartition = false;
                if (isPreviouslyReturnedPartition(key) && !partition.hasNext())
                {
                    partition.close();
                    return null;
                }
            }

            return Transformation.apply(counter.applyTo(partition), this);
        }

        @Override
        public void onClose()
        {
            recordLast(lastKey, lastRow);

            int counted = counter.counted();
            remaining -= counted;
            // If the clustering of the last row returned is a static one, it means that the partition was only
            // containing data within the static columns. If the clustering of the last row returned is empty
            // it means that there is only one row per partition. Therefore, in both cases there are no data remaining
            // within the partition.
            if (lastRow != null && (lastRow.clustering() == Clustering.STATIC_CLUSTERING
                    || lastRow.clustering() == Clustering.EMPTY))
            {
                remainingInPartition = 0;
            }
            else
            {
                remainingInPartition -= counter.countedInCurrentPartition();
            }
            exhausted = counted < pageLimits.count();
        }

        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                lastRow = row;
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            lastRow = row;
            return row;
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
        return exhausted || remaining == 0 || ((this instanceof SinglePartitionPager) && remainingInPartition == 0);
    }

    public int maxRemaining()
    {
        return remaining;
    }

    protected int remainingInPartition()
    {
        return remainingInPartition;
    }

    protected abstract ReadCommand nextPageReadCommand(int pageSize);
    protected abstract void recordLast(DecoratedKey key, Row row);
    protected abstract boolean isPreviouslyReturnedPartition(DecoratedKey key);
}
