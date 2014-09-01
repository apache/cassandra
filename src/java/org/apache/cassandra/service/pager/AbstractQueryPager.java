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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;

abstract class AbstractQueryPager implements QueryPager
{
    protected final ReadCommand command;
    protected final DataLimits limits;

    private int remaining;

    // This is the last key we've been reading from (or can still be reading within). This the key for
    // which remainingInPartition makes sense: if we're starting another key, we should reset remainingInPartition
    // (and this is done in PagerIterator). This can be null (when we start).
    private DecoratedKey lastKey;
    private int remainingInPartition;

    private boolean exhausted;

    protected AbstractQueryPager(ReadCommand command)
    {
        this.command = command;
        this.limits = command.limits();

        this.remaining = limits.count();
        this.remainingInPartition = limits.perPartitionCount();
    }

    public ReadOrderGroup startOrderGroup()
    {
        return command.startOrderGroup();
    }

    public PartitionIterator fetchPage(int pageSize, ConsistencyLevel consistency, ClientState clientState) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return PartitionIterators.EMPTY;

        pageSize = Math.min(pageSize, remaining);
        return new PagerIterator(nextPageReadCommand(pageSize).execute(consistency, clientState), limits.forPaging(pageSize), command.nowInSec());
    }

    public PartitionIterator fetchPageInternal(int pageSize, ReadOrderGroup orderGroup) throws RequestValidationException, RequestExecutionException
    {
        if (isExhausted())
            return PartitionIterators.EMPTY;

        pageSize = Math.min(pageSize, remaining);
        return new PagerIterator(nextPageReadCommand(pageSize).executeInternal(orderGroup), limits.forPaging(pageSize), command.nowInSec());
    }

    private class PagerIterator extends CountingPartitionIterator
    {
        private final DataLimits pageLimits;

        private Row lastRow;

        private PagerIterator(PartitionIterator iter, DataLimits pageLimits, int nowInSec)
        {
            super(iter, pageLimits, nowInSec);
            this.pageLimits = pageLimits;
        }

        @Override
        @SuppressWarnings("resource") // iter is closed by closing the result
        public RowIterator next()
        {
            RowIterator iter = super.next();
            try
            {
                DecoratedKey key = iter.partitionKey();
                if (lastKey == null || !lastKey.equals(key))
                    remainingInPartition = limits.perPartitionCount();

                lastKey = key;
                return new RowPagerIterator(iter);
            }
            catch (RuntimeException e)
            {
                iter.close();
                throw e;
            }
        }

        @Override
        public void close()
        {
            super.close();
            recordLast(lastKey, lastRow);

            int counted = counter.counted();
            remaining -= counted;
            remainingInPartition -= counter.countedInCurrentPartition();
            exhausted = counted < pageLimits.count();
        }

        private class RowPagerIterator extends WrappingRowIterator
        {
            RowPagerIterator(RowIterator iter)
            {
                super(iter);
            }

            @Override
            public Row next()
            {
                lastRow = super.next();
                return lastRow;
            }
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
}
