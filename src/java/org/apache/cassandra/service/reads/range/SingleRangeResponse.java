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

package org.apache.cassandra.service.reads.range;

import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.untracked.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.tracked.TrackedResolver;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

abstract class SingleRangeResponse extends AbstractIterator<RowIterator> implements PartitionIterator
{
    private final ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler;
    protected final ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair;

    private PartitionIterator result;

    SingleRangeResponse(ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler,
                        ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair)
    {
        this.handler = handler;
        this.readRepair = readRepair;
    }

    ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> getReadRepair()
    {
        return readRepair;
    }

    abstract PartitionIterator getResult();

    private void waitForResponse() throws ReadTimeoutException
    {
        if (result != null)
            return;

        handler.awaitResults();
        result = getResult();
    }

    @Override
    protected RowIterator computeNext()
    {
        waitForResponse();
        return result.hasNext() ? result.next() : endOfData();
    }

    @Override
    public void close()
    {
        if (result != null)
            result.close();
    }

    static class Untracked extends SingleRangeResponse
    {
        private final DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver;

        public Untracked(DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver, ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler, ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair)
        {
            super(handler, readRepair);
            this.resolver = resolver;
        }

        @Override
        PartitionIterator getResult()
        {
            return resolver.resolve();
        }
    }

    static class Tracked extends SingleRangeResponse
    {
        private final TrackedResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver;

        public Tracked(ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler, ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair, TrackedResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver)
        {
            super(handler, readRepair);
            this.resolver = resolver;
        }

        @Override
        PartitionIterator getResult()
        {
            if (resolver.responsesMatch())
                return resolver.getData();

            // TODO (prefer): this should work more like AbstractReadExecutor#executeAsync
            // TODO (prefer): this doesn't allow any speculation - legacy replication doesn't either though
            AsyncPromise<PartitionIterator> result = new AsyncPromise<>();
            readRepair.startRepair(resolver, result::trySuccess);
            readRepair.maybeSendAdditionalReads();
            readRepair.awaitReads();
            try
            {
                return result.get();
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
