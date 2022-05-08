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
package org.apache.cassandra.service.paxos.v1;

import org.apache.cassandra.utils.concurrent.CountDownLatch;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public abstract class AbstractPaxosCallback<T> implements RequestCallback<T>
{
    protected final CountDownLatch latch;
    protected final int targets;
    private final ConsistencyLevel consistency;
    private final long queryStartNanoTime;

    public AbstractPaxosCallback(int targets, ConsistencyLevel consistency, long queryStartNanoTime)
    {
        this.targets = targets;
        this.consistency = consistency;
        latch = newCountDownLatch(targets);
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public int getResponseCount()
    {
        return targets - latch.count();
    }

    public void await() throws WriteTimeoutException
    {
        try
        {
            long timeout = DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS) - (nanoTime() - queryStartNanoTime);
            if (!latch.await(timeout, NANOSECONDS))
                throw new WriteTimeoutException(WriteType.CAS, consistency, getResponseCount(), targets);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }
}
