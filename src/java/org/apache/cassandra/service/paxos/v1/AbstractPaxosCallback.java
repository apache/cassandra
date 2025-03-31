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

import java.util.Collection;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CallbackResponseTracker;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class AbstractPaxosCallback<T> implements RequestCallback<T>
{
    protected final CountDownLatch latch;
    private final ConsistencyLevel consistency;
    private final Dispatcher.RequestTime requestTime;

    protected final CallbackResponseTracker tracker;

    protected AbstractPaxosCallback(Collection<InetAddressAndPort> endpoints, int requiredResponses, ConsistencyLevel consistency, Dispatcher.RequestTime requestTime)
    {
        tracker = new CallbackResponseTracker(endpoints, requiredResponses);
        latch = CountDownLatch.newCountDownLatch(endpoints.size());
        this.consistency = consistency;
        this.requestTime = requestTime;
    }

    public int getResponseCount()
    {
        return tracker.participantCount() - latch.count();
    }

    public void await() throws WriteTimeoutException
    {
        try
        {
            long now = Clock.Global.nanoTime();
            long timeout = requestTime.computeTimeout(now, DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS));

            if (!latch.await(timeout, NANOSECONDS))
            {
                String errorMessage = RequestFailureReason.buildErrorMessage("CAS operation timed out", tracker.endProcessing());
                throw new WriteTimeoutException(WriteType.CAS, consistency, getResponseCount(), tracker.requiredResponses, errorMessage);
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    public Map<InetAddressAndPort, RequestFailureReason> getFailureMap()
    {
        return tracker.endProcessing();
    }

    protected void signal()
    {
        while (latch.count() > 0)
            latch.decrement();
    }

    @VisibleForTesting
    public int blockFor()
    {
        return tracker.requiredResponses;
    }

    @VisibleForTesting
    public CallbackResponseTracker responseTracker()
    {
        return tracker;
    }
}
