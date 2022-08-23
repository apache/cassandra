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
package org.apache.cassandra.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.concurrent.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;


import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getTruncateRpcTimeout;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class TruncateResponseHandler implements RequestCallback<TruncateResponse>
{
    protected static final Logger logger = LoggerFactory.getLogger(TruncateResponseHandler.class);
    protected final Condition condition = newOneTimeCondition();
    private final int responseCount;
    protected final AtomicInteger responses = new AtomicInteger(0);
    private final long start;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint = new ConcurrentHashMap<>();

    public TruncateResponseHandler(int responseCount)
    {
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        assert 1 <= responseCount: "invalid response count " + responseCount;

        this.responseCount = responseCount;
        start = nanoTime();
    }

    public void get() throws TimeoutException
    {
        long timeoutNanos = getTruncateRpcTimeout(NANOSECONDS) - (nanoTime() - start);
        boolean signaled;
        try
        {
            signaled = condition.await(timeoutNanos, NANOSECONDS); // TODO truncate needs a much longer timeout
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }

        if (!signaled)
            throw new TimeoutException("Truncate timed out - received only " + responses.get() + " responses");

        if (!failureReasonByEndpoint.isEmpty())
        {
            // clone to make sure no race condition happens
            Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint = new HashMap<>(this.failureReasonByEndpoint);
            if (RequestCallback.isTimeout(failureReasonByEndpoint))
                throw new TimeoutException("Truncate timed out - received only " + responses.get() + " responses");

            StringBuilder sb = new StringBuilder("Truncate failed on ");
            for (Map.Entry<InetAddressAndPort, RequestFailureReason> e : failureReasonByEndpoint.entrySet())
                sb.append("replica ").append(e.getKey()).append(" -> ").append(e.getValue()).append(", ");
            sb.setLength(sb.length() - 2);
            throw new TruncateException(sb.toString());
        }
    }

    @Override
    public void onResponse(Message<TruncateResponse> message)
    {
        responses.incrementAndGet();
        if (responses.get() >= responseCount)
            condition.signalAll();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        // If the truncation hasn't succeeded on some replica, abort and indicate this back to the client.
        failureReasonByEndpoint.put(from, failureReason);
        condition.signalAll();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }
}
