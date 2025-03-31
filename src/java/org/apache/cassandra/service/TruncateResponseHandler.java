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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.net.CallbackResponseTracker;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Condition;

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

    protected final Condition condition = newOneTimeCondition();
    private final long start;
    private final CallbackResponseTracker tracker;

    public TruncateResponseHandler(Set<InetAddressAndPort> endpoints)
    {
        // at most one node per range can bootstrap at a time, and these will be added to the write until
        // bootstrap finishes (at which point we no longer need to write to the old ones).
        Preconditions.checkArgument(!endpoints.isEmpty(), "Need non-zero number of replicas to send truncate to.");

        // We need all to ack a truncate for it to be good.
        tracker = new CallbackResponseTracker(endpoints, endpoints.size());
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

        Map<InetAddressAndPort, RequestFailureReason> failures = tracker.endProcessing();
        if (signaled && tracker.isSuccessful())
            return;

        String msgRider = failures.isEmpty()
                          ? String.format("received %d of %d required successes to truncate", tracker.responseCount(), tracker.requiredResponses)
                          : String.format("must have all %d replicas succeed to truncate; saw %d failures", tracker.requiredResponses, failures.size());

        if (!signaled && tracker.isTimeout())
            throw new TimeoutException(tracker.buildParticipantString(String.format("truncate timeout: %s", msgRider)));
        else if (!tracker.isSuccessful())
            throw new TruncateException(tracker.buildParticipantString(String.format("truncate failure: %s", msgRider)));
    }

    @Override
    public void onResponse(Message<TruncateResponse> msg)
    {
        tracker.recordResponse(msg == null ? FBUtilities.getBroadcastAddressAndPort() : msg.from());
        if (tracker.isSuccessful())
            condition.signalAll();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        // If the truncation hasn't succeeded on any replica, abort and indicate this back to the client.
        tracker.recordFailure(from, failureReason);
        condition.signalAll();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    @VisibleForTesting
    public int blockFor()
    {
        return tracker.requiredResponses;
    }

    public String getTrackerStatus()
    {
        return tracker.toString();
    }
}
