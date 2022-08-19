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
package org.apache.cassandra.net;

import java.util.Map;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * implementors of {@link RequestCallback} need to make sure that any public methods
 * are threadsafe with respect to {@link #onResponse} being called from the message
 * service.  In particular, if any shared state is referenced, making
 * response alone synchronized will not suffice.
 */
public interface RequestCallback<T>
{
    /**
     * @param msg response received.
     */
    void onResponse(Message<T> msg);

    /**
     * Called when there is an exception on the remote node or timeout happens
     */
    default void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
    }

    /**
     * Returns true if the callback handles failure reporting - in which case the remove host will be asked to
     * report failures to us in the event of a problem processing the request.
     *
     * TODO: this is an error prone method, and we should be handling failures everywhere
     *       so we should probably just start doing that, and remove this method
     *
     * @return true if the callback should be invoked on failure
     */
    default boolean invokeOnFailure()
    {
        return false;
    }

    /**
     * @return true if this callback is on the read path and its latency should be
     * given as input to the dynamic snitch.
     */
    default boolean trackLatencyForSnitch()
    {
        return false;
    }

    static boolean isTimeout(Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint)
    {
        // The reason that all must be timeout to be called a timeout is as follows
        // Assume RF=6, QUORUM, and failureReasonByEndpoint.size() == 3
        // R1 -> TIMEOUT
        // R2 -> TIMEOUT
        // R3 -> READ_TOO_MANY_TOMBSTONES
        // Since we got a reply back, and that was a failure, we should return a failure letting the user know.
        // When all failures are a timeout, then this is a race condition with
        // org.apache.cassandra.utils.concurrent.Awaitable.await(long, java.util.concurrent.TimeUnit)
        // The race is that the message expire path runs and expires all messages, this then casues the condition
        // to signal telling the caller "got all replies!".
        return failureReasonByEndpoint.values().stream().allMatch(RequestFailureReason.TIMEOUT::equals);
    }

}
