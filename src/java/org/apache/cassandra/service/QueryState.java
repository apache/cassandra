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

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Primarily used as a recorder for server-generated timestamps (timestamp, in microseconds, and nowInSeconds - in, well, seconds).
 *
 * The goal is to be able to use a single consistent server-generated value for both timestamps across the whole request,
 * and later be able to inspect QueryState for the generated values - for logging or other purposes.
 */
public class QueryState
{
    private final ClientState clientState;

    private long timestamp = Long.MIN_VALUE;
    private int nowInSeconds = Integer.MIN_VALUE;
    private final long startTimeNanos;
    private long timeoutInNanos = Long.MAX_VALUE;

    public QueryState(ClientState clientState)
    {
        this(clientState, System.nanoTime());
    }

    public QueryState(ClientState clientState, long timestamp, int nowInSeconds)
    {
        this(clientState, System.nanoTime());
        this.timestamp = timestamp;
        this.nowInSeconds = nowInSeconds;
    }

    @VisibleForTesting
    public QueryState(ClientState clientState, long startTimeNanos)
    {
        this.clientState = clientState;
        this.startTimeNanos = startTimeNanos;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    /**
     * Generate, cache, and record a timestamp value on the server-side.
     *
     * Used in reads for all live and expiring cells, and all kinds of deletion infos.
     *
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getTimestampWithFallback(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public long getTimestamp()
    {
        if (timestamp == Long.MIN_VALUE)
            timestamp = clientState.getTimestamp();
        return timestamp;
    }

    public long getStartTimeNanos()
    {
        return startTimeNanos;
    }

    /**
     * Generate, cache, and record a nowInSeconds value on the server-side.
     *
     * In writes is used for calculating localDeletionTime for tombstones and expiring cells and other deletion infos.
     * In reads used to determine liveness of expiring cells and rows.
     *
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getNowInSecondsWithFallback(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public int getNowInSeconds()
    {
        if (nowInSeconds == Integer.MIN_VALUE)
            nowInSeconds = FBUtilities.nowInSeconds();
        return nowInSeconds;
    }

    public void setTimeoutInNanos(long timeoutInNanos)
    {
        this.timeoutInNanos = timeoutInNanos;
    }

    /**
     * Return the timeout for the query.
     * @param fallback, the function to provide the timeout in the desired {@link TimeUnit} as a fallback.
     *                  It is ignored if there is resolved timeout value.
     * @param timeUnit, the target time unit to convert the timeout value to.
     * @return timeout
     */
    public long getTimeoutWithFallback(ToLongFunction<TimeUnit> fallback, TimeUnit timeUnit)
    {
         if (timeoutInNanos == Long.MAX_VALUE)
         {
             long timeout = fallback.applyAsLong(TimeUnit.NANOSECONDS);
             setTimeoutInNanos(timeout);
         }
         return timeUnit.convert(timeoutInNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Calculate the future time the query should timeout, i.e. `queryStartTime + timeout`
     * @param fallback, the function to provide the timeout in the desired {@link TimeUnit} as a fallback.
     *                  It is ignored if there is resolved timeout value.
     * @param timeUnit, the target time unit to convert the timeout value to.
     * @return the future time
     */
    public long getTimeoutAtWithFallback(ToLongFunction<TimeUnit> fallback, TimeUnit timeUnit)
    {
        if (timeoutInNanos == Long.MAX_VALUE)
        {
            long timeout = fallback.applyAsLong(TimeUnit.NANOSECONDS);
            setTimeoutInNanos(timeout);
        }
        return timeUnit.convert(getStartTimeNanos() + timeoutInNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Calculate the remaning timeout, i.e. `timeout - elapsed`.
     * If there is resolved timeout value, the {@param fallback} is ignored.
     * If there is no resolved timeout value, the calculation falls back to the {@param fallback}.
     *
     * @param fallback, the function to provide the timeout in the desired {@link TimeUnit} as a fallback.
     *                  It is ignored if there is resolved timeout value.
     * @param timeUnit, the target time unit to convert the timeout value to.
     * @return the remaning timeout.
     */
    public long getRemainingTimeoutWithFallback(ToLongFunction<TimeUnit> fallback, TimeUnit timeUnit)
    {
        long timeout = getTimeoutWithFallback(fallback, timeUnit);
        return timeUnit.convert(timeout - getElapsedInNanos(), TimeUnit.NANOSECONDS);
    }

    public long getElapsedInNanos()
    {
        return System.nanoTime() - getStartTimeNanos();
    }

    /**
     * @return server-generated timestamp value, if one had been requested, or Long.MIN_VALUE otherwise
     */
    public long generatedTimestamp()
    {
        return timestamp;
    }

    /**
     * @return server-generated nowInSeconds value, if one had been requested, or Integer.MIN_VALUE otherwise
     */
    public int generatedNowInSeconds()
    {
        return nowInSeconds;
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    public InetAddress getClientAddress()
    {
        return clientState.getClientAddress();
    }
}
