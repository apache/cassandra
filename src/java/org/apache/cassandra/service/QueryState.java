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
    private long nowInSeconds = Integer.MIN_VALUE;

    public QueryState(ClientState clientState)
    {
        this.clientState = clientState;
    }

    public QueryState(ClientState clientState, long timestamp, long nowInSeconds)
    {
        this(clientState);
        this.timestamp = timestamp;
        this.nowInSeconds = nowInSeconds;
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
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getTimestamp(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public long getTimestamp()
    {
        if (timestamp == Long.MIN_VALUE)
            timestamp = ClientState.getTimestamp();
        return timestamp;
    }

    /**
     * Generate, cache, and record a nowInSeconds value on the server-side.
     *
     * In writes is used for calculating localDeletionTime for tombstones and expiring cells and other deletion infos.
     * In reads used to determine liveness of expiring cells and rows.
     *
     * Shouldn't be used directly. {@link org.apache.cassandra.cql3.QueryOptions#getNowInSeconds(QueryState)} should be used
     * by all consumers.
     *
     * @return server-generated, recorded timestamp in seconds
     */
    public long getNowInSeconds()
    {
        if (nowInSeconds == Integer.MIN_VALUE)
            nowInSeconds = FBUtilities.nowInSeconds();
        return nowInSeconds;
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
    public long generatedNowInSeconds()
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
