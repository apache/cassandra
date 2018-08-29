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
 * Represents the state related to a given query.
 */
public class QueryState
{
    private final ClientState clientState;

    private long timestamp = Long.MIN_VALUE;
    private int nowInSeconds = Integer.MIN_VALUE;

    public QueryState(ClientState clientState)
    {
        this.clientState = clientState;
    }

    /**
     * @return a QueryState object for internal C* calls (not limited by any kind of auth).
     */
    public static QueryState forInternalCalls()
    {
        return new QueryState(ClientState.forInternalCalls());
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    public InetAddress getClientAddress()
    {
        return clientState.getClientAddress();
    }

    public long getTimestamp()
    {
        if (timestamp == Long.MIN_VALUE)
            timestamp = clientState.getTimestamp();
        return timestamp;
    }

    public int getNowInSeconds()
    {
        if (nowInSeconds == Integer.MIN_VALUE)
            nowInSeconds = FBUtilities.nowInSeconds();
        return nowInSeconds;
    }
}
