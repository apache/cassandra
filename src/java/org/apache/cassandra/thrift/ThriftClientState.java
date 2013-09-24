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
package org.apache.cassandra.thrift;

import java.net.SocketAddress;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

/**
 * ClientState used by thrift that also provide a QueryState.
 *
 * Thrift is intrinsically synchronous so there could be only one query per
 * client at a given time. So ClientState and QueryState can be merge into the
 * same object.
 */
public class ThriftClientState extends ClientState
{
    private static final int MAX_CACHE_PREPARED = 10000;    // Enough to keep buggy clients from OOM'ing us

    private final QueryState queryState;

    // An LRU map of prepared statements
    private final Map<Integer, CQLStatement> prepared = new LinkedHashMap<Integer, CQLStatement>(16, 0.75f, true)
    {
        protected boolean removeEldestEntry(Map.Entry<Integer, CQLStatement> eldest)
        {
            return size() > MAX_CACHE_PREPARED;
        }
    };

    public ThriftClientState(SocketAddress remoteAddress)
    {
        super(remoteAddress);
        this.queryState = new QueryState(this);
    }

    public QueryState getQueryState()
    {
        return queryState;
    }

    public Map<Integer, CQLStatement> getPrepared()
    {
        return prepared;
    }

    public String getSchedulingValue()
    {
        switch(DatabaseDescriptor.getRequestSchedulerId())
        {
            case keyspace: return getRawKeyspace();
        }
        return "default";
    }
}
