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

import java.net.InetSocketAddress;

import org.apache.cassandra.config.DatabaseDescriptor;
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
    private final QueryState queryState;

    public ThriftClientState(InetSocketAddress remoteAddress)
    {
        super(remoteAddress);
        this.queryState = new QueryState(this);
    }

    public QueryState getQueryState()
    {
        return queryState;
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
