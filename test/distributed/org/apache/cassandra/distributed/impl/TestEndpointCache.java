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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;

public class TestEndpointCache
{
    private static final Map<InetAddressAndPort, InetSocketAddress> cache = new ConcurrentHashMap<>();
    private static final Map<InetSocketAddress, InetAddressAndPort> cacheInverse = new ConcurrentHashMap<>();

    public static InetAddressAndPort toCassandraInetAddressAndPort(InetSocketAddress addressAndPort)
    {
        InetAddressAndPort m = cacheInverse.get(addressAndPort);
        if (m == null)
        {
            m = InetAddressAndPort.getByAddressOverrideDefaults(addressAndPort.getAddress(), addressAndPort.getPort());
            cacheInverse.put(addressAndPort, m);
        }
        return m;
    }

    public static InetSocketAddress fromCassandraInetAddressAndPort(InetAddressAndPort addressAndPort)
    {
        InetSocketAddress m = cache.get(addressAndPort);
        if (m == null)
        {
            m = NetworkTopology.addressAndPort(addressAndPort.getAddress(), addressAndPort.getPort());
            cache.put(addressAndPort, m);
        }
        return m;
    }
}
