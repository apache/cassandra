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
package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * This class tracks the last 100 connections per protocol version
 */
public class ProtocolVersionTracker
{
    private static final int DEFAULT_MAX_CAPACITY = 100;

    private final EnumMap<ProtocolVersion, LoadingCache<InetAddress, Long>> clientsByProtocolVersion;

    ProtocolVersionTracker()
    {
        this(DEFAULT_MAX_CAPACITY);
    }

    private ProtocolVersionTracker(int capacity)
    {
        clientsByProtocolVersion = new EnumMap<>(ProtocolVersion.class);

        for (ProtocolVersion version : ProtocolVersion.values())
        {
            clientsByProtocolVersion.put(version, Caffeine.newBuilder().maximumSize(capacity)
                                                          .build(key -> currentTimeMillis()));
        }
    }

    void addConnection(InetAddress addr, ProtocolVersion version)
    {
        clientsByProtocolVersion.get(version).put(addr, currentTimeMillis());
    }

    List<ClientStat> getAll()
    {
        List<ClientStat> result = new ArrayList<>();

        clientsByProtocolVersion.forEach((version, cache) ->
            cache.asMap().forEach((address, lastSeenTime) -> result.add(new ClientStat(address, version, lastSeenTime))));

        return result;
    }

    List<ClientStat> getAll(ProtocolVersion version)
    {
        List<ClientStat> result = new ArrayList<>();

        clientsByProtocolVersion.get(version).asMap().forEach((address, lastSeenTime) ->
            result.add(new ClientStat(address, version, lastSeenTime)));

        return result;
    }

    public void clear()
    {
        clientsByProtocolVersion.values().forEach(Cache::invalidateAll);
    }
}
