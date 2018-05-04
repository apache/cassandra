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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.utils.Clock;

/**
 * This class tracks the last 100 connections per protocol version
 */
public class ProtocolVersionTracker
{
    public static final int DEFAULT_MAX_CAPACITY = 100;

    @VisibleForTesting
    final EnumMap<ProtocolVersion, LoadingCache<InetAddress, Long>> clientsByProtocolVersion;

    public ProtocolVersionTracker()
    {
        this(DEFAULT_MAX_CAPACITY);
    }

    public ProtocolVersionTracker(final int capacity)
    {
        clientsByProtocolVersion = new EnumMap<>(ProtocolVersion.class);

        for (ProtocolVersion version : ProtocolVersion.values())
        {
            clientsByProtocolVersion.put(version, Caffeine.newBuilder().maximumSize(capacity)
                                                          .build(key -> Clock.instance.currentTimeMillis()));
        }
    }

    void addConnection(final InetAddress addr, final ProtocolVersion version)
    {
        if (addr == null || version == null) return;

        LoadingCache<InetAddress, Long> clients = clientsByProtocolVersion.get(version);
        clients.put(addr, Clock.instance.currentTimeMillis());
    }

    public LinkedHashMap<ProtocolVersion, ImmutableSet<ClientIPAndTime>> getAll()
    {
        LinkedHashMap<ProtocolVersion, ImmutableSet<ClientIPAndTime>> result = new LinkedHashMap<>();
        for (ProtocolVersion version : ProtocolVersion.values())
        {
            ImmutableSet.Builder<ClientIPAndTime> ips = ImmutableSet.builder();
            for (Map.Entry<InetAddress, Long> e : clientsByProtocolVersion.get(version).asMap().entrySet())
                ips.add(new ClientIPAndTime(e.getKey(), e.getValue()));
            result.put(version, ips.build());
        }
        return result;
    }

    public void clear()
    {
        for (Map.Entry<ProtocolVersion, LoadingCache<InetAddress, Long>> entry : clientsByProtocolVersion.entrySet())
        {
            entry.getValue().invalidateAll();
        }
    }

    public static class ClientIPAndTime
    {
        final InetAddress inetAddress;
        final long lastSeen;

        public ClientIPAndTime(final InetAddress inetAddress, final long lastSeen)
        {
            Preconditions.checkNotNull(inetAddress);
            this.inetAddress = inetAddress;
            this.lastSeen = lastSeen;
        }

        @Override
        public String toString()
        {
            return "ClientIPAndTime{" +
                   "inetAddress=" + inetAddress +
                   ", lastSeen=" + lastSeen +
                   '}';
        }
    }
}
