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

package org.apache.cassandra.service.accord;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;

import accord.local.Node;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

public class EndpointMapping
{
    static Node.Id endpointToId(InetAddressAndPort endpoint)
    {
        Preconditions.checkArgument(endpoint.getAddress() instanceof Inet4Address);
        Inet4Address address = (Inet4Address) endpoint.getAddress();
        byte[] bytes = address.getAddress();
        long id = 0;
        for (int i=0; i<4; i++)
            id = (id * 1000) + Byte.toUnsignedLong(bytes[i]);
        id = (id * 100000) + endpoint.getPort();
        return new Node.Id(id);
    }

    static InetAddressAndPort idToEndpoint(Node.Id node)
    {
        long id = node.id;
        Preconditions.checkArgument(id >= 0);

        int port = (int) (id % 100000);
        id = id / 100000;
        byte[] bytes = new byte[4];
        for (int i=0; i<4; i++)
        {
            long octet = id % 1000;
            Preconditions.checkArgument(octet >= 0 && octet <= 255, "Malformed id");
            bytes[3-i] = (byte) (octet);
            id = id / 1000;
        }
        Preconditions.checkArgument(id == 0);
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), port);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    // TODO: Remove this if its one usage in AccordConfigurationService is removed.
    public static ImmutableCollection<Node.Id> knownIds()
    {
        return mapping.endpointToId.values();
    }

    private static class Mapping
    {
        private static final Mapping EMPTY = new Mapping(ImmutableMap.of(), ImmutableMap.of());
        final ImmutableMap<Node.Id, InetAddressAndPort> idToEndpoint;
        final ImmutableMap<InetAddressAndPort, Node.Id> endpointToId;

        public Mapping(ImmutableMap<Node.Id, InetAddressAndPort> idToEndpoint,
                       ImmutableMap<InetAddressAndPort, Node.Id> endpointToId)
        {
            this.idToEndpoint = idToEndpoint;
            this.endpointToId = endpointToId;
        }

        private static <K, V> ImmutableMap<K, V> put(ImmutableMap<K, V> current, K key, V val)
        {
            return ImmutableMap.<K, V>builderWithExpectedSize(current.size() + 1).putAll(current).put(key, val).build();
        }

        public Mapping add(InetAddressAndPort endpoint)
        {
            if (endpointToId.containsKey(endpoint))
                return this;
            Node.Id id = endpointToId(endpoint);
            return new Mapping(put(idToEndpoint, id, endpoint), put(endpointToId, endpoint, id));
        }

        public Mapping add(Node.Id id)
        {
            if (idToEndpoint.containsKey(id))
                return this;

            InetAddressAndPort endpoint = idToEndpoint(id);
            return new Mapping(put(idToEndpoint, id, endpoint), put(endpointToId, endpoint, id));
        }
    }

    private static volatile Mapping mapping = Mapping.EMPTY;

    private EndpointMapping() {}

    public static Node.Id getId(InetAddressAndPort endpoint)
    {
        Node.Id id = mapping.endpointToId.get(endpoint);
        if (id == null)
        {
            synchronized (EndpointMapping.class)
            {
                mapping = mapping.add(endpoint);
                id = mapping.endpointToId.get(endpoint);
            }
        }
        return id;
    }

    // FIXME: put this stuff into the configuration service, where it will eventually live
    public static Node.Id getId(Replica replica)
    {
        return getId(replica.endpoint());
    }

    public static InetAddressAndPort getEndpoint(Node.Id id)
    {
        InetAddressAndPort endpoint = mapping.idToEndpoint.get(id);
        if (endpoint == null)
        {
            synchronized (EndpointMapping.class)
            {
                mapping = mapping.add(id);
                endpoint = mapping.idToEndpoint.get(id);
            }
        }
        return endpoint;
    }
}
