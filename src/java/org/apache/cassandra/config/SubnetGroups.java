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
package org.apache.cassandra.config;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import inet.ipaddr.IPAddressNetwork;
import inet.ipaddr.IPAddressString;

/**
 * When a group of subnets are needed, this class can be used to represent the group as if it was a single subnet.
 *
 * This class supports IPV4 and IPV6 subnets
 */
public class SubnetGroups
{
    public Set<Group> subnets = Collections.emptySet();

    public SubnetGroups()
    {
    }

    public SubnetGroups(List<String> values)
    {
        this.subnets = ImmutableSet.copyOf(values.stream().map(Group::new).collect(Collectors.toSet()));
    }

    public boolean contains(SocketAddress address)
    {
        if (address instanceof InetSocketAddress)
        {
            return contains(((InetSocketAddress) address).getAddress());
        }
        throw new IllegalArgumentException("Unsupported socket address type: " + (address == null ? null : address.getClass()));
    }

    public boolean contains(InetAddress address)
    {
        for (Group group : subnets)
        {
            if (group.contains(address))
            {
                return true;
            }
        }
        return false;
    }

    public static class Group
    {
        private static final IPAddressNetwork.IPAddressGenerator IP_ADDRESS_GENERATOR = new IPAddressNetwork.IPAddressGenerator();

        private final IPAddressString subnet;

        public Group(String range)
        {
            subnet = new IPAddressString(range);
        }

        public boolean contains(InetAddress address)
        {
            return subnet.contains(IP_ADDRESS_GENERATOR.from(address).toAddressString());
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Group group = (Group) o;
            return Objects.equals(subnet, group.subnet);
        }

        public int hashCode()
        {
            return Objects.hash(subnet);
        }

        public String toString()
        {
            return subnet.toString();
        }
    }
}
