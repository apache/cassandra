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

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql3.CIDR;

/**
 * This interface defines functionality to be provided by algorithm(s) implementing CIDR to CIDR groups mappings,
 * to facilitate the efficient way to find the longest matching CIDR for a given IP
 */
public interface CIDRGroupsMappingTable<V>
{
    default String getIPTypeString(boolean isIPv6)
    {
        return isIPv6 ? "IPv6" : "IPv4";
    }

    static <V> Builder<V> builder(boolean isIPv6)
    {
        return new Builder<>(isIPv6);
    }

    /**
     * Finds the longest matching CIDR for the given IP
     * @param ip IP to lookup CIDR group
     * @return returns CIDR group(s) associated with the longest matching CIDR for the given IP, returns null if no match found
     */
    public Set<V> lookupLongestMatchForIP(InetAddress ip);


    /**
     * Builder to add CIDR to CIDR groups mappings and construct a mapping table with them
     */
    class Builder<V>
    {
        private final Map<CIDR, Set<V>> cidrMappings = new HashMap<>();
        private final boolean isIPv6;

        private Builder(boolean isIPv6)
        {
            this.isIPv6 = isIPv6;
        }

        public void add(CIDR cidr, V cidrGroup)
        {
            cidrMappings.computeIfAbsent(cidr, k -> new HashSet<>()).add(cidrGroup);
        }

        /**
         * Provides an instance of algorithm implementing CIDR to CIDR groups mappings
         * @return returns instance of CIDRGroupsMappingTable
         */
        public CIDRGroupsMappingTable<V> build()
        {
            return new CIDRGroupsMappingIntervalTree<>(isIPv6, cidrMappings);
        }
    }
}
