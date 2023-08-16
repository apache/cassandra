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

import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Sets;

import accord.local.Node;
import accord.utils.Invariants;
import org.apache.cassandra.locator.InetAddressAndPort;

class EndpointMapping implements AccordEndpointMapper
{
    public static final EndpointMapping EMPTY = new EndpointMapping(0, ImmutableBiMap.of());
    private final long epoch;
    private final ImmutableBiMap<Node.Id, InetAddressAndPort> mapping;

    private EndpointMapping(long epoch,
                            ImmutableBiMap<Node.Id, InetAddressAndPort> mapping)
    {
        this.epoch = epoch;
        this.mapping = mapping;
    }

    long epoch()
    {
        return epoch;
    }

    public boolean containsId(Node.Id id)
    {
        return mapping.containsKey(id);
    }

    public Set<Node.Id> differenceIds(Builder builder)
    {
        return Sets.difference(mapping.keySet(), builder.mapping.keySet());
    }

    @Override
    public Node.Id mappedId(InetAddressAndPort endpoint)
    {
        return mapping.inverse().get(endpoint);
    }

    @Override
    public InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        return mapping.get(id);
    }

    static class Builder
    {
        private final long epoch;
        private final BiMap<Node.Id, InetAddressAndPort> mapping = HashBiMap.create();

        public Builder(long epoch)
        {
            this.epoch = epoch;
        }

        public Builder add(InetAddressAndPort endpoint, Node.Id id)
        {
            Invariants.checkArgument(!mapping.containsKey(id), "Mapping already exists for Node.Id %s", id);
            Invariants.checkArgument(!mapping.containsValue(endpoint), "Mapping already exists for %s", endpoint);
            mapping.put(id, endpoint);
            return this;
        }

        public EndpointMapping build()
        {
            return new EndpointMapping(epoch, ImmutableBiMap.copyOf(mapping));
        }
    }

    static Builder builder(long epoch)
    {
        return new Builder(epoch);
    }
}
