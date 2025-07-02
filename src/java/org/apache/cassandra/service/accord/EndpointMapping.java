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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import accord.local.Node;
import accord.utils.Invariants;
import org.apache.cassandra.locator.InetAddressAndPort;

class EndpointMapping implements AccordEndpointMapper
{
    public static final EndpointMapping EMPTY = new EndpointMapping(0, ImmutableBiMap.of(), ImmutableMap.of());
    private final long epoch;
    private final ImmutableBiMap<Node.Id, InetAddressAndPort> mapping;
    private final ImmutableMap<Node.Id, Long> removedNodes;

    private EndpointMapping(long epoch,
                            ImmutableBiMap<Node.Id, InetAddressAndPort> mapping,
                            ImmutableMap<Node.Id, Long> removedNodes)
    {
        this.epoch = epoch;
        this.mapping = mapping;
        this.removedNodes = removedNodes;
    }

    long epoch()
    {
        return epoch;
    }

    public boolean containsId(Node.Id id)
    {
        return mapping.containsKey(id);
    }

    public List<Node.Id> nodes()
    {
        return new ArrayList<>(mapping.keySet());
    }

    public Map<Node.Id, Long> removedNodes()
    {
        return removedNodes;
    }

    @Override
    public Node.Id mappedIdOrNull(InetAddressAndPort endpoint)
    {
        return mapping.inverse().get(endpoint);
    }

    @Override
    public InetAddressAndPort mappedEndpointOrNull(Node.Id id)
    {
        return mapping.get(id);
    }

    static class Builder
    {
        private final long epoch;
        private final BiMap<Node.Id, InetAddressAndPort> mapping = HashBiMap.create();
        private final ImmutableMap.Builder<Node.Id, Long> removed = new ImmutableMap.Builder<>();

        public Builder(long epoch)
        {
            this.epoch = epoch;
        }

        public Builder add(InetAddressAndPort endpoint, Node.Id id)
        {
            Invariants.requireArgument(!mapping.containsKey(id), "Mapping already exists for Node.Id %s", id);
            Invariants.requireArgument(!mapping.containsValue(endpoint), "Mapping already exists for %s", endpoint);
            mapping.put(id, endpoint);
            return this;
        }

        public Builder removed(InetAddressAndPort endpoint, Node.Id id, long epoch)
        {
            Invariants.requireArgument(!mapping.containsKey(id), "Mapping already exists for Node.Id %s", id);
            Invariants.requireArgument(!mapping.containsValue(endpoint), "Mapping already exists for %s", endpoint);
            mapping.put(id, endpoint);
            removed.put(id, epoch);
            return this;
        }

        public EndpointMapping build()
        {
            return new EndpointMapping(epoch, ImmutableBiMap.copyOf(mapping), removed.build());
        }
    }

    static Builder builder(long epoch)
    {
        return new Builder(epoch);
    }
}
