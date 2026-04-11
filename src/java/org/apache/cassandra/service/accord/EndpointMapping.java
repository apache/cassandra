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

import javax.annotation.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.utils.Invariants;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.MINUTES;

class EndpointMapping implements AccordEndpointMapper
{
    static class Updateable implements AccordEndpointMapper
    {
        private volatile EndpointMapping mapping = EMPTY;

        @Nullable
        @Override
        public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, @Nullable Object logIdentityIfUnmapped)
        {
            return mapping.mappedIdOrNull(endpoint, logIdentityIfUnmapped);
        }

        @Nullable
        @Override
        public InetAddressAndPort mappedEndpointOrNull(Node.Id id, @Nullable Object logIdentityIfUnmapped)
        {
            return mapping.mappedEndpointOrNull(id, logIdentityIfUnmapped);
        }

        @Override
        public Map<Node.Id, Long> removedNodes()
        {
            return mapping.removedNodes;
        }

        @Override
        public NodeStatus nodeStatus(Node.Id id)
        {
            return mapping.nodeStatus(id);
        }

        @Override
        public synchronized void updateMapping(ClusterMetadata metadata)
        {
            if (metadata.epoch.getEpoch() > this.mapping.epoch())
                this.mapping = AccordTopology.directoryToMapping(metadata.epoch.getEpoch(), metadata.directory);
        }

        public synchronized void updateMapping(EndpointMapping newMapping)
        {
            if (newMapping.epoch > mapping.epoch)
                mapping = newMapping;
        }
    }


    private static final Logger logger = LoggerFactory.getLogger(EndpointMapping.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, MINUTES);

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

    @Override
    public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, Object logIdentityIfUnmapped)
    {
        Node.Id id = mapping.inverse().get(endpoint);
        if (id != null)
            return id;
        if (logIdentityIfUnmapped == null) noSpamLogger.warn("Could not find Node.Id for endpoint {}", endpoint);
        else noSpamLogger.warn("Could not find Node.Id for endpoint {} on behalf of {}", endpoint, logIdentityIfUnmapped);
        return null;
    }

    @Override
    public InetAddressAndPort mappedEndpointOrNull(Node.Id id, Object logIdentityIfUnmapped)
    {
        InetAddressAndPort ep = mapping.get(id);
        if (ep != null)
            return ep;
        if (logIdentityIfUnmapped == null) noSpamLogger.warn("Could not find InetAddressAndPort for Node.Id {}", id);
        else noSpamLogger.warn("Could not find InetAddressAndPort for Node.Id {} on behalf of {}", id, logIdentityIfUnmapped);
        return null;
    }

    @Override
    public Map<Node.Id, Long> removedNodes()
    {
        return removedNodes;
    }

    @Override
    public NodeStatus nodeStatus(Node.Id id)
    {
        InetAddressAndPort ep = mappedEndpointOrNull(id);
        if (ep == null)
            return NodeStatus.UNKNOWN;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(ep);
        if (epState == null)
            return NodeStatus.UNKNOWN;

        if (!epState.isAlive())
            return NodeStatus.UNHEALTHY;

        VersionedValue event = epState.getApplicationState(ApplicationState.SEVERITY);
        if (event == null)
            return NodeStatus.HEALTHY; // should we delineate this status better?

        return Double.parseDouble(event.value) == 0.0 ? NodeStatus.UNHEALTHY : NodeStatus.HEALTHY;
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
