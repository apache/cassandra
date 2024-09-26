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

package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.TokenMap;

/**
 * CMS Placement Strategy is how CMS keeps the number of its members at a configured level, given current
 * cluster topolgy. It allows to add and remove CMS members when cluster topology changes. For example, during
 * node replacement or decommission. This attempts to achieve rack diversity, while keeping CMS placements
 * close to how "regular" data would get replicated to keep the bounces safe, as long as the user
 * bounces at most `f` members of the replica group, where `f = (RF - 1)/2`.
 */
public class CMSPlacementStrategy
{
    public final Map<String, Integer> rf;
    public final BiFunction<ClusterMetadata, NodeId, Boolean> filter;

    public CMSPlacementStrategy(Map<String, Integer> rf, Predicate<NodeId> filter)
    {
        this(rf, new DefaultNodeFilter(filter));
    }

    @VisibleForTesting
    public CMSPlacementStrategy(Map<String, Integer> rf, BiFunction<ClusterMetadata, NodeId, Boolean> filter)
    {
        // todo: verify only test uses with other filter
        this.rf = rf;
        this.filter = filter;
    }

    public Set<NodeId> reconfigure(ClusterMetadata metadata)
    {
        Map<String, ReplicationFactor> rf = new HashMap<>(this.rf.size());
        for (Map.Entry<String, Integer> e : this.rf.entrySet())
        {
            Collection<InetAddressAndPort> nodesInDc = metadata.directory.allDatacenterEndpoints().get(e.getKey());
            if (nodesInDc.isEmpty())
                throw new IllegalStateException(String.format("There are no nodes in %s datacenter", e.getKey()));
            if (nodesInDc.size() < e.getValue())
                throw new Transformation.RejectedTransformationException(String.format("There are not enough nodes in %s datacenter to satisfy replication factor", e.getKey()));

            rf.put(e.getKey(), ReplicationFactor.fullOnly(e.getValue()));
        }

        Directory tmpDirectory = metadata.directory;
        TokenMap tmpTokenMap = metadata.tokenMap;
        for (NodeId peerId : metadata.directory.peerIds())
        {
            if (!filter.apply(metadata, peerId))
            {
                tmpDirectory = tmpDirectory.without(peerId);
                tmpTokenMap = tmpTokenMap.unassignTokens(peerId);
            }
        }

        // Although MetaStrategy has its own entireRange, it uses a custom partitioner which isn't compatible with
        // regular, non-CMS placements. For that reason, we select replicas here using tokens provided by the
        // globally configured partitioner.
        Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        EndpointsForRange endpoints = NetworkTopologyStrategy.calculateNaturalReplicas(minToken,
                                                                                       new Range<>(minToken, minToken),
                                                                                       tmpDirectory,
                                                                                       tmpTokenMap,
                                                                                       rf);

        return endpoints.endpoints().stream().map(metadata.directory::peerId).collect(Collectors.toSet());
    }

    static class DefaultNodeFilter implements BiFunction<ClusterMetadata, NodeId, Boolean>
    {
        private final Predicate<NodeId> filter;

        public DefaultNodeFilter(Predicate<NodeId> filter)
        {
            this.filter = filter;
        }

        public Boolean apply(ClusterMetadata metadata, NodeId nodeId)
        {
            if (metadata.directory.peerState(nodeId) != NodeState.JOINED)
                return false;

            if (metadata.inProgressSequences.contains(nodeId))
                return false;

            if (!filter.test(nodeId))
                return false;

            return true;
        }
    }
}