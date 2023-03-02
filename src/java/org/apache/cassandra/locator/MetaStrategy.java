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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.tcm.transformations.cms.EntireRange;

public class MetaStrategy extends SystemStrategy
{
    public MetaStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata tokenMetadata)
    {
        return replicas();
    }

    @Override
    public DataPlacement calculateDataPlacement(List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        PlacementForRange placement = PlacementForRange.builder(1).withReplicaGroup(replicas()).build();
        return new DataPlacement(placement, placement);
    }

    private static EndpointsForRange replicas()
    {
        Set<InetAddressAndPort> members = ClusterMetadata.current().fullCMSMembers();
        return EndpointsForRange.builder(EntireRange.entireRange, members.size())
                                .addAll(members.stream().map(EntireRange::replica).collect(Collectors.toList()))
                                .build();
    }
    @Override
    public ReplicationFactor getReplicationFactor()
    {
        int rf = ClusterMetadata.current().fullCMSMembers().size();
        return ReplicationFactor.fullOnly(rf);
    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        return getClass().equals(other.getClass());
    }

    @Override
    public boolean hasTransientReplicas()
    {
        return false;
    }

    public String toString()
    {
        return "MetaStrategy{" +
               "configOptions=" + configOptions +
               ", keyspaceName='" + keyspaceName + '\'' +
               '}';
    }
}