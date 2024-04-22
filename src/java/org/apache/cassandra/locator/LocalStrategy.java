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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.FBUtilities;

public class LocalStrategy extends SystemStrategy
{
    private static final ReplicationFactor RF = ReplicationFactor.fullOnly(1);
    public LocalStrategy(String keyspaceName, Map<String, String> configOptions)
    {
        super(keyspaceName, configOptions);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token token, ClusterMetadata metadata)
    {
        return EntireRange.localReplicas;
    }

    @Override
    public DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        return EntireRange.placement;
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return RF;
    }

    /**
     * For lazy initialisation. In some circumstances, we may want to instantiate LocalStrategy without initialising
     * DatabaseDescriptor; FQL replay is one such usage as we initialise the KeyspaceMetadata objects, which now eagerly
     * creates the replication strategy.
     */
    static class EntireRange
    {
        public static final Range<Token> entireRange = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(), DatabaseDescriptor.getPartitioner().getMinimumToken());
        public static final EndpointsForRange localReplicas = EndpointsForRange.of(new Replica(FBUtilities.getBroadcastAddressAndPort(), entireRange, true));
        public static final DataPlacement placement = new DataPlacement(ReplicaGroups.builder().withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, localReplicas)).build(),
                                                                        ReplicaGroups.builder().withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, localReplicas)).build());
    }
}
