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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.dht.IPartitioner;
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
    private static Map<IPartitioner, EntireRange> perPartitionerRanges = new ConcurrentHashMap<>();

    public LocalStrategy(String keyspaceName, Map<String, String> configOptions)
    {
        super(keyspaceName, configOptions);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token token, ClusterMetadata metadata)
    {
        return getRange(token.getPartitioner()).localReplicas;
    }

    @Override
    public DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        return getRange(ranges.get(0).left.getPartitioner()).placement;
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return RF;
    }

    private EntireRange getRange(IPartitioner partitioner)
    {
        // No need to synchronize here. In the unlikely event of a race, it's
        // safe and cheap to create duplicates and overwrite in the cache.
        EntireRange range = perPartitionerRanges.get(partitioner);
        if (range == null)
        {
            range = new EntireRange(partitioner);
            perPartitionerRanges.put(partitioner, range);
        }
        return range;
    }

    /**
     * For lazy initialisation. In some circumstances, we may want to instantiate LocalStrategy without initialising
     * DatabaseDescriptor; FQL replay is one such usage as we initialise the KeyspaceMetadata objects, which now eagerly
     * creates the replication strategy.
     */
    static class EntireRange
    {
        public final Range<Token> entireRange;
        public final EndpointsForRange localReplicas;
        public final DataPlacement placement;

        private EntireRange(IPartitioner partitioner)
        {
            entireRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
            localReplicas = EndpointsForRange.of(new Replica(FBUtilities.getBroadcastAddressAndPort(), entireRange, true));
            ReplicaGroups rg = ReplicaGroups.builder(1).withReplicaGroup(VersionedEndpoints.forRange(Epoch.FIRST, localReplicas)).build();
            placement = new DataPlacement(rg, rg);
        }
    }
}
