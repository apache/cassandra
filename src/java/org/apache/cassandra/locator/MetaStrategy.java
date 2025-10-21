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

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.ReversedLongLocalPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.apache.cassandra.locator.NetworkTopologyStrategy.REPLICATION_FACTOR;

/**
 * MetaStrategy is designed for distributed cluster metadata keyspace, and should not be used by
 * the users directly. This strategy allows a configurable number of nodes to own an entire range and
 * be responsible for cluster metadata queries.
 *
 * Nodes are added to and removed from placements using ReconfigureCMS sequence, and PrepareCMSReconfiguration/
 * AdvanceCMSReconfiguration, according to CMSPlacementStrategy derived from params specified in
 * options of MetaStrategy.
 */
public class MetaStrategy extends SystemStrategy
{
    public static final IPartitioner partitioner = ReversedLongLocalPartitioner.instance;
    public static final Range<Token> entireRange = new Range<>(partitioner.getMinimumToken(),
                                                               partitioner.getMinimumToken());

    public static LockedRanges.AffectedRanges affectedRanges(ClusterMetadata metadata)
    {
        return LockedRanges.AffectedRanges.singleton(ReplicationParams.meta(metadata), entireRange);
    }

    public static Replica replica(InetAddressAndPort addr)
    {
        return new Replica(addr, entireRange, true);
    }

    private final ReplicationFactor rf;

    public MetaStrategy(String keyspaceName, Map<String, String> configOptions)
    {
        super(keyspaceName, configOptions);
        int replicas = 0;
        if (configOptions != null)
        {
            for (Map.Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                // prepareOptions should have transformed any "replication_factor" options by now
                if (dc.equalsIgnoreCase(REPLICATION_FACTOR))
                    continue;
                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                replicas += rf.allReplicas;
            }
        }

        rf = ReplicationFactor.fullOnly(replicas);
    }

    @Override
    public EndpointsForRange calculateNaturalReplicas(Token token, ClusterMetadata metadata)
    {
        return metadata.placements.get(ReplicationParams.meta(metadata)).reads.forRange(entireRange).get();
    }

    @Override
    public DataPlacement calculateDataPlacement(Epoch epoch, List<Range<Token>> ranges, ClusterMetadata metadata)
    {
        return metadata.placements.get(ReplicationParams.meta(metadata));
    }

    @Override
    public ReplicationFactor getReplicationFactor()
    {
        return rf;
    }

    @Override
    public RangesAtEndpoint getAddressReplicas(ClusterMetadata metadata, InetAddressAndPort endpoint)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(endpoint);
        if (metadata.fullCMSMembers().contains(endpoint))
            builder.add(replica(endpoint));
        return builder.build();
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