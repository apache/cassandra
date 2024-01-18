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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.DataPlacement;

import static org.apache.cassandra.tcm.ownership.EntireRange.entireRange;

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
    public MetaStrategy(String keyspaceName, Map<String, String> configOptions)
    {
        super(keyspaceName, configOptions);
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
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.epoch.isEqualOrBefore(Epoch.FIRST))
            return ReplicationFactor.fullOnly(1);
        int rf = metadata.placements.get(ReplicationParams.meta(metadata)).writes.forRange(entireRange).get().byEndpoint.size();
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