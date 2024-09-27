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

package org.apache.cassandra.tcm.migration;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.GossipHelper;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.utils.CassandraVersion;

public class GossipCMSListener implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipCMSListener.class);
    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        ClusterMetadata metadata = ClusterMetadata.current();

        if (!metadata.epoch.is(Epoch.UPGRADE_GOSSIP))
            return;

        NodeId nodeId = metadata.directory.peerId(endpoint);
        if (nodeId == null)
        {
            VersionedValue hostIdValue = epState.getApplicationState(ApplicationState.HOST_ID);
            if (hostIdValue != null)
            {
                UUID hostId = UUID.fromString(hostIdValue.value);
                nodeId = metadata.directory.nodeIdFromHostId(hostId);
                logger.info("Node {} (hostId = {}) changing IP from {} to {}", nodeId, hostId, metadata.directory.endpoint(nodeId), endpoint);
                Gossiper.instance.removeEndpoint(endpoint);
            }
            else
            {
                logger.warn("Could not find NodeId for endpoint {}", endpoint);
                return;
            }
        }
        // only thing that can change is the release version and addresses
        CassandraVersion gossipVersion = epState.getReleaseVersion();
        NodeAddresses newAddresses = GossipHelper.getAddressesFromEndpointState(endpoint, epState);
        while (true)
        {
            NodeVersion cmVersion = metadata.directory.versions.get(nodeId);
            if (cmVersion.cassandraVersion.equals(gossipVersion) && newAddresses.equals(metadata.directory.getNodeAddresses(nodeId)))
            {
                return;
            }
            else
            {
                ClusterMetadata.Transformer transformer = metadata.transformer();
                if (gossipVersion != null && !cmVersion.cassandraVersion.equals(gossipVersion))
                    transformer = transformer.withVersion(nodeId, NodeVersion.fromCassandraVersion(gossipVersion));

                if (!newAddresses.equals(metadata.directory.getNodeAddresses(nodeId)))
                {
                    transformer = transformer.withNewAddresses(nodeId, newAddresses);
                    DataPlacements newPlacement = ClusterMetadataService.instance()
                                                                        .placementProvider()
                                                                        .calculatePlacements(Epoch.UPGRADE_GOSSIP,
                                                                                             metadata.tokenMap.toRanges(),
                                                                                             transformer.build().metadata,
                                                                                             metadata.schema.getKeyspaces());
                    transformer = transformer.with(newPlacement);
                }

                ClusterMetadata newCM = transformer.buildForGossipMode();
                if (ClusterMetadataService.instance().applyFromGossip(metadata, newCM))
                    return;
                metadata = ClusterMetadata.current();
            }
        }
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        onJoin(endpoint, state);
    }
}
