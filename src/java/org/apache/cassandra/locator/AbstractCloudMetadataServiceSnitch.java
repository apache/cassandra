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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

abstract class AbstractCloudMetadataServiceSnitch extends AbstractNetworkTopologySnitch
{
    static final Logger logger = LoggerFactory.getLogger(AbstractCloudMetadataServiceSnitch.class);

    static final String DEFAULT_DC = "UNKNOWN-DC";
    static final String DEFAULT_RACK = "UNKNOWN-RACK";

    protected final AbstractCloudMetadataServiceConnector connector;

    private final String localRack;
    private final String localDc;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;

    public AbstractCloudMetadataServiceSnitch(AbstractCloudMetadataServiceConnector connector, Pair<String, String> dcAndRack)
    {
        this.connector = connector;
        this.localDc = dcAndRack.left;
        this.localRack = dcAndRack.right;

        logger.info(format("%s using datacenter: %s, rack: %s, connector: %s, properties: %s",
                           getClass().getName(), getLocalDatacenter(), getLocalRack(), connector, connector.getProperties()));
    }

    @Override
    public final String getLocalRack()
    {
        return localRack;
    }

    @Override
    public final String getLocalDatacenter()
    {
        return localDc;
    }

    @Override
    public final String getRack(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return getLocalRack();
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = metadata.directory.peerId(endpoint);
        if (nodeId == null)
            return DEFAULT_RACK;
        return metadata.directory.location(nodeId).rack;
    }

    @Override
    public final String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return getLocalDatacenter();
        ClusterMetadata metadata = ClusterMetadata.current();
        NodeId nodeId = metadata.directory.peerId(endpoint);
        if (nodeId == null)
            return DEFAULT_DC;
        return metadata.directory.location(nodeId).datacenter;
    }
}
