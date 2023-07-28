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

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
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
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.RACK) == null)
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("rack");
            return DEFAULT_RACK;
        }
        return state.getApplicationState(ApplicationState.RACK).value;
    }

    @Override
    public final String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return getLocalDatacenter();
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.DC) == null)
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("data_center");
            return DEFAULT_DC;
        }
        return state.getApplicationState(ApplicationState.DC).value;
    }
}
