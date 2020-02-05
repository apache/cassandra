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

package org.apache.cassandra.distributed.impl;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DistributedTestSnitch extends AbstractNetworkTopologySnitch
{
    private static NetworkTopology mapping = null;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;
    private static final String DEFAULT_DC = "UNKNOWN_DC";
    private static final String DEFAULT_RACK = "UNKNOWN_RACK";

    public String getRack(InetAddress endpoint)
    {
        int storage_port = Config.getOverrideLoadConfig().get().storage_port;
        return getRack(InetAddressAndPort.getByAddressOverrideDefaults(endpoint, storage_port));
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return maybeGetFromEndpointState(mapping.localRack(endpoint), endpoint, ApplicationState.RACK, DEFAULT_RACK);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        int storage_port = Config.getOverrideLoadConfig().get().storage_port;
        return getDatacenter(InetAddressAndPort.getByAddressOverrideDefaults(endpoint, storage_port));
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return maybeGetFromEndpointState(mapping.localDC(endpoint), endpoint, ApplicationState.DC, DEFAULT_DC);
    }

    // Here, the logic is slightly different from what we have in GossipingPropertyFileSnitch since we have a different
    // goal. Passed argument (topology that was set on the node) overrides anything that is passed elsewhere.
    private String maybeGetFromEndpointState(String current, InetAddressAndPort endpoint, ApplicationState state, String defaultValue)
    {
        if (current != null)
            return current;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint.address);
        if (epState == null || epState.getApplicationState(state) == null)
        {
            if (savedEndpoints == null)
            {
                savedEndpoints = new HashMap<>();
                int storage_port = Config.getOverrideLoadConfig().get().storage_port;
                for (Map.Entry<InetAddress, Map<String, String>> entry : SystemKeyspace.loadDcRackInfo().entrySet())
                {
                    savedEndpoints.put(InetAddressAndPort.getByAddressOverrideDefaults(endpoint.address, storage_port),
                                       entry.getValue());
                }
            }
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("data_center");

            return defaultValue;
        }

        return epState.getApplicationState(state).value;
    }

    static void assign(NetworkTopology newMapping)
    {
        mapping = new NetworkTopology(newMapping);
    }

    public void gossiperStarting()
    {
        super.gossiperStarting();


        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                                                   StorageService.instance.valueFactory.internalIP(FBUtilities.getLocalAddress().getHostAddress()));
    }
}
