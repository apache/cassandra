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
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

public class NetworkTopology extends HashMap<InetAddressAndPort, Pair<String,String>>
{
    private NetworkTopology()
    {
        super();
    }

    NetworkTopology(NetworkTopology networkTopology)
    {
        super(networkTopology);
    }

    public static NetworkTopology build(String ipPrefix, int broadcastPort, Map<Integer,Pair<String,String>> nodeIdTopology)
    {
        NetworkTopology networkTopology = new NetworkTopology();

        for (int nodeId = 1; nodeId <= nodeIdTopology.size(); nodeId++)
        {
            String broadcastAddress = ipPrefix + nodeId;

            try
            {
                Pair<String,String> dcAndRack = nodeIdTopology.get(nodeId);
                if (dcAndRack == null)
                {
                    throw new IllegalStateException("nodeId " + nodeId + "not found in instanceMap");
                }

                InetAddressAndPort broadcastAddressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(
                    InetAddress.getByName(broadcastAddress), broadcastPort);
                networkTopology.put(broadcastAddressAndPort, dcAndRack);
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown broadcast_address '" + broadcastAddress + "'", false);
            }
        }
        return networkTopology;
    }
}
