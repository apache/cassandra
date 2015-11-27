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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.locator.YamlFileNetworkTopologySnitch.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.net.InetAddresses;

/**
 * Unit tests for {@link YamlFileNetworkTopologySnitch}.
 */
public class YamlFileNetworkTopologySnitchTest
{
    private String confFile;

    private VersionedValue.VersionedValueFactory valueFactory;
    private Map<InetAddress, Set<Token>> tokenMap;

    @Before
    public void setup() throws ConfigurationException, IOException
    {
        confFile = YamlFileNetworkTopologySnitch.DEFAULT_TOPOLOGY_CONFIG_FILENAME;

        InetAddress[] hosts = {
                              InetAddress.getByName("127.0.0.1"), // this exists in the config file
                              InetAddress.getByName("127.0.0.2"), // this exists in the config file
                              InetAddress.getByName("127.0.0.9"), // this does not exist in the config file
        };

        IPartitioner partitioner = new RandomPartitioner();
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        tokenMap = new HashMap<>();

        for (InetAddress host : hosts)
        {
            Set<Token> tokens = Collections.singleton(partitioner.getRandomToken());
            Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
            Gossiper.instance.injectApplicationState(host, ApplicationState.TOKENS, valueFactory.tokens(tokens));

            setNodeShutdown(host);
            tokenMap.put(host, tokens);
        }
    }

    /**
     * A basic test case.
     *
     * @throws Exception on failure
     */
    @Test
    public void testBasic() throws Exception
    {
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, FBUtilities.getBroadcastAddress().getHostAddress(), "DC1", "RAC1");
        checkEndpoint(snitch, "192.168.1.100", "DC1", "RAC1");
        checkEndpoint(snitch, "10.0.0.12", "DC1", "RAC2");
        checkEndpoint(snitch, "127.0.0.3", "DC1", "RAC3");
        checkEndpoint(snitch, "10.20.114.10", "DC2", "RAC1");
        checkEndpoint(snitch, "127.0.0.8", "DC3", "RAC8");
        checkEndpoint(snitch, "6.6.6.6", "DC1", "r1");
    }

    /**
     * Asserts that a snitch's determination of data center and rack for an endpoint match what we expect.
     *
     * @param snitch
     *            snitch
     * @param endpointString
     *            endpoint address as a string
     * @param expectedDatacenter
     *            expected data center
     * @param expectedRack
     *            expected rack
     */
    public static void checkEndpoint(final AbstractNetworkTopologySnitch snitch,
                                     final String endpointString, final String expectedDatacenter,
                                     final String expectedRack)
    {
        final InetAddress endpoint = InetAddresses.forString(endpointString);
        Assert.assertEquals(expectedDatacenter, snitch.getDatacenter(endpoint));
        Assert.assertEquals(expectedRack, snitch.getRack(endpoint));
    }


    private static TopologyConfig moveNode(TopologyConfig topologyConfig,
                                           String broadcastAddress, String dcLocalAddress,
                                           String oldDC, String newDC,
                                           String oldRack, String newRack)
    {

        for (Datacenter dc : topologyConfig.topology)
        {
            if (oldDC != null && oldRack != null)
            {
                if (dc.dc_name.equals(oldDC))
                {
                    for (Rack rack : dc.racks)
                    {
                        if (rack.rack_name.equals(oldRack))
                        {
                            for (Node node : rack.nodes)
                            {
                                if (node.broadcast_address.equals(broadcastAddress))
                                {
                                    rack.nodes.remove(node);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if (newDC != null && newRack != null)
            {
                if (dc.dc_name.equals(newDC))
                {
                    for (Rack rack : dc.racks)
                    {
                        if (rack.rack_name.equals(newRack))
                        {
                            Node node = new Node();
                            node.broadcast_address = broadcastAddress;
                            node.dc_local_address = dcLocalAddress;
                            rack.nodes.add(node);
                        }
                    }
                }
            }
        }

        return topologyConfig;
    }

    private void setNodeShutdown(InetAddress host)
    {
        StorageService.instance.getTokenMetadata().removeEndpoint(host);
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.shutdown(true));
        Gossiper.instance.markDead(host, Gossiper.instance.getEndpointStateForEndpoint(host));
    }

    private void setNodeLive(InetAddress host)
    {
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.normal(tokenMap.get(host)));
        Gossiper.instance.realMarkAlive(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokenMap.get(host), host);
    }

    /**
     * Test that changing rack for a host in the configuration file is only effective if the host is not live.
     * The original configuration file contains DC1, RAC1 for broadcast address 127.0.0.1 and dc_local_address 9.0.0.1.
     */
    @Test
    public void testChangeHostRack() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.1");
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

        try
        {
            final TopologyConfig topologyConfig = snitch.readConfig();
            moveNode(topologyConfig, host.getHostAddress(), "9.0.0.1", "DC1", "DC1", "RAC1", "RAC2");

            setNodeLive(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

            setNodeShutdown(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2");
        }
        finally
        {
            setNodeShutdown(host);
        }
    }

    /**
     * Test that changing dc for a host in the configuration file is only effective if the host is not live.
     * The original configuration file contains DC1, RAC1 for broadcast address 127.0.0.1 and dc_local_address 9.0.0.1.
     */
    @Test
    public void testChangeHostDc() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.1");
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

        try
        {
            final TopologyConfig topologyConfig = snitch.readConfig();
            moveNode(topologyConfig, host.getHostAddress(), "9.0.0.1", "DC1", "DC2", "RAC1", "RAC1");

            setNodeLive(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

            setNodeShutdown(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "RAC1");
        }
        finally
        {
            setNodeShutdown(host);
        }
    }

    /**
     * Test that adding a host to the configuration file changes the host dc and rack only if the host
     * is not live. The original configuration file does not contain 127.0.0.9 and so it should use
     * the default data center DC1 and rack r1.
     */
    @Test
    public void testAddHost() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.9");
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default

        try
        {
            final TopologyConfig topologyConfig = snitch.readConfig();
            moveNode(topologyConfig, host.getHostAddress(), "9.0.0.9", null, "DC2", null, "RAC2");

            setNodeLive(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // unchanged

            setNodeShutdown(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "RAC2"); // changed
        }
        finally
        {
            setNodeShutdown(host);
        }
    }

    /**
     * Test that removing a host from the configuration file changes the host rack only if the host
     * is not live. The original configuration file contains 127.0.0.2 in DC1, RAC2 and default DC1, r1 so removing
     * this host should result in a different rack if the host is not live.
     */
    @Test
    public void testRemoveHost() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.2");
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2");

        try
        {
            final TopologyConfig topologyConfig = snitch.readConfig();
            moveNode(topologyConfig, host.getHostAddress(), "9.0.0.2", "DC1", null, "RAC2", null);

            setNodeLive(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC2"); // unchanged

            setNodeShutdown(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default
        }
        finally
        {
            setNodeShutdown(host);
        }
    }

    /**
     * Test that we can change the default only if this does not result in any live node changing dc or rack.
     * The configuration file contains default DC1 and r1 and we change it to DC2 and r2. Let's use host 127.0.0.9
     * since it is not in the configuration file.
     */
    @Test
    public void testChangeDefault() throws Exception
    {
        final InetAddress host = InetAddress.getByName("127.0.0.9");
        final YamlFileNetworkTopologySnitch snitch = new YamlFileNetworkTopologySnitch(confFile);
        checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // default

        try
        {
            final TopologyConfig topologyConfig = snitch.readConfig();
            topologyConfig.default_dc_name = "DC2";
            topologyConfig.default_rack_name = "r2";

            setNodeLive(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC1", "r1"); // unchanged

            setNodeShutdown(host);
            snitch.loadTopologyConfiguration(true, topologyConfig);
            checkEndpoint(snitch, host.getHostAddress(), "DC2", "r2"); // default updated
        }
        finally
        {
            setNodeShutdown(host);
        }
    }
}
