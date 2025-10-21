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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector;
import org.apache.cassandra.locator.Ec2MultiRegionAddressConfig;
import org.apache.cassandra.locator.Ec2MultiRegionSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.InitialLocationProvider;
import org.apache.cassandra.locator.NetworkTopologyProximity;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.tcm.membership.Location;
import org.awaitility.Awaitility;

import static org.apache.cassandra.locator.Ec2LocationProvider.ZONE_NAME_QUERY;
import static org.apache.cassandra.locator.Ec2MultiRegionAddressConfig.PRIVATE_IP_QUERY;
import static org.apache.cassandra.locator.Ec2MultiRegionAddressConfig.PUBLIC_IP_QUERY;
import static org.apache.cassandra.utils.Pair.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReconnectToInternalIPTest extends TestBaseImpl
{
    @Test
    public void testWithSnitchConfig() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .withConfig(c -> c.set("endpoint_snitch", TestMultiRegionSnitch.class.getName())
                                                               .set("node_proximity", null)
                                                               .set("initial_location_provider", null)
                                                               .set("listen_on_broadcast_address", true)
                                                               .with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            doTest(cluster);
        }
    }

    @Test
    public void testWithModernConfig() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                            .withConfig(c -> c.set("endpoint_snitch", null)
                                                              .set("node_proximity", NetworkTopologyProximity.class.getName())
                                                              .set("initial_location_provider", TestMultiRegionLocationProvider.class.getName())
                                                              .set("listen_on_broadcast_address", true)
                                                              .set("addresses_config", TestMultiRegionAddressConfig.class.getName())
                                                              .set("prefer_local_connections", true)
                                                 .with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            doTest(cluster);
        }
    }

    private static void doTest(Cluster cluster) throws TimeoutException
    {
        cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
        cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (1)"), ConsistencyLevel.ALL);
        // node1 should only reconnect to node2, but currently this is still driven by gossip so may not happen immediately
        InetSocketAddress ep = cluster.get(2).config().broadcastAddress();
        String pattern = "Initiated reconnect to an Internal IP "+toInternalIp(ep)+" for the " + ep;
        cluster.get(1).logs().watchFor(Duration.ofSeconds(10), pattern);

        // node1 should not reconnect to node3 or node4
        for (int i = 3; i <= 4; i++)
        {
            ep = cluster.get(i).config().broadcastAddress();
            pattern = "Initiated reconnect to an Internal IP "+toInternalIp(ep)+" for the " + ep;
            assertTrue(cluster.get(1).logs().grep(pattern).getResult().isEmpty());
        }

        cluster.forEach(inst -> inst.runOnInstance(() -> {
            for (InetAddressAndPort endpoint : Gossiper.instance.endpointStateMap.keySet())
            {
                InetAddressAndPort internal = toInternalIp(endpoint);
                Awaitility.await()
                          .atMost(30L, TimeUnit.SECONDS)
                          .until(() -> Gossiper.instance.getApplicationState(endpoint, ApplicationState.INTERNAL_ADDRESS_AND_PORT) != null);
                InetAddressAndPort fromGossip = InetAddressAndPort.getByNameUnchecked(Gossiper.instance.getApplicationState(endpoint, ApplicationState.INTERNAL_ADDRESS_AND_PORT));
                assertEquals(internal, fromGossip);
            }
        }));
    }

    public static class TestMultiRegionLocationProvider implements InitialLocationProvider
    {
        @Override
        public Location initialLocation()
        {
            InetAddressAndPort configuredBA = InetAddressAndPort.getByNameUnchecked(DatabaseDescriptor.getRawConfig().broadcast_address);
            byte lastByte = configuredBA.addressBytes[configuredBA.addressBytes.length - 1];
            String zone = (lastByte == 1 || lastByte == 2) ? "us-east-1" : "us-west-1";
            return new Location(zone, zone + 'a');
        }
    }

    public static class TestMultiRegionAddressConfig extends Ec2MultiRegionAddressConfig
    {
        public TestMultiRegionAddressConfig() throws IOException
        {
            super(new TestCloudMetadataConnector());
        }
    }

    public static class TestMultiRegionSnitch extends Ec2MultiRegionSnitch
    {
        public TestMultiRegionSnitch() throws IOException, ConfigurationException
        {
            super(new TestCloudMetadataConnector());
        }
    }

    public static class TestCloudMetadataConnector extends AbstractCloudMetadataServiceConnector
    {
        public TestCloudMetadataConnector()
        {
            super(new SnitchProperties(create(METADATA_URL_PROPERTY, "http://apache.org")));
        }

        @Override
        public String apiCall(String url,
                              String query,
                              String method,
                              Map<String, String> extraHeaders,
                              int expectedResponseCode)
        {
            InetAddressAndPort configuredBA = InetAddressAndPort.getByNameUnchecked(DatabaseDescriptor.getRawConfig().broadcast_address);
            switch (query)
            {
                case ZONE_NAME_QUERY:
                    // "us-east-1a"
                    byte lastByte = configuredBA.addressBytes[configuredBA.addressBytes.length - 1];
                    if (lastByte == 1 || lastByte == 2)
                        return "us-east-1a";
                    else
                        return "us-west-1a";
                case PUBLIC_IP_QUERY:
                    // public ip is configured ip "+ 4"
                    return toPublicIp(configuredBA).getHostAddress(false);
                case PRIVATE_IP_QUERY:
                    // private ip is configured ip "+ 8"
                    return toInternalIp(configuredBA).getHostAddress(false);
                default:
                    throw new AssertionError("Bad query: " + query);
            }
        }
    }

    private static InetAddressAndPort toInternalIp(InetSocketAddress ep)
    {
        return convertIp(ep, 8);
    }

    private static InetAddressAndPort toPublicIp(InetSocketAddress ep)
    {
        return convertIp(ep, 4);
    }

    private static InetAddressAndPort convertIp(InetSocketAddress ep, int offset)
    {
        byte lastByte = ep.getAddress().getAddress()[ep.getAddress().getAddress().length - 1];
        return InetAddressAndPort.getByNameUnchecked("127.0.0." + (lastByte + offset)+":"+ep.getPort());
    }

}
