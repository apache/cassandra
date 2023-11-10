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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.OfflineTokenAllocator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.mockito.Mockito;

import static org.apache.cassandra.locator.InetAddressAndPort.getByAddress;

public class TokenAllocationTest
{
    public static class TestingSnitch extends AbstractNetworkTopologySnitch
    {
        Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<>();

        public TestingSnitch(Map<InetAddressAndPort, NodeInfo> nodes)
        {
            this.nodes.putAll(nodes);
        }

        @Override
        public String getRack(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).rack;
        }

        @Override
        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return nodes.get(endpoint).dc;
        }
    }

    private static class NodeInfo
    {
        String dc;
        String rack;
        Set<String> tag;

        public NodeInfo(String dc, String rack, String tag)
        {
            this.dc = dc;
            this.rack = rack;
            this.tag = Collections.singleton(tag);
        }

        public static NodeInfo create(String dc, String rack)
        {
            return create(dc, rack, null);
        }

        public static NodeInfo create(String dc, String rack, String tag)
        {
            return new NodeInfo(dc, rack, tag);
        }
    }

    private static final String DC1 = "dc1";
    private static final String r1 = "rack1";
    private static final String r2 = "rack2";
    private static final String r3 = "rack3";
    private static final String r4 = "rack4";

    private static InetAddressAndPort n1;
    private static InetAddressAndPort n2;
    private static InetAddressAndPort n3;
    private static InetAddressAndPort n4;
    private static InetAddressAndPort n5;
    private static InetAddressAndPort n6;
    private static InetAddressAndPort n7;
    private static InetAddressAndPort n8;
    private static InetAddressAndPort n9;
    private static InetAddressAndPort n10;
    private static InetAddressAndPort n11;
    private static InetAddressAndPort n12;

    static
    {
        try
        {
            n1 = getByAddress(new byte[]{ (byte) 172, 19, 0, 1 });
            n2 = getByAddress(new byte[]{ (byte) 172, 19, 0, 2 });
            n3 = getByAddress(new byte[]{ (byte) 172, 19, 0, 3 });
            n4 = getByAddress(new byte[]{ (byte) 172, 19, 0, 4 });
            n5 = getByAddress(new byte[]{ (byte) 172, 19, 0, 5 });
            n6 = getByAddress(new byte[]{ (byte) 172, 19, 0, 6 });
            n7 = getByAddress(new byte[]{ (byte) 172, 19, 0, 7 });
            n8 = getByAddress(new byte[]{ (byte) 172, 19, 0, 8 });
            n9 = getByAddress(new byte[]{ (byte) 172, 19, 0, 9 });
            n10 = getByAddress(new byte[]{ (byte) 172, 19, 0, 10 });
            n11 = getByAddress(new byte[]{ (byte) 172, 19, 0, 11 });
            n12 = getByAddress(new byte[]{ (byte) 172, 19, 0, 12 });
        }
        catch (Exception ex)
        {
            // ignore
        }
    }

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(new Murmur3Partitioner());
    }

    private static class TopologyBuilder
    {
        private Map<InetAddressAndPort, NodeInfo> nodes = new HashMap<>();

        public static TopologyBuilder create()
        {
            return new TopologyBuilder();
        }

        public TopologyBuilder add(String dc, String rack, InetAddressAndPort addr)
        {
            nodes.put(addr, NodeInfo.create(dc, rack));
            return this;
        }

        private TopologyBuilder()
        {
        }

        public Map<InetAddressAndPort, NodeInfo> build()
        {
            return Collections.unmodifiableMap(nodes);
        }
    }

    @Test
    public void allocationTest()
    {
        Map<InetAddressAndPort, NodeInfo> topology = TopologyBuilder.create()
                                                                    .add(DC1, r1, n1)
                                                                    .add(DC1, r1, n2)
                                                                    .add(DC1, r1, n3)
                                                                    .add(DC1, r1, n4)
                                                                    .add(DC1, r2, n5)
                                                                    .add(DC1, r2, n6)
                                                                    .add(DC1, r2, n7)
                                                                    .add(DC1, r2, n8)
                                                                    .add(DC1, r3, n9)
                                                                    .add(DC1, r3, n10)
                                                                    .add(DC1, r3, n11)
                                                                    .add(DC1, r3, n12)
                                                                    .build();

        IEndpointSnitch snitch = new TestingSnitch(topology);
        DatabaseDescriptor.setEndpointSnitch(snitch);

        TokenMetadata.Topology tokenMetadataTopology = Mockito.mock(TokenMetadata.Topology.class);
        Multimap<String, InetAddressAndPort> datacenterEndpoints = ImmutableMultimap.<String, InetAddressAndPort>builder()
                                                                                    .put(DC1, n1)
                                                                                    .put(DC1, n2)
                                                                                    .put(DC1, n3)
                                                                                    .put(DC1, n4)
                                                                                    .put(DC1, n5)
                                                                                    .put(DC1, n6)
                                                                                    .put(DC1, n7)
                                                                                    .put(DC1, n8)
                                                                                    .put(DC1, n9)
                                                                                    .put(DC1, n10)
                                                                                    .put(DC1, n11)
                                                                                    .put(DC1, n12)
                                                                                    .build();

        ImmutableMap<String, ImmutableMultimap<String, InetAddressAndPort>> racks = ImmutableMap.<String, ImmutableMultimap<String, InetAddressAndPort>>builder()
                                                                                                .put(DC1, ImmutableMultimap.<String, InetAddressAndPort>builder()
                                                                                                                           .put(r1, n1)
                                                                                                                           .put(r1, n2)
                                                                                                                           .put(r1, n3)
                                                                                                                           .put(r1, n4)
                                                                                                                           .put(r2, n5)
                                                                                                                           .put(r2, n6)
                                                                                                                           .put(r2, n7)
                                                                                                                           .put(r2, n8)
                                                                                                                           .put(r3, n9)
                                                                                                                           .put(r3, n10)
                                                                                                                           .put(r3, n11)
                                                                                                                           .put(r3, n12).build()).build();

        Mockito.when(tokenMetadataTopology.getDatacenterEndpoints()).thenReturn(datacenterEndpoints);
        Mockito.when(tokenMetadataTopology.getDatacenterRacks()).thenReturn(racks);

        TokenMetadata metadata = new TokenMetadata(snitch);
        TokenMetadata spy = Mockito.spy(metadata);

        Mockito.when(spy.getTopology()).thenReturn(tokenMetadataTopology);

        Map<String, String> configOptions = new HashMap<String, String>()
        {{
            put("dc1", "5");
        }};
        AbstractReplicationStrategy strategy = new NetworkTopologyStrategy("ks1", metadata, snitch, configOptions);

        for (int run = 0; run < 10; run++)
        {
            metadata.cloneOnlyTokenMap();

            InetAddressAndPort[] nodes = new InetAddressAndPort[]{ n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12 };

            List<OfflineTokenAllocator.FakeNode> allocation = OfflineTokenAllocator.allocate(5, 256,
                                                                                             new int[]{ 3, 3, 3, 3 },
                                                                                             new OutputHandler.SystemOutput(false, true, true),
                                                                                             FBUtilities.newPartitioner(Murmur3Partitioner.class.getSimpleName()));

            for (int i = 0; i < nodes.length; i++)
            {
                OfflineTokenAllocator.FakeNode fakeNode = allocation.get(i);
                metadata.updateNormalTokens(fakeNode.tokens(), nodes[i]);
            }

            Map<Token, Set<InetAddressAndPort>> violations = new HashMap<>();
            Map<String, Integer> placement = new HashMap<>();

            long numberOfViolations = 0;

            for (long i = 0; i < 1_000_000_000L; i++)
            {
                if (!violations.isEmpty())
                    break;

                placement.clear();

                LongToken token = Murmur3Partitioner.instance.getRandomToken();
                Set<InetAddressAndPort> replicas = strategy.getNaturalReplicasForToken(token).endpoints();

                for (InetAddressAndPort replica : replicas)
                {
                    String rack = topology.get(replica).rack;
                    Integer replicasInRack = placement.getOrDefault(rack, 0);
                    placement.put(rack, ++replicasInRack);
                }

                for (Map.Entry<String, Integer> placementEntry : placement.entrySet())
                    if (placementEntry.getValue() >= 3)
                    {
                        numberOfViolations++;
                        //violations.put(token, replicas);
                    }
            }

            System.out.println(run + " " + (numberOfViolations / 3));
        }
    }
}
