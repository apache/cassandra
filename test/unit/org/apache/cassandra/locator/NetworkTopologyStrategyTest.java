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
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner.StringToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata.Topology;
import org.apache.cassandra.service.StorageService;

public class NetworkTopologyStrategyTest
{
    private String keyspaceName = "Keyspace1";
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategyTest.class);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testProperties() throws IOException, ConfigurationException
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata metadata = new TokenMetadata();
        createDummyTokens(metadata, true);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC1", "3");
        configOptions.put("DC2", "2");
        configOptions.put("DC3", "1");

        // Set the localhost to the tokenmetadata. Embedded cassandra way?
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy(keyspaceName, metadata, snitch, configOptions);
        assert strategy.getReplicationFactor("DC1") == 3;
        assert strategy.getReplicationFactor("DC2") == 2;
        assert strategy.getReplicationFactor("DC3") == 1;
        // Query for the natural hosts
        ArrayList<InetAddress> endpoints = strategy.getNaturalEndpoints(new StringToken("123"));
        assert 6 == endpoints.size();
        assert 6 == new HashSet<InetAddress>(endpoints).size(); // ensure uniqueness
    }

    @Test
    public void testPropertiesWithEmptyDC() throws IOException, ConfigurationException
    {
        IEndpointSnitch snitch = new PropertyFileSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata metadata = new TokenMetadata();
        createDummyTokens(metadata, false);

        Map<String, String> configOptions = new HashMap<String, String>();
        configOptions.put("DC1", "3");
        configOptions.put("DC2", "3");
        configOptions.put("DC3", "0");

        // Set the localhost to the tokenmetadata. Embedded cassandra way?
        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy(keyspaceName, metadata, snitch, configOptions);
        assert strategy.getReplicationFactor("DC1") == 3;
        assert strategy.getReplicationFactor("DC2") == 3;
        assert strategy.getReplicationFactor("DC3") == 0;
        // Query for the natural hosts
        ArrayList<InetAddress> endpoints = strategy.getNaturalEndpoints(new StringToken("123"));
        assert 6 == endpoints.size();
        assert 6 == new HashSet<InetAddress>(endpoints).size(); // ensure uniqueness
    }

    @Test
    public void testLargeCluster() throws UnknownHostException, ConfigurationException
    {
        int[] dcRacks = new int[]{2, 4, 8};
        int[] dcEndpoints = new int[]{128, 256, 512};
        int[] dcReplication = new int[]{2, 6, 6};

        IEndpointSnitch snitch = new RackInferringSnitch();
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata metadata = new TokenMetadata();
        Map<String, String> configOptions = new HashMap<String, String>();
        Multimap<InetAddress, Token> tokens = HashMultimap.create();

        int totalRF = 0;
        for (int dc = 0; dc < dcRacks.length; ++dc)
        {
            totalRF += dcReplication[dc];
            configOptions.put(Integer.toString(dc), Integer.toString(dcReplication[dc]));
            for (int rack = 0; rack < dcRacks[dc]; ++rack)
            {
                for (int ep = 1; ep <= dcEndpoints[dc]/dcRacks[dc]; ++ep)
                {
                    byte[] ipBytes = new byte[]{10, (byte)dc, (byte)rack, (byte)ep};
                    InetAddress address = InetAddress.getByAddress(ipBytes);
                    StringToken token = new StringToken(String.format("%02x%02x%02x", ep, rack, dc));
                    logger.debug("adding node {} at {}", address, token);
                    tokens.put(address, token);
                }
            }
        }
        metadata.updateNormalTokens(tokens);

        NetworkTopologyStrategy strategy = new NetworkTopologyStrategy(keyspaceName, metadata, snitch, configOptions);

        for (String testToken : new String[]{"123456", "200000", "000402", "ffffff", "400200"})
        {
            List<InetAddress> endpoints = strategy.calculateNaturalEndpoints(new StringToken(testToken), metadata);
            Set<InetAddress> epSet = new HashSet<InetAddress>(endpoints);

            Assert.assertEquals(totalRF, endpoints.size());
            Assert.assertEquals(totalRF, epSet.size());
            logger.debug("{}: {}", testToken, endpoints);
        }
    }

    public void createDummyTokens(TokenMetadata metadata, boolean populateDC3) throws UnknownHostException
    {
        // DC 1
        tokenFactory(metadata, "123", new byte[]{ 10, 0, 0, 10 });
        tokenFactory(metadata, "234", new byte[]{ 10, 0, 0, 11 });
        tokenFactory(metadata, "345", new byte[]{ 10, 0, 0, 12 });
        // Tokens for DC 2
        tokenFactory(metadata, "789", new byte[]{ 10, 20, 114, 10 });
        tokenFactory(metadata, "890", new byte[]{ 10, 20, 114, 11 });
        //tokens for DC3
        if (populateDC3)
        {
            tokenFactory(metadata, "456", new byte[]{ 10, 21, 119, 13 });
            tokenFactory(metadata, "567", new byte[]{ 10, 21, 119, 10 });
        }
        // Extra Tokens
        tokenFactory(metadata, "90A", new byte[]{ 10, 0, 0, 13 });
        if (populateDC3)
            tokenFactory(metadata, "0AB", new byte[]{ 10, 21, 119, 14 });
        tokenFactory(metadata, "ABC", new byte[]{ 10, 20, 114, 15 });
    }

    public void tokenFactory(TokenMetadata metadata, String token, byte[] bytes) throws UnknownHostException
    {
        Token token1 = new StringToken(token);
        InetAddress add1 = InetAddress.getByAddress(bytes);
        metadata.updateNormalToken(token1, add1);
    }

    @Test
    public void testCalculateEndpoints() throws UnknownHostException
    {
        final int NODES = 100;
        final int VNODES = 64;
        final int RUNS = 10;
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        Map<String, Integer> datacenters = ImmutableMap.of("rf1", 1, "rf3", 3, "rf5_1", 5, "rf5_2", 5, "rf5_3", 5);
        List<InetAddress> nodes = new ArrayList<>(NODES);
        for (byte i=0; i<NODES; ++i)
            nodes.add(InetAddress.getByAddress(new byte[]{127, 0, 0, i}));
        for (int run=0; run<RUNS; ++run)
        {
            Random rand = new Random();
            IEndpointSnitch snitch = generateSnitch(datacenters, nodes, rand);
            DatabaseDescriptor.setEndpointSnitch(snitch);

            TokenMetadata meta = new TokenMetadata();
            for (int i=0; i<NODES; ++i)  // Nodes
                for (int j=0; j<VNODES; ++j) // tokens/vnodes per node
                    meta.updateNormalToken(Murmur3Partitioner.instance.getRandomToken(rand), nodes.get(i));
            testEquivalence(meta, snitch, datacenters, rand);
        }
    }

    void testEquivalence(TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, Integer> datacenters, Random rand)
    {
        NetworkTopologyStrategy nts = new NetworkTopologyStrategy("ks", tokenMetadata, snitch,
                                                                  datacenters.entrySet().stream().
                                                                      collect(Collectors.toMap(x -> x.getKey(), x -> Integer.toString(x.getValue()))));
        for (int i=0; i<1000; ++i)
        {
            Token token = Murmur3Partitioner.instance.getRandomToken(rand);
            List<InetAddress> expected = calculateNaturalEndpoints(token, tokenMetadata, datacenters, snitch);
            List<InetAddress> actual = nts.calculateNaturalEndpoints(token, tokenMetadata);
            if (endpointsDiffer(expected, actual))
            {
                System.err.println("Endpoints mismatch for token " + token);
                System.err.println(" expected: " + expected);
                System.err.println(" actual  : " + actual);
                Assert.assertEquals("Endpoints for token " + token + " mismatch.", expected, actual);
            }
        }
    }

    private boolean endpointsDiffer(List<InetAddress> ep1, List<InetAddress> ep2)
    {
        // Because the old algorithm does not put the nodes in the correct order in the case where more replicas
        // are required than there are racks in a dc, we accept different order as long as the primary
        // replica is the same.
        if (ep1.equals(ep2))
            return false;
        if (!ep1.get(0).equals(ep2.get(0)))
            return true;
        Set<InetAddress> s1 = new HashSet<>(ep1);
        Set<InetAddress> s2 = new HashSet<>(ep2);
        return !s1.equals(s2);
    }

    IEndpointSnitch generateSnitch(Map<String, Integer> datacenters, Collection<InetAddress> nodes, Random rand)
    {
        final Map<InetAddress, String> nodeToRack = new HashMap<>();
        final Map<InetAddress, String> nodeToDC = new HashMap<>();
        Map<String, List<String>> racksPerDC = new HashMap<>();
        datacenters.forEach((dc, rf) -> racksPerDC.put(dc, randomRacks(rf, rand)));
        int rf = datacenters.values().stream().mapToInt(x -> x).sum();
        String[] dcs = new String[rf];
        int pos = 0;
        for (Map.Entry<String, Integer> dce : datacenters.entrySet())
        {
            for (int i = 0; i < dce.getValue(); ++i)
                dcs[pos++] = dce.getKey();
        }

        for (InetAddress node : nodes)
        {
            String dc = dcs[rand.nextInt(rf)];
            List<String> racks = racksPerDC.get(dc);
            String rack = racks.get(rand.nextInt(racks.size()));
            nodeToRack.put(node, rack);
            nodeToDC.put(node, dc);
        }

        return new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddress endpoint)
            {
                return nodeToRack.get(endpoint);
            }

            public String getDatacenter(InetAddress endpoint)
            {
                return nodeToDC.get(endpoint);
            }
        };
    }

    private List<String> randomRacks(int rf, Random rand)
    {
        int rc = rand.nextInt(rf * 3 - 1) + 1;
        List<String> racks = new ArrayList<>(rc);
        for (int i=0; i<rc; ++i)
            racks.add(Integer.toString(i));
        return racks;
    }

    // Copy of older endpoints calculation algorithm for comparison
    public static List<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata, Map<String, Integer> datacenters, IEndpointSnitch snitch)
    {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Set<InetAddress> replicas = new LinkedHashSet<>();
        // replicas we have found in each DC
        Map<String, Set<InetAddress>> dcReplicas = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            dcReplicas.put(dc.getKey(), new HashSet<InetAddress>(dc.getValue()));

        Topology topology = tokenMetadata.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, ImmutableMultimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        // tracks the racks we have already placed replicas in
        Map<String, Set<String>> seenRacks = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            seenRacks.put(dc.getKey(), new HashSet<String>());

        // tracks the endpoints that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result so we don't have to wind back the iterator
        Map<String, Set<InetAddress>> skippedDcEndpoints = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            skippedDcEndpoints.put(dc.getKey(), new LinkedHashSet<InetAddress>());

        Iterator<Token> tokenIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);
        while (tokenIter.hasNext() && !hasSufficientReplicas(dcReplicas, allEndpoints, datacenters))
        {
            Token next = tokenIter.next();
            InetAddress ep = tokenMetadata.getEndpoint(next);
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc?
            if (!datacenters.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                continue;
            // can we skip checking the rack?
            if (seenRacks.get(dc).size() == racks.get(dc).keySet().size())
            {
                dcReplicas.get(dc).add(ep);
                replicas.add(ep);
            }
            else
            {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (seenRacks.get(dc).contains(rack))
                {
                    skippedDcEndpoints.get(dc).add(ep);
                }
                else
                {
                    dcReplicas.get(dc).add(ep);
                    replicas.add(ep);
                    seenRacks.get(dc).add(rack);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (seenRacks.get(dc).size() == racks.get(dc).keySet().size())
                    {
                        Iterator<InetAddress> skippedIt = skippedDcEndpoints.get(dc).iterator();
                        while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                        {
                            InetAddress nextSkipped = skippedIt.next();
                            dcReplicas.get(dc).add(nextSkipped);
                            replicas.add(nextSkipped);
                        }
                    }
                }
            }
        }

        return new ArrayList<InetAddress>(replicas);
    }

    private static boolean hasSufficientReplicas(String dc, Map<String, Set<InetAddress>> dcReplicas, Multimap<String, InetAddress> allEndpoints, Map<String, Integer> datacenters)
    {
        return dcReplicas.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc, datacenters));
    }

    private static boolean hasSufficientReplicas(Map<String, Set<InetAddress>> dcReplicas, Multimap<String, InetAddress> allEndpoints, Map<String, Integer> datacenters)
    {
        for (String dc : datacenters.keySet())
            if (!hasSufficientReplicas(dc, dcReplicas, allEndpoints, datacenters))
                return false;
        return true;
    }

    public static int getReplicationFactor(String dc, Map<String, Integer> datacenters)
    {
        Integer replicas = datacenters.get(dc);
        return replicas == null ? 0 : replicas;
    }
}
