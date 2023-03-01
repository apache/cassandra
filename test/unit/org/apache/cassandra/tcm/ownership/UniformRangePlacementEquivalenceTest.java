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

package org.apache.cassandra.tcm.ownership;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.MembershipUtils;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.tcm.membership.MembershipUtils.nodeAddresses;
import static org.junit.Assert.assertEquals;

public class UniformRangePlacementEquivalenceTest
{
    private static Map<String, Integer> DATACENTERS = ImmutableMap.of("rf1", 1, "rf3", 3, "rf5_1", 5, "rf5_2", 5, "rf5_3", 5);
    private static final String KEYSPACE = "ks";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testSSPlacement()
    {
        int[] rfValues = new int[] {1, 2, 3, 5, 12};
        for (int rf : rfValues)
        {
            KeyspaceParams params = KeyspaceParams.simple(rf);
            KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, params);
            BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy = ssProvider(params.replication);
            testCalculateEndpoints(1, ksm, strategy);
            testCalculateEndpoints(16, ksm, strategy);
            testCalculateEndpoints(64, ksm, strategy);
        }
    }

    @Test
    public void testNTSPlacement()
    {
        KeyspaceParams params = toNTSParams(DATACENTERS);
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, params);
        BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy = ntsProvider(params.replication);
        testCalculateEndpoints(1, ksm, strategy);
        testCalculateEndpoints(16, ksm, strategy);
        testCalculateEndpoints(64, ksm, strategy);
    }

    private void testCalculateEndpoints(int tokensPerNode,
                                        KeyspaceMetadata keyspace,
                                        BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy)
    {
        final int NODES = 100;
        final int VNODES = tokensPerNode;
        final int RUNS = 10;

        List<InetAddressAndPort> endpoints = nodes(NODES);

        for (int run = 0; run < RUNS; ++run)
        {
            Random rand = new Random();
            IEndpointSnitch snitch = generateSnitch(DATACENTERS, endpoints, rand);
            DatabaseDescriptor.setEndpointSnitch(snitch);
            TokenMetadata tokenMetadata = new TokenMetadata(snitch);
            Directory directory = directory(endpoints, snitch);
            ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, directory);


            for (int i = 0; i < NODES; ++i)
                metadata = joinNode(metadata, tokenMetadata, endpoints, i, VNODES, rand);

            AbstractReplicationStrategy strat = strategy.apply(tokenMetadata, snitch);
            Function<Token, List<InetAddressAndPort>> oldLocatorFn = token ->  new ArrayList<>(strat.calculateNaturalReplicas(token, tokenMetadata).endpoints());

            UniformRangePlacement layout = new UniformRangePlacement();
            DataPlacement placement = layout.calculatePlacements(metadata, Keyspaces.of(keyspace)).get(keyspace.params.replication);
            Function<Token, List<InetAddressAndPort>> newLocatorFn = t -> placement.reads.forToken(t).endpointList();

            testEquivalence(oldLocatorFn, newLocatorFn, rand);
        }
    }

    @Test
    public void testSSPlacementTransformation()
    {
        int[] rfValues = new int[] {1, 2, 3, 5, 12};
        for (int rf : rfValues)
        {
            KeyspaceParams params = KeyspaceParams.simple(rf);
            KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, params);
            BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy = ssProvider(params.replication);
            doTransformationTest(1, ksm, strategy);
            doTransformationTest(16, ksm, strategy);
            doTransformationTest(64, ksm, strategy);
        }
    }

    @Test
    public void testNTSPlacementTransformation()
    {
        KeyspaceParams params = toNTSParams(DATACENTERS);
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, params);
        BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy = ntsProvider(params.replication);
        doTransformationTest(1, ksm, strategy);
        doTransformationTest(16, ksm, strategy);
        doTransformationTest(64, ksm, strategy);
    }

    private void doTransformationTest(int tokensPerNode,
                                      KeyspaceMetadata ksm,
                                      BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> strategy)
    {
        final int NODES = 100;
        final int VNODES = tokensPerNode;

        UniformRangePlacement layout = new UniformRangePlacement();
        Keyspaces keyspaces = Keyspaces.of(ksm);
        long seed = System.nanoTime();
        String log = String.format("Running transformation test with params; " +
                                   "Seed: %d, Nodes: %d, VNodes: %d, Replication: %s",
                                   seed, NODES, VNODES, ksm.params.replication);
        System.out.println(log);
        Random rand = new Random(seed);

        // initialise nodes for the test
        List<InetAddressAndPort> endpoints = nodes(NODES);
        IEndpointSnitch snitch = generateSnitch(DATACENTERS, endpoints, rand);
        DatabaseDescriptor.setEndpointSnitch(snitch);
        TokenMetadata tokenMetadata = new TokenMetadata(snitch);
        Directory directory = directory(endpoints, snitch);
        ClusterMetadata metadata = new ClusterMetadata(Murmur3Partitioner.instance, directory);

        // join all but one of the nodes
        for (int i = 0; i < NODES - 1; ++i)
            metadata = joinNode(metadata, tokenMetadata, endpoints, i, VNODES, rand);


        // verify that the old and new placement/location methods agree
        PlacementForRange p1 = layout.calculatePlacements(metadata, keyspaces).get(ksm.params.replication).reads;
        AbstractReplicationStrategy strat = strategy.apply(tokenMetadata, snitch);
        Function<Token, List<InetAddressAndPort>> oldLocatorFn = oldLocatorFn(strat, tokenMetadata);
        Function<Token, List<InetAddressAndPort>> newLocatorFn = t -> p1.forToken(t).endpointList();
        testEquivalence(oldLocatorFn, newLocatorFn, rand);

        // now add the remaining node
        metadata = joinNode(metadata, tokenMetadata, endpoints, NODES - 1, VNODES, rand);

        // re-check the placements
        PlacementForRange p2 = layout.calculatePlacements(metadata, keyspaces).get(ksm.params.replication).reads;
        strat = strategy.apply(tokenMetadata, snitch);
        oldLocatorFn = oldLocatorFn(strat, tokenMetadata);
        newLocatorFn = t -> p2.forToken(t).endpointList();
        testEquivalence(oldLocatorFn, newLocatorFn, rand);

        // get the specific operations needed to transform the first placement with
        // the initial nodes to the new one with all nodes. Then apply those operations
        // to the first placement and assert the result matches the second placement
        // which was directly calculated with all nodes having joined.
        Delta delta = p1.difference(p2);
        PlacementForRange p3 = p1.without(delta.removals).with(delta.additions);
        newLocatorFn = t -> p3.forToken(t).endpointList();
        testEquivalence(oldLocatorFn, newLocatorFn, rand);
        testRangeEquivalence(p2, p3);
    }

    private Directory directory(List<InetAddressAndPort> endpoints, IEndpointSnitch snitch)
    {
        Directory directory = new Directory();
        for (InetAddressAndPort endpoint : endpoints)
            directory = directory.with(nodeAddresses(endpoint), new Location(snitch.getDatacenter(endpoint), snitch.getRack(endpoint)));
        return directory;
    }

    private ClusterMetadata joinNode(ClusterMetadata metadata,
                                     TokenMetadata tokenMetadata,
                                     List<InetAddressAndPort> endpoints,
                                     int peerIndex,
                                     int tokensPerNode,
                                     Random rand)
    {
        return joinNode(metadata,
                        tokenMetadata,
                        endpoints,
                        peerIndex,
                        tokensPerNode,
                        () -> Murmur3Partitioner.instance.getRandomToken(rand));
    }

    private ClusterMetadata joinNode(ClusterMetadata metadata,
                                     TokenMetadata tokenMetadata,
                                     List<InetAddressAndPort> endpoints,
                                     int peerIndex,
                                     int tokensPerNode,
                                     Supplier<Token> token)
    {
        InetAddressAndPort endpoint = endpoints.get(peerIndex);
        NodeId id = metadata.directory.peerId(endpoint);
        Set<Token> tokens = new HashSet<>();
        for (int j = 0; j < tokensPerNode; ++j)
            tokens.add(token.get());
        tokenMetadata.updateNormalTokens(tokens, endpoint);
        tokenMetadata.updateHostId(id.uuid, endpoint);
        return metadata.transformer().proposeToken(id, tokens).build().metadata;
    }

    private void testEquivalence(Function<Token, List<InetAddressAndPort>> oldLocatorFn,
                                 Function<Token, List<InetAddressAndPort>> newLocatorFn,
                                 Random rand)
    {
        for (int i=0; i<1000; ++i)
        {
            Token token = Murmur3Partitioner.instance.getRandomToken(rand);
            List<InetAddressAndPort> expected = oldLocatorFn.apply(token);
            List<InetAddressAndPort> actual = newLocatorFn.apply(token);
            if (endpointsDiffer(expected, actual))
            {
                System.err.println("Endpoints mismatch for token " + token);
                System.err.println(" expected: " + expected);
                System.err.println(" actual  : " + actual);
                assertEquals("Endpoints for token " + token + " mismatch.", expected, actual);
            }
        }
    }

    private boolean endpointsDiffer(List<InetAddressAndPort> ep1, List<InetAddressAndPort> ep2)
    {
        if (ep1.equals(ep2))
            return false;
        // Because the old algorithm does not put the nodes in the correct order in the case where more replicas
        // are required than there are racks in a dc, we accept different order as long as the primary
        // replica is the same.
        if (!ep1.get(0).equals(ep2.get(0)))
            return true;
        Set<InetAddressAndPort> s1 = new HashSet<>(ep1);
        Set<InetAddressAndPort> s2 = new HashSet<>(ep2);
        return !s1.equals(s2);
    }

    private void testRangeEquivalence(PlacementForRange p1, PlacementForRange p2)
    {
        RangesByEndpoint byEndpoint1 = p1.byEndpoint();
        RangesByEndpoint byEndpoint2 = p2.byEndpoint();
        assertEquals(byEndpoint1.keySet(), byEndpoint2.keySet());

        for (InetAddressAndPort endpoint : byEndpoint1.keySet())
        {
            assertEquals(Range.normalize(byEndpoint1.get(endpoint).ranges()),
                         Range.normalize(byEndpoint2.get(endpoint).ranges()));
        }
    }

    private Function<Token, List<InetAddressAndPort>> oldLocatorFn(AbstractReplicationStrategy strategy,
                                                                   TokenMetadata tokenMetadata)
    {
        return token ->  new ArrayList<>(strategy.calculateNaturalReplicas(token, tokenMetadata).endpoints());
    }

    private BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> ssProvider(ReplicationParams params)
    {
        return (tokenMetadata, snitch) -> {
            Map<String, String> p = new HashMap<>(params.options);
            p.remove(ReplicationParams.CLASS);
            return new SimpleStrategy(KEYSPACE, tokenMetadata, snitch, p);
        };
    }

    private BiFunction<TokenMetadata, IEndpointSnitch, AbstractReplicationStrategy> ntsProvider(ReplicationParams params)
    {
        return (tokenMetadata, snitch) -> {
            Map<String, String> p = new HashMap<>(params.options);
            p.remove(ReplicationParams.CLASS);
            return new NetworkTopologyStrategy(KEYSPACE, tokenMetadata, snitch, p);
        };
    }

    private List<InetAddressAndPort> nodes(int count)
    {
        List<InetAddressAndPort> nodes = new ArrayList<>(count);
        for (byte i=1; i<=count; ++i)
            nodes.add(MembershipUtils.endpoint(i));
        return nodes;
    }

    private KeyspaceParams toNTSParams(Map<String, Integer> datacenters)
    {
        List<String> args = new ArrayList<>(datacenters.size() * 2);
        datacenters.forEach((key, val) -> { args.add(key); args.add(Integer.toString(val));});
        return KeyspaceParams.nts(args.toArray());
    }

    IEndpointSnitch generateSnitch(Map<String, Integer> datacenters, Collection<InetAddressAndPort> nodes, Random rand)
    {
        final Map<InetAddressAndPort, String> nodeToRack = new HashMap<>();
        final Map<InetAddressAndPort, String> nodeToDC = new HashMap<>();
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

        for (InetAddressAndPort node : nodes)
        {
            String dc = dcs[rand.nextInt(rf)];
            List<String> racks = racksPerDC.get(dc);
            String rack = racks.get(rand.nextInt(racks.size()));
            nodeToRack.put(node, rack);
            nodeToDC.put(node, dc);
        }

        return new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return nodeToRack.get(endpoint);
            }

            public String getDatacenter(InetAddressAndPort endpoint)
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
}
