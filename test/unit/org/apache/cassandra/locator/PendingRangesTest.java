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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PendingRangesTest
{
    private static final String RACK1 = "RACK1";
    private static final String DC1 = "DC1";
    private static final String KEYSPACE = "ks";
    private static final InetAddressAndPort PEER1 = peer(1);
    private static final InetAddressAndPort PEER2 = peer(2);
    private static final InetAddressAndPort PEER3 = peer(3);
    private static final InetAddressAndPort PEER4 = peer(4);
    private static final InetAddressAndPort PEER5 = peer(5);
    private static final InetAddressAndPort PEER6 = peer(6);

    private static final InetAddressAndPort PEER1A = peer(11);
    private static final InetAddressAndPort PEER4A = peer(14);

    private static final Token TOKEN1 = token(0);
    private static final Token TOKEN2 = token(10);
    private static final Token TOKEN3 = token(20);
    private static final Token TOKEN4 = token(30);
    private static final Token TOKEN5 = token(40);
    private static final Token TOKEN6 = token(50);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setup()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(ClusterMetadataTestHelper.syncInstanceForTest());
        ClusterMetadataService.instance().log().bootstrap(FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void calculatePendingRangesForConcurrentReplacements()
    {
        /*
         * As described in CASSANDRA-14802, concurrent range movements can generate pending ranges
         * which are far larger than strictly required, which in turn can impact availability.
         *
         * In the narrow case of straight replacement, the pending ranges should mirror the owned ranges
         * of the nodes being replaced.
         *
         * Note: the following example is purely illustrative as the iteration order for processing
         * bootstrapping endpoints is not guaranteed. Because of this, precisely which endpoints' pending
         * ranges are correct/incorrect depends on the specifics of the ring. Concretely, the bootstrap tokens
         * are ultimately backed by a HashMap, so iteration of bootstrapping nodes is based on the hashcodes
         * of the endpoints.
         *
         * E.g. a 6 node cluster with tokens:
         *
         * nodeA : 0
         * nodeB : 10
         * nodeC : 20
         * nodeD : 30
         * nodeE : 40
         * nodeF : 50
         *
         * with an RF of 3, this gives an initial ring of :
         *
         * nodeA : (50, 0], (40, 50], (30, 40]
         * nodeB : (0, 10], (50, 0], (40, 50]
         * nodeC : (10, 20], (0, 10], (50, 0]
         * nodeD : (20, 30], (10, 20], (0, 10]
         * nodeE : (30, 40], (20, 30], (10, 20]
         * nodeF : (40, 50], (30, 40], (20, 30]
         *
         * If nodeA is replaced by node1A, then the pending ranges map should be:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A]
         * }
         *
         * Starting a second concurrent replacement of a node with non-overlapping ranges
         * (i.e. node4 for node4A) should result in a pending range map of:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A],
         *   (20, 30] : [node4A],
         *   (10, 20] : [node4A],
         *   (0, 10]  : [node4A]
         * }
         *
         * But, the bug in CASSANDRA-14802 causes it to be:
         * {
         *   (50, 0]  : [node1A],
         *   (40, 50] : [node1A],
         *   (30, 40] : [node1A],
         *   (20, 30] : [node4A],
         *   (10, 20] : [node4A],
         *   (50, 10] : [node4A]
         * }
         *
         * so node4A incorrectly becomes a pending endpoint for an additional sub-range: (50, 0).
         *
         */

        // setup initial ring
        addNode(PEER1, TOKEN1);
        addNode(PEER2, TOKEN2);
        addNode(PEER3, TOKEN3);
        addNode(PEER4, TOKEN4);
        addNode(PEER5, TOKEN5);
        addNode(PEER6, TOKEN6);

        AbstractReplicationStrategy replicationStrategy = simpleStrategy(3);
        ClusterMetadataTestHelper.createKeyspace(KEYSPACE, KeyspaceParams.simple(3));
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ks = metadata.schema.getKeyspaceMetadata(KEYSPACE);

        // no pending ranges before any replacements
        assertEquals(0, metadata.pendingRanges(ks).size());

        // Ranges initially owned by PEER1 and PEER4
        RangesAtEndpoint peer1Ranges = replicationStrategy.getAddressReplicas(metadata).get(PEER1).unwrap();
        RangesAtEndpoint peer4Ranges = replicationStrategy.getAddressReplicas(metadata).get(PEER4).unwrap();
        // Replace PEER1 with PEER1A
        replace(PEER1, PEER1A, TOKEN1);
        // The only pending ranges should be the ones previously belonging to PEER1
        // and these should have a single pending endpoint, PEER1A
        RangesByEndpoint.Builder b1 = new RangesByEndpoint.Builder();
        peer1Ranges.iterator().forEachRemaining(replica -> b1.put(PEER1A, new Replica(PEER1A, replica.range(), replica.isFull())));
        RangesByEndpoint expected = b1.build();
        assertPendingRanges(ClusterMetadata.current().pendingRanges(ks), expected);

        // Replace PEER4 with PEER4A
        replace(PEER4, PEER4A, TOKEN4);
        // Pending ranges should now include the ranges originally belonging
        // to PEER1 (now pending for PEER1A) and the ranges originally belonging to PEER4
        // (now pending for PEER4A).
        RangesByEndpoint.Builder b2 = new RangesByEndpoint.Builder();
        peer1Ranges.iterator().forEachRemaining(replica -> b2.put(PEER1A, new Replica(PEER1A, replica.range(), replica.isFull())));
        peer4Ranges.iterator().forEachRemaining(replica -> b2.put(PEER4A, new Replica(PEER4A, replica.range(), replica.isFull())));
        expected = b2.build();
        assertPendingRanges(ClusterMetadata.current().pendingRanges(ks), expected);
    }

    @Test
    public void testConcurrentAdjacentLeaveAndMove()
    {
        // TODO This fails with TCM as the constraints regarding concurrent overlapping range movements and
        //      replication factors have moved into the operations themselves. Previously, these were enforced
        //      at the StorageService level.
        ClusterMetadataTestHelper.createKeyspace(KEYSPACE, KeyspaceParams.simple(3));
        Token newToken = token(0);
        Token token1 = token(-9);
        Token token2 = token(-5);
        Token token3 = token(-1);
        Token token4 = token(1);
        Token token5 = token(5);

        InetAddressAndPort node1 = peer(1);
        InetAddressAndPort node2 = peer(2);
        InetAddressAndPort node3 = peer(3);
        InetAddressAndPort node4 = peer(4);
        InetAddressAndPort node5 = peer(5);

        // setup initial ring
        addNode(node1, token1);
        addNode(node2, token2);
        addNode(node3, token3);
        addNode(node4, token4);
        addNode(node5, token5);

        leave(node5);
        move(node3, newToken);
        Map<InetAddressAndPort, RangesAtEndpoint> pendingByEndpoint = getPendingRangesByEndpoint();
        assertRangesAtEndpoint(RangesAtEndpoint.of(new Replica(node1, new Range<>(token2, token3), true)),
                               pendingByEndpoint.get(node1));
        assertRangesAtEndpoint(RangesAtEndpoint.of(new Replica(node2, new Range<>(token3, token4), true)),
                               pendingByEndpoint.get(node2));
        assertRangesAtEndpoint(RangesAtEndpoint.of(new Replica(node3, new Range<>(token3, newToken), true),
                                                   new Replica(node3, new Range<>(token4, token5), true)),
                               pendingByEndpoint.get(node3));
        assertRangesAtEndpoint(RangesAtEndpoint.empty(node4), pendingByEndpoint.get(node4));
        assertRangesAtEndpoint(RangesAtEndpoint.empty(node5), pendingByEndpoint.get(node5));
    }

    @Test
    public void testConcurrentAdjacentLeavingNodes()
    {
        // TODO This fails with TCM as the constraints regarding concurrent overlapping range movements and
        //      replication factors have moved into the operations themselves. Previously, these were enforced
        //      at the StorageService level.
        ClusterMetadataTestHelper.createKeyspace(KEYSPACE, KeyspaceParams.simple(2));

        Token token1 = token(-9);
        Token token2 = token(-4);
        Token token3 = token(0);
        Token token4 = token(4);

        InetAddressAndPort node1 = peer(1);
        InetAddressAndPort node2 = peer(2);
        InetAddressAndPort node3 = peer(3);
        InetAddressAndPort node4 = peer(4);

        addNode(node1, token1);
        addNode(node2, token2);
        addNode(node3, token3);
        addNode(node4, token4);

        leave(node2);
        leave(node3);
        Map<InetAddressAndPort, RangesAtEndpoint> pendingByEndpoint = getPendingRangesByEndpoint();
        assertRangesAtEndpoint(RangesAtEndpoint.of(new Replica(node1, new Range<>(token1, token3), true)),
                               pendingByEndpoint.get(node1));
        assertRangesAtEndpoint(RangesAtEndpoint.empty(node2), pendingByEndpoint.get(node2));
        assertRangesAtEndpoint(RangesAtEndpoint.empty(node3), pendingByEndpoint.get(node3));
        assertRangesAtEndpoint(RangesAtEndpoint.of(new Replica(node4, new Range<>(token4, token1), true),
                                                   new Replica(node4, new Range<>(token1, token2), true)),
                               pendingByEndpoint.get(node4));
    }

    // TODO assess whether it's valuable to port these randomised tests to TCM given that we have a largely equivalent
    //      test in MetadataChangeSimulationTest
/*
    @Test
    public void testBootstrapLeaveAndMovePermutationsWithoutVnodes()
    {
        // In a non-vnode cluster (i.e. where tokensPerNode == 1), we can
        // add, remove and move nodes
        int maxRf = 5;
        int nodes = 50;
        Gen<Integer> rfs = rf(maxRf);
        Gen<Input> inputs = rfs.flatMap(rf -> input(rf, nodes));

        qt().forAll(inputs.flatMap(this::clustersWithChangedTopology))
            .checkAssert(Cluster::calculateAndGetPendingRanges);
    }

    @Test
    public void testBootstrapAndLeavePermutationsWithVnodes()
    {
        // In a vnode cluster (i.e. where tokensPerNode > 1), move is not
        // supported, so only leave and bootstrap operations will occur
        int maxRf = 5;
        int nodes = 50;
        int maxTokensPerNode = 16;

        Gen<Integer> rfs = rf(maxRf);
        Gen<Input> inputs = rfs.flatMap(rf -> input(rf, nodes, maxTokensPerNode));

        qt().forAll(inputs.flatMap(this::clustersWithChangedTopology))
            .checkAssert(Cluster::calculateAndGetPendingRanges);
    }

    private Gen<Integer> rf(int maxRf)
    {
        return integers().between(1, maxRf);
    }

    private Gen<Input> input(int rf, int maxNodes)
    {
        return integers().between(rf, maxNodes).map(n -> new Input(rf, n, 1));
    }

    private Gen<Input> input(int rf, int maxNodes, int maxTokensPerNode)
    {
        Gen<Integer> tokensPerNode = integers().between(1, maxTokensPerNode);
        return integers().between(rf, maxNodes)
                         .zip(tokensPerNode, (n, tokens) -> new Input(rf, n, tokens));
    }

    private Gen<Integer> bootstrappedNodes(Input input)
    {
        // at most double in size
        return integers().between(0, input.nodes);
    }

    private Gen<Integer> leftNodes(Input input)
    {
        return integers().between(0, input.nodes - input.rf);
    }

    private Gen<Integer> movedNodes(Input input)
    {
        // Move is not supported in vnode clusters
        if (input.tokensPerNode > 1)
            return integers().between(0, 0);

        return integers().between(0, input.nodes);
    }

    private Gen<Cluster> clusters(Input input)
    {
        return Generate.constant(() -> new Cluster(input.rf, input.nodes, input.tokensPerNode));
    }

    private Gen<Cluster> clustersWithChangedTopology(Input input)
    {
        Gen<Cluster> clusters = clusters(input);
        Gen<Integer> leftNodes = leftNodes(input);
        Gen<Integer> bootstrappedNodes = bootstrappedNodes(input);
        Gen<Integer> movedNodes = movedNodes(input);

        return clusters.zip(leftNodes, bootstrappedNodes, movedNodes,
                            (cluster, left, bootstrapped, moved) -> cluster.decommissionNodes(left)
                                                                           .bootstrapNodes(bootstrapped)
                                                                           .moveNodes(moved));
    }

    static class Input
    {
        final int rf;
        final int nodes;
        final int tokensPerNode;

        Input(int rf, int nodes, int tokensPerNode)
        {
            this.rf = rf;
            this.nodes = nodes;
            this.tokensPerNode = tokensPerNode;
        }

        public String toString()
        {
            return String.format("Input(rf=%s, nodes=%s, tokensPerNode=%s)", rf, nodes, tokensPerNode);
        }
    }

    private static class Cluster
    {
        private TokenMetadata tm;
        private final int tokensPerNode;
        private final AbstractReplicationStrategy strategy;

        private final List<InetAddressAndPort> nodes;
        Random random = new Random();

        Cluster(int rf, int initialNodes, int tokensPerNode)
        {
            this.nodes = new ArrayList<>(initialNodes);

            this.tokensPerNode = tokensPerNode;
            this.tm = buildTmd(initialNodes);

            this.strategy = simpleStrategy(rf);
        }

        private TokenMetadata buildTmd(int initialNodes)
        {
            TokenMetadata tmd = new DefaultTokenMetadata();
            TokenMetadata.Transformer tmdTransformer = tmd.transformer();

            for (int i = 0; i < initialNodes; i++)
            {
                int id = nodes.size() + 1;
                InetAddressAndPort node = peer(id);
                tmdTransformer = tmdTransformer.withHostId(new UUID(0, id), node)
                                               .withNormalTokens(tokens(), node);
                nodes.add(node);
            }
            return tmdTransformer.build();
        }

        private void bootstrapNode()
        {
            int id = nodes.size() + 1;
            InetAddressAndPort node = peer(id);
            TokenMetadata.Transformer transformer = tm.transformer()
                                                      .withHostId(new UUID(0, id), node);
            transformer.withBootstrapTokens(tokens(), node, null);
            nodes.add(node);
            tm = transformer.build();
        }

        void calculateAndGetPendingRanges()
        {
            // test that it does not crash
            for (InetAddressAndPort node : nodes)
            {
                ((DefaultTokenMetadata) tm).getPendingRanges(KEYSPACE, strategy).atEndpoint(node);
            }
        }

        Cluster decommissionNodes(int cnt)
        {
            TokenMetadata.Transformer transformer = tm.transformer();
            for (int i = 0; i < cnt; i++)
                transformer = transformer.withLeavingEndpoint(nodes.get(random.nextInt(nodes.size())));

            tm = transformer.build();
            return this;
        }

        Cluster bootstrapNodes(int cnt)
        {
            for (int i = 0; i < cnt; i++)
                bootstrapNode();
            return this;
        }

        Cluster moveNodes(int cnt)
        {
            assert cnt == 0 || tokensPerNode == 1 : "Moving tokens is not supported when tokensPerNode";

            for (int i = 0; i < cnt; i++)
                moveNode();
            return this;
        }

        synchronized private void moveNode()
        {
            if (tm.getMovingEndpoints().size() >= nodes.size())
                throw new IllegalStateException("Number of movements should not exceed total nodes in the cluster");

            // we want to ensure that any given node is only marked as moving once.
            // todo: made sure we don't try to move a leaving node - this seems to have been allowed before, investigate why
            List<InetAddressAndPort> moveCandidates = nodes.stream()
                                                           .filter(node -> tm.getMovingEndpoints().stream().noneMatch(p -> p.right.equals(node)))
                                                           .filter(node -> tm.getLeavingEndpoints().stream().noneMatch(node::equals))
                                                           .collect(Collectors.toList());
            if (moveCandidates.size() > 0)
            {
                InetAddressAndPort node = moveCandidates.get(random.nextInt(moveCandidates.size()));
                tm = tm.transformer().withMovingEndpoint(token(random.nextLong()), node).build();
            }
        }

        private Collection<Token> tokens()
        {
            Collection<Token> tokens = new ArrayList<>(tokensPerNode);
            for (int i=0; i< tokensPerNode; i++)
                tokens.add(token(random.nextLong()));
            return tokens;
        }

        @Override
        public String toString()
        {
            return String.format("Nodes: %s\n" +
                                 "Metadata: %s",
                                 nodes.size(),
                                 tm.toString());
        }
    }
*/

    private static Map<InetAddressAndPort, RangesAtEndpoint> getPendingRangesByEndpoint()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        KeyspaceMetadata ks = metadata.schema.getKeyspaceMetadata(KEYSPACE);
        Map<Range<Token>, EndpointsForRange> pending = metadata.pendingRanges(ks);
        Map<InetAddressAndPort, RangesAtEndpoint.Builder> byEndpointBuilder = new HashMap<>();
        pending.forEach((range, endpoints) -> {
            endpoints.forEach(r -> {
                RangesAtEndpoint.Builder builder = byEndpointBuilder.computeIfAbsent(r.endpoint(), RangesAtEndpoint::builder);
                builder.add(r);
            });
        });
        Map<InetAddressAndPort, RangesAtEndpoint> pendingByEndpoint = new HashMap<>();
        byEndpointBuilder.forEach((endpoint, builder) -> pendingByEndpoint.put(endpoint, builder.build()));
        return pendingByEndpoint;
    }

    private void assertPendingRanges(Map<Range<Token>, EndpointsForRange> pending, RangesByEndpoint expected)
    {
        RangesByEndpoint.Builder actual = new RangesByEndpoint.Builder();
        pending.entrySet().forEach(pendingRange -> {
            Replica replica = Iterators.getOnlyElement(pendingRange.getValue().iterator());
            actual.put(replica.endpoint(), replica);
        });
        assertRangesByEndpoint(expected, actual.build());
    }

    private void assertPendingRanges(EndpointsByRange pending, RangesByEndpoint expected)
    {
        RangesByEndpoint.Builder actual = new RangesByEndpoint.Builder();
        pending.flattenEntries().forEach(entry -> actual.put(entry.getValue().endpoint(), entry.getValue()));
        assertRangesByEndpoint(expected, actual.build());
    }

    private void assertRangesAtEndpoint(RangesAtEndpoint expected, RangesAtEndpoint actual)
    {
        assertEquals("expected = "+expected +", actual = " + actual, expected.size(), actual.size());
        assertTrue("expected = "+expected +", actual = " + actual , Iterables.all(expected, actual::contains));
    }

    private void assertRangesByEndpoint(RangesByEndpoint expected, RangesByEndpoint actual)
    {
        assertEquals(expected.keySet(), actual.keySet());
        for (InetAddressAndPort endpoint : expected.keySet())
        {
            RangesAtEndpoint expectedReplicas = expected.get(endpoint);
            RangesAtEndpoint actualReplicas = actual.get(endpoint);
            assertRangesAtEndpoint(expectedReplicas, actualReplicas);
        }
    }

    private void addNode(InetAddressAndPort replica, Token token)
    {
        registerNode(replica);
        ClusterMetadataTestHelper.join(replica, Collections.singleton(token));
    }

    private void registerNode(InetAddressAndPort replica)
    {
        ClusterMetadataTestHelper.register(replica, DC1, RACK1);
    }

    private void replace(InetAddressAndPort toReplace,
                         InetAddressAndPort replacement,
                         Token token)
    {
        // begin replacement, but don't complete it
        ClusterMetadata metadata = ClusterMetadata.current();
        assertEquals(toReplace, metadata.directory.endpoint(metadata.tokenMap.owner(token)));
        registerNode(replacement);
        ClusterMetadataTestHelper.lazyReplace(toReplace, replacement)
                                 .prepareReplace()
                                 .startReplace();
    }

    private void leave(InetAddressAndPort leaving)
    {
        // begin a decommission, but don't complete it
        ClusterMetadataTestHelper.lazyLeave(leaving, false)
                                 .prepareLeave()
                                 .startLeave();
    }

    private void move(InetAddressAndPort moving, Token newToken)
    {
        // begin a move, but don't complete it
        ClusterMetadataTestHelper.lazyMove(moving, Collections.singleton(newToken))
                                 .prepareMove()
                                 .startMove();
    }

    private static Token token(long token)
    {
        return Murmur3Partitioner.instance.getTokenFactory().fromString(Long.toString(token));
    }

    private static InetAddressAndPort peer(int addressSuffix)
    {
        try
        {
            return InetAddressAndPort.getByAddress(new byte[]{ 127, 0, 0, (byte) addressSuffix});
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static IEndpointSnitch snitch()
    {
        return new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                return DC1;
            }
        };
    }

    private static AbstractReplicationStrategy simpleStrategy(int replicationFactor)
    {
        return new SimpleStrategy(KEYSPACE,
                                  Collections.singletonMap("replication_factor", Integer.toString(replicationFactor)));
    }
}
