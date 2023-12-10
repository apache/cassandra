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

package org.apache.cassandra.distributed.test.log;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.CMSPlacementStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.SealPeriod;

import static org.apache.cassandra.distributed.test.log.PlacementSimulator.*;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.*;

public class MetadataChangeSimulationTest extends CMSTestBase
{
    static
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Random rng = new Random(1);

    @Test
    public void simulateNTS() throws Throwable
    {
        // TODO: right now, we pick a candidate only if there is enough rf to execute operation
        // but the problem is that if we start multiple operations that would take us under rf, we will screw up the placements
        // this was not happening before, and test is crafted now to disallow such states, but this is a bug.
        // we should either forbid this, or allow it, but make it work.
        for (int concurrency : new int[]{ 1, 3, 5 })
        {
            for (int rf : new int[]{ 2, 3, 5 })
            {
                simulate(50, new NtsReplicationFactor(3, rf), concurrency);
            }
        }
    }

    @Test
    public void simulateSimple() throws Throwable
    {
        for (int concurrency : new int[]{ 1, 3, 5 })
        {
            for (int rf : new int[]{ 2, 3, 5 })
            {
                simulate(50, new SimpleReplicationFactor(rf), concurrency);
            }
        }
    }

    @Test
    public void testMoveReal() throws Throwable
    {
        for (int i = 0; i < 4; i++)
        {
            testMoveReal(new NtsReplicationFactor(1, 3), i, 150, 4);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 350, 4);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 550, 4);
        }

        for (int i = 0; i < 5; i++)
        {
            testMoveReal(new NtsReplicationFactor(1, 3), i, 150, 5);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 350, 5);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 650, 5);
        }

        for (int i = 0; i < 10; i++)
        {
            testMoveReal(new NtsReplicationFactor(1, 3), i, 150, 10);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 550, 10);
            testMoveReal(new NtsReplicationFactor(1, 3), i, 1050, 10);
        }

        for (int i = 0; i < 9; i++)
        {
            testMoveReal(new NtsReplicationFactor(3, 3), i, 350, 9);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 550, 9);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 1050, 9);
        }

        for (int i = 0; i < 10; i++)
        {
            testMoveReal(new NtsReplicationFactor(3, 3), i, 350, 10);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 550, 10);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 1050, 10);
        }

        for (int i = 0; i < 18; i++)
        {
            testMoveReal(new NtsReplicationFactor(3, 3), i, 350, 18);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 1050, 18);
            testMoveReal(new NtsReplicationFactor(3, 3), i, 2050, 18);
        }

    }

    public void testMoveReal(ReplicationFactor rf, int moveNodeId, long moveToken, int numberOfNodes) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState state = ModelState.empty(nodeFactoryHumanReadable(), 10, 1);
            Node movingNode = null;
            for (int i = 0; i < numberOfNodes; i++)
            {
                int dc = (i % rf.dcs()) + 1;
                ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc , 1);
                if (moveNodeId == i)
                    movingNode = registration.r;
                state = SimulatedOperation.joinWithoutBootstrap(registration.l, sut, registration.r);
            }

            Node movingTo = movingNode.overrideToken(moveToken);
            state = SimulatedOperation.move(sut, state, movingNode, movingTo);

            while (!state.inFlightOperations.isEmpty())
            {
                state = state.inFlightOperations.get(0).advance(state);
                validatePlacements(sut, state);
            }
        }
    }

    @Test
    public void testLeaveReal() throws Throwable
    {
        testLeaveReal(new NtsReplicationFactor(1, 3), 1);
        testLeaveReal(new NtsReplicationFactor(1, 3), 5);
        testLeaveReal(new NtsReplicationFactor(3, 3), 1);
        testLeaveReal(new NtsReplicationFactor(3, 3), 5);

    }

    public void testLeaveReal(ReplicationFactor rf, int decomNodeId) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState state = ModelState.empty(nodeFactoryHumanReadable(), 10, 1);

            Node decomNode = null;
            for (int i = 0; i < 12; i++)
            {
                int dc = (i % rf.dcs()) + 1;
                ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc, 1);
                if (decomNodeId == i)
                    decomNode = registration.r;
                state = SimulatedOperation.joinWithoutBootstrap(registration.l, sut, registration.r);
            }

            state = SimulatedOperation.leave(sut, state, decomNode);

            while (!state.inFlightOperations.isEmpty())
            {
                state = state.inFlightOperations.get(0).advance(state);
                validatePlacements(sut, state);
            }
        }
    }

    @Test
    public void testJoinReal() throws Throwable
    {
        testJoinReal(new NtsReplicationFactor(3, 3), 1);
        testJoinReal(new NtsReplicationFactor(3, 3), 5);
    }

    public void testJoinReal(ReplicationFactor rf, int joinNodeId) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState state = ModelState.empty(nodeFactoryHumanReadable(), 10, 1);

            Node joiningNode = null;
            for (int i = 1; i <= 12; i++)
            {
                int dc = (i % rf.dcs()) + 1;
                ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc, 1);
                state = registration.l;
                if (joinNodeId == i)
                    joiningNode = registration.r;
                else
                    state = SimulatedOperation.joinWithoutBootstrap(registration.l, sut, registration.r);
            }

            state = SimulatedOperation.join(sut, state, joiningNode);

            while (!state.inFlightOperations.isEmpty())
            {
                state = state.inFlightOperations.get(0).advance(state);
                validatePlacements(sut, state);
            }

        }
    }

    public void simulate(int toBootstrap, ReplicationFactor rf, int concurrency) throws Throwable
    {
        System.out.printf("RUNNING SIMULATION. TO BOOTSTRAP: %s, RF: %s, CONCURRENCY: %s%n",
                          toBootstrap, rf, concurrency);
        long startTime = System.currentTimeMillis();
        ModelChecker<ModelState, CMSSut> modelChecker = new ModelChecker<>();
        ClusterMetadataService.unsetInstance();
        modelChecker.init(ModelState.empty(nodeFactory(), toBootstrap, concurrency),
                          new CMSSut(AtomicLongBackedProcessor::new, false, rf))
                    // Sequentially bootstrap rf nodes first
                    .step((state, sut) -> state.currentNodes.isEmpty(),
                          (state, sut, entropySource) -> {
                              for (Map.Entry<String, Integer> e : rf.asMap().entrySet())
                              {
                                  int dcRf = e.getValue();
                                  int dc = Integer.parseInt(e.getKey().replace("datacenter", ""));

                                  for (int i = 0; i < dcRf + 1; i++)
                                  {
                                      ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc, 1);
                                      state = SimulatedOperation.joinWithoutBootstrap(registration.l, sut, registration.r);
                                  }
                              }
                              return new ModelChecker.Pair<>(state, sut);
                          })
                    // Plan the bootstrap of a new node
                    .step((state, sut) -> state.uniqueNodes >= rf.total() && state.shouldBootstrap(),
                          (state, sut, entropySource) -> {
                              int dc = rf.asMap().size() == 1 ? 1 : entropySource.nextInt(rf.asMap().size() - 1) + 1;
                              Node toAdd;
                              if (!state.registeredNodes.isEmpty())
                              {
                                  toAdd = state.registeredNodes.remove(0);
                              }
                              else
                              {
                                  ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc, 1);
                                  state = registration.l;
                                  toAdd = registration.r;
                              }

                              return new ModelChecker.Pair<>(SimulatedOperation.join(sut, state, toAdd), sut);
                          })
                    // Plan the decommission of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldLeave(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toRemove = getRemovalCandidate(state, entropySource);
                              return new ModelChecker.Pair<>(SimulatedOperation.leave(sut, state, toRemove), sut);
                          })
                    // Plan the move of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldMove(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toMove = getMoveCandidate(state, entropySource);
                              return new ModelChecker.Pair<>(SimulatedOperation.move(sut, state, toMove, toMove.withNewToken()), sut);
                          })
                    // Plan the replacement of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldReplace(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toReplace = getRemovalCandidate(state, entropySource);
                              ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, toReplace.tokenIdx(), toReplace.dcIdx(), toReplace.rackIdx());
                              state = registration.l;
                              Node replacement = registration.r;
                              return new ModelChecker.Pair<>(SimulatedOperation.replace(sut, state, toReplace, replacement), sut);
                          })
                    // If there are any planned or in-flight operations, pick one at random. Then, if the op can be
                    // cancelled, either cancel it completely or execute its next step.
                    .step((state, sut) -> state.shouldCancel(rng) && !state.inFlightOperations.isEmpty(),
                          (state, sut, entropySource) -> {
                              int idx = entropySource.nextInt(state.inFlightOperations.size());
                              ModelState.Transformer transformer = state.transformer();
                              SimulatedOperation oldOperationState = state.inFlightOperations.get(idx);
                              oldOperationState.cancel(sut, state.simulatedPlacements, transformer);
                              return pair(transformer.transform(), sut);
                          })
                    // If not cancellable, just execute its next step.
                    .step((state, sut) -> !state.inFlightOperations.isEmpty(),
                          (state, sut, entropySource) -> {
                              int idx = entropySource.nextInt(state.inFlightOperations.size());
                              SimulatedPlacements simulatedState = state.simulatedPlacements;
                              ModelState.Transformer transformer = state.transformer();

                              SimulatedOperation oldOperationState = state.inFlightOperations.get(idx);
                              oldOperationState.advance(simulatedState, transformer);
                              return pair(transformer.transform(), sut);
                          })
                    .step((state, sut) -> rng.nextDouble() < 0.05,
                          (state, sut, entropySource) -> {
                              try
                              {
                                  sut.service.commit(SealPeriod.instance);
                              }
                              catch (IllegalStateException e)
                              {
                                  Assert.assertTrue(e.getMessage().contains("Have just sealed this period"));
                              }
                              return pair(state, sut);
                          })
                    .invariant((state, sut) -> {
                        if (state.currentNodes.size() > 0)
                            validatePlacements(sut, state);

                        return true;
                    })
                    .exitCondition((state, sut) -> {
                        if (state.currentNodes.size() >= toBootstrap && state.inFlightOperations.size() == 0)
                        {
                            validatePlacementsFinal(sut, state);
                            sut.close();
                            System.out.printf("(RF: %d, CONCURRENCY: %d, RUN TIME: %dms) - " +
                                              "REGISTERED: %d, CURRENT SIZE: %d, REJECTED %d, INFLIGHT: %d" +
                                              "FINISHED  (join,replace,leave,move): %s" +
                                              "CANCELLED (join,replace,leave,move): %s%n",
                                              sut.rf.total(), concurrency, System.currentTimeMillis() - startTime,
                                              state.uniqueNodes, state.currentNodes.size(), state.rejected, state.inFlightOperations.size(),
                                              Arrays.toString(state.finished),
                                              Arrays.toString(state.cancelled));
                            return true;
                        }

                        return false;
                    })
                    .run();
    }

    @Test
    public void simulateDCAwareBounces() throws Throwable
    {
        Random random = new Random(1L);
        for (int i = 0; i < 10; i++)
        {
            ReplicationFactor ntsRf = new NtsReplicationFactor(3, 3);
            Map<String, Integer> cmsRf = new HashMap<>();
            for (String s : ntsRf.asMap().keySet())
                cmsRf.put(s, 3);

            simulateBounces(ntsRf, new CMSPlacementStrategy.DatacenterAware(cmsRf, (cm, n) -> random.nextInt(10) > 1), random);
        }
    }

    public void simulateBounces(ReplicationFactor rf, CMSPlacementStrategy CMSConfigurationStrategy, Random random) throws Throwable
    {

        try(CMSSut sut = new CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState state = ModelState.empty(nodeFactory(), 300, 1);

            for (Map.Entry<String, Integer> e : rf.asMap().entrySet())
            {
                int dc = Integer.parseInt(e.getKey().replace("datacenter", ""));

                for (int i = 0; i < 100; i++)
                {
                    ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, dc, random.nextInt(5) + 1);
                    state = SimulatedOperation.joinWithoutBootstrap(registration.l, sut, registration.r);
                }
            }

            Set<NodeId> newCms = CMSConfigurationStrategy.reconfigure(sut.service.metadata().directory.toNodeIds(sut.service.metadata().fullCMSMembers()),
                                                                      sut.service.metadata());

            ClusterMetadata metadata = sut.service.metadata();

            for (int i = 0; i < 100; i++)
            {
                Set<NodeId> bouncing = new HashSet<>();
                Set<NodeId> replicasFromBouncedReplicaSets = new HashSet<>();
                int j = 0;
                outer:
                for (VersionedEndpoints.ForRange placements : sut.service.metadata().placements.get(rf.asKeyspaceParams().replication).writes.replicaGroups().values())
                {
                    List<NodeId> replicas = new ArrayList<>(metadata.directory.toNodeIds(placements.get().endpoints()));
                    List<NodeId> bounceCandidates = new ArrayList<>();
                    for (NodeId replica : replicas)
                    {
                        if (!replicasFromBouncedReplicaSets.contains(replica))
                            bounceCandidates.add(replica);
                        else
                            continue outer;
                    }

                    if (!bounceCandidates.isEmpty())
                    {
                        NodeId toBounce = bounceCandidates.get(random.nextInt(bounceCandidates.size()));
                        bouncing.add(toBounce);
                        replicasFromBouncedReplicaSets.addAll(replicas);
                    }
                    j++;
                }

                int majority = newCms.size() / 2 + 1;
                String msg = String.format("In a %d node cluster, %d nodes picked for bounce, %d out of %d CMS nodes%n",
                                           metadata.directory.peerIds().size(),
                                           bouncing.size(), Sets.intersection(newCms, bouncing).size(), newCms.size());
                if (Sets.intersection(newCms, bouncing).size() >= majority)
                    throw new AssertionError(msg);
                else
                    System.out.print(msg);
            }
        }
    }

    public static void validatePlacementsFinal(CMSTestBase.CMSSut sut, ModelState modelState) throws Throwable
    {
        ClusterMetadata actualMetadata = sut.service.metadata();
        ReplicationParams replication = actualMetadata.schema.getKeyspaces().get("test").get().params.replication;
        Assert.assertEquals(replication, sut.rf.asKeyspaceParams().replication);
        match(actualMetadata.placements.get(replication).reads, sut.rf.replicate(modelState.simulatedPlacements.nodes).asMap());
        match(actualMetadata.placements.get(replication).writes, sut.rf.replicate(modelState.simulatedPlacements.nodes).asMap());
    }

    public static void validatePlacements(CMSTestBase.CMSSut sut, ModelState modelState) throws Throwable
    {
        ClusterMetadata actualMetadata = sut.service.metadata();

        ReplicationParams replication = actualMetadata.schema.getKeyspaces().get("test").get().params.replication;
        Assert.assertEquals(replication, sut.rf.asKeyspaceParams().replication);

        Assert.assertEquals(modelState.simulatedPlacements.nodes.stream().map(Node::token).collect(Collectors.toSet()),
                            actualMetadata.tokenMap.tokens().stream().map(t -> ((LongToken) t).token).collect(Collectors.toSet()));

        for (Map.Entry<ReplicationParams, DataPlacement> e : actualMetadata.placements.asMap().entrySet())
        {
            if (!e.getKey().equals(replication))
                continue;

            DataPlacement placement = e.getValue();
            match(placement.writes, modelState.simulatedPlacements.writePlacements);
            match(placement.reads, modelState.simulatedPlacements.readPlacements);
        }

        validatePlacements(sut.partitioner, sut.rf, modelState, actualMetadata.placements);
    }

    public static ModelChecker.Pair<ModelState, Node> registerNewNode(ModelState state, CMSSut sut, int dcIdx, int rackIdx)
    {
        ModelState newState = state.transformer().incrementUniqueNodes().transform();
        Node node = state.nodeFactory.make(newState.uniqueNodes, dcIdx, rackIdx);
        sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
        return pair(newState, node);
    }

    private ModelChecker.Pair<ModelState, Node> registerNewNode(ModelState state, CMSSut sut, int tokenIdx, int dcIdx, int rackIdx)
    {
        ModelState newState = state.transformer().incrementUniqueNodes().transform();
        Node node = state.nodeFactory.make(newState.uniqueNodes, dcIdx, rackIdx).withToken(tokenIdx);
        sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
        return pair(newState, node);
    }

    private Node getRemovalCandidate(ModelState state, ModelChecker.EntropySource entropySource)
    {
        return getCandidate(state, entropySource);
    }

    private Node getMoveCandidate(ModelState state, ModelChecker.EntropySource entropySource)
    {
        return getCandidate(state, entropySource);
    }

    private Node getCandidate(ModelState modelState, ModelChecker.EntropySource entropySource)
    {
        List<String> dcs = new ArrayList<>(modelState.simulatedPlacements.rf.asMap().keySet());
        while (!dcs.isEmpty())
        {
            String dc;
            if (dcs.size() == 1)
                dc = dcs.remove(0);
            else
                dc = dcs.remove(entropySource.nextInt(dcs.size() - 1));

            Set<Node> candidates = new HashSet<>(modelState.nodesByDc.get(dc));
            for (SimulatedOperation op : modelState.inFlightOperations)
                candidates.removeAll(Arrays.asList(op.nodes));

            int rf = modelState.simulatedPlacements.rf.asMap().get(dc);
            if (candidates.size() <= rf)
                continue;

            Iterator<Node> iter = candidates.iterator();
            if (candidates.size() == 1)
                return iter.next();
            int idx = entropySource.nextInt(candidates.size() - 1);
            while (--idx >= 0)
                iter.next();

            return iter.next();
        }

        throw new IllegalStateException("Could not find a candidate for removal in " + modelState.nodesByDc);
    }


    public static String toString(Map<?, ?> predicted)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : predicted.entrySet())
        {
            sb.append(e.getKey()).append('=').append(e.getValue()).append(",\n");
        }

        return sb.toString();
    }

    public static void match(PlacementForRange actual, Map<TokenPlacementModel.Range, List<Node>> predicted) throws Throwable
    {
        Map<Range<Token>, VersionedEndpoints.ForRange> actualGroups = actual.replicaGroups();
        assert predicted.size() == actualGroups.size() :
        String.format("\nPredicted:\n%s(%d)" +
                      "\nActual:\n%s(%d)", toString(predicted), predicted.size(), toString(actual.replicaGroups()), actualGroups.size());

        for (Map.Entry<TokenPlacementModel.Range, List<Node>> entry : predicted.entrySet())
        {
            TokenPlacementModel.Range range = entry.getKey();
            List<Node> predictedNodes = entry.getValue();
            Range<Token> predictedRange = new Range<Token>(new Murmur3Partitioner.LongToken(range.start),
                                                           new Murmur3Partitioner.LongToken(range.end));
            EndpointsForRange endpointsForRange = actualGroups.get(predictedRange).get();
            assertNotNull(() -> String.format("Could not find %s in ranges %s", predictedRange, actualGroups.keySet()),
                          endpointsForRange);
            assertEquals(() -> String.format("Predicted to have different endpoints for range %s" +
                                             "\nExpected: %s" +
                                             "\nActual:   %s",
                                             range,
                                             predictedNodes.stream().sorted().collect(Collectors.toList()),
                                             endpointsForRange.endpoints().stream().sorted().collect(Collectors.toList())),
                         predictedNodes.size(), endpointsForRange.size());
            for (Node node : predictedNodes)
            {
                assertTrue(() -> String.format("Endpoints for range %s should have contained %s, but they have not." +
                                               "\nExpected: %s" +
                                               "\nActual:   %s.",
                                               endpointsForRange.range(),
                                               node.id(),
                                               predictedNodes,
                                               endpointsForRange.endpoints()),
                           endpointsForRange.endpoints().contains(InetAddressAndPort.getByAddress(InetAddress.getByName(node.id()))));
            }
        }
    }


    private static void assertEquals(Supplier<String> s, Object o1, Object o2)
    {
        try
        {
            Assert.assertEquals(o1, o2);
        }
        catch (AssertionError e)
        {
            throw new AssertionError(s.get(), e);
        }
    }

    private static void assertNotNull(Supplier<String> s, Object v)
    {
        if (v == null)
            Assert.fail(s.get());
    }

    private static void assertTrue(Supplier<String> s, boolean res)
    {
        if (!res)
            Assert.fail(s.get());
    }

    private static <L, R> ModelChecker.Pair<L, R> pair(L l, R r)
    {
        return new ModelChecker.Pair<>(l, r);
    }

    public static List<Token> toTokens(List<Node> nodes)
    {
        List<Token> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.longToken());

        tokens.sort(Token::compareTo);
        return tokens;
    }

    public static List<Range<Token>> toRanges(Collection<Token> ownedTokens, IPartitioner partitioner)
    {
        Set<Token> allTokens = new HashSet<>();
        allTokens.add(partitioner.getMinimumToken());
        allTokens.addAll(ownedTokens);

        List<Token> allTokensArr = new ArrayList<>(allTokens);
        allTokensArr.sort(Token::compareTo);
        allTokensArr.add(partitioner.getMinimumToken());

        Iterator<Token> tokenIter = allTokensArr.iterator();
        Token previous = tokenIter.next();
        List<Range<Token>> ranges = new ArrayList<>();
        while (tokenIter.hasNext())
        {
            Token current = tokenIter.next();
            ranges.add(new Range<>(previous, current));
            previous = current;
        }
        return ranges;
    }

    public static void validatePlacements(IPartitioner partitioner,
                                          ReplicationFactor rf,
                                          ModelState modelState,
                                          DataPlacements placements)
    {

        Set<Token> allTokens = new HashSet<>(toTokens(modelState.currentNodes));
        for (SimulatedOperation bootstrappedNode : modelState.inFlightOperations)
            allTokens.add(bootstrappedNode.nodes[0].longToken());

        List<Range<Token>> expectedRanges = toRanges(allTokens, partitioner);

        DataPlacement actualPlacements = placements.get(rf.asKeyspaceParams().replication);

        assertRanges(expectedRanges, actualPlacements.writes.ranges());
        assertRanges(expectedRanges, actualPlacements.reads.ranges());

        validatePlacementsInternal(rf, modelState.inFlightOperations, expectedRanges, actualPlacements.reads, false);
        validatePlacementsInternal(rf, modelState.inFlightOperations, expectedRanges, actualPlacements.writes, true);
    }

    public static void assertRanges(List<Range<Token>> l, List<Range<Token>> r)
    {
        Assert.assertEquals(new TreeSet<>(l), new TreeSet<>(r));
    }

    public static void validatePlacementsInternal(ReplicationFactor rf, List<SimulatedOperation> opStates, List<Range<Token>> expectedRanges, PlacementForRange placements, boolean allowPending)
    {
        int overreplicated = 0;
        for (Range<Token> range : expectedRanges)
        {
            EndpointsForRange endpointsForRange = placements.forRange(range).get();
            Directory directory = ClusterMetadata.current().directory;
            Map<String, Set<InetAddressAndPort>> endpointsByDc = new TreeMap<>();
            for (Replica replica : endpointsForRange)
            {
                Location location = directory.location(directory.peerId(replica.endpoint()));
                endpointsByDc.computeIfAbsent(location.datacenter, (k) -> new HashSet<>())
                             .add(replica.endpoint());
            }

            for (Map.Entry<String, Integer> e : rf.asMap().entrySet())
            {
                int expectedRf = e.getValue();
                String dc = e.getKey();
                int actualRf = endpointsByDc.get(dc).size();

                if (allowPending)
                {
                    int diff = actualRf - expectedRf;

                    // We may have many overreplicated ranges, but each range is overreplicated by one
                    assertTrue(() -> String.format("Overreplicated by %d", diff), diff == 0 || diff == 1);
                    overreplicated += diff;
                    assertTrue(() -> String.format("Expected a replication factor of %d, for range %s in dc %s  but got %d",
                                                   expectedRf, range, dc, actualRf),
                               actualRf >= expectedRf);
                }
                else
                {
                    assertEquals(() -> String.format("Expected a replication factor of %d, for range in dc %s %s but got %d",
                                                     expectedRf, range, dc, actualRf),
                                 expectedRf, actualRf);
                }
            }
        }

        if (allowPending && rf instanceof SimpleReplicationFactor)
        {
            int bootstrappingNodes = 0;
            int movingNodes = 0;
            int leavingNodes = 0;
            int replacedNodes = 0;
            int expectedOverReplicated = 0;

            for (SimulatedOperation opState : opStates)
            {
                if (opState.status == SimulatedOperation.Status.STARTED)
                {
                    if (opState.type == SimulatedOperation.Type.MOVE ) movingNodes += 1;
                    if (opState.type == SimulatedOperation.Type.REPLACE) replacedNodes += 1;
                    if (opState.type == SimulatedOperation.Type.JOIN) bootstrappingNodes += 1;
                    if (opState.type == SimulatedOperation.Type.LEAVE) leavingNodes += 1;
                }
            }
            expectedOverReplicated = (replacedNodes + leavingNodes + bootstrappingNodes + movingNodes); // When the node is moving, a split range and original range to be over-replicated
            // This is a trivial check for over-replication. +2 comes from wraparound ranges, since
            // they have identical placements. Lower bound comes from the fact that during moves
            // we may end up moving closely to the node in question, which means that we'll move by
            // one range at most. Upper bound, also comes from the move, since it touches twice as many
            // ranges.
            //
            // It is not difficult to work out when exactly when they
            // are involved, but since simulator already does predicts exact placements, we leave
            // this check as a failsafe for cases when simulator may have a bug identical to SUT.
            int finalBootstrappingNodes = bootstrappingNodes;
            int finalMovingNodes = movingNodes;
            int finalLeavingNodes = leavingNodes;
            int finalReplacedNodes = replacedNodes;
            int finalExpectedOverReplicated = expectedOverReplicated;
            int finalOverreplicated = overreplicated;
            assertTrue(() -> String.format("Because there are %d nodes joining/moving/leaving/replaced (%d/%d/%d/%d) that have added themselves " +
                                           "to write set, we expect to have at least %d ranges over-replicated, but got %d. RF %s (%s)",
                                           finalBootstrappingNodes + finalMovingNodes + finalLeavingNodes, finalBootstrappingNodes, finalMovingNodes, finalLeavingNodes, finalReplacedNodes, finalExpectedOverReplicated, finalOverreplicated,
                                           rf, placements),
                       overreplicated >= expectedOverReplicated && overreplicated <= (expectedOverReplicated * rf.total() + 2 + movingNodes * rf.total()));
        }
    }
}