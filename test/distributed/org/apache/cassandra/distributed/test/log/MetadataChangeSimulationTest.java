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
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.distributed.test.log.PlacementSimulator.SimulatedPlacements;
import org.apache.cassandra.distributed.test.log.PlacementSimulator.Transformations;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.SealPeriod;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.schema.ReplicationParams;

import static org.apache.cassandra.distributed.test.log.PlacementSimulator.*;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.bootstrap_diffBased;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.leave_diffBased;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.move_diffBased;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.replace_directly;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.replicate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetadataChangeSimulationTest extends CMSTestBase
{
    private static final Random rng = new Random(1);

    @Test
    public void simulate() throws Throwable
    {
        for (int concurrency : new int[]{ 1, 5 })
        {
            for (int rf : new int[]{ 2, 3, 5 })
            {
                simulate(50, rf, concurrency);
            }
        }
    }

    @Test
    public void testMoveReal() throws Throwable
    {
        testMoveReal(3, 0, 350, 3);
        testMoveReal(3, 0, 350, 4);
        testMoveReal(3, 0, 550, 10);
        testMoveReal(3, 1, 350, 4);
        testMoveReal(3, 1, 550, 10);
        testMoveReal(3, 2, 350, 4);
        testMoveReal(3, 5, 350, 10);
    }

    public static Node n(int idx, long token)
    {
        return new Node(token, idx);
    }

    public void testMoveReal(int rf, int moveNodeId, long moveToken, int numberOfNodes) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState modelState = ModelState.empty(10, 1);
            Node movingNode = null;
            for (int i = 0; i < numberOfNodes; i++)
            {
                LongToken token = new Murmur3Partitioner.LongToken((i + 1) * 100L);
                ModelChecker.Pair<MetadataChangeSimulationTest.ModelState, Node> registration = registerNewNode(modelState, sut, () -> token);
                if (moveNodeId == i)
                    movingNode = registration.r;
                modelState = registration.l;
                modelState = joinWithoutBootstrap(registration.r, modelState, sut).l;
            }

            Node movingTo = movingNode.withNewToken(moveToken);
            Move move = prepareMove(sut, movingNode, movingTo.longToken()).get();
            modelState = scheduleMoveEvents(modelState, sut, movingNode, movingTo.longToken(), move).l;

            while (modelState.operationStates.get(0).remaining.hasNext())
            {
                OperationState operationState = modelState.operationStates.get(0);
                operationState.remaining.next();

                modelState = modelState.transformer()
                             .removeOperation(operationState)
                             .updateSimulation(operationState.stashedSteps.advance(modelState.simulatedPlacements))
                             .addOperation(operationState.copyAndMarkStarted())
                             .transform();

                if (!operationState.remaining.hasNext())
                {
                    modelState = modelState.transformer()
                                           .withMoved(movingNode, movingTo)
                                           .transform();
                }

                validatePlacements(sut, modelState);
            }
        }
    }

    public static void validatePlacementsFinal(CMSTestBase.CMSSut sut, ModelState modelState) throws Throwable
    {
        ClusterMetadata actualMetadata =  sut.service.metadata();
        ReplicationParams replication = actualMetadata.schema.getKeyspaces().get("test").get().params.replication;

        int rf = Integer.parseInt(replication.options.get("replication_factor"));

        match(actualMetadata.placements.get(replication).reads, replicate(modelState.simulatedPlacements.nodes, rf));
        match(actualMetadata.placements.get(replication).writes, replicate(modelState.simulatedPlacements.nodes, rf));

    }

    public static void validatePlacements(CMSTestBase.CMSSut sut, ModelState modelState) throws Throwable
    {
        ClusterMetadata actualMetadata =  sut.service.metadata();
        ReplicationParams replication = actualMetadata.schema.getKeyspaces().get("test").get().params.replication;

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

        validatePlacements(sut.partitioner, replication, modelState, actualMetadata.placements);
    }

    @Test
    public void testDecomReal() throws Throwable
    {
        testDecomReal(3, 1);
        testDecomReal(3, 5);
    }

    public void testDecomReal(int rf, int decomNodeId) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState state = ModelState.empty(10, 1);

            Node decomNode = null;
            for (int i = 0; i < 10; i++)
            {
                LongToken token = new Murmur3Partitioner.LongToken((i + 1) * 100);
                ModelChecker.Pair<MetadataChangeSimulationTest.ModelState, Node> registration = registerNewNode(state, sut, () -> token);
                if (decomNodeId == i)
                    decomNode = registration.r;
                state = registration.l;
                state = joinWithoutBootstrap(registration.r, state, sut).l;
            }

            UnbootstrapAndLeave leave = prepareLeave(sut, decomNode).get();
            ModelChecker.Pair<ModelState, CMSSut> events = scheduleLeaveEvents(state, sut, decomNode, leave);
            OperationState operationState = events.l.operationStates.get(0);
            state = events.l;

            while (operationState.remaining.hasNext())
            {
                // Execute _actual_ operation
                operationState.remaining.next();
                Transformations steps = operationState.stashedSteps;

                state = state.transformer()
                             .removeOperation(operationState)
                             .updateSimulation(steps.advance(state.simulatedPlacements))
                             .transform();

                if (!operationState.remaining.hasNext())
                {
                    state = state.transformer()
                                 .withLeft(decomNode)
                                 .transform();
                }

                validatePlacements(sut, state);
            }
        }
    }

    @Test
    public void testJoinReal() throws Throwable
    {
        testJoinReal(3, 1);
        testJoinReal(3, 5);
    }

    public void testJoinReal(int rf, int decomNodeId) throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            ModelState modelState = ModelState.empty(10, 1);

            Node joiningNode = null;
            for (int i = 0; i < 10; i++)
            {
                LongToken token = new Murmur3Partitioner.LongToken((i + 1) * 100);
                ModelChecker.Pair<MetadataChangeSimulationTest.ModelState, Node> registration = registerNewNode(modelState, sut, () -> token);
                modelState = registration.l;
                if (decomNodeId == i)
                    joiningNode = registration.r;
                else
                    modelState = joinWithoutBootstrap(registration.r, modelState, sut).l;
            }

            BootstrapAndJoin plan = prepareJoin(sut, joiningNode).get();
            modelState = scheduleJoinEvents(modelState, sut, joiningNode, plan).l;
            while (modelState.operationStates.get(0).remaining.hasNext())
            {
                OperationState operationState = modelState.operationStates.get(0);

                // Execute _actual_ operation
                operationState.remaining.next();
                Transformations steps = operationState.stashedSteps;

                modelState = modelState.transformer()
                                       .removeOperation(operationState)
                                       .updateSimulation(steps.advance(modelState.simulatedPlacements))
                                       .addOperation(operationState.copyAndMarkStarted())
                                       .transform();

                if (!modelState.operationStates.get(0).remaining.hasNext())
                {
                    modelState = modelState.transformer()
                                 .withJoined(joiningNode)
                                 .transform();
                }

                validatePlacements(sut, modelState);
            }
        }
    }

    @Test
    public void testSimpleJoinNTS() throws Throwable
    {
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, 3))
        {
            ModelState modelState = ModelState.empty(10, 1);

            Random rng = new Random(1);
            for (int i = 0; i < 10; i++)
            {
                LongToken token = new Murmur3Partitioner.LongToken(rng.nextLong());
                ModelChecker.Pair<MetadataChangeSimulationTest.ModelState, Node> registration = registerNewNode(modelState, sut, () -> token, (((i + 1) % 3) + 1), 1);
                modelState = registration.l;
                modelState = joinWithoutBootstrap(registration.r, modelState, sut).l;
            }

            DataPlacements placements = new UniformRangePlacement().calculatePlacements(ClusterMetadata.current(),
                                                                                        ClusterMetadata.current().schema.getKeyspaces());
            DataPlacements actual = ClusterMetadata.current().placements;

            for (Map.Entry<ReplicationParams, DataPlacement> e : placements)
            {
                dataPlacementsEqual(e.getValue().reads,
                                    actual.get(e.getKey()).reads);
                dataPlacementsEqual(e.getValue().writes,
                                    actual.get(e.getKey()).writes);

            }
        }
    }

    public static void dataPlacementsEqual(PlacementForRange l, PlacementForRange r)
    {
        Map<Range<Token>, EndpointsForRange> lbe = l.replicaGroups();
        Map<Range<Token>, EndpointsForRange> rbe = r.replicaGroups();

        assertEquals(rbe.keySet(), lbe.keySet());
        for (Range<Token> range : rbe.keySet())
        {
            assertEquals(lbe.get(range).stream().collect(Collectors.toSet()),
                         rbe.get(range).stream().collect(Collectors.toSet()));
        }

    }

    // TODO: use several keyspaces in the test, preferrably with different replication factors, since this will meddle with
    // range locks in a non-trivial way and will probably break things
    // TODO: add keyspace _creation_ to the mix, since this should also preserve placements
    public void simulate(int toBootstrap, int rf, int concurrency) throws Throwable
    {
        System.out.println(String.format("RUNNING SIMULATION. TO BOOTSTRAP: %s, RF: %s, CONCURRENCY: %s",
                                         toBootstrap, rf, concurrency));
        long startTime = System.currentTimeMillis();
        final List<Long> longs;
        final Iterator<Long> tokens;
        longs = new ArrayList<>();
        for (int i = 0; i < toBootstrap * 30; i++)
            longs.add(rng.nextLong());

        Collections.shuffle(longs, rng);
        tokens = longs.iterator();

        ModelChecker<ModelState, CMSSut> modelChecker = new ModelChecker<>();
        ClusterMetadataService.unsetInstance();
        modelChecker.init(ModelState.empty(toBootstrap, concurrency),
                          new CMSSut(AtomicLongBackedProcessor::new, false, rf))
                    // Plan the bootstrap of a new node
                    .step((state, sut) -> (state.uniqueNodes <= rf && state.operationStates.size() == 0) || // sequentially bootstrap rf nodes first
                                          (state.shouldBootstrap()),
                          (state, sut, entropySource) -> {
                              long bootstrapToken = tokens.next();
                              ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, () -> new LongToken(bootstrapToken));
                              state = registration.l;
                              Node toAdd = registration.r;

                              // don't bootstrap the first rf nodes, just join them immediately
                              if (state.uniqueNodes < rf)
                              {
                                  return joinWithoutBootstrap(toAdd, state, sut);
                              }
                              else
                              {
                                  Optional<BootstrapAndJoin> plan = prepareJoin(sut, toAdd);
                                  return plan.isPresent()
                                         ? scheduleJoinEvents(state, sut, toAdd, plan.get())
                                         : rejected(state, sut);
                              }
                          })
                    // Plan the decommission of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldDecommission(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toRemove = getRemovalCandidate(state, entropySource);
                              Optional<UnbootstrapAndLeave> plan = prepareLeave(sut, toRemove);
                              return plan.isPresent()
                                     ? scheduleLeaveEvents(state, sut, toRemove, plan.get())
                                     : rejected(state, sut);
                          })
                    // Plan the move of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldMove(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toMove = getMoveCandidate(state, entropySource);
                              LongToken moveToken = new LongToken(tokens.next());
                              Optional<Move> plan = prepareMove(sut, toMove, moveToken);
                              return plan.map(move -> scheduleMoveEvents(state, sut, toMove, moveToken, move))
                                         .orElseGet(() -> rejected(state, sut));
                          })
                    // Plan the replacement of one of the previously bootstrapped nodes
                    .step((state, sut) -> state.shouldReplace(rf, rng),
                          (state, sut, entropySource) -> {
                              Node toReplace = getRemovalCandidate(state, entropySource);
                              ModelChecker.Pair<ModelState, Node> registration = registerNewNode(state, sut, toReplace::longToken);
                              state = registration.l;
                              Node replacement = registration.r;
                              Optional<BootstrapAndReplace> plan = prepareReplace(sut, toReplace, replacement);
                              return plan.isPresent()
                                     ? scheduleReplaceEvents(state, sut, toReplace, replacement, plan.get())
                                     : rejected(state, sut);
                          })
                    // If there are any planned or inflight operations, pick one at random. Then, if the op can be
                    // cancelled, either cancel it completely or execute its next step. If not cancellable, just execute
                   // its next step.
                    .step((state, sut) -> state.operationStates.size() > 0,
                          (state, sut, entropySource) -> {
                              int idx = entropySource.nextInt(state.operationStates.size());
                              SimulatedPlacements simulatedState = state.simulatedPlacements;
                              ModelState.Transformer transformer = state.transformer();

                              for (int i = 0; i < state.operationStates.size(); i++)
                              {
                                  OperationState oldOperationState = state.operationStates.get(i);
                                  if (i == idx)
                                  {
                                      if (state.shouldCancelOperation(oldOperationState.type, rng))
                                      {
                                          Node node = oldOperationState.type == OperationState.Type.REPLACEMENT
                                                      ? oldOperationState.nodes[1]
                                                      : oldOperationState.nodes[0];
                                          ClusterMetadata metadata = sut.service.metadata();
                                          InProgressSequence<?> operation = metadata.inProgressSequences.get(node.nodeId());
                                          assert operation != null : "No in-progress sequence found for node " + node.nodeId();
                                          sut.service.commit(new CancelInProgressSequence(node.nodeId()));
                                          Transformations steps = oldOperationState.stashedSteps;
                                          simulatedState = steps.revertPublishedEffects(simulatedState);
                                          switch (oldOperationState.type)
                                          {
                                              case BOOTSTRAP:
                                                  transformer.incrementCancelledJoin();
                                                  break;
                                              case MOVE:
                                                  transformer.incrementCancelledMove();
                                                  break;
                                              case REPLACEMENT:
                                                  transformer.incrementCancelledReplace();
                                                  break;
                                              case DECOMMISSION:
                                                  transformer.incrementCancelledLeave();
                                                  break;
                                          }
                                          transformer.removeOperation(oldOperationState)
                                                     .updateSimulation(simulatedState);
                                      }
                                      else
                                      {
                                          // Iterator is not immutable, but it still works here
                                          oldOperationState.remaining.next();
                                          Transformations steps = oldOperationState.stashedSteps;
                                          simulatedState = steps.advance(simulatedState);

                                          transformer.removeOperation(oldOperationState)
                                                     .updateSimulation(simulatedState);

                                          if (oldOperationState.remaining.hasNext())
                                          {
                                              transformer.addOperation(oldOperationState.copyAndMarkStarted());
                                          }
                                          else
                                          {
                                              switch (oldOperationState.type)
                                              {
                                                  case BOOTSTRAP:
                                                      transformer.withJoined(oldOperationState.nodes[0]);
                                                      break;
                                                  case MOVE:
                                                      transformer.withMoved(oldOperationState.nodes[1],
                                                                            oldOperationState.nodes[0]);
                                                      break;
                                                  case DECOMMISSION:
                                                      transformer.withLeft(oldOperationState.nodes[0]);
                                                      break;
                                                  case REPLACEMENT:
                                                      transformer.withReplaced(oldOperationState.nodes[0],
                                                                               oldOperationState.nodes[1]);
                                                      break;
                                              }
                                          }
                                      }
                                  }
                              }
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

                        if (state.currentNodes.size() >= rf)
                            validatePlacements(sut, state);

                        return true;
                    })
                    .exitCondition((state, sut) -> {
                        if (state.currentNodes.size() >= toBootstrap && state.operationStates.size() == 0)
                        {
                            validatePlacementsFinal(sut, state);
                            sut.close();
                            System.out.println(String.format("(RF: %d, CONCURRENCY: %d, RUN TIME: %dms) - " +
                                                             "REGISTERED: %d, CURRENT SIZE: %d, JOINED: %d, LEFT: %d, " +
                                                             "REPLACED: %d, MOVED: %s, REJECTED OPS: %d, " +
                                                             "CANCELLED OPS (join/replace/leave/move): %d/%d/%d/%d, INFLIGHT OPS: %d",
                                                             sut.replication.fullReplicas, concurrency, System.currentTimeMillis() - startTime,
                                                             state.uniqueNodes, state.currentNodes.size(), state.joined, state.left,
                                                             state.replaced, state.moved, state.rejected,
                                                             state.cancelled[0], state.cancelled[1], state.cancelled[2], state.cancelled[3],
                                                             state.operationStates.size()));
                            return true;
                        }

                        return false;
                    })
                    .run();
    }

    private ModelChecker.Pair<ModelState, CMSSut> rejected(ModelState state, CMSSut sut)
    {
        return pair(state.transformer().incrementRejected().transform(), sut);
    }

    private Optional<BootstrapAndJoin> prepareJoin(CMSSut sut, Node node)
    {
        try
        {
            ClusterMetadata metadata = sut.service.commit(new PrepareJoin(node.nodeId(),
                                                                          ImmutableSet.of(node.longToken()),
                                                                          sut.service.placementProvider(),
                                                                          true,
                                                                          false));
            return Optional.of((BootstrapAndJoin) metadata.inProgressSequences.get(node.nodeId()));
        }
        catch (Throwable t)
        {
            return Optional.empty();
        }
    }

    private Optional<Move> prepareMove(CMSSut sut, Node node, LongToken newToken)
    {
        try
        {
            ClusterMetadata metadata = sut.service.commit(new PrepareMove(node.nodeId(), Collections.singleton(newToken), sut.service.placementProvider(), false));
            return Optional.of((Move) metadata.inProgressSequences.get(node.nodeId()));
        }
        catch (Throwable t) // TODO: do we really want to catch _every_ exception?
        {
            return Optional.empty();
        }
    }

    private ModelChecker.Pair<ModelState, CMSSut> scheduleJoinEvents(ModelState state,
                                                                     CMSSut sut,
                                                                     Node node,
                                                                     BootstrapAndJoin plan)
    {
        Iterator<ClusterMetadata> iter = toIter(sut.service, plan.startJoin, plan.midJoin, plan.finishJoin);

        SimulatedPlacements simulatedPlacements = state.simulatedPlacements;
        OperationState bootstrapOperation;
        int rf = sut.replication.fullReplicas;
        if (simulatedPlacements == null)
        {
            List<Node> orig = Collections.singletonList(node);
            Transformations stashedSteps = new Transformations();
            simulatedPlacements = new SimulatedPlacements(rf,
                                                          orig,
                                                          replicate(orig, rf),
                                                          replicate(orig, rf),
                                                          Collections.emptyList());

            bootstrapOperation = OperationState.newBootstrap(node, iter, stashedSteps);
        }
        else
        {
            ModelChecker.Pair<SimulatedPlacements, Transformations> nextSimulated = bootstrap_diffBased(simulatedPlacements, node.idx(), node.token());
            simulatedPlacements = nextSimulated.l;
            Transformations transformations = nextSimulated.r;
            // immediately execute the first step of bootstrap transformations, the splitting of existing ranges. This
            // is so that subsequent planned operations base their transformations on the split ranges.
            simulatedPlacements = transformations.advance(simulatedPlacements);
            bootstrapOperation = OperationState.newBootstrap(node, iter, transformations);
        }
        return pair(state.transformer()
                         .addOperation(bootstrapOperation)
                         .updateSimulation(simulatedPlacements)
                         .transform(),
                    sut);
    }

    private ModelChecker.Pair<ModelState, CMSSut> scheduleMoveEvents(ModelState state,
                                                                     CMSSut sut,
                                                                     Node movingNode,
                                                                     LongToken newToken,
                                                                     Move plan)
    {
        Iterator<ClusterMetadata> iter = toIter(sut.service, plan.startMove, plan.midMove, plan.finishMove);

        SimulatedPlacements simulatedPlacements = state.simulatedPlacements;
        assert simulatedPlacements != null : "Cannot move in an empty cluster";

        ModelChecker.Pair<SimulatedPlacements, Transformations> nextSimulated = move_diffBased(simulatedPlacements, movingNode.idx(), newToken.token);
        Transformations transformations = nextSimulated.r;
        simulatedPlacements = nextSimulated.l;
        // immediately execute the first step of move transformations, the splitting of existing ranges. This
        // is so that subsequent planned operations base their transformations on the split ranges.
        simulatedPlacements = transformations.advance(simulatedPlacements);
        OperationState move = OperationState.newMove(movingNode,
                                                     movingNode.withNewToken(newToken.token),
                                                     iter, transformations);
        return pair(state.transformer()
                         .addOperation(move)
                         .updateSimulation(simulatedPlacements)
                         .markMoving(movingNode)
                         .transform(),
                    sut);
    }

    private Optional<UnbootstrapAndLeave> prepareLeave(CMSSut sut, Node toRemove)
    {
        try
        {
            ClusterMetadata metadata =  sut.service.commit(new PrepareLeave(toRemove.nodeId(),
                                                                            true,
                                                                            sut.service.placementProvider()));
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) metadata.inProgressSequences.get(toRemove.nodeId());
            return Optional.of(plan);
        }
        catch (Throwable e)
        {
            return Optional.empty();
        }
    }

    private ModelChecker.Pair<ModelState, CMSSut> scheduleLeaveEvents(ModelState state,
                                                                      CMSSut sut,
                                                                      Node toRemove,
                                                                      UnbootstrapAndLeave events)
    {
        Iterator<ClusterMetadata> iter = toIter(sut.service, events.startLeave, events.midLeave, events.finishLeave);

        SimulatedPlacements simulatedPlacements = state.simulatedPlacements;
        ModelChecker.Pair<SimulatedPlacements, Transformations> nextSimulated = leave_diffBased(simulatedPlacements, toRemove.token());
        simulatedPlacements = nextSimulated.l;
        // we don't remove from bootstrapped nodes until the finish leave event executes
        OperationState leaveOperation = OperationState.newDecommission(toRemove, iter, nextSimulated.r);
        return pair(state.transformer()
                         .addOperation(leaveOperation)
                         .markLeaving(toRemove)
                         .updateSimulation(simulatedPlacements)
                         .transform(),
                    sut);
    }

    private Optional<BootstrapAndReplace> prepareReplace(CMSSut sut, Node toReplace, Node replacement)
    {
        try
        {
            ClusterMetadata result = sut.service.commit(new PrepareReplace(toReplace.nodeId(),
                                                                           replacement.nodeId(),
                                                                           sut.service.placementProvider(),
                                                                           true,
                                                                           false));
            BootstrapAndReplace plan = (BootstrapAndReplace) result.inProgressSequences.get(replacement.nodeId());
            return Optional.of(plan);
        }
        catch (Throwable t)
        {
            return Optional.empty();
        }
    }

    private ModelChecker.Pair<ModelState, CMSSut> scheduleReplaceEvents(ModelState state,
                                                                        CMSSut sut,
                                                                        Node replaced,
                                                                        Node replacement,
                                                                        BootstrapAndReplace plan)
    {
        Iterator<ClusterMetadata> iter = toIter(sut.service, plan.startReplace, plan.midReplace, plan.finishReplace);

        SimulatedPlacements simulatedPlacements = state.simulatedPlacements;
        assert simulatedPlacements != null : "Cannot replace in an empty cluster";
        OperationState bootstrapOperation;
        ModelChecker.Pair<SimulatedPlacements, Transformations> nextSimulated = replace_directly(simulatedPlacements, replaced.token(), replacement.idx());
        simulatedPlacements = nextSimulated.l;
        bootstrapOperation = OperationState.newReplacement(replaced, replacement, iter, nextSimulated.r);
        return pair(state.transformer()
                         .addOperation(bootstrapOperation)
                         .updateSimulation(simulatedPlacements)
                         .transform(),
                    sut);
    }

    private ModelChecker.Pair<ModelState, CMSSut> joinWithoutBootstrap(Node node,
                                                                       ModelState state,
                                                                       CMSSut sut)
    {
        sut.service.commit(new UnsafeJoin(node.nodeId(), ImmutableSet.of(node.longToken()), sut.service.placementProvider()));

        List<Node> nodes = new ArrayList<>();
        nodes.add(node);
        if (state.simulatedPlacements != null)
            nodes.addAll(state.simulatedPlacements.nodes);
        nodes.sort(Node::compareTo);

        int rf = sut.replication.fullReplicas;
        SimulatedPlacements simulatedState = new SimulatedPlacements(rf, nodes,
                                                                     replicate(nodes, rf),
                                                                     replicate(nodes, rf),
                                                                     Collections.emptyList());
        return pair(state.transformer()
                         .withJoined(node)
                         .updateSimulation(simulatedState)
                         .transform(),
                    sut);
    }

    private ModelChecker.Pair<ModelState, Node> registerNewNode(ModelState state, CMSSut sut, Supplier<LongToken> token)
    {
        return registerNewNode(state, sut, token, 1, 1);
    }

    private ModelChecker.Pair<ModelState, Node> registerNewNode(ModelState state, CMSSut sut, Supplier<LongToken> token, int dc, int rack)
    {
        ModelState newState = state.transformer().incrementUniqueNodes().transform();
        Node node = new Node(token.get().token, newState.uniqueNodes, dc, rack);
        sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
        return pair(newState, node);
    }

    private Node getRemovalCandidate(ModelState state, ModelChecker.EntropySource entropySource)
    {
        Node node = null;
        while (node == null)
        {
            node = state.currentNodes.get(entropySource.nextInt(state.currentNodes.size() - 1));
            if (state.leavingNodes.contains(node))
                node = null;
        }
        return node;
    }

    private Node getMoveCandidate(ModelState state, ModelChecker.EntropySource entropySource)
    {
        Node node = null;
        while (node == null)
        {
            node = state.currentNodes.get(entropySource.nextInt(state.currentNodes.size() - 1));
            if (state.movingNodes.contains(node))
                node = null;
        }
        return node;
    }

    public static String toString(Map<?, ?> predicted)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : predicted.entrySet())
        {
            sb.append(e.getKey()).append("=").append(e.getValue()).append(",\n");
        }

        return sb.toString();
    }

    public static void match(PlacementForRange actual, Map<PlacementSimulator.Range, List<Node>> predicted) throws Throwable
    {
        Map<Range<Token>, EndpointsForRange> groups = actual.replicaGroups();
        assert predicted.size() == groups.size() :
        String.format("\nPredicted:\n%s(%d)" +
                      "\nActual:\n%s(%d)", toString(predicted), predicted.size(), toString(actual.replicaGroups()), groups.size());

        for (Map.Entry<PlacementSimulator.Range, List<Node>> entry : predicted.entrySet())
        {
            PlacementSimulator.Range range = entry.getKey();
            List<Node> nodes = entry.getValue();
            Range<Token> predictedRange = new Range<Token>(new Murmur3Partitioner.LongToken(range.start),
                                                           new Murmur3Partitioner.LongToken(range.end));
            EndpointsForRange endpointsForRange = groups.get(predictedRange);
            Assert.assertNotNull(String.format("Could not find %s in ranges %s", predictedRange, groups.keySet()),
                                 endpointsForRange);
            assertEquals(String.format("Predicted to have different endpoints for range %s" +
                                              "\nExpected: %s" +
                                              "\nActual:   %s",
                                              range, nodes, endpointsForRange.endpoints()),
                                nodes.size(), endpointsForRange.size());
            for (Node node : nodes)
            {
                assertTrue(String.format("Endpoints for range %s should have contained %s, but they have not." +
                                                "\nExpected: %s" +
                                                "\nActual:   %s.",
                                                endpointsForRange.range(),
                                                node.id(),
                                                nodes,
                                                endpointsForRange.endpoints()),
                                  endpointsForRange.endpoints().contains(InetAddressAndPort.getByAddress(InetAddress.getByName(node.id()))));
            }
        }
    }

    private static <L, R> ModelChecker.Pair<L, R> pair(L l, R r)
    {
        return new ModelChecker.Pair<>(l, r);
    }

    public static class ModelState
    {
        public final int maxClusterSize;
        public final int maxConcurrency;
        public final int uniqueNodes;
        public final int rejected;
        public final int[] cancelled;
        public final int joined;
        public final int left;
        public final int replaced;
        public final int moved;
        public final int bootstrappingCount;
        public final List<Node> currentNodes;
        public final List<Node> leavingNodes;
        public final List<Node> movingNodes;
        public final List<OperationState> operationStates;
        public final SimulatedPlacements simulatedPlacements;

        public static ModelState empty(int maxClusterSize, int maxConcurrency)
        {
            return new ModelState(maxClusterSize, maxConcurrency,
                                  0, 0, new int [] {0,0,0,0}, 0, 0, 0, 0,
                                  Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),  Collections.emptyList(),
                                  null);
        }

        private ModelState(int maxClusterSize,
                           int maxConcurrency,
                           int uniqueNodes,
                           int rejected,
                           int[] cancelled,
                           int joined,
                           int left,
                           int replaced,
                           int moved,
                           List<Node> currentNodes,
                           List<Node> leavingNodes,
                           List<Node> movingNodes,
                           List<OperationState> operationStates,
                           SimulatedPlacements simulatedPlacements)
        {
            this.maxClusterSize = maxClusterSize;
            this.maxConcurrency = maxConcurrency;
            this.uniqueNodes = uniqueNodes;
            this.rejected = rejected;
            this.cancelled = cancelled;
            this.joined = joined;
            this.left = left;
            this.replaced = replaced;
            this.moved = moved;
            this.currentNodes = currentNodes;
            this.leavingNodes = leavingNodes;
            this.movingNodes = movingNodes;
            this.operationStates = operationStates;
            this.simulatedPlacements = simulatedPlacements;
            bootstrappingCount = (int)operationStates.stream()
                                                     .filter(s -> s.type == OperationState.Type.BOOTSTRAP)
                                                     .count();
        }

        public Transformer transformer()
        {
            return new Transformer(this);
        }

        private boolean withinConcurrencyLimit()
        {
            return operationStates.size() < maxConcurrency;
        }

        public boolean shouldBootstrap()
        {
            return withinConcurrencyLimit() && bootstrappingCount + currentNodes.size() < maxClusterSize;
        }

        public boolean shouldDecommission(int rf, Random rng)
        {
            return withinConcurrencyLimit() && currentNodes.size() > rf && rng.nextDouble() > 0.7;
        }

        public boolean shouldMove(int rf, Random rng)
        {
            return withinConcurrencyLimit() && currentNodes.size() > rf && rng.nextDouble() > 0.7;
        }

        public boolean shouldReplace(int rf, Random rng)
        {
            return withinConcurrencyLimit() && currentNodes.size() > rf && rng.nextDouble() > 0.8;
        }

        public boolean shouldCancelOperation(OperationState.Type type, Random rng)
        {
            return rng.nextDouble() > 0.95;
        }

        public String toString()
        {
            return "ModelState{" +
                   "uniqueNodes=" + uniqueNodes +
                   ", rejectedOps=" + rejected +
                   ", cancelledOps=" + cancelled +
                   ", joined=" + joined +
                   ", left=" + left +
                   ", replaced=" + replaced +
                   ", bootstrappedNodes=" + currentNodes +
                   ", leavingNodes=" + leavingNodes +
                   ", operationStates=" + operationStates +
                   ", maxClusterSize=" + maxClusterSize +
                   ", maxConcurrency=" + maxConcurrency +
                   '}';
        }


        private static class Transformer
        {
            private int maxClusterSize;
            private int maxConcurrency;
            private int uniqueNodes;
            private int rejected;
            private int[] cancelled;
            private int joined;
            private int left;
            private int replaced;
            private int moved;
            private List<Node> currentNodes;
            private List<Node> leavingNodes;
            private List<Node> movingNodes;
            private List<OperationState> operationStates;
            private SimulatedPlacements simulatedPlacements;

            private Transformer(ModelState source)
            {
                this.maxClusterSize = source.maxClusterSize;
                this.maxConcurrency = source.maxConcurrency;
                this.uniqueNodes = source.uniqueNodes;
                this.rejected = source.rejected;
                this.cancelled = source.cancelled;
                this.joined = source.joined;
                this.left = source.left;
                this.moved = source.moved;
                this.replaced = source.replaced;
                this.currentNodes = source.currentNodes;
                this.leavingNodes = source.leavingNodes;
                this.movingNodes = source.movingNodes;
                this.operationStates = source.operationStates;
                this.simulatedPlacements = source.simulatedPlacements;
            }

            Transformer incrementUniqueNodes()
            {
                uniqueNodes++;
                return this;
            }

            Transformer incrementRejected()
            {
                rejected++;
                return this;
            }

            Transformer incrementCancelledJoin()
            {
                cancelled[0]++;
                return this;
            }

            Transformer incrementCancelledReplace()
            {
                cancelled[1]++;
                return this;
            }

            Transformer incrementCancelledLeave()
            {
                cancelled[2]++;
                return this;
            }

            Transformer incrementCancelledMove()
            {
                cancelled[3]++;
                return this;
            }

            Transformer addOperation(OperationState operation)
            {
                operationStates = new ArrayList<>(operationStates);
                operationStates.add(operation);
                return this;
            }

            Transformer removeOperation(OperationState operation)
            {
                operationStates = new ArrayList<>(operationStates);
                operationStates.remove(operation);
                return this;
            }

            Transformer withJoined(Node node)
            {
                addToCluster(node);
                joined++;
                return this;
            }

            Transformer withMoved(Node movingNode, Node movedTo)
            {
                assert currentNodes.contains(movingNode) : movingNode;
                List<Node> tmp = currentNodes;
                currentNodes = new ArrayList<>();
                for (Node n : tmp)
                {
                    if (n.idx() == movingNode.idx())
                        currentNodes.add(movedTo);
                    else
                        currentNodes.add(n);
                }

                this.moved++;

                assert movingNodes.contains(movingNode);
                movingNodes = new ArrayList<>(movingNodes);
                movingNodes.remove(movingNode);
                return this;
            }

            private void addToCluster(Node node)
            {
                // called during both join and replacement
                currentNodes = new ArrayList<>(currentNodes);
                currentNodes.add(node);
            }

            Transformer markMoving(Node moving)
            {
                assert currentNodes.contains(moving);
                movingNodes = new ArrayList<>(movingNodes);
                movingNodes.add(moving);
                return this;
            }

            Transformer markLeaving(Node leaving)
            {
                assert currentNodes.contains(leaving);
                leavingNodes = new ArrayList<>(leavingNodes);
                leavingNodes.add(leaving);
                return this;
            }

            Transformer withLeft(Node node)
            {
                assert currentNodes.contains(node);
                // for now... assassinate may change this assertion
                assert leavingNodes.contains(node);
                left++;
                removeFromCluster(node);
                return this;
            }

            private void removeFromCluster(Node node)
            {
                // called during both decommission and replacement
                currentNodes = new ArrayList<>(currentNodes);
                currentNodes.remove(node);
                leavingNodes = new ArrayList<>(leavingNodes);
                leavingNodes.remove(node);
            }

            Transformer withReplaced(Node oldNode, Node newNode)
            {
                addToCluster(newNode);
                removeFromCluster(oldNode);
                replaced++;
                return this;
            }

            Transformer updateSimulation(SimulatedPlacements simulatedPlacements)
            {
                this.simulatedPlacements = simulatedPlacements;
                return this;
            }

            ModelState transform()
            {
                return new ModelState(maxClusterSize,
                                      maxConcurrency,
                                      uniqueNodes,
                                      rejected,
                                      cancelled,
                                      joined,
                                      left,
                                      replaced,
                                      moved,
                                      currentNodes,
                                      leavingNodes,
                                      movingNodes,
                                      operationStates,
                                      simulatedPlacements);
            }
        }
    }

    public static class OperationState
    {
        enum Type { BOOTSTRAP, DECOMMISSION, REPLACEMENT, MOVE }
        enum Status { READY, STARTED }
        public final Type type;
        public final Node[] nodes;
        public final Iterator<ClusterMetadata> remaining;
        public final Status status;
        public Transformations stashedSteps;

        public static OperationState newBootstrap(Node node,
                                                  Iterator<ClusterMetadata> remaining,
                                                  Transformations stashedSteps)
        {
            return new OperationState(Type.BOOTSTRAP, new Node[] {node}, remaining, Status.READY, stashedSteps);
        }

        public static OperationState newDecommission(Node node,
                                                     Iterator<ClusterMetadata> remaining,
                                                     Transformations stashedSteps)
        {
            return new OperationState(Type.DECOMMISSION, new Node[] {node}, remaining, Status.READY, stashedSteps);
        }

        public static OperationState newReplacement(Node replaced,
                                                    Node replacement,
                                                    Iterator<ClusterMetadata> remaining,
                                                    Transformations stashedSteps)
        {
            return new OperationState(Type.REPLACEMENT, new Node[] {replaced, replacement}, remaining, Status.READY, stashedSteps);
        }

        public static OperationState newMove(Node movingNode,
                                             Node newNode,
                                             Iterator<ClusterMetadata> remaining,
                                             Transformations stashedSteps)
        {
            return new OperationState(Type.MOVE,
                                      new Node[] {newNode, movingNode},
                                      remaining, Status.READY, stashedSteps);
        }


        public OperationState(Type type,
                              Node[] nodes,
                              Iterator<ClusterMetadata> remaining,
                              Status status,
                              Transformations stashedSteps)
        {
            this.type = type;
            this.nodes = nodes;
            this.remaining = remaining;
            this.status = status;
            this.stashedSteps = stashedSteps;
        }

        public OperationState copyAndMarkStarted()
        {
            return new OperationState(type, nodes, remaining, Status.STARTED, stashedSteps);
        }

        public String toString()
        {
            return "OperationState{" +
                   "type=" + type +
                   ", nodes=" + Arrays.toString(nodes) +
                   ", remaining=" + remaining.hasNext() +
                   ", status=" + status +
                   '}';
        }
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
                                          ReplicationParams replicationParams,
                                          ModelState modelState,
                                          DataPlacements placements)
    {

        Set<Token> allTokens = new HashSet<>(toTokens(modelState.currentNodes));
        for (OperationState bootstrappedNode : modelState.operationStates)
            allTokens.add(bootstrappedNode.nodes[0].longToken());

        List<Range<Token>> expectedRanges = toRanges(allTokens, partitioner);

        DataPlacement actualPlacements = placements.get(replicationParams);

        assertRanges(expectedRanges, actualPlacements.writes.ranges());
        assertRanges(expectedRanges, actualPlacements.reads.ranges());
        int rf = Integer.parseInt(replicationParams.options.get("replication_factor"));
        // TODO; assert that the node that "owns" the token actually belongs here
        validatePlacementsInternal(rf, modelState.operationStates, expectedRanges, actualPlacements.reads, false);
        validatePlacementsInternal(rf, modelState.operationStates, expectedRanges, actualPlacements.writes, true);
    }

    public static void assertRanges(List<Range<Token>> l, List<Range<Token>> r)
    {
        Assert.assertEquals(new TreeSet<>(l), new TreeSet<Range<Token>>(r));
    }

    public static void validatePlacementsInternal(int rf, List<OperationState> opStates, List<Range<Token>> expectedRanges, PlacementForRange placements, boolean allowPending)
    {
        int overreplicated = 0;
        for (Range<Token> range : expectedRanges)
        {
            if (allowPending)
            {
                int diff = placements.forRange(range).size() - rf;
                // We may have many overreplicated ranges, but each range is overreplicated by one
                Assert.assertTrue(String.format("Overreplicated by %d", diff), diff == 0 || diff == 1);
                overreplicated += diff;
                assertTrue(String.format("Expected a replication factor of %d, for range %s but got %d",
                                           rf, range, placements.forRange(range).size()),
                           placements.forRange(range).size() >= rf);
            }
            else
            {
                assertEquals(String.format("Expected a replication factor of %d, for range %s but got %d",
                                           rf, range, placements.forRange(range).size()),
                             rf, placements.forRange(range).size());
            }
        }

        if (allowPending && opStates.size() > rf)
        {
            int bootstrappingNodes = 0;
            int movingNodes = 0;
            int leavingNodes = 0;
            int replacedNodes = 0;
            int expectedOverReplicated = 0;
            for (OperationState opState : opStates)
            {
                if (opState.status == OperationState.Status.STARTED)
                {
                    if ( opState.type == OperationState.Type.MOVE ) movingNodes += 1;
                    if ( opState.type == OperationState.Type.REPLACEMENT) replacedNodes += 1;
                    if ( opState.type == OperationState.Type.BOOTSTRAP ) bootstrappingNodes += 1;
                    if ( opState.type == OperationState.Type.DECOMMISSION ) leavingNodes += 1;
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
            assertTrue(String.format("Because there are %d nodes joining/moving/leaving/replaced (%d/%d/%d/%d) that have added themselves " +
                                     "to write set, we expect to have at least %d ranges over-replicated, but got %d. RF %d (%s)",
                                     bootstrappingNodes + movingNodes + leavingNodes, bootstrappingNodes, movingNodes, leavingNodes, replacedNodes, expectedOverReplicated, overreplicated,
                                     rf, placements),
                       overreplicated >= expectedOverReplicated && overreplicated <= (expectedOverReplicated * rf + 2 + movingNodes * rf));
        }
    }

    public static Iterator<ClusterMetadata> toIter(ClusterMetadataService cms, Transformation... transforms)
    {
        Iterator<Transformation> iter = Iterators.forArray(transforms);
        return new Iterator<ClusterMetadata>()
        {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public ClusterMetadata next()
            {
                try
                {
                    return cms.commit(iter.next());
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}

// TODO: simulator should assert that splits are equivalent to splitting things sequentially and then replicating
// TODO: this should _also_ be consistent with just taking the ring with all tokens and making sure things are properly replicated
// TODO: interrupted bootstraps
// TODO: network topology strategy