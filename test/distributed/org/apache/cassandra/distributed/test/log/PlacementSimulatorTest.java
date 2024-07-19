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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.harry.checker.ModelChecker;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.TokenPlacementModel.Replica;

import static org.apache.cassandra.distributed.test.log.PlacementSimulator.SimulatedPlacements;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.Transformations;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.assertPlacements;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.assertRanges;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.filter;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.join;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.leave;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.move;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.replace;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.split;
import static org.apache.cassandra.distributed.test.log.PlacementSimulator.superset;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.Node;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.NodeFactory;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.Range;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.ReplicationFactor;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.SimpleReplicationFactor;
import static org.junit.Assert.assertTrue;

public class PlacementSimulatorTest
{
    @Test
    public void testMove()
    {
        testMove(100);
        testMove(Long.MIN_VALUE);
    }

    public void testMove(long minToken)
    {
        testMove(minToken, 200, 300, 400, 350, new SimpleReplicationFactor(3));

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testMove(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), new SimpleReplicationFactor(3));
        }
    }

    public void testMove(long t1, long t2, long t3, long t4, long newToken, ReplicationFactor rf)
    {
        NodeFactory factory = TokenPlacementModel.nodeFactory();
        Node movingNode = factory.make(1, 1, 1).overrideToken(t1);
        List<Node> orig = Arrays.asList(movingNode,
                                        factory.make(2, 1, 1).overrideToken(t2),
                                        factory.make(3, 1, 1).overrideToken(t3),
                                        factory.make(4, 1, 1).overrideToken(t4));
        orig.sort(Node::compareTo);

        SimulatedPlacements placements = new SimulatedPlacements(rf,
                                                                 orig,
                                                                 rf.replicate(orig).asMap(),
                                                                 rf.replicate(orig).asMap(),
                                                                 Collections.emptyList());
        Transformations steps = move(placements, movingNode, newToken);

        List<Node> afterSplit = split(orig, newToken);
        List<Node> finalState = moveFinalState(orig, movingNode, newToken);

        placements = steps.advance(placements);
        placements = steps.advance(placements);

        assertPlacements(placements,
                         rf.replicate(afterSplit).asMap(),
                         superset(rf.replicate(afterSplit).asMap(),
                                  rf.replicate(split(finalState, movingNode.token())).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(split(finalState, movingNode.token())).asMap(),
                         superset(rf.replicate(afterSplit).asMap(),
                                  rf.replicate(split(finalState, movingNode.token())).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(finalState).asMap(),
                         rf.replicate(finalState).asMap());
    }

    @Test
    public void testBootstrap()
    {
        testBootstrap(350);
        testBootstrap(Long.MIN_VALUE);
    }

    public void testBootstrap(long newToken)
    {
        testBootstrap(100, 200, 300, 400, newToken, new SimpleReplicationFactor(3));

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testBootstrap(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), new SimpleReplicationFactor(3));
        }
    }

    public void testBootstrap(long t1, long t2, long t3, long t4, long newToken, ReplicationFactor rf)
    {
        NodeFactory factory = TokenPlacementModel.nodeFactory();
        List<Node> orig = Arrays.asList(factory.make(1, 1, 1).overrideToken(t1),
                                        factory.make(2, 1, 1).overrideToken(t2),
                                        factory.make(3, 1, 1).overrideToken(t3),
                                        factory.make(4, 1, 1).overrideToken(t4));
        orig.sort(Node::compareTo);

        Node newNode = factory.make(5, 1, 1).overrideToken(newToken);
        SimulatedPlacements placements = new SimulatedPlacements(rf,
                                                                 orig,
                                                                 rf.replicate(orig).asMap(),
                                                                 rf.replicate(orig).asMap(),
                                                                 Collections.emptyList());
        Transformations steps = join(placements, newNode);

        List<Node> afterSplit = split(orig, newToken);
        List<Node> finalState = bootstrapFinalState(orig, newNode, newToken);

        placements = steps.advance(placements);
        placements = steps.advance(placements);

        assertPlacements(placements,
                         rf.replicate(afterSplit).asMap(),
                         superset(rf.replicate(afterSplit).asMap(),
                                  rf.replicate(finalState).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(finalState).asMap(),
                         superset(rf.replicate(afterSplit).asMap(),
                                  rf.replicate(finalState).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(finalState).asMap(),
                         rf.replicate(finalState).asMap());
    }


    @Test
    public void testDecommission()
    {
        testDecommission(100);
        testDecommission(Long.MIN_VALUE);
    }

    public void testDecommission(long minToken)
    {
        testDecommission(minToken, 200, 300, 400, 350, new SimpleReplicationFactor(3));

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testDecommission(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), new SimpleReplicationFactor(3));
        }
    }

    public void testDecommission(long t1, long t2, long t3, long t4, long t5, ReplicationFactor rf)
    {
        NodeFactory factory = TokenPlacementModel.nodeFactory();
        Node leavingNode = factory.make(1, 1, 1).overrideToken(t1);
        List<Node> orig = Arrays.asList(leavingNode,
                                        factory.make(2, 1, 1).overrideToken(t2),
                                        factory.make(3, 1, 1).overrideToken(t3),
                                        factory.make(4, 1, 1).overrideToken(t4),
                                        factory.make(4, 1, 1).overrideToken(t5));
        orig.sort(Node::compareTo);

        SimulatedPlacements placements = new SimulatedPlacements(rf,
                                                                 orig,
                                                                 rf.replicate(orig).asMap(),
                                                                 rf.replicate(orig).asMap(),
                                                                 Collections.emptyList());
        Transformations steps = leave(placements, leavingNode);

        List<Node> finalState = leaveFinalState(orig, leavingNode.token());

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(orig).asMap(),
                         superset(rf.replicate(orig).asMap(),
                                  rf.replicate(split(finalState, leavingNode.token())).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(split(finalState, leavingNode.token())).asMap(),
                         superset(rf.replicate(orig).asMap(),
                                  rf.replicate(split(finalState, leavingNode.token())).asMap()));

        placements = steps.advance(placements);
        assertPlacements(placements,
                         rf.replicate(finalState).asMap(),
                         rf.replicate(finalState).asMap());
    }

    public static List<Node> moveFinalState(List<Node> nodes, Node target, long newToken)
    {
        nodes = filter(nodes, n -> n.idx() != target.idx()); // filter out current owner
        nodes = split(nodes, newToken);                      // materialize new token
        nodes = move(nodes, newToken, target);               // move new token to the node
        return nodes;
    }

    public static List<Node> bootstrapFinalState(List<Node> nodes, Node newNode, long newToken)
    {
        nodes = split(nodes, newToken);               // materialize new token
        nodes = move(nodes, newToken, newNode);       // move new token to the node
        return nodes;
    }

    public static List<Node> leaveFinalState(List<Node> nodes, long leavingToken)
    {
        nodes = filter(nodes, n -> n.token() != leavingToken);
        return nodes;
    }

    @Test
    public void simulate() throws Throwable
    {
        for (int rf : new int[]{ 2, 3, 5 })
        {
            simulate(new SimpleReplicationFactor(rf));
        }
    }

    public void simulate(ReplicationFactor rf) throws Throwable
    {
        NodeFactory factory = TokenPlacementModel.nodeFactory();
        List<Node> orig = Collections.singletonList(factory.make(1, 1, 1));

        ModelChecker<SimulatedPlacements, SUTState> modelChecker = new ModelChecker<>();
        AtomicInteger addressCounter = new AtomicInteger(1);
        AtomicInteger operationCounter = new AtomicInteger(1);

        modelChecker.init(new SimulatedPlacements(rf,
                                                  orig,
                                                  rf.replicate(orig).asMap(),
                                                  rf.replicate(orig).asMap(),
                                                  Collections.emptyList()),
                          new SUTState())
                    .step((state, sut) -> state.nodes.size() < rf.total(),
                          (state, sut, rng) -> new ModelChecker.Pair<>(PlacementSimulator.joinFully(state, factory.make(addressCounter.incrementAndGet(), 1, 1)),
                                                                       sut))
                    .step((state, sut) -> state.nodes.size() >= rf.total() && state.stashedStates.size() < 1,
                          (state, sut, rng) -> {
                              if (operationCounter.getAndIncrement() % rf.total() == 1)
                              {
                                  // randomly schedule either decommission or replacement of an existing node
                                  Node toRemove = state.nodes.get(rng.nextInt(0, state.nodes.size()));
                                  state = state.withStashed(rng.nextBoolean()
                                                            ? replace(state, toRemove, factory.make(addressCounter.incrementAndGet(), 1, 1).overrideToken(toRemove.token()))
                                                            : leave(state, toRemove));
                                  return new ModelChecker.Pair<>(state, sut);
                              }
                              else
                              {
                                  // schedule bootstrapping an additional node
                                  return new ModelChecker.Pair<>(state.withStashed(join(state,
                                                                                        factory.make(addressCounter.incrementAndGet(), 1, 1))),
                                                                 sut);
                              }
                          })
                    .step((state, sut) -> !state.stashedStates.isEmpty(),
                          (state, sut, rng) -> {
                              int idx = rng.nextInt(0, state.stashedStates.size());
                              state = state.stashedStates.get(idx).advance(state);
                              return new ModelChecker.Pair<>(state, sut);
                          })
                    .exitCondition((state, sut) -> {
                        if (addressCounter.get() >= 100 && state.stashedStates.isEmpty())
                        {
                            // After all commands are done, we should arrive to correct placements
                            assertRanges(state.writePlacements,
                                         rf.replicate(state.nodes).asMap());
                            assertRanges(state.readPlacements,
                                         rf.replicate(state.nodes).asMap());
                            return true;
                        }
                        return false;
                    })
                    .run();
    }

    @Test
    public void revertPartialBootstrap() throws Throwable
    {
        for (int n : new int[]{ 2, 3, 5 })
        {
            ReplicationFactor rf = new SimpleReplicationFactor(n);
            NodeFactory factory = TokenPlacementModel.nodeFactoryHumanReadable();
            List<Node> nodes = new ArrayList<>(10);
            for (int i = 1; i <= 10; i++)
                nodes.add(factory.make(i, 1, 1));
            nodes.sort(Comparator.comparing(Node::token));

            SimulatedPlacements sim = new SimulatedPlacements(rf, nodes, rf.replicate(nodes).asMap(), rf.replicate(nodes).asMap(), Collections.emptyList());
            Node newNode = factory.make(11, 1, 1);
            revertPartiallyCompleteOp(sim, () -> join(sim, newNode), 3);
        }
    }

    @Test
    public void revertPartialLeave()
    {
        for (int n : new int[]{ 2, 3, 5 })
        {
            ReplicationFactor rf = new SimpleReplicationFactor(n);
            NodeFactory factory = TokenPlacementModel.nodeFactoryHumanReadable();
            List<Node> nodes = new ArrayList<>(10);
            for (int i = 1; i <= 10; i++)
                nodes.add(factory.make(i, 1, 1));
            nodes.sort(Comparator.comparing(Node::token));
            Node toRemove = nodes.get(5);
            SimulatedPlacements sim = new SimulatedPlacements(rf, nodes, rf.replicate(nodes).asMap(), rf.replicate(nodes).asMap(), Collections.emptyList());
            revertPartiallyCompleteOp(sim, () -> leave(sim, toRemove), 2);
        }
    }

    @Test
    public void revertPartialReplacement()
    {
        for (int n : new int[]{ 2, 3, 5 })
        {
            ReplicationFactor rf = new SimpleReplicationFactor(n);
            NodeFactory factory = TokenPlacementModel.nodeFactoryHumanReadable();
            List<Node> nodes = new ArrayList<>(10);
            for (int i = 1; i <= 10; i++)
                nodes.add(factory.make(i, 1, 1));
            nodes.sort(Comparator.comparing(Node::token));

            Node toReplace = nodes.get(5);
            SimulatedPlacements sim = new SimulatedPlacements(rf,
                                                              nodes,
                                                              rf.replicate(nodes).asMap(),
                                                              rf.replicate(nodes).asMap(),
                                                              Collections.emptyList());
            Node replacement = factory.make(11, 1, 1).overrideToken(toReplace.token());
            revertPartiallyCompleteOp(sim, () -> replace(sim, toReplace, replacement), 2);
        }
    }


    private void revertPartiallyCompleteOp(SimulatedPlacements startingState,
                                           Supplier<Transformations> opProvider,
                                           int maxStepsBeforeRevert)
    {
        // reverting the bootstrap after only n steps have been executed
        // for the various operations steps that may be performed before revert are:
        // bootstrap_diffBased:             [split, start, mid]
        // bootstrap_explicitPlacement:     [split, start, mid]
        // replace_directly:                [start, mid]
        // leave_diffBased:                 [start, mid]

        for (int i = 1; i <= maxStepsBeforeRevert; i++)
            startThenRevertOp(startingState, opProvider, i);
    }

    private void startThenRevertOp(SimulatedPlacements sim,
                                   Supplier<Transformations> opProvider,
                                   int stepsToExecute)
    {
        Map<Range, List<Replica>> startingReadPlacements = sim.readPlacements;
        Map<Range, List<Replica>> startingWritePlacements = sim.writePlacements;
        Transformations steps = opProvider.get();
        sim = sim.withStashed(steps);
        // execute the required steps
        for (int i = 0; i < stepsToExecute; i++)
            sim = steps.advance(sim);

        // now revert them
        sim = steps.revertPublishedEffects(sim);

        assertRanges(startingReadPlacements, sim.readPlacements);
        assertRanges(startingWritePlacements, sim.writePlacements);
        assertTrue(sim.stashedStates.isEmpty());
    }

    public static class SUTState
    {
    }
}