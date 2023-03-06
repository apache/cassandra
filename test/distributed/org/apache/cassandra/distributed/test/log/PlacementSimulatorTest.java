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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import static org.apache.cassandra.distributed.test.log.PlacementSimulator.*;
import static org.junit.Assert.assertTrue;

public class PlacementSimulatorTest
{

    @Test
    public void testMove()
    {
        testMove(100, 200, 300, 400, 350, 3);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testMove(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), 3);
        }
    }

    public void testMove(long t1, long t2, long t3, long t4, long newToken, int rf)
    {
        Node movingNode = n(1, t1);
        List<Node> orig = Arrays.asList(movingNode,
                                        n(2,t2),
                                        n(3,t3),
                                        n(4,t4));
        orig.sort(Node::compareTo);

        SimulatedPlacements placements = new SimulatedPlacements(rf, orig, replicate(orig, rf), replicate(orig, rf), Collections.emptyList());
        ModelChecker.Pair<SimulatedPlacements, Transformations> steps = move_diffBased(placements, "127.0.0.1", newToken);

        List<Node> afterSplit = split(orig, newToken);
        List<Node> finalState = moveFinalState(orig, movingNode, newToken);

        placements = steps.r.advance(placements);
        placements = steps.r.advance(placements);

        assertPlacements(placements,
                         replicate(afterSplit, rf),
                         superset(replicate(afterSplit, rf),
                                  replicate(split(finalState, movingNode.token), rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(split(finalState, movingNode.token), rf),
                         superset(replicate(afterSplit, rf),
                                  replicate(split(finalState, movingNode.token), rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(finalState, rf),
                         replicate(finalState, rf));
    }

    @Test
    public void testBootstrap()
    {
        testBootstrap(100, 200, 300, 400, 350, 3);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testBootstrap(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), 3);
        }
    }

    public void testBootstrap(long t1, long t2, long t3, long t4, long newToken, int rf)
    {
        List<Node> orig = Arrays.asList(n(1,t1),
                                        n(2,t2),
                                        n(3,t3),
                                        n(4,t4));
        orig.sort(Node::compareTo);

        Node newNode = n(5, newToken);
        SimulatedPlacements placements = new SimulatedPlacements(rf, orig, replicate(orig, rf), replicate(orig, rf), Collections.emptyList());
        ModelChecker.Pair<SimulatedPlacements, Transformations> steps = bootstrap_diffBased(placements, "127.0.0.5", newToken);

        List<Node> afterSplit = split(orig, newToken);
        List<Node> finalState = bootstrapFinalState(orig, newNode, newToken);

        placements = steps.r.advance(placements);
        placements = steps.r.advance(placements);

        assertPlacements(placements,
                         replicate(afterSplit, rf),
                         superset(replicate(afterSplit, rf),
                                  replicate(finalState, rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(finalState, rf),
                         superset(replicate(afterSplit, rf),
                                  replicate(finalState, rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(finalState, rf),
                         replicate(finalState, rf));
    }


    @Test
    public void testDecomsission()
    {
        testDecomsission(100, 200, 300, 400, 350, 3);

        Random rng = new Random();
        for (int i = 0; i < 1000; i++)
        {
            PrimitiveIterator.OfInt ints = rng.ints(5).distinct().iterator();
            testDecomsission(ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), ints.nextInt(), 3);
        }
    }

    public void testDecomsission(long t1, long t2, long t3, long t4, long t5, int rf)
    {
        Node leavingNode = n(1, t1);
        List<Node> orig = Arrays.asList(leavingNode,
                                        n(2,t2),
                                        n(3,t3),
                                        n(4,t4),
                                        n(4,t5));
        orig.sort(Node::compareTo);

        SimulatedPlacements placements = new SimulatedPlacements(rf, orig, replicate(orig, rf), replicate(orig, rf), Collections.emptyList());
        ModelChecker.Pair<SimulatedPlacements, Transformations> steps = leave_diffBased(placements, leavingNode.token);

        List<Node> finalState = leaveFinalState(orig, leavingNode.token);

        //TODO: for some reason diff-based leave is a 3 step operation
        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(orig, rf),
                         superset(replicate(orig, rf),
                                  replicate(split(finalState, leavingNode.token), rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(split(finalState, leavingNode.token), rf),
                         superset(replicate(orig, rf),
                                  replicate(split(finalState, leavingNode.token), rf)));

        placements = steps.r.advance(placements);
        assertPlacements(placements,
                         replicate(finalState, rf),
                         replicate(finalState, rf));
    }

    public static List<Node> moveFinalState(List<Node> nodes, Node target, long newToken)
    {
        nodes = filter(nodes, n -> !n.id.equals(target.id)); // filter out current owner
        nodes = split(nodes, newToken);                      // materialize new token
        nodes = move(nodes, newToken, target.id);            // move new token to the node
        return nodes;
    }

    public static List<Node> bootstrapFinalState(List<Node> nodes, Node newNode, long newToken)
    {
        nodes = split(nodes, newToken);            // materialize new token
        nodes = move(nodes, newToken, newNode.id); // move new token to the node
        return nodes;
    }

    public static List<Node> leaveFinalState(List<Node> nodes, long leavingToken)
    {
        nodes = filter(nodes, n -> n.token != leavingToken);
        return nodes;
    }


    public static PlacementSimulator.Node n(int id, long token)
    {
        return new PlacementSimulator.Node(token, "127.0.0." + id);
    }

    @Test
    public void simulate() throws Throwable
    {
        for (int rf : new int[]{ 2, 3, 5 })
        {
            simulate(rf);
        }
    }

    public void simulate(int rf) throws Throwable
    {
        List<Long> source = readableTokens(100);
        Iterator<Long> tokens = source.iterator();
        List<Node> orig = Collections.singletonList(new Node(tokens.next(), "127.0.0.1"));

        ModelChecker<SimulatedPlacements, SUTState> modelChecker = new ModelChecker<>();
        AtomicInteger  addressCounter = new AtomicInteger(1);
        AtomicInteger  operationCounter = new AtomicInteger(1);

        modelChecker.init(new SimulatedPlacements(rf, orig, replicate(orig, rf), replicate(orig, rf), Collections.emptyList()),
                          new SUTState())
                    .step((state, sut) -> state.nodes.size() < rf,
                          (state, sut, rng) -> new ModelChecker.Pair<>(bootstrapFully(state,
                                                                                      "127.0.0." + addressCounter.incrementAndGet(),
                                                                                      tokens.next()),
                                                                       sut))
                    .step((state, sut) -> state.nodes.size() >= rf && state.stashedStates.size() < 1,
                          (state, sut, rng) -> {
                              if (operationCounter.getAndIncrement() % rf == 1)
                              {
                                  // randomly schedule either decommission or replacement of an existing node
                                  Node toRemove = state.nodes.get(rng.nextInt(0, state.nodes.size() - 1));
                                  return rng.nextBoolean()
                                         ? new ModelChecker.Pair<>(replace_directly(state, toRemove.token, "127.0.0." + addressCounter.incrementAndGet()).l, sut)
                                         : new ModelChecker.Pair<>(leave_diffBased(state, toRemove.token).l, sut);
                              }
                              else
                              {
                                  // schedule bootstrapping an additional node
                                  return new ModelChecker.Pair<>(bootstrap_diffBased(state,
                                                                                     "127.0.0." + addressCounter.incrementAndGet(),
                                                                                     tokens.next()).l,
                                                                 sut);
                              }
                          })
                    .step((state, sut) -> !state.stashedStates.isEmpty(),
                          (state, sut, rng) -> {
                              int idx = rng.nextInt(0, state.stashedStates.size() - 1);
                              state = state.stashedStates.get(idx).advance(state);
                              return new ModelChecker.Pair<>(state, sut);
                          })
                    .exitCondition((state, sut) -> {
                        if (addressCounter.get() >= source.size() && state.stashedStates.isEmpty())
                        {
                            // After all commands are done, we should arrive to correct placements
                            assertRanges(state.writePlacements,
                                         replicate(state.nodes, rf));
                            assertRanges(state.readPlacements,
                                         replicate(state.nodes, rf));
                            return true;
                        }
                        return false;
                    })
                    .run();
    }

    @Test
    public void revertPartialBootstrap() throws Throwable
    {
        for (int rf : new int[]{2, 3, 5})
        {
            List<Long> source = readableTokens(100);
            Iterator<Long> tokens = source.iterator();
            List<Node> nodes = nodes(10, tokens);
            long nextToken = tokens.next();
            String newNode = "127.0.0." + nodes.size() + 1;
            SimulatedPlacements sim = new SimulatedPlacements(rf, nodes, replicate(nodes, rf), replicate(nodes, rf), Collections.emptyList());
            revertPartiallyCompleteOp(sim, () -> bootstrap_diffBased(sim, newNode, nextToken), 3);
        }
    }

    @Test
    public void revertPartialLeave()
    {
        for (int rf : new int[]{2, 3, 5})
        {
            List<Long> source = readableTokens(100);
            Iterator<Long> tokens = source.iterator();
            List<Node> nodes = nodes(10, tokens);
            Node toRemove = nodes.get(5);
            SimulatedPlacements sim = new SimulatedPlacements(rf, nodes, replicate(nodes, rf), replicate(nodes, rf), Collections.emptyList());
            revertPartiallyCompleteOp(sim, () -> leave_diffBased(sim, toRemove.token), 2);
        }
    }

    @Test
    public void revertPartialReplacement()
    {
        for (int rf : new int[]{2, 3, 5})
        {
            List<Long> source = readableTokens(100);
            Iterator<Long> tokens = source.iterator();
            List<Node> nodes = nodes(10, tokens);
            Node toReplace = nodes.get(5);
            SimulatedPlacements sim = new SimulatedPlacements(rf, nodes, replicate(nodes, rf), replicate(nodes, rf), Collections.emptyList());
            revertPartiallyCompleteOp(sim, () -> replace_directly(sim, toReplace.token, "127.0.0.99"), 2);
        }
    }

    private List<Node> nodes(int n, Iterator<Long> tokens)
    {
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < n; i++)
            nodes.add(new Node(tokens.next(), "127.0.0." + (i+1)));
        nodes.sort(Node::compareTo);
        return nodes;
    }

    private void revertPartiallyCompleteOp(SimulatedPlacements startingState,
                                           Supplier<ModelChecker.Pair<SimulatedPlacements, Transformations>> opProvider,
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
                                   Supplier<ModelChecker.Pair<SimulatedPlacements, Transformations>> opProvider,
                                   int stepsToExecute)
    {
        Map<Range, List<Node>> startingReadPlacements = sim.readPlacements;
        Map<Range, List<Node>> startingWritePlacements = sim.writePlacements;
        ModelChecker.Pair<SimulatedPlacements, Transformations> op = opProvider.get();;
        sim = op.l;
        Transformations steps = op.r;
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
