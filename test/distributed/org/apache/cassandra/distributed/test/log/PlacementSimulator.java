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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;

import harry.generators.PCGFastPure;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.locator.InetAddressAndPort;
;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * A small class that helps to avoid doing mental arithmetics on ranges.
 */
public class PlacementSimulator
{
    @SuppressWarnings("unused") // for debugging convenience
    public static List<Long> readableTokens(int number)
    {
        final List<Long> longs;
        longs = new ArrayList<>();
        for (int i = 0; i < number; i++)
        {
            longs.add((i + 1) * 100L);
        }
        Collections.shuffle(longs, new Random(1));

        return longs;
    }

    public static DebugLog debug = new DebugLog();

    public static class SimulatedPlacements
    {
        public final ReplicationFactor rf;
        public final List<Node> nodes;
        public final NavigableMap<Range, List<Node>> readPlacements;
        public final NavigableMap<Range, List<Node>> writePlacements;
        // Stashed states are steps required to finish the operation. For example, in case of
        // bootstrap, this could be adding replicas to write (and then read) sets after splitting ranges.
        public final List<Transformations> stashedStates;

        public SimulatedPlacements(ReplicationFactor rf,
                                   List<Node> nodes,
                                   NavigableMap<Range, List<Node>> readPlacements,
                                   NavigableMap<Range, List<Node>> writePlacements,
                                   List<Transformations> stashedStates)
        {
            this.rf = rf;
            this.nodes = nodes;
            this.readPlacements = readPlacements;
            this.writePlacements = writePlacements;
            this.stashedStates = stashedStates;
        }

        public SimulatedPlacements withNodes(List<Node> newNodes)
        {
            return new SimulatedPlacements(rf, newNodes, readPlacements, writePlacements, stashedStates);
        }

        public SimulatedPlacements withReadPlacements(NavigableMap<Range, List<Node>> newReadPlacements)
        {
            return new SimulatedPlacements(rf, nodes, newReadPlacements, writePlacements, stashedStates);
        }

        public SimulatedPlacements withWritePlacements(NavigableMap<Range, List<Node>> newWritePlacements)
        {
            return new SimulatedPlacements(rf, nodes, readPlacements, newWritePlacements, stashedStates);
        }

        public SimulatedPlacements withStashed(Transformations steps)
        {
            List<Transformations> newStashed = new ArrayList<>();
            newStashed.addAll(stashedStates);
            newStashed.add(steps);
            return new SimulatedPlacements(rf, nodes, readPlacements, writePlacements, newStashed);
        }

        private SimulatedPlacements withoutStashed(Transformations finished)
        {
            List<Transformations> newStates = new ArrayList<>();
            for (Transformations s : stashedStates)
                if (s != finished)
                    newStates.add(s);
            return new SimulatedPlacements(rf, nodes, readPlacements, writePlacements, newStates);
        }

        public boolean isWriteTargetFor(long token, Predicate<Node> predicate)
        {
            return writePlacementsFor(token).stream().anyMatch(predicate);
        }

        public boolean isReadReplicaFor(long token, Predicate<Node> predicate)
        {
            return readReplicasFor(token).stream().anyMatch(predicate);
        }

        public boolean isReadReplicaFor(long minToken, long maxToken, Predicate<Node> predicate)
        {
            return readReplicasFor(minToken, maxToken).stream().anyMatch(predicate);
        }

        public List<Node> writePlacementsFor(long token)
        {
            for (Map.Entry<Range, List<Node>> e : writePlacements.entrySet())
            {
                if (e.getKey().contains(token))
                    return e.getValue();
            }

            throw new AssertionError();
        }

        public List<Node> readReplicasFor(long minToken, long maxToken)
        {
            for (Map.Entry<Range, List<Node>> e : readPlacements.entrySet())
            {
                if (e.getKey().contains(minToken, maxToken))
                    return e.getValue();
            }

            throw new AssertionError();
        }


        public List<Node> readReplicasFor(long token)
        {
            for (Map.Entry<Range, List<Node>> e : readPlacements.entrySet())
            {
                if (e.getKey().contains(token))
                    return e.getValue();
            }

            throw new AssertionError();
        }

        public String toString()
        {
            return "ModelState{" +
                   "\nrf=" + rf +
                   "\nnodes=" + nodes +
                   "\nreadPlacements=\n" + placementsToString(readPlacements) +
                   "\nwritePlacements=\n" + placementsToString(writePlacements) +
                   '}';
        }
    }

    public interface SimulatedPlacementHolder
    {
        SimulatedPlacements get();

        /**
         * Applies _one_ of the transformations given to the current state, returning the resulting state.
         * _Does_ set the state within the holder as well.
         */
        SimulatedPlacements applyNext(Transformations fn);
        SimulatedPlacementHolder set(SimulatedPlacements placements);
        SimulatedPlacementHolder fork();
    }

    public static class RefSimulatedPlacementHolder implements SimulatedPlacementHolder
    {
        private SimulatedPlacements state;

        public RefSimulatedPlacementHolder(SimulatedPlacements state)
        {
            this.state = state;
        }

        public SimulatedPlacements get()
        {
            return state;
        }

        public SimulatedPlacements applyNext(Transformations fn)
        {
            return state = fn.advance(state);
        }

        public SimulatedPlacementHolder set(SimulatedPlacements newState)
        {
            state = newState;
            return this;
        }

        public SimulatedPlacementHolder fork()
        {
            return new RefSimulatedPlacementHolder(state);
        }
    }

    public static class Transformation
    {
        private Function<SimulatedPlacements, SimulatedPlacements> apply;
        private Function<SimulatedPlacements, SimulatedPlacements> revert;

        Transformation(Function<SimulatedPlacements, SimulatedPlacements> apply,
                       Function<SimulatedPlacements, SimulatedPlacements> revert)
        {
            this.apply = apply;
            this.revert = revert;
        }

        public Transformation prepare(Function<SimulatedPlacements, SimulatedPlacements> apply,
                                      Function<SimulatedPlacements, SimulatedPlacements> revert)
        {
            this.apply = apply;
            this.revert = revert;
            return this;
        }
    }

    public static class Transformations
    {
        private final List<Transformation> steps = new ArrayList<>();
        private int idx = 0;

        public void add(Transformation step)
        {
            steps.add(step);
        }

        public boolean hasNext()
        {
            return idx < steps.size();
        }

        public SimulatedPlacements advance(SimulatedPlacements prev)
        {
            if (idx >= steps.size())
                throw new IllegalStateException("Cannot advance transformations, no more steps remaining");

            SimulatedPlacements next = steps.get(idx++).apply.apply(prev);
            if (!hasNext())
                next = next.withoutStashed(this);

            return next;
        }

        public boolean hasPrevious()
        {
            return idx > 0;
        }

        public SimulatedPlacements revertPublishedEffects(SimulatedPlacements state)
        {
            while (hasPrevious())
                state = steps.get(--idx).revert.apply(state);

            return state.withoutStashed(this);
        }
    }

    public static SimulatedPlacements joinFully(SimulatedPlacements baseState, Node node)
    {
        Transformations transformations = join(baseState, node);
        baseState = baseState.withStashed(transformations);

        while (transformations.hasNext())
            baseState = transformations.advance(baseState);

        return baseState;
    }

    /**
     * Diff-based bootstrap (very close implementation-wise to what production code does)
     */
    public static Transformations join(SimulatedPlacements baseState, Node bootstrappingNode)
    {
        long token = bootstrappingNode.token();
        List<Node> splitNodes = split(baseState.nodes, token);
        Map<Range, List<Node>> maximalStateWithPlacement = baseState.rf.replicate(move(splitNodes, token, bootstrappingNode)).placementsForRange;

        NavigableMap<Range, List<Node>> splitReadPlacements = baseState.rf.replicate(splitNodes).placementsForRange;
        NavigableMap<Range, List<Node>> splitWritePlacements = baseState.rf.replicate(splitNodes).placementsForRange;

        Map<Range, Diff<Node>> allWriteCommands = diff(splitWritePlacements, maximalStateWithPlacement);
        Map<Range, Diff<Node>> step1WriteCommands = map(allWriteCommands, Diff::onlyAdditions);
        Map<Range, Diff<Node>> step3WriteCommands = map(allWriteCommands, Diff::onlyRemovals);
        Map<Range, Diff<Node>> readCommands = diff(splitReadPlacements, maximalStateWithPlacement);

        Transformations steps = new Transformations();

        steps.add(new Transformation(
            (model) -> { // apply
                // add the new node to the system and split ranges according to its token, while retaining current
                // placement. This step will always be executed immediately, whereas subsequent steps may be deferred
                debug.log("Splitting ranges to prepare for join of " + bootstrappingNode + "\n");
                return model.withReadPlacements(splitReplicated(baseState.readPlacements, token))
                            .withWritePlacements(splitReplicated(baseState.writePlacements, token));
            },
            (model) -> { // revert
                // final stage of reverting a join is to undo the range splits performed by preparing the operation
                debug.log("Reverting range splits from prepare-join of " + bootstrappingNode + "\n");
                return model.withWritePlacements(mergeReplicated(model.writePlacements, token))
                            .withReadPlacements(mergeReplicated(model.readPlacements, token));
            })
        );

        // Step 1: add new node as a write replica to all ranges it is gaining
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing start-join of " + bootstrappingNode + "\n");
                debug.log(String.format("Commands for step 1 of bootstrap of %s.\n" +
                          "\twriteModifications=\n%s",
                          bootstrappingNode, diffsToString(step1WriteCommands)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, step1WriteCommands));
            },
            (model) -> { // revert
                debug.log("Reverting start-join of " + bootstrappingNode + "\n");
                Map<Range, Diff<Node>> inverted = map(step1WriteCommands, Diff::invert);
                debug.log("Commands for reverting step 1 of bootstrap of %s.\n" +
                          "\twriteModifications=\n%s",
                          bootstrappingNode, diffsToString(inverted));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            })
        );

        // Step 2: add new node as a read replica to the ranges it is gaining; remove old node from reads at the same time
        steps.add(new Transformation(
            (model) -> {  // apply
                debug.log("Executing mid-join of " + bootstrappingNode + "\n");
                debug.log(String.format("Commands for step 2 of bootstrap of %s.\n" +
                                     "\treadCommands=\n%s",
                                     bootstrappingNode, diffsToString(readCommands)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, readCommands));
            },
            (model) -> {  // revert
                debug.log("Reverting mid-join of " + bootstrappingNode + "\n");
                Map<Range, Diff<Node>> inverted = map(readCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 2 of bootstrap of %s.\n" +
                                        "\treadCommands=\n%s",
                                        bootstrappingNode, diffsToString(inverted)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, inverted));
            })
        );


        // Step 3: finally remove the old node from writes
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing finish-join  of " + bootstrappingNode + "\n");
                debug.log(String.format("Commands for step 3 of bootstrap of %s.\n" +
                                     "\twriteModifications=\n%s",
                                     bootstrappingNode,
                                     diffsToString(step3WriteCommands)));
                List<Node> newNodes = new ArrayList<>(model.nodes);
                newNodes.add(bootstrappingNode);
                newNodes.sort(Node::compareTo);
                return model.withNodes(newNodes)
                            .withWritePlacements(PlacementSimulator.apply(model.writePlacements, step3WriteCommands));
            },
            (model) -> { //revert
                throw new IllegalStateException("Can't revert finish-join of " + bootstrappingNode + ", operation is already complete\n");
            })
        );

        debug.log("Planned bootstrap of " + bootstrappingNode + "\n");
        return steps;
    }

    public static Transformations move(SimulatedPlacements baseState, Node movingNode, long newToken)
    {
        List<Node> origNodes = new ArrayList<>(baseState.nodes);
        List<Node> finalNodes = new ArrayList<>();
        for (int i = 0; i < origNodes.size(); i++)
        {
            if (origNodes.get(i).nodeIdx == movingNode.nodeIdx)
                continue;
            finalNodes.add(origNodes.get(i));
        }
        finalNodes.add(movingNode.overrideToken(newToken));
        finalNodes.sort(Node::compareTo);

        Map<Range, List<Node>> start = splitReplicated(baseState.rf.replicate(origNodes).placementsForRange, newToken);
        Map<Range, List<Node>> end = splitReplicated(baseState.rf.replicate(finalNodes).placementsForRange, movingNode.token());

        Map<Range, Diff<Node>> fromStartToEnd = diff(start, end);

        Transformations steps = new Transformations();

        // Step 1: Prepare Move,
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Splitting ranges to prepare for move of %s to %d\n", movingNode, newToken));
                return model.withReadPlacements(splitReplicated(model.readPlacements, newToken))
                            .withWritePlacements(splitReplicated(model.writePlacements, newToken));
            },
            (model) -> { // revert
                debug.log(String.format("Reverting range splits from prepare move of %s to %d\n", movingNode, newToken));
                return model.withWritePlacements(mergeReplicated(model.writePlacements, newToken))
                            .withReadPlacements(mergeReplicated(model.readPlacements, newToken));
            })
        );

        // Step 2: Start Move, add all potential owners to write quorums
        steps.add(new Transformation(
            (model) -> { // apply
                Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyAdditions);
                debug.log("Executing start-move of " + movingNode + "\n");
                debug.log(String.format("Commands for step 1 of move of %s to %d.\n" +
                                        "\twriteModifications=\n%s",
                                        movingNode, newToken, diffsToString(diff)));

                NavigableMap<Range, List<Node>> placements = model.writePlacements;
                placements = PlacementSimulator.apply(placements, diff);
                return model.withWritePlacements(placements);
            },
            (model) -> { // revert
                debug.log("Reverting start-move of " + movingNode + "\n");
                Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyAdditions);
                Map<Range, Diff<Node>> inverted = map(diff, Diff::invert);
                debug.log(String.format("Commands for reverting step 1 of move of %s to %d.\n" +
                                        "\twriteModifications=\n%s",
                                        movingNode, newToken, diffsToString(inverted)));

                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            }
        ));
        // Step 3: Mid Move, remove all nodes that are losing ranges from read quorums, add all nodes gaining ranges to read quorums
        steps.add(new Transformation(
            (model) -> {
                debug.log("Executing mid-move of " + movingNode + "\n");
                debug.log(String.format("Commands for step 2 of move of %s to %d.\n" +
                                        "\treadModifications=\n%s",
                                        movingNode, newToken, diffsToString(fromStartToEnd)));

                NavigableMap<Range, List<Node>> placements = model.readPlacements;
                placements = PlacementSimulator.apply(placements, fromStartToEnd);
                return model.withReadPlacements(placements);
            },
            (model) -> {
                NavigableMap<Range, List<Node>> placements = PlacementSimulator.apply(model.readPlacements, map(fromStartToEnd, Diff::invert));
                return model.withReadPlacements(placements);
            })
        );

        // Step 4: Finish Move, remove all nodes that are losing ranges from write quorums
        steps.add(new Transformation(
            (model) -> {
                Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyRemovals);

                debug.log("Executing finish-move of " + movingNode + "\n");
                debug.log(String.format("Commands for step 2 of move of %s to %d.\n" +
                                        "\twriteModifications=\n%s",
                                        movingNode, newToken, diffsToString(diff)));

                List<Node> currentNodes = new ArrayList<>(model.nodes);
                List<Node> newNodes = new ArrayList<>();
                for (int i = 0; i < currentNodes.size(); i++)
                {
                    if (currentNodes.get(i).idx() == movingNode.idx())
                        continue;
                    newNodes.add(currentNodes.get(i));
                }
                newNodes.add(movingNode.overrideToken(newToken));
                newNodes.sort(Node::compareTo);

                Map<Range, List<Node>> writePlacements = model.writePlacements;
                writePlacements = PlacementSimulator.apply(writePlacements, diff);

                return model.withWritePlacements(mergeReplicated(writePlacements, movingNode.token()))
                            .withReadPlacements(mergeReplicated(model.readPlacements, movingNode.token()))
                            .withNodes(newNodes);
            },
            (model) -> {
                throw new IllegalStateException(String.format("Can't revert finish-move of %d, operation is already complete", newToken));
            })
        );

        return steps;
    }

    public static Transformations leave(SimulatedPlacements baseState, Node toRemove)
    {
        // calculate current placements - this is start state
        Map<Range, List<Node>> start = baseState.rf.replicate(baseState.nodes).placementsForRange;

        List<Node> afterLeaveNodes = new ArrayList<>(baseState.nodes);
        afterLeaveNodes.remove(toRemove);
        // calculate placements based on existing ranges but final set of nodes - this is end state
        Map<Range, List<Node>> end = baseState.rf.replicate(toRanges(baseState.nodes), afterLeaveNodes).placementsForRange;
        // maximal state is union of start & end

        Map<Range, Diff<Node>> allWriteCommands = diff(start, end);
        Map<Range, Diff<Node>> step1WriteCommands = map(allWriteCommands, Diff::onlyAdditions);
        Map<Range, Diff<Node>> step3WriteCommands = map(allWriteCommands, Diff::onlyRemovals);
        Map<Range, Diff<Node>> readCommands = diff(start, end);
        Transformations steps = new Transformations();
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing start-leave of " + toRemove + "\n");
                debug.log(String.format("Commands for step 1 of decommission of %s.\n" +
                                     "\twriteModifications=\n%s",
                                     toRemove, diffsToString(step1WriteCommands)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, step1WriteCommands));
            },
            (model) -> { // revert
                debug.log("Reverting start-leave of " + toRemove + "\n");
                Map<Range, Diff<Node>> inverted = map(step1WriteCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 1 of decommission of %s.\n" +
                                        "\twriteModifications=\n%s",
                                        toRemove, diffsToString(inverted)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing mid-leave of " + toRemove + "\n");
                debug.log(String.format("Commands for step 2 of decommission of %s.\n" +
                                     "\treadModifications=\n%s",
                                     toRemove,
                                     diffsToString(readCommands)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, readCommands));
            },
            (model) -> { // revert
                debug.log("Reverting mid-leave of " + toRemove + "\n");
                Map<Range, Diff<Node>> inverted = map(readCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 2 of decommission of %s.\n" +
                                        "\treadModifications=\n%s",
                                        toRemove,
                                        diffsToString(inverted)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing finish-leave decommission of " + toRemove + "\n");
                debug.log(String.format("Commands for step 3 of decommission of %s.\n" +
                                     "\twriteModifications=\n%s",
                                     toRemove,
                                     diffsToString(step3WriteCommands)));
                List<Node> newNodes = new ArrayList<>(model.nodes);
                newNodes.remove(toRemove);
                newNodes.sort(Node::compareTo);
                Map<Range, List<Node>> writes = PlacementSimulator.apply(model.writePlacements, step3WriteCommands);
                return model.withReadPlacements(mergeReplicated(model.readPlacements, toRemove.token()))
                            .withWritePlacements(mergeReplicated(writes, toRemove.token()))
                            .withNodes(newNodes);
            },
            (model) -> { // revert
                throw new IllegalStateException("Can't revert finish-leave of " + toRemove + ", operation is already complete\n");
            }));

        debug.log("Planned decommission of " + toRemove + "\n");
        return steps;
    }

    public static Transformations replace(SimulatedPlacements baseState, Node toReplace, Node replacement)
    {
        Map<Range, List<Node>> start = baseState.rf.replicate(baseState.nodes).placementsForRange;
        Map<Range, Diff<Node>> allCommands = new TreeMap<>();
        start.forEach((range, nodes) -> {
            if (nodes.contains(toReplace))
            {
                allCommands.put(range, new Diff<>(Collections.singletonList(replacement),
                                                  Collections.singletonList(toReplace)));
            }
        });
        Map<Range, Diff<Node>> step1WriteCommands = map(allCommands, Diff::onlyAdditions);
        Map<Range, Diff<Node>> step3WriteCommands = map(allCommands, Diff::onlyRemovals);
        Map<Range, Diff<Node>> readCommands = allCommands;
        Transformations steps = new Transformations();
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Executing start-replace of %s for  %s\n", replacement, toReplace));
                debug.log(String.format("Commands for step 1 of bootstrap of %s for replacement of %s.\n" +
                                        "\twriteModifications=\n%s",
                                        replacement, toReplace, diffsToString(step1WriteCommands)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, step1WriteCommands));
            },
            (model) -> { // revert
                debug.log(String.format("Reverting start-replace of %s for  %s\n", replacement, toReplace));
                Map<Range, Diff<Node>> inverted = map(step1WriteCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 1 of bootstrap of %s for replacement of %s.\n" +
                                        "\twriteModifications=\n%s",
                                        replacement, toReplace, diffsToString(inverted)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Executing mid-replace of %s for %s\n", replacement, toReplace));
                debug.log(String.format("Commands for step 2 of bootstrap of %s for replacement of %s.\n" +
                                        "\treadModifications=\n%s",
                                        replacement, toReplace,
                                        diffsToString(readCommands)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, readCommands));
            },
            (model) -> { // revert
                debug.log(String.format("Reverting mid-replace of %s for %s\n", replacement, toReplace));
                Map<Range, Diff<Node>> inverted = map(readCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 2 of bootstrap of %s for replacement of %s.\n" +
                                        "\treadModifications=\n%s",
                                        replacement, toReplace,
                                        diffsToString(inverted)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Executing finish-replace of %s for %s\n", replacement, toReplace));
                debug.log(String.format("Commands for step 3 of bootstrap of %sfor replacement of %s.\n" +
                                     "\twriteModifications=\n%s",
                                     replacement, toReplace,
                                     diffsToString(step3WriteCommands)));
                List<Node> newNodes = new ArrayList<>(model.nodes);
                newNodes.remove(toReplace);
                newNodes.add(replacement);
                newNodes.sort(Node::compareTo);
                return model.withNodes(newNodes)
                            .withWritePlacements(PlacementSimulator.apply(model.writePlacements, step3WriteCommands));
            },
            (model) -> { // revert
                throw new IllegalStateException(String.format("Can't revert finish-replace of %s for %s, operation is already complete\n", replacement, toReplace));
            })
        );

        debug.log(String.format("Planned bootstrap of %s for replacement of %s\n", replacement, toReplace));
        return steps;
    }

    public static void assertPlacements(SimulatedPlacements placements, Map<Range, List<Node>> r, Map<Range, List<Node>> w)
    {
        assertRanges(r, placements.readPlacements);
        assertRanges(w, placements.writePlacements);
    }

    public static void assertRanges(Map<Range, List<Node>> expected, Map<Range, List<Node>> actual)
    {
        Assert.assertEquals(expected.keySet(), actual.keySet());
        expected.forEach((k, v) -> {
            // When comparing replica sets, we only care about the endpoint (i.e. the node.id). For the purpose
            // of simulation, during split operations we duplicate the node holding the range being split as if giving
            // it two tokens, the original one and the split point. e.g. With N1@100, N2@200 then splitting at 150,
            // we will end up with (100, 150] -> N2@150 and (150, 200] -> N2@200. As this is purely an artefact of the
            // bootstrap_diffBased implementation and the real code doesn't do this, only the endpoint matters for
            // correctness, so we limit this comparison to endpoints only.
            Assert.assertEquals(String.format("For key: %s\n", k),
                                expected.get(k).stream().map(n -> n.nodeIdx).sorted().collect(Collectors.toList()),
                                actual.get(k).stream().map(n -> n.nodeIdx).sorted().collect(Collectors.toList()));
        });
    }

    public static <T> boolean containsAll(Set<T> a, Set<T> b)
    {
        if (a.isEmpty() && !b.isEmpty())
            return false; // empty set does not contain all entries of a non-empty one
        for (T v : b)
            if (!a.contains(v))
                return false;

        return true;
    }

    /**
     * Applies a given diff to the placement map
     */
    public static NavigableMap<Range, List<Node>> apply(Map<Range, List<Node>> orig, Map<Range, Diff<Node>> diff)
    {
        assert containsAll(orig.keySet(), diff.keySet()) : String.format("Can't apply diff to a map with different sets of keys:" +
                                                                         "\nOrig ks: %s" +
                                                                         "\nDiff ks: %s" +
                                                                         "\nDiff: %s",
                                                                         orig.keySet(), diff.keySet(), diff);
        NavigableMap<Range, List<Node>> res = new TreeMap<>();
        for (Map.Entry<Range, List<Node>> entry : orig.entrySet())
        {
            Range range = entry.getKey();
            if (diff.containsKey(range))
                res.put(range, apply(entry.getValue(), diff.get(range)));
            else
                res.put(range, entry.getValue());
        }
        return Collections.unmodifiableNavigableMap(res);
    }

    /**
     * Apply diff to a list of nodes
     */
    public static List<Node> apply(List<Node> nodes, Diff<Node> diff)
    {
        Set<Node> tmp = new HashSet<>(nodes);
        tmp.addAll(diff.additions);
        for (Node removal : diff.removals)
            tmp.remove(removal);
        List<Node> newNodes = new ArrayList<>(tmp);
        newNodes.sort(Node::compareTo);
        return Collections.unmodifiableList(newNodes);
    }

    /**
     * Diff two placement maps
     */
    public static Map<Range, Diff<Node>> diff(Map<Range, List<Node>> l, Map<Range, List<Node>> r)
    {
        assert l.keySet().equals(r.keySet()) : String.format("Can't diff events from different bases %s %s", l.keySet(), r.keySet());
        Map<Range, Diff<Node>> diff = new TreeMap<>();
        for (Map.Entry<Range, List<Node>> entry : l.entrySet())
        {
            Range range = entry.getKey();
            Diff<Node> d = diff(entry.getValue(), r.get(range));
            if (!d.removals.isEmpty() || !d.additions.isEmpty())
                diff.put(range, d);
        }
        return Collections.unmodifiableMap(diff);
    }

    public static <T> Map<Range, T> map(Map<Range, T> diff, Function<T, T> fn)
    {
        Map<Range, T> newDiff = new TreeMap<>();
        for (Map.Entry<Range, T> entry : diff.entrySet())
        {
            T newV = fn.apply(entry.getValue());
            if (newV != null)
                newDiff.put(entry.getKey(), newV);
        }
        return Collections.unmodifiableMap(newDiff);
    }

    public static <T> List<T> map(List<T> coll, Function<T, T> map)
    {
        List<T> newColl = new ArrayList<>(coll);
        for (T v : coll)
            newColl.add(map.apply(v));
        return newColl;
    }

    /**
     * Produce a diff (i.e. set of additions/subtractions that should be applied to the list of nodes in order to produce
     * r from l)
     */
    public static Diff<Node> diff(List<Node> l, List<Node> r)
    {
        // additions things present in r but not in l
        List<Node> additions = new ArrayList<>();
        // removals are things present in l but not r
        List<Node> removals = new ArrayList<>();

        for (Node i : r)
        {
            boolean isPresentInL = false;
            for (Node j : l)
            {
                if (i.equals(j))
                {
                    isPresentInL = true;
                    break;
                }
            }

            if (!isPresentInL)
                additions.add(i);
        }

        for (Node i : l)
        {
            boolean isPresentInR = false;
            for (Node j : r)
            {
                if (i.equals(j))
                {
                    isPresentInR = true;
                    break;
                }
            }

            if (!isPresentInR)
                removals.add(i);
        }
        return new Diff<>(additions, removals);
    }

    public static Map<Range, List<Node>> superset(Map<Range, List<Node>> l, Map<Range, List<Node>> r)
    {
        assert l.keySet().equals(r.keySet()) : String.format("%s != %s", l.keySet(), r.keySet());

        Map<Range, List<Node>> newState = new TreeMap<>();
        for (Map.Entry<Range, List<Node>> entry : l.entrySet())
        {
            Range range = entry.getKey();
            Set<Node> nodes = new HashSet<>();
            nodes.addAll(entry.getValue());
            nodes.addAll(r.get(range));
            newState.put(range, new ArrayList<>(nodes));
        }

        return newState;
    }

    public static NavigableMap<Range, List<Node>> mergeReplicated(Map<Range, List<Node>> orig, long removingToken)
    {
        NavigableMap<Range, List<Node>> newState = new TreeMap<>();
        Iterator<Map.Entry<Range, List<Node>>> iter = orig.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<Range, List<Node>> current = iter.next();
            if (current.getKey().end == removingToken)
            {
                assert iter.hasNext() : "Cannot merge range, no more ranges in list";
                Map.Entry<Range, List<Node>> next = iter.next();
                assert current.getValue().containsAll(next.getValue()) && current.getValue().size() == next.getValue().size()
                : "Cannot merge ranges with different replica groups";
                Range merged = new Range(current.getKey().start, next.getKey().end);
                newState.put(merged, current.getValue());
            }
            else
            {
                newState.put(current.getKey(), current.getValue());
            }
        }

        return newState;
    }

    public static NavigableMap<Range, List<Node>> splitReplicated(Map<Range, List<Node>> orig, long splitAt)
    {
        NavigableMap<Range, List<Node>> newState = new TreeMap<>();
        for (Map.Entry<Range, List<Node>> entry : orig.entrySet())
        {
            Range range = entry.getKey();
            if (range.contains(splitAt))
            {
                newState.put(new Range(range.start, splitAt), entry.getValue());
                newState.put(new Range(splitAt, range.end), entry.getValue());
            }
            else
            {
                newState.put(range, entry.getValue());
            }
        }
        return newState;
    }

    /**
     * "Split" the list of nodes at splitAt, without changing ownership
     */
    public static List<Node> split(List<Node> nodes, long splitAt)
    {
        List<Node> newNodes = new ArrayList<>();
        boolean inserted = false;
        Node previous = null;
        for (int i = nodes.size() - 1; i >= 0; i--)
        {
            Node node = nodes.get(i);
            if (!inserted && splitAt > node.token())
            {
                // We're trying to split rightmost range
                if (previous == null)
                {
                    newNodes.add(nodes.get(0).overrideToken(splitAt));
                }
                else
                {
                    newNodes.add(previous.overrideToken(splitAt));
                }
                inserted = true;
            }

            newNodes.add(node);
            previous = node;
        }

        // Leftmost is split
        if (!inserted)
            newNodes.add(previous.overrideToken(splitAt));

        newNodes.sort(Node::compareTo);
        return Collections.unmodifiableList(newNodes);
    }

    /**
     * Change the ownership of the freshly split token
     */
    public static List<Node> move(List<Node> nodes, long tokenToMove, Node newOwner)
    {
        List<Node> newNodes = new ArrayList<>();
        for (Node node : nodes)
        {
            if (node.token() == tokenToMove)
                newNodes.add(newOwner.overrideToken(tokenToMove));
            else
                newNodes.add(node);
        }
        newNodes.sort(Node::compareTo);
        return Collections.unmodifiableList(newNodes);
    }

    public static List<Node> filter(List<Node> nodes, Predicate<Node> pred)
    {
        List<Node> newNodes = new ArrayList<>();
        for (Node node : nodes)
        {
            if (pred.test(node))
                newNodes.add(node);
        }
        newNodes.sort(Node::compareTo);
        return Collections.unmodifiableList(newNodes);
    }

    /**
     * Replicate ranges to rf nodes.
     */
    private static ReplicatedRanges combine(NavigableMap<Range, Map<String, List<Node>>> orig)
    {

        Range[] ranges = new Range[orig.size()];
        int idx = 0;
        NavigableMap<Range, List<Node>> flattened = new TreeMap<>();
        for (Map.Entry<Range, Map<String, List<Node>>> e : orig.entrySet())
        {
            List<Node> placementsForRange = new ArrayList<>();
            for (List<Node> v : e.getValue().values())
                placementsForRange.addAll(v);
            ranges[idx++] = e.getKey();
            flattened.put(e.getKey(), placementsForRange);
        }
        return new ReplicatedRanges(ranges, flattened);
    }

    public static class ReplicatedRanges
    {
        private final Range[] ranges;
        private final NavigableMap<Range, List<Node>> placementsForRange;

        public ReplicatedRanges(Range[] ranges, NavigableMap<Range, List<Node>> placementsForRange)
        {
            this.ranges = ranges;
            this.placementsForRange = placementsForRange;
        }

        public List<Node> replicasFor(long token)
        {
            int idx = indexedBinarySearch(ranges, range -> {
                // exclusive start, so token at the start belongs to a lower range
                if (token <= range.start)
                    return 1;
                // ie token > start && token <= end
                if (token <= range.end ||range.end == Long.MIN_VALUE)
                    return 0;

                return -1;
            });
            assert idx >= 0 : String.format("Somehow ranges %s do not contain token %d", Arrays.toString(ranges), token);
            return placementsForRange.get(ranges[idx]);
        }

        public NavigableMap<Range, List<Node>> asMap()
        {
            return placementsForRange;
        }

        private static <T> int indexedBinarySearch(T[] arr, CompareTo<T> comparator)
        {
            int low = 0;
            int high = arr.length - 1;

            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                T midEl = arr[mid];
                int cmp = comparator.compareTo(midEl);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid;
            }
            return -(low + 1); // key not found
        }
    }

    public interface CompareTo<V>
    {
        int compareTo(V v);
    }

    private static <K extends Comparable<K>, T1, T2> Map<K, T2> mapValues(Map<K, T1> allDCs, Function<T1, T2> map)
    {
        NavigableMap<K, T2> res = new TreeMap<>();
        for (Map.Entry<K, T1> e : allDCs.entrySet())
        {
            res.put(e.getKey(), map.apply(e.getValue()));
        }
        return res;
    }

    public static Map<String, List<Node>> nodesByDC(List<Node> nodes)
    {
        Map<String, List<Node>> nodesByDC = new HashMap<>();
        for (Node node : nodes)
            nodesByDC.computeIfAbsent(node.dc(), (k) -> new ArrayList<>()).add(node);

        return nodesByDC;
    }

    public static Map<String, Set<String>> racksByDC(List<Node> nodes)
    {
        Map<String, Set<String>> racksByDC = new HashMap<>();
        for (Node node : nodes)
            racksByDC.computeIfAbsent(node.dc(), (k) -> new HashSet<>()).add(node.rack());

        return racksByDC;
    }

    private static final class DatacenterNodes
    {
        private final List<Node> nodes = new ArrayList<>();
        private final Set<Location> racks = new HashSet<>();

        /** Number of replicas left to fill from this DC. */
        int rfLeft;
        int acceptableRackRepeats;

        public DatacenterNodes copy()
        {
            return new DatacenterNodes(rfLeft, acceptableRackRepeats);
        }

        DatacenterNodes(int rf,
                        int rackCount,
                        int nodeCount)
        {
            this.rfLeft = Math.min(rf, nodeCount);
            acceptableRackRepeats = rf - rackCount;
        }

        // for copying
        DatacenterNodes(int rfLeft, int acceptableRackRepeats)
        {
            this.rfLeft = rfLeft;
            this.acceptableRackRepeats = acceptableRackRepeats;
        }

        boolean addAndCheckIfDone(Node node, Location location)
        {
            if (done())
                return false;

            if (nodes.contains(node))
                // Cannot repeat a node.
                return false;

            if (racks.add(location))
            {
                // New rack.
                --rfLeft;
                nodes.add(node);
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            nodes.add(node);

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --acceptableRackRepeats;
            --rfLeft;
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }
    }


    public static void addIfUnique(List<Node> nodes, Set<Integer> names, Node node)
    {
        if (names.contains(node.idx()))
            return;
        nodes.add(node);
        names.add(node.idx());
    }

    /**
     * Finds a primary replica
     */
    public static int primaryReplica(List<Node> nodes, Range range)
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            if (range.end != Long.MIN_VALUE && nodes.get(i).token() >= range.end)
                return i;
        }
        return -1;
    }

    /**
     * Generates token ranges from the list of nodes
     */
    public static Range[] toRanges(List<Node> nodes)
    {
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.token());
        tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        Range[] ranges = new Range[nodes.size() + 1];
        long prev = tokens.get(0);
        int cnt = 0;
        for (int i = 1; i < tokens.size(); i++)
        {
            long current = tokens.get(i);
            ranges[cnt++] = new Range(prev, current);
            prev = current;
        }
        ranges[ranges.length - 1] = new Range(prev, Long.MIN_VALUE);
        return ranges;

    }

    public static class Diff<T> {
        public final List<T> additions;
        public final List<T> removals;

        public Diff(List<T> additions, List<T> removals)
        {
            this.additions = additions;
            this.removals = removals;
        }

        public String toString()
        {
            return "Diff{" +
                   "additions=" + additions +
                   ", removals=" + removals +
                   '}';
        }

        public Diff<T> onlyAdditions()
        {
            if (additions.isEmpty()) return null;
            return new Diff<>(additions, Collections.emptyList());
        }

        public Diff<T> onlyRemovals()
        {
            if (removals.isEmpty()) return null;
            return new Diff<>(Collections.emptyList(), removals);
        }

        public Diff<T> invert()
        {
            // invert removals & additions
            return new Diff<>(removals, additions);
        }
    }


    /**
     * A Range is responsible for the tokens between (start, end].
     */
    public static class Range implements Comparable<Range>
    {
        public final long start;
        public final long end;

        public Range(long start, long end)
        {
            assert end > start || end == Long.MIN_VALUE : String.format("Start (%d) should be smaller than end (%d)", start, end);
            this.start = start;
            this.end = end;
        }

        public boolean contains(long min, long max)
        {
            assert max > min;
            return min > start && (max <= end || end == Long.MIN_VALUE);
        }

        public boolean contains(long token)
        {
            return token > start && (token <= end || end == Long.MIN_VALUE);
        }

        public int compareTo(Range o)
        {
            int res = Long.compare(start, o.start);
            if (res == 0)
                return Long.compare(end, o.end);
            return res;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return start == range.start && end == range.end;
        }

        public int hashCode()
        {
            return Objects.hash(start, end);
        }

        public String toString()
        {
            return "(" +
                   "" + (start == Long.MIN_VALUE ? "MIN" : start) +
                   ", " + (end == Long.MIN_VALUE ? "MIN" : end) +
                   ']';
        }
    }

    public interface Lookup
    {
        String id(int nodeIdx);
        String dc(int dcIdx);
        String rack(int rackIdx);
        NodeId nodeId(int nodeIdx);
        long token(int tokenIdx);
        Lookup forceToken(int tokenIdx, long token);
        InetAddressAndPort addr(int idx);
        void reset();
    }

    public static class DefaultLookup implements Lookup
    {
        protected final Map<Integer, Long> overrides = new HashMap<>(2);

        public String id(int nodeIdx)
        {
            return String.format("127.0.%d.%d", nodeIdx / 256, nodeIdx % 256);
        }

        public NodeId nodeId(int nodeIdx)
        {
            return ClusterMetadata.current().directory.peerId(addr(nodeIdx));
        }

        public long token(int tokenIdx)
        {
            Long override = overrides.get(tokenIdx);
            if (override != null)
                return override;
            return PCGFastPure.next(tokenIdx, 1L);
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup newLookup = new DefaultLookup();
            newLookup.overrides.putAll(overrides);
            newLookup.overrides.put(tokenIdx, token);
            return newLookup;
        }

        public InetAddressAndPort addr(int idx)
        {
            try
            {
                return InetAddressAndPort.getByName(id(idx));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void reset()
        {
            overrides.clear();
        }

        public String dc(int dcIdx)
        {
            return String.format("datacenter%d", dcIdx);
        }

        public String rack(int rackIdx)
        {
            return String.format("rack%d", rackIdx);
        }
    }

    public static class HumanReadableTokensLookup extends DefaultLookup {
        @Override
        public long token(int tokenIdx)
        {
            Long override = overrides.get(tokenIdx);
            if (override != null)
                return override;
            return tokenIdx * 100L;
        }

        public Lookup forceToken(int tokenIdx, long token)
        {
            DefaultLookup lookup = new HumanReadableTokensLookup();
            lookup.overrides.putAll(overrides);
            lookup.overrides.put(tokenIdx, token);
            return lookup;
        }
    }
    public static NodeFactory nodeFactory()
    {
        return new NodeFactory(new DefaultLookup());
    }

    public static NodeFactory nodeFactoryHumanReadable()
    {
        return new NodeFactory(new HumanReadableTokensLookup());
    }

    public static class NodeFactory implements TokenSupplier
    {
        private final Lookup lookup;

        public NodeFactory(Lookup lookup)
        {
            this.lookup = lookup;
        }

        public Node make(int idx, int dc, int rack)
        {
            return new Node(idx, idx, dc, rack, lookup);
        }

        public Lookup lookup()
        {
            return lookup;
        }

        public Collection<String> tokens(int i)
        {
            return Collections.singletonList(Long.toString(lookup.token(i)));
        }
    }

    public static class Node implements Comparable<Node>
    {
        private final int tokenIdx;
        private final int nodeIdx;
        private final int dcIdx;
        private final int rackIdx;
        private final Lookup lookup;

        private Node(int tokenIdx, int idx, int dcIdx, int rackIdx, Lookup lookup)
        {
            this.tokenIdx = tokenIdx;
            this.nodeIdx = idx;
            this.dcIdx = dcIdx;
            this.rackIdx = rackIdx;
            this.lookup = lookup;
        }

        public InetAddressAndPort addr()
        {
            return lookup.addr(nodeIdx);
        }

        public NodeId nodeId()
        {
            return lookup.nodeId(nodeIdx);
        }

        public String id()
        {
            return lookup.id(nodeIdx);
        }

        public int idx()
        {
            return nodeIdx;
        }

        public int dcIdx()
        {
            return dcIdx;
        }

        public int rackIdx()
        {
            return rackIdx;
        }

        public String dc()
        {
            return lookup.dc(dcIdx);
        }

        public String rack()
        {
            return lookup.rack(rackIdx);
        }

        public long token()
        {
            return lookup.token(tokenIdx);
        }

        public int tokenIdx()
        {
            return tokenIdx;
        }

        public Murmur3Partitioner.LongToken longToken()
        {
            return new Murmur3Partitioner.LongToken(token());
        }

        public Node withNewToken()
        {
            return new Node(tokenIdx + 100_000, nodeIdx, dcIdx, rackIdx, lookup);
        }

        public Node withToken(int tokenIdx)
        {
            return new Node(tokenIdx, nodeIdx, dcIdx, rackIdx, lookup);
        }

        public Node overrideToken(long override)
        {
            return new Node(tokenIdx, nodeIdx, dcIdx, rackIdx, lookup.forceToken(tokenIdx, override));
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || !Node.class.isAssignableFrom(o.getClass())) return false;
            Node node = (Node) o;
            return Objects.equals(nodeIdx, node.nodeIdx);
        }

        public int hashCode()
        {
            return Objects.hash(nodeIdx);
        }

        public int compareTo(Node o)
        {
            return Long.compare(token(), o.token());
        }

        public String toString()
        {
            return String.format("%s@%d", id(), token());
        }
    }

    public static String diffsToString(Map<Range, Diff<Node>> placements)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Range, Diff<Node>> e : placements.entrySet())
        {
            builder.append("\t\t").append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        return builder.toString();
    }

    public static String placementsToString(Map<Range, List<Node>> placements)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Range, List<Node>> e : placements.entrySet())
        {
            builder.append("\t\t").append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        return builder.toString();
    }

    public static class DebugLog
    {
        private final BufferedWriter operationLog;
        public DebugLog()
        {
            File f = new File("simulated.log");
            try
            {
                operationLog = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
            }
            catch (FileNotFoundException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void log(long seq, Object t)
        {
            log("%d: %s\n", seq, t);
        }

        private void log(String format, Object... objects)
        {
            try
            {
                operationLog.write(String.format(format, objects));
                operationLog.flush();
            }
            catch (IOException e)
            {
                // ignore
            }
        }
    }

    public abstract static class ReplicationFactor
    {
        private final int nodesTotal;

        public ReplicationFactor(int total)
        {
            this.nodesTotal = total;
        }

        public int total()
        {
            return nodesTotal;
        }

        public abstract int dcs();

        public abstract KeyspaceParams asKeyspaceParams();

        public abstract Map<String, Integer> asMap();

        public ReplicatedRanges replicate(List<Node> nodes)
        {
            return replicate(toRanges(nodes), nodes);
        }
        public abstract ReplicatedRanges replicate(Range[] ranges, List<Node> nodes);
    }

    public static class NtsReplicationFactor extends ReplicationFactor
    {
        private final Lookup lookup = new DefaultLookup();
        public final int[] nodesPerDc;
        private KeyspaceParams keyspaceParams;
        private Map<String, Integer> map;

        public NtsReplicationFactor(int... nodesPerDc)
        {
            super(total(nodesPerDc));
            this.nodesPerDc = nodesPerDc;
        }

        public NtsReplicationFactor(int dcs, int nodesPerDc)
        {
            super(dcs * nodesPerDc);
            this.nodesPerDc = new int[dcs];
            Arrays.fill(this.nodesPerDc, nodesPerDc);
        }

        private static int total(int... num)
        {
            int tmp = 0;
            for (int i : num)
                tmp += i;
            return tmp;
        }

        public int dcs()
        {
            return nodesPerDc.length;
        }

        public KeyspaceParams asKeyspaceParams()
        {
            if (this.keyspaceParams == null)
                this.keyspaceParams = toKeyspaceParams(lookup);
            return this.keyspaceParams;
        }


        public Map<String, Integer> asMap()
        {
            if (this.map == null)
                this.map = toMap(lookup);
            return this.map;
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, asMap());
        }

        private static <T extends Comparable<T>> void assertStrictlySorted(Collection<T> coll)
        {
            if (coll.size() <= 1) return;

            Iterator<T> iter = coll.iterator();
            T prev = iter.next();
            while (iter.hasNext())
            {
                T next = iter.next();
                assert next.compareTo(prev) > 0 : String.format("Collection does not seem to be sorted. %s and %s are in wrong order", prev, next);
                prev = next;
            }
        }
        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, Map<String, Integer> rfs)
        {
            assertStrictlySorted(nodes);
            Map<String, DatacenterNodes> template = new HashMap<>();

            Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
            Map<String, Set<String>> racksByDC = racksByDC(nodes);

            for (Map.Entry<String, Integer> entry : rfs.entrySet())
            {
                String dc = entry.getKey();
                int rf = entry.getValue();
                List<Node> nodesInThisDC = nodesByDC.get(dc);
                Set<String> racksInThisDC = racksByDC.get(dc);
                int nodeCount = nodesInThisDC == null ? 0 : nodesInThisDC.size();
                int rackCount = racksInThisDC == null ? 0 : racksInThisDC.size();
                if (rf <= 0 || nodeCount == 0)
                    continue;

                template.put(dc, new DatacenterNodes(rf, rackCount, nodeCount));
            }

            NavigableMap<Range, Map<String, List<Node>>> replication = new TreeMap<>();

            for (Range range : ranges)
            {
                final int idx = primaryReplica(nodes, range);
                int cnt = 0;
                if (idx >= 0)
                {
                    int dcsToFill = template.size();

                    Map<String, DatacenterNodes> nodesInDCs = new HashMap<>();
                    for (Map.Entry<String, DatacenterNodes> e : template.entrySet())
                        nodesInDCs.put(e.getKey(), e.getValue().copy());

                    while (dcsToFill > 0 && cnt < nodes.size())
                    {
                        Node node = nodes.get((idx + cnt) % nodes.size());
                        DatacenterNodes dcNodes = nodesInDCs.get(node.dc());
                        if (dcNodes != null && dcNodes.addAndCheckIfDone(node, new Location(node.dc(), node.rack())))
                            dcsToFill--;

                        cnt++;
                    }

                    replication.put(range, mapValues(nodesInDCs, v -> v.nodes));
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by a decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return combine(replication);
        }

        private KeyspaceParams toKeyspaceParams(Lookup lookup)
        {
            Object[] args = new Object[nodesPerDc.length * 2];
            for (int i = 0; i < nodesPerDc.length; i++)
            {
                args[i * 2] = lookup.dc(i + 1);
                args[i * 2 + 1] = nodesPerDc[i];
            }
            return KeyspaceParams.nts(args);
        }

        private Map<String, Integer> toMap(Lookup lookup)
        {
            Map<String, Integer> map = new TreeMap<>();
            for (int i = 0; i < nodesPerDc.length; i++)
            {
                map.put(lookup.dc(i + 1), nodesPerDc[i]);
            }
            return map;
        }

        public String toString()
        {
            return "NtsReplicationFactor{" +
                   "map=" + asMap() +
                   '}';
        }
    }

    public static class SimpleReplicationFactor extends ReplicationFactor
    {
        private final Lookup lookup = new DefaultLookup();
        public SimpleReplicationFactor(int total)
        {
            super(total);
        }

        public int dcs()
        {
            return 1;
        }

        public KeyspaceParams asKeyspaceParams()
        {
            return KeyspaceParams.simple(total());
        }

        public Map<String, Integer> asMap()
        {
            return Collections.singletonMap(lookup.dc(1), total());
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, total());
        }

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, int rf)
        {
            NavigableMap<Range, List<Node>> replication = new TreeMap<>();
            for (Range range : ranges)
            {
                Set<Integer> names = new HashSet<>();
                List<Node> replicas = new ArrayList<>();
                int idx = primaryReplica(nodes, range);
                if (idx >= 0)
                {
                    for (int i = idx; i < nodes.size() && replicas.size() < rf; i++)
                        addIfUnique(replicas, names, nodes.get(i));

                    for (int i = 0; replicas.size() < rf && i < idx; i++)
                        addIfUnique(replicas, names, nodes.get(i));
                    if (range.start == Long.MIN_VALUE)
                        replication.put(ranges[ranges.length - 1], replicas);
                    replication.put(range, replicas);
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by a decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return new ReplicatedRanges(ranges, Collections.unmodifiableNavigableMap(replication));
        }
    }
}
