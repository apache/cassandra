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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.harry.sut.TokenPlacementModel.Replica;
import org.junit.Assert;

import static org.apache.cassandra.harry.sut.TokenPlacementModel.Node;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.Range;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.ReplicationFactor;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.toRanges;

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
        public final NavigableMap<Range, List<Replica>> readPlacements;
        public final NavigableMap<Range, List<Replica>> writePlacements;
        // Stashed states are steps required to finish the operation. For example, in case of
        // bootstrap, this could be adding replicas to write (and then read) sets after splitting ranges.
        public final List<Transformations> stashedStates;

        public SimulatedPlacements(ReplicationFactor rf,
                                   List<Node> nodes,
                                   NavigableMap<Range, List<Replica>> readPlacements,
                                   NavigableMap<Range, List<Replica>> writePlacements,
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

        public SimulatedPlacements withReadPlacements(NavigableMap<Range, List<Replica>> newReadPlacements)
        {
            return new SimulatedPlacements(rf, nodes, newReadPlacements, writePlacements, stashedStates);
        }

        public SimulatedPlacements withWritePlacements(NavigableMap<Range, List<Replica>> newWritePlacements)
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
            return writePlacementsFor(token).stream().map(Replica::node).anyMatch(predicate);
        }

        public boolean isReadReplicaFor(long token, Predicate<Node> predicate)
        {
            return readReplicasFor(token).stream().map(Replica::node).anyMatch(predicate);
        }

        public boolean isReadReplicaFor(long minToken, long maxToken, Predicate<Node> predicate)
        {
            return readReplicasFor(minToken, maxToken).stream().map(Replica::node).anyMatch(predicate);
        }

        public List<Replica> writePlacementsFor(long token)
        {
            for (Map.Entry<Range, List<Replica>> e : writePlacements.entrySet())
            {
                if (e.getKey().contains(token))
                    return e.getValue();
            }

            throw new AssertionError();
        }

        public List<Replica> readReplicasFor(long minToken, long maxToken)
        {
            for (Map.Entry<Range, List<Replica>> e : readPlacements.entrySet())
            {
                if (e.getKey().contains(minToken, maxToken))
                    return e.getValue();
            }

            throw new AssertionError();
        }


        public List<Replica> readReplicasFor(long token)
        {
            for (Map.Entry<Range, List<Replica>> e : readPlacements.entrySet())
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
        Map<Range, List<Replica>> maximalStateWithPlacement = baseState.rf.replicate(move(splitNodes, token, bootstrappingNode)).placementsForRange;

        NavigableMap<Range, List<Replica>> splitReadPlacements = baseState.rf.replicate(splitNodes).placementsForRange;
        NavigableMap<Range, List<Replica>> splitWritePlacements = baseState.rf.replicate(splitNodes).placementsForRange;

        Map<Range, Diff<Replica>> allWriteCommands = diff(splitWritePlacements, maximalStateWithPlacement);
        Map<Range, Diff<Replica>> step1WriteCommands = map(allWriteCommands, PlacementSimulator::additionsAndTransientToFull);
        Map<Range, Diff<Replica>> step3WriteCommands = map(allWriteCommands, PlacementSimulator::removalsAndFullToTransient);
        Map<Range, Diff<Replica>> readCommands = diff(splitReadPlacements, maximalStateWithPlacement);

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
                Map<Range, Diff<Replica>> inverted = map(step1WriteCommands, Diff::invert);
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
                Map<Range, Diff<Replica>> inverted = map(readCommands, Diff::invert);
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
            if (origNodes.get(i).idx() == movingNode.idx())
                continue;
            finalNodes.add(origNodes.get(i));
        }
        finalNodes.add(movingNode.overrideToken(newToken));
        finalNodes.sort(Node::compareTo);

        Map<Range, List<Replica>> start = splitReplicated(baseState.rf.replicate(origNodes).placementsForRange, newToken);
        Map<Range, List<Replica>> end = splitReplicated(baseState.rf.replicate(finalNodes).placementsForRange, movingNode.token());

        Map<Range, Diff<Replica>> fromStartToEnd = diff(start, end);
        Map<Range, Diff<Replica>> startMoveCommands = map(fromStartToEnd, PlacementSimulator::additionsAndTransientToFull);
        Map<Range, Diff<Replica>> finishMoveCommands = map(fromStartToEnd, PlacementSimulator::removalsAndFullToTransient);

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
                debug.log("Executing start-move of " + movingNode + "\n");
                debug.log(String.format("Commands for step 1 of move of %s to %d.\n" +
                                        "\twriteModifications=\n%s",
                                        movingNode, newToken, diffsToString(startMoveCommands)));

                NavigableMap<Range, List<Replica>> placements = model.writePlacements;
                placements = PlacementSimulator.apply(placements, startMoveCommands);
                return model.withWritePlacements(placements);
            },
            (model) -> { // revert
                debug.log("Reverting start-move of " + movingNode + "\n");
                Map<Range, Diff<Replica>> inverted = map(startMoveCommands, Diff::invert);
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

                NavigableMap<Range, List<Replica>> placements = model.readPlacements;
                placements = PlacementSimulator.apply(placements, fromStartToEnd);
                return model.withReadPlacements(placements);
            },
            (model) -> {
                NavigableMap<Range, List<Replica>> placements = PlacementSimulator.apply(model.readPlacements, map(fromStartToEnd, Diff::invert));
                return model.withReadPlacements(placements);
            })
        );

        // Step 4: Finish Move, remove all nodes that are losing ranges from write quorums
        steps.add(new Transformation(
            (model) -> {
                debug.log("Executing finish-move of " + movingNode + "\n");
                debug.log(String.format("Commands for step 2 of move of %s to %d.\n" +
                                        "\twriteModifications=\n%s",
                                        movingNode, newToken, diffsToString(finishMoveCommands)));

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

                Map<Range, List<Replica>> writePlacements = model.writePlacements;
                writePlacements = PlacementSimulator.apply(writePlacements, finishMoveCommands);

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
        Map<Range, List<Replica>> start = baseState.rf.replicate(baseState.nodes).placementsForRange;

        List<Node> afterLeaveNodes = new ArrayList<>(baseState.nodes);
        afterLeaveNodes.remove(toRemove);
        // calculate placements based on existing ranges but final set of nodes - this is end state
        Map<Range, List<Replica>> end = baseState.rf.replicate(toRanges(baseState.nodes), afterLeaveNodes).placementsForRange;

        // maximal state is union of start & end
        Map<Range, Diff<Replica>> allWriteCommands = diff(start, end);
        Map<Range, Diff<Replica>> step1WriteCommands = map(allWriteCommands, PlacementSimulator::additionsAndTransientToFull);
        Map<Range, Diff<Replica>> step3WriteCommands = map(allWriteCommands, PlacementSimulator::removalsAndFullToTransient);
        Map<Range, Diff<Replica>> readCommands = diff(start, end);

        // Assert that the proposed plan would not violate consistency if used with the current streaming
        // implementation (i.e. UnbootstrapStreams::movementMap).
        AtomicBoolean safeForStreaming = new AtomicBoolean(true);
        step1WriteCommands.forEach((range, diff) -> {
            for (Replica add : diff.additions)
            {
                if (add.isFull())  // for each new FULL replica
                {
                    diff.removals.stream()
                                 .filter(r -> r.node().equals(add.node()) && r.isTransient())  // if the same node is being removed as a TRANSIENT replica
                                 .findFirst()
                                 .ifPresent(r -> {
                                     if (!start.get(range).contains(new Replica(toRemove, true)))  // check the leaving node is a FULL replica for the range
                                     {
                                         debug.log(String.format("In prepare-leave of %s, node %s moving from transient to " +
                                                                 "full, but the leaving node is not a full replica for " +
                                                                 "the transitioning range %s.",
                                                                 toRemove, add, range));
                                         safeForStreaming.getAndSet(false);
                                     }
                                 });
                }
            }
        });
        assert safeForStreaming.get() : String.format("Removal of node %s causes some nodes to move from transient to " +
                                                      "full replicas for some range where the leaving node is not " +
                                                      "initially a full replica. This may violate consistency as the " +
                                                      "streaming implementation assumes that the leaving node is a " +
                                                      "valid source for streaming in this case. See ./simulated.log " +
                                                      "for details of the ranges and nodes in question", toRemove);

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
                Map<Range, Diff<Replica>> inverted = map(step1WriteCommands, Diff::invert);
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
                Map<Range, Diff<Replica>> inverted = map(readCommands, Diff::invert);
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
                Map<Range, List<Replica>> writes = PlacementSimulator.apply(model.writePlacements, step3WriteCommands);
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
        Map<Range, List<Replica>> start = baseState.rf.replicate(baseState.nodes).placementsForRange;
        Map<Range, Diff<Replica>> allCommands = new TreeMap<>();
        start.forEach((range, replicas) -> {
            replicas.forEach(r -> {
                if (r.node().equals(toReplace)) {
                    allCommands.put(range, new Diff<>(Collections.singletonList(new Replica(replacement, r.isFull())),
                                                      Collections.singletonList(r)));
                }
            });
        });
        Map<Range, Diff<Replica>> step1WriteCommands = map(allCommands, Diff::onlyAdditions);
        Map<Range, Diff<Replica>> step3WriteCommands = map(allCommands, Diff::onlyRemovals);
        Map<Range, Diff<Replica>> readCommands = allCommands;
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
                Map<Range, Diff<Replica>> inverted = map(step1WriteCommands, Diff::invert);
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
                Map<Range, Diff<Replica>> inverted = map(readCommands, Diff::invert);
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

    public static void assertPlacements(SimulatedPlacements placements, Map<Range, List<Replica>> r, Map<Range, List<Replica>> w)
    {
        assertRanges(r, placements.readPlacements);
        assertRanges(w, placements.writePlacements);
    }

    public static void assertRanges(Map<Range, List<Replica>> expected, Map<Range, List<Replica>> actual)
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
                                expected.get(k).stream().map(r -> r.node().idx()).sorted().collect(Collectors.toList()),
                                actual.get(k).stream().map(r -> r.node().idx()).sorted().collect(Collectors.toList()));
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
    public static NavigableMap<Range, List<Replica>> apply(Map<Range, List<Replica>> orig, Map<Range, Diff<Replica>> diff)
    {
        assert containsAll(orig.keySet(), diff.keySet()) : String.format("Can't apply diff to a map with different sets of keys:" +
                                                                         "\nOrig ks: %s" +
                                                                         "\nDiff ks: %s" +
                                                                         "\nDiff: %s",
                                                                         orig.keySet(), diff.keySet(), diff);
        NavigableMap<Range, List<Replica>> res = new TreeMap<>();
        for (Map.Entry<Range, List<Replica>> entry : orig.entrySet())
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
    public static List<Replica> apply(List<Replica> nodes, Diff<Replica> diff)
    {
        Set<Replica> tmp = new HashSet<>(nodes);
        tmp.addAll(diff.additions);
        for (Replica removal : diff.removals)
            tmp.remove(removal);
        List<Replica> newReplicas = new ArrayList<>(tmp);
        newReplicas.sort(Comparator.comparing(Replica::node));
        return Collections.unmodifiableList(newReplicas);
    }

    /**
     * Diff two placement maps
     */
    public static Map<Range, Diff<Replica>> diff(Map<Range, List<Replica>> l, Map<Range, List<Replica>> r)
    {
        assert l.keySet().equals(r.keySet()) : String.format("Can't diff events from different bases %s %s", l.keySet(), r.keySet());
        Map<Range, Diff<Replica>> diff = new TreeMap<>();
        for (Map.Entry<Range, List<Replica>> entry : l.entrySet())
        {
            Range range = entry.getKey();
            Diff<Replica> d = diff(entry.getValue(), r.get(range));
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
    public static Diff<Replica> diff(List<Replica> l, List<Replica> r)
    {
        // additions things present in r but not in l
        List<Replica> additions = new ArrayList<>();
        // removals are things present in l but not r
        List<Replica> removals = new ArrayList<>();

        for (Replica i : r)
        {
            boolean isPresentInL = false;
            for (Replica j : l)
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

        for (Replica i : l)
        {
            boolean isPresentInR = false;
            for (Replica j : r)
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

    public static Map<Range, List<Replica>> superset(Map<Range, List<Replica>> l, Map<Range, List<Replica>> r)
    {
        assert l.keySet().equals(r.keySet()) : String.format("%s != %s", l.keySet(), r.keySet());

        Map<Range, List<Replica>> newState = new TreeMap<>();
        for (Map.Entry<Range, List<Replica>> entry : l.entrySet())
        {
            Range range = entry.getKey();
            Set<Replica> nodes = new HashSet<>();
            nodes.addAll(entry.getValue());
            nodes.addAll(r.get(range));
            newState.put(range, new ArrayList<>(nodes));
        }

        return newState;
    }

    public static NavigableMap<Range, List<Replica>> mergeReplicated(Map<Range, List<Replica>> orig, long removingToken)
    {
        if (removingToken == Long.MIN_VALUE)
        {
            Assert.assertEquals(Long.MIN_VALUE, orig.entrySet().iterator().next().getKey().start);
            return new TreeMap<>(orig);
        }
        NavigableMap<Range, List<Replica>> newState = new TreeMap<>();
        Iterator<Map.Entry<Range, List<Replica>>> iter = orig.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<Range, List<Replica>> current = iter.next();
            if (current.getKey().end == removingToken)
            {
                assert iter.hasNext() : "Cannot merge range, no more ranges in list";
                Map.Entry<Range, List<Replica>> next = iter.next();
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

    public static NavigableMap<Range, List<Replica>> splitReplicated(Map<Range, List<Replica>> orig, long splitAt)
    {
        if (splitAt == Long.MIN_VALUE)
        {
            Assert.assertEquals(Long.MIN_VALUE, orig.entrySet().iterator().next().getKey().start);
            return new TreeMap<>(orig);
        }
        NavigableMap<Range, List<Replica>> newState = new TreeMap<>();
        for (Map.Entry<Range, List<Replica>> entry : orig.entrySet())
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

    public static class Diff<T>
    {
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

    public static Diff<Replica> additionsAndTransientToFull(Diff<Replica> unfiltered)
    {
        if (unfiltered.additions.isEmpty())
            return null;
        List<Replica> additions = new ArrayList<>(unfiltered.additions.size());
        List<Replica> removals = new ArrayList<>(unfiltered.additions.size());
        for (Replica added : unfiltered.additions)
        {
            // Include any new FULL replicas here. If there exists the removal of a corresponding Transient
            // replica (i.e. a switch from T -> F), add that too. We want T -> F transitions to happen early
            // in a multi-step operation, at the same time as new write replicas are added.
            if (added.isFull())
            {
                additions.add(added);
                Optional<Replica> removed = unfiltered.removals.stream()
                                                               .filter(r -> r.isTransient() && r.node().equals(added.node()))
                                                               .findFirst();

                removed.ifPresent(removals::add);
            }
            else
            {
                // Conversely, when a replica transitions from F -> T, it's enacted late in a multi-step operation.
                // So only include TRANSIENT additions if there is no removal of a corresponding FULL replica.
                boolean include = unfiltered.removals.stream()
                                                     .noneMatch(removed -> removed.node().equals(added.node())
                                                                           && removed.isFull());
                if (include)
                    additions.add(added);
            }
        }
        return new Diff<>(additions, removals);
    }

    public static Diff<Replica> removalsAndFullToTransient(Diff<Replica> unfiltered)
    {
        if (unfiltered.removals.isEmpty())
            return null;
        List<Replica> additions = new ArrayList<>(unfiltered.removals.size());
        List<Replica> removals = new ArrayList<>(unfiltered.removals.size());
        for (Replica removed : unfiltered.removals)
        {
            // Include any new FULL replicas here. If there exists the removal of a corresponding Transient
            // replica (i.e. a switch from T -> F), add that too. We want T -> F transitions to happen early
            // in a multi-step operation, at the same time as new write replicas are added.
            if (removed.isFull())
            {
                removals.add(removed);
                Optional<Replica> added = unfiltered.additions.stream()
                                                               .filter(r -> r.isTransient() && r.node().equals(removed.node()))
                                                               .findFirst();

                added.ifPresent(additions::add);
            }
            else
            {
                // Conversely, when a replica transitions from F -> T, it's enacted late in a multi-step operation.
                // So only include TRANSIENT additions if there is no removal of a corresponding FULL replica.
                boolean include = unfiltered.additions.stream()
                                                     .noneMatch(added -> added.node().equals(removed.node())
                                                                           && added.isFull());
                if (include)
                    removals.add(removed);
            }
        }
        return new Diff<>(additions, removals);
    }

    public static String diffsToString(Map<Range, Diff<Replica>> placements)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Range, Diff<Replica>> e : placements.entrySet())
        {
            builder.append("\t\t").append(e.getKey()).append(": ").append(e.getValue()).append("\n");
        }
        return builder.toString();
    }

    public static String placementsToString(Map<Range, List<Replica>> placements)
    {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<Range, List<Replica>> e : placements.entrySet())
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
}
