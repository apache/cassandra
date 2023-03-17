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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Assert;

/**
 * A small class that helps to avoid doing mental arithmetics on ranges.
 */
public class PlacementSimulator
{
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
        public final int rf;
        public final List<Node> nodes;
        // TODO: should be navigable map!
        public final Map<Range, List<Node>> readPlacements;
        public final Map<Range, List<Node>> writePlacements;
        // Stashed states are steps required to finish the operation. For example, in case of
        // bootstrap, this could be adding replicas to write (and then read) sets after splitting ranges.
        public final List<Transformations> stashedStates;

        public SimulatedPlacements(int rf,
                                   List<Node> nodes,
                                   Map<Range, List<Node>> readPlacements,
                                   Map<Range, List<Node>> writePlacements,
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

        public SimulatedPlacements withReadPlacements(Map<Range, List<Node>> newReadPlacements)
        {
            return new SimulatedPlacements(rf, nodes, newReadPlacements, writePlacements, stashedStates);
        }

        public SimulatedPlacements withWritePlacements(Map<Range, List<Node>> newWritePlacements)
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

    public static interface SimulatedPlacementHolder
    {
        public SimulatedPlacements get();
        public SimulatedPlacements apply(Transformations fn);
        public SimulatedPlacementHolder set(SimulatedPlacements placements);
        public SimulatedPlacementHolder fork();
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

        public SimulatedPlacements apply(Transformations fn)
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
        boolean applied = false;

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

    /**
     * Diff-based bootstrap (very close implementation-wise to what production code does)
     */
    public static ModelChecker.Pair<SimulatedPlacements, Transformations> bootstrap_diffBased(SimulatedPlacements baseState, String node, long token)
    {
        List<Node> splitNodes = split(baseState.nodes, token);
        Map<Range, List<Node>> maximalStateWithPlacement = replicate(move(splitNodes, token, node), baseState.rf);

        Map<Range, List<Node>> splitReadPlacements = replicate(splitNodes, baseState.rf);
        Map<Range, List<Node>> splitWritePlacements = replicate(splitNodes, baseState.rf);

        Map<Range, Diff<Node>> allWriteCommands = diff(splitWritePlacements, maximalStateWithPlacement);
        Map<Range, Diff<Node>> step1WriteCommands = map(allWriteCommands, Diff::onlyAdditions);
        Map<Range, Diff<Node>> step3WriteCommands = map(allWriteCommands, Diff::onlyRemovals);
        Map<Range, Diff<Node>> readCommands = diff(splitReadPlacements, maximalStateWithPlacement);

        Transformations steps = new Transformations();

        steps.add(new Transformation(
            (model) -> { // apply
                // add the new node to the system and split ranges according to its token, while retaining current
                // placement. This step will always be executed immediately, whereas subsequent steps may be deferred
                debug.log("Splitting ranges to prepare for join of " + node + "\n");
                return model.withReadPlacements(splitReplicated(baseState.readPlacements, token))
                            .withWritePlacements(splitReplicated(baseState.writePlacements, token));
            },
            (model) -> { // revert
                // final stage of reverting a join is to undo the range splits performed by preparing the operation
                debug.log("Reverting range splits from prepare-join of " + node + "\n");
                return model.withWritePlacements(mergeReplicated(model.writePlacements, token))
                            .withReadPlacements(mergeReplicated(model.readPlacements, token));
            })
        );

        // Step 1: add new node as a write replica to all ranges it is gaining
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing start-join of " + node + "\n");
                debug.log(String.format("Commands for step 1 of bootstrap of %s@%d.\n" +
                          "\twriteModifications=\n%s",
                          node, token, diffsToString(step1WriteCommands)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, step1WriteCommands));
            },
            (model) -> { // revert
                debug.log("Reverting start-join of " + node + "\n");
                Map<Range, Diff<Node>> inverted = map(step1WriteCommands, Diff::invert);
                debug.log("Commands for reverting step 1 of bootstrap of %s@%d.\n" +
                          "\twriteModifications=\n%s",
                          node, token, diffsToString(inverted));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            })
        );

        // Step 2: add new node as a read replica to the ranges it is gaining; remove old node from reads at the same time
        steps.add(new Transformation(
            (model) -> {  // apply
                debug.log("Executing mid-join of " + node + "\n");
                debug.log(String.format("Commands for step 2 of bootstrap of %s@%d.\n" +
                                     "\treadCommands=\n%s",
                                     node, token, diffsToString(readCommands)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, readCommands));
            },
            (model) -> {  // revert
                debug.log("Reverting mid-join of " + node + "\n");
                Map<Range, Diff<Node>> inverted = map(readCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 2 of bootstrap of %s@%d.\n" +
                                        "\treadCommands=\n%s",
                                        node, token, diffsToString(inverted)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, inverted));
            })
        );


        // Step 3: finally remove the old node from writes
        steps.add(new Transformation(
            (model) -> { // apply
                debug.log("Executing finish-join  of " + node + "\n");
                debug.log(String.format("Commands for step 3 of bootstrap of %s@%d.\n" +
                                     "\twriteModifications=\n%s",
                                     node, token,
                                     diffsToString(step3WriteCommands)));
                List<Node> newNodes = new ArrayList<>(model.nodes);
                newNodes.add(new Node(token, node));
                Collections.sort(newNodes, Node::compareTo);
                return model.withNodes(newNodes)
                            .withWritePlacements(PlacementSimulator.apply(model.writePlacements, step3WriteCommands));
            },
            (model) -> { //revert
                throw new IllegalStateException("Can't revert finish-join of " + node + ", operation is already complete\n");
            })
        );

        debug.log("Planned bootstrap of " + node + "@" + token + "\n");
        return new ModelChecker.Pair<>(baseState.withStashed(steps), steps);
    }

    public static ModelChecker.Pair<SimulatedPlacements, Transformations>  move_diffBased(SimulatedPlacements baseState, String node, long newToken)
    {
        Node oldLocation = baseState.nodes.stream()
                           .filter(n -> n.id.equals(node))
                           .findFirst()
                           .get();

        List<Node> origNodes = new ArrayList<>(baseState.nodes);
        List<Node> finalNodes = new ArrayList<>();
        for (int i = 0; i < origNodes.size(); i++)
        {
            if (origNodes.get(i).id == oldLocation.id)
                continue;
            finalNodes.add(origNodes.get(i));
        }
        finalNodes.add(new Node(newToken, node));
        Collections.sort(finalNodes, Node::compareTo);

        Map<Range, List<Node>> start = splitReplicated(replicate(origNodes, baseState.rf), newToken);
        Map<Range, List<Node>> end = splitReplicated(replicate(finalNodes, baseState.rf), oldLocation.token);

        Map<Range, Diff<Node>> fromStartToEnd = diff(start, end);

        Transformations steps = new Transformations();

        // Step 1: Prepare Move,
        steps.add(new Transformation(
        (model) -> { // apply
            debug.log(String.format("Splitting ranges to prepare for move of %s to %d\n", node, newToken));
            return model.withReadPlacements(splitReplicated(model.readPlacements, newToken))
                        .withWritePlacements(splitReplicated(model.writePlacements, newToken));
        },
        (model) -> { // revert
            debug.log(String.format("Reverting range splits from prepare move of %s to %d\n", node, newToken));
            return model.withWritePlacements(mergeReplicated(model.writePlacements, newToken))
                        .withReadPlacements(mergeReplicated(model.readPlacements, newToken));
        }));

        // Step 2: Start Move, add all potential owners to write quorums
        steps.add(new Transformation(
        (model) -> { // apply
            Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyAdditions);
            debug.log("Executing start-move of " + node + "\n");
            debug.log(String.format("Commands for step 1 of move of %s to %d.\n" +
                                    "\twriteModifications=\n%s",
                                    node, newToken, diffsToString(diff)));

            Map<Range, List<Node>> placements = model.writePlacements;
            placements = PlacementSimulator.apply(placements, diff);
            return model.withWritePlacements(placements);
        },
        (model) -> { // revert
            debug.log("Reverting start-move of " + node + "\n");
            Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyAdditions);
            Map<Range, Diff<Node>> inverted = map(diff, Diff::invert);
            debug.log(String.format("Commands for reverting step 1 of move of %s to %d.\n" +
                                    "\twriteModifications=\n%s",
                                    node, newToken, diffsToString(inverted)));

            return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
        }
        ));
        // Step 3: Mid Move, remove all nodes that are losing ranges from read quorums, add all nodes gaining ranges to read quorums
        steps.add(new Transformation(
        (model) -> {
            debug.log("Executing mid-move of " + node + "\n");
            debug.log(String.format("Commands for step 2 of move of %s to %d.\n" +
                                    "\treadModifications=\n%s",
                                    node, newToken, diffsToString(fromStartToEnd)));

            Map<Range, List<Node>> placements = model.readPlacements;
            placements = PlacementSimulator.apply(placements, fromStartToEnd);
            return model.withReadPlacements(placements);
        },
        (model) -> {
            Map<Range, List<Node>> placements = PlacementSimulator.apply(model.readPlacements, map(fromStartToEnd, Diff::invert));
            return model.withReadPlacements(placements);
        }));

        // Step 4: Finish Move, remove all nodes that are losing ranges from write quorums
        steps.add(new Transformation(
        (model) -> {
            Map<Range, Diff<Node>> diff = map(fromStartToEnd, Diff::onlyRemovals);

            debug.log("Executing finish-move of " + node + "\n");
            debug.log(String.format("Commands for step 2 of move of %s to %d.\n" +
                                    "\twriteModifications=\n%s",
                                    node, newToken, diffsToString(diff)));

            List<Node> currentNodes = new ArrayList<>(model.nodes);
            List<Node> newNodes = new ArrayList<>();
            for (int i = 0; i < currentNodes.size(); i++)
            {
                if (currentNodes.get(i).id == oldLocation.id)
                    continue;
                newNodes.add(currentNodes.get(i));
            }
            newNodes.add(new Node(newToken, node));
            Collections.sort(newNodes, Node::compareTo);

            Map<Range, List<Node>> writePlacements = model.writePlacements;
            writePlacements = PlacementSimulator.apply(writePlacements, diff);

            return model.withWritePlacements(mergeReplicated(writePlacements, oldLocation.token))
                        .withReadPlacements(mergeReplicated(model.readPlacements, oldLocation.token))
                        .withNodes(newNodes);
        },
        (model) -> {
            throw new IllegalStateException(String.format("Can't revert finish-move of %d, operation is already complete", newToken));
        }));

        return new ModelChecker.Pair<>(baseState.withStashed(steps), steps);
    }

    public static SimulatedPlacements bootstrapFully(SimulatedPlacements baseState, String node, long token)
    {
        ModelChecker.Pair<SimulatedPlacements, Transformations> a = bootstrap_diffBased(baseState, node, token);
        baseState = a.l;
        Transformations transformations = a.r;

        while (transformations.hasNext())
            baseState = transformations.advance(baseState);

        return baseState;
    }

    public static ModelChecker.Pair<SimulatedPlacements, Transformations> leave_diffBased(SimulatedPlacements baseState, long token)
    {
        // calculate current placements - this is start state
        Map<Range, List<Node>> start = replicate(baseState.nodes, baseState.rf);
        List<Range> currentRanges = toRanges(baseState.nodes);

        // remove leaving node from list of nodes
        Node toRemove = baseState.nodes.stream()
                                       .filter(n -> n.token == token)
                                       .findFirst()
                                       .orElseThrow(() -> new IllegalArgumentException("Can't find node to remove with token" + token));
        List<Node> afterLeaveNodes = new ArrayList<>(baseState.nodes);
        afterLeaveNodes.remove(toRemove);
        // calculate placements based on existing ranges but final set of nodes - this is end state
        Map<Range, List<Node>> end = replicate(currentRanges, afterLeaveNodes, baseState.rf);
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
                Collections.sort(newNodes, Node::compareTo);
                Map<Range, List<Node>> writes = PlacementSimulator.apply(model.writePlacements, step3WriteCommands);
                return model.withReadPlacements(mergeReplicated(model.readPlacements, toRemove.token))
                            .withWritePlacements(mergeReplicated(writes, toRemove.token))
                            .withNodes(newNodes);
            },
            (model) -> { // revert
                throw new IllegalStateException("Can't revert finish-leave of " + toRemove + ", operation is already complete\n");
            }));

        debug.log("Planned decommission of " + toRemove + "\n");
        return new ModelChecker.Pair<>(baseState.withStashed(steps), steps);
    }

    public static ModelChecker.Pair<SimulatedPlacements, Transformations> replace_directly(SimulatedPlacements baseState, long token, String newNode)
    {
        // find the node with the specified token
        Node toReplace = baseState.nodes.stream()
                                        .filter(n -> n.token == token)
                                        .findFirst()
                                        .orElseThrow(IllegalArgumentException::new);

        Node replacement = new Node(token, newNode);
        List<Node> nodesAfterReplacement = new ArrayList<>(baseState.nodes);
        nodesAfterReplacement.remove(toReplace);
        nodesAfterReplacement.add(replacement);
        nodesAfterReplacement.sort(Node::compareTo);

        Map<Range, List<Node>> start = replicate(baseState.nodes, baseState.rf);
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
                debug.log(String.format("Executing start-replace of %s@%d for  %s\n", replacement, token, toReplace));
                debug.log(String.format("Commands for step 1 of bootstrap of %s@%d for replacement of %s.\n" +
                                        "\twriteModifications=\n%s",
                                        replacement, token, toReplace, diffsToString(step1WriteCommands)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, step1WriteCommands));
            },
            (model) -> { // revert
                debug.log(String.format("Reverting start-replace of %s@%d for  %s\n", replacement, token, toReplace));
                Map<Range, Diff<Node>> inverted = map(step1WriteCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 1 of bootstrap of %s@%d for replacement of %s.\n" +
                                        "\twriteModifications=\n%s",
                                        replacement, token, toReplace, diffsToString(inverted)));
                return model.withWritePlacements(PlacementSimulator.apply(model.writePlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Executing mid-replace of %s@%d for %s\n", replacement, token, toReplace));
                debug.log(String.format("Commands for step 2 of bootstrap of %s@%d for replacement of %s.\n" +
                                        "\treadModifications=\n%s",
                                        replacement, token, toReplace,
                                        diffsToString(readCommands)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, readCommands));
            },
            (model) -> { // revert
                debug.log(String.format("Reverting mid-replace of %s@%d for %s\n", replacement, token, toReplace));
                Map<Range, Diff<Node>> inverted = map(readCommands, Diff::invert);
                debug.log(String.format("Commands for reverting step 2 of bootstrap of %s@%d for replacement of %s.\n" +
                                        "\treadModifications=\n%s",
                                        replacement, token, toReplace,
                                        diffsToString(inverted)));
                return model.withReadPlacements(PlacementSimulator.apply(model.readPlacements, inverted));
            })
        );

        steps.add(new Transformation(
            (model) -> { // apply
                debug.log(String.format("Executing finish-replace of %s@%d for %s\n", replacement, token, toReplace));
                debug.log(String.format("Commands for step 3 of bootstrap of %s@%d for replacement of %s.\n" +
                                     "\twriteModifications=\n%s",
                                     replacement, token, toReplace,
                                     diffsToString(step3WriteCommands)));
                List<Node> newNodes = new ArrayList<>(model.nodes);
                newNodes.remove(toReplace);
                newNodes.add(replacement);
                newNodes.sort(Node::compareTo);
                return model.withNodes(newNodes)
                            .withWritePlacements(PlacementSimulator.apply(model.writePlacements, step3WriteCommands));
            },
            (model) -> { // revert
                throw new IllegalStateException(String.format("Can't revert finish-replace of %s@%d for %s, operation is already complete\n", replacement, token, toReplace));
            })
        );

        debug.log(String.format("Planned bootstrap of %s@%d for replacement of %s\n", replacement, token, toReplace));
        return new ModelChecker.Pair<>(baseState.withStashed(steps), steps);
    }

    public static void assertPlacements(SimulatedPlacements placements, Map<Range, List<Node>> r, Map<Range, List<Node>> w)
    {
        assertRanges(r,
                     placements.readPlacements);
        assertRanges(w,
                     placements.writePlacements);
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
                                expected.get(k).stream().map(n -> n.id).sorted().collect(Collectors.toList()),
                                actual.get(k).stream().map(n -> n.id).sorted().collect(Collectors.toList()));
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
    public static Map<Range, List<Node>> apply(Map<Range, List<Node>> orig, Map<Range, Diff<Node>> diff)
    {
        assert containsAll(orig.keySet(), diff.keySet()) : String.format("Can't apply diff to a map with different sets of keys:" +
                                                                         "\nOrig ks: %s" +
                                                                         "\nDiff ks: %s" +
                                                                         "\nDiff: %s",
                                                                         orig.keySet(), diff.keySet(), diff);
        Map<Range, List<Node>> res = new TreeMap<>();
        for (Range range : orig.keySet())
        {
            if (diff.containsKey(range))
                res.put(range, apply(orig.get(range), diff.get(range)));
            else
                res.put(range, orig.get(range));
        }
        return Collections.unmodifiableMap(res);
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
        for (Range range : l.keySet())
        {
            Diff<Node> d = diff(l.get(range), r.get(range));
            if (!d.removals.isEmpty() || !d.additions.isEmpty())
                diff.put(range, d);
        }
        return Collections.unmodifiableMap(diff);
    }

    public static <T> Map<Range, T> map(Map<Range, T> diff, Function<T, T> fn)
    {
        Map<Range, T> newDiff = new TreeMap<>();
        for (Range range : diff.keySet())
        {
            T newV = fn.apply(diff.get(range));
            if (newV != null)
                newDiff.put(range, newV);
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
        for (Range range : l.keySet())
        {
            Set<Node> nodes = new HashSet<>();
            nodes.addAll(l.get(range));
            nodes.addAll(r.get(range));
            newState.put(range, new ArrayList<>(nodes));
        }

        return newState;
    }

    public static Map<Range, List<Node>> mergeReplicated(Map<Range, List<Node>> orig, long removingToken)
    {
        Map<Range, List<Node>> newState = new TreeMap<>();
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

    public static Map<Range, List<Node>> splitReplicated(Map<Range, List<Node>> orig, long splitAt)
    {
        Map<Range, List<Node>> newState = new TreeMap<>();
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
            if (!inserted && splitAt > node.token)
            {
                // We're trying to split rightmost range
                if (previous == null)
                {
                    newNodes.add(new Node(splitAt, nodes.get(0).id));
                }
                else
                {
                    newNodes.add(new Node(splitAt, previous.id));
                }
                inserted = true;
            }

            newNodes.add(node);
            previous = node;
        }

        // Leftmost is split
        if (!inserted)
            newNodes.add(new Node(splitAt, previous.id));

        newNodes.sort(Node::compareTo);
        return Collections.unmodifiableList(newNodes);
    }

    /**
     * Change the ownership of the freshly split token
     */
    public static List<Node> move(List<Node> nodes, long tokenToMove, String newOwner)
    {
        List<Node> newNodes = new ArrayList<>();
        for (Node node : nodes)
        {
            if (node.token == tokenToMove)
                newNodes.add(new Node(node.token, newOwner));
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
    public static Map<Range, List<Node>> replicate(List<Node> nodes, int rf)
    {
        List<Range> ranges = toRanges(nodes);
        return replicate(ranges, nodes, rf);
    }

    public static Map<Range, List<Node>> replicate(List<Range> ranges, List<Node> nodes, int rf)
    {
        Map<Range, List<Node>> replication = new TreeMap<>();
        for (Range range : ranges)
        {
            Set<String> names = new HashSet<>();
            List<Node> replicas = new ArrayList<>();
            int idx = primaryReplica(nodes, range);
            if (idx >= 0)
            {
                for (int i = idx; i < nodes.size() && replicas.size() < rf; i++)
                    addIfUnique(replicas, names, nodes.get(i));

                for (int i = 0; replicas.size() < rf && i < idx; i++)
                    addIfUnique(replicas, names, nodes.get(i));
                if (range.start == Long.MIN_VALUE)
                    replication.put(ranges.get(ranges.size() - 1), replicas);
                replication.put(range, replicas);
            }
            else
            {
                // if the range end is larger than the highest assigned token, then treat it
                // as part of the wraparound and replicate it to the same nodes as the first
                // range. This is most likely caused by a decommission removing the node with
                // the largest token.
                replication.put(range, replication.get(ranges.get(0)));
            }
        }
        return Collections.unmodifiableMap(replication);
    }

    public static void addIfUnique(List<Node> nodes, Set<String> names, Node node)
    {
        if (names.contains(node.id))
            return;
        nodes.add(node);
        names.add(node.id);
    }

    public static List<Node> uniq(List<Node> nodes)
    {
        List<Node> newNodes = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (Node node : nodes)
        {
            if (!ids.contains(node.id))
            {
                ids.add(node.id);
                newNodes.add(node);
            }
        }
        return Collections.unmodifiableList(newNodes);
    }

    /**
     * Finds a primary replica
     */
    public static int primaryReplica(List<Node> nodes, Range range)
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            if (range.end != Long.MIN_VALUE && nodes.get(i).token >= range.end)
                return i;
        }
        return -1;
    }

    /**
     * Generates token ranges from the list of nodes
     */
    public static List<Range> toRanges(List<Node> nodes)
    {
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.token);
        tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        List<Range> ranges = new ArrayList<>(tokens.size() + 1);
        long prev = tokens.get(0);
        for (int i = 1; i < tokens.size(); i++)
        {
            long current = tokens.get(i);
            ranges.add(new Range(prev, current));
            prev = current;
        }
        ranges.add(new Range(prev, Long.MIN_VALUE));
        return Collections.unmodifiableList(ranges);

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
            return min >= start && (max <= end || end == Long.MIN_VALUE);
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

    public static class Node implements Comparable<Node>
    {
        public final long token;
        public final String id;

        public Node(long token, String id)
        {
            this.token = token;
            this.id = id;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(id, node.id);
        }

        public int hashCode()
        {
            return Objects.hash(id);
        }

        public int compareTo(Node o)
        {
            return Long.compare(token, o.token);
        }

        public String toString()
        {
            return "" + id + "@" + token;
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
}
