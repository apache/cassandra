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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.junit.Assert;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.PlacementSimulator.SimulatedPlacements;
import org.apache.cassandra.distributed.test.log.PlacementSimulator.Transformations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;

import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import static org.apache.cassandra.distributed.test.log.CMSTestBase.CMSSut;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.*;

public abstract class SimulatedOperation
{
    enum Type{JOIN, LEAVE, REPLACE, MOVE}
    enum Status{READY, STARTED}

    public final Type type;
    public final Node[] nodes;
    public final Status status;
    public Iterator<ClusterMetadata> sutActions;
    public Transformations simulatedActions;

    public static ModelState join(CMSSut sut, ModelState state, Node node)
    {
        ModelState.Transformer transformer = state.transformer();
        new Join(node).create(sut, state.simulatedPlacements, transformer);
        return transformer.transform();
    }

    public static ModelState joinWithoutBootstrap(ModelState state,
                                                  CMSSut sut,
                                                  Node node)
    {
        sut.service.commit(new UnsafeJoin(node.nodeId(), Collections.singleton(node.longToken()), sut.service.placementProvider()));
        SimulatedPlacements simulatedState = state.simulatedPlacements;
        if (simulatedState == null)
        {
            List<Node> nodes = Collections.singletonList(node);
            simulatedState = new SimulatedPlacements(sut.rf,
                                                     nodes,
                                                     sut.rf.replicate(nodes).asMap(),
                                                     sut.rf.replicate(nodes).asMap(),
                                                     Collections.emptyList());
        }
        else
        {
            Transformations simulatedActions = PlacementSimulator.join(simulatedState, node);
            while (simulatedActions.hasNext())
                simulatedState = simulatedActions.advance(simulatedState);
        }
        return state.transformer()
                    .withJoined(node)
                    .updateSimulation(simulatedState)
                    .transform();

    }

    public static ModelState leave(CMSSut sut, ModelState state, Node node)
    {
        ModelState.Transformer transformer = state.transformer();
        new Leave(node).create(sut, state.simulatedPlacements, transformer);
        return transformer.transform();
    }

    public static ModelState replace(CMSSut sut, ModelState state, Node toReplace, Node replacement)
    {
        assert toReplace.tokenIdx() == replacement.tokenIdx();
        ModelState.Transformer transformer = state.transformer();
        new Replace(toReplace, replacement).create(sut, state.simulatedPlacements, transformer);
        return transformer.transform();
    }

    public static ModelState move(CMSSut sut, ModelState state, Node movingNode, Node newNode)
    {
        ModelState.Transformer transformer = state.transformer();
        new Move(movingNode, newNode).create(sut, state.simulatedPlacements, transformer);
        return transformer.transform();
    }

    public SimulatedOperation(Type type,
                              Node[] nodes,
                              Iterator<ClusterMetadata> sutActions,
                              Status status,
                              Transformations simulatedActions)
    {
        this.type = type;
        this.nodes = nodes;
        this.sutActions = sutActions;
        this.status = status;
        this.simulatedActions = simulatedActions;
    }

    public abstract void create(CMSSut cmsSut, SimulatedPlacements simulatedPlacements, ModelState.Transformer transformer);

    protected abstract void finishOperation(ModelState.Transformer transformer);

    protected abstract void incrementCancel(ModelState.Transformer transformer);

    public ModelState advance(ModelState simulatedState)
    {
        ModelState.Transformer transformer = simulatedState.transformer();
        advance(simulatedState.simulatedPlacements, transformer);
        return transformer.transform();
    }

    public void advance(SimulatedPlacements simulatedState, ModelState.Transformer transformer)
    {
        ClusterMetadata m1 = ClusterMetadata.current();

        sutActions.next();
        ClusterMetadata m2 = ClusterMetadata.current();

        Map<Range<Token>, VersionedEndpoints.ForRange> after = m2.placements.get(simulatedState.rf.asKeyspaceParams().replication).reads.asMap();
        m1.placements.get(simulatedState.rf.asKeyspaceParams().replication).reads.forEach((k, beforePlacements) -> {
            if (after.containsKey(k))
            {
                VersionedEndpoints.ForRange afterPlacements = after.get(k);
                if (!beforePlacements.get().stream().collect(Collectors.toSet()).equals(after.get(k).get().stream().collect(Collectors.toSet())))
                {
                    Assert.assertTrue(String.format("Expected the range %s to bump epoch from %s, but it was %s, because the endpoints have changed:\n%s\n%s",
                                                    k, beforePlacements.lastModified(), afterPlacements.lastModified(),
                                                    beforePlacements.get(), afterPlacements.get()),
                                      afterPlacements.lastModified().isAfter(beforePlacements.lastModified()));
                }
                else
                {
                    Assert.assertTrue(String.format("Expected the range %s to have the same epoch (%s), but it was %s.",
                                                    k, beforePlacements.lastModified(), afterPlacements.lastModified()),
                                      afterPlacements.lastModified().is(beforePlacements.lastModified()));
                }
            }
        });


        simulatedState = simulatedActions.advance(simulatedState);

        transformer.removeOperation(this)
                   .updateSimulation(simulatedState);

        if (sutActions.hasNext())
            transformer.addOperation(started());
        else
            finishOperation(transformer);
    }

    public void cancel(CMSSut sut, SimulatedPlacements simulatedPlacements, ModelState.Transformer transformer)
    {
        ClusterMetadata metadata = sut.service.metadata();
        Node node = targetNode();
        MultiStepOperation<?> operation = metadata.inProgressSequences.get(node.nodeId());
        assert operation != null : "No in-progress sequence found for node " + node.nodeId();
        sut.service.commit(new CancelInProgressSequence(node.nodeId()));

        simulatedPlacements = simulatedActions.revertPublishedEffects(simulatedPlacements);
        transformer.removeOperation(this)
                   .updateSimulation(simulatedPlacements);
        incrementCancel(transformer);
    }

    protected abstract Node targetNode();

    protected abstract SimulatedOperation started();

    public String toString()
    {
        return "OperationState{" +
               "type=" + type +
               ", nodes=" + Arrays.toString(nodes) +
               ", remaining=" + sutActions.hasNext() +
               ", status=" + status +
               '}';
    }

    static class Join extends SimulatedOperation
    {
        protected Join(Node node)
        {
            this(new Node[]{ node }, null, Status.READY, null);
        }

        public Join(Node[] nodes, Iterator<ClusterMetadata> sutActions, Status status, Transformations simulatedActions)
        {
            super(Type.JOIN, nodes, sutActions, status, simulatedActions);
        }

        @Override
        public void create(CMSSut sut, SimulatedPlacements simulatedState, ModelState.Transformer transformer)
        {
            assert simulatedActions == null;
            Node toAdd = targetNode();
            Optional<BootstrapAndJoin> maybePlan = prepareJoin(sut, toAdd);
            if (!maybePlan.isPresent())
            {
                transformer.incrementRejected()
                           .recycleRejected(toAdd);
                return;
            }

            BootstrapAndJoin plan = maybePlan.get();
            sutActions = toIter(sut.service, plan.startJoin, plan.midJoin, plan.finishJoin);
            simulatedActions = PlacementSimulator.join(simulatedState, toAdd);

            // immediately execute the first step of bootstrap transformations, the splitting of existing ranges. This
            // is so that subsequent planned operations base their transformations on the split ranges.
            simulatedState = simulatedActions.advance(simulatedState.withStashed(simulatedActions));

            transformer.addOperation(this)
                       .updateSimulation(simulatedState);
        }

        @Override
        protected void finishOperation(ModelState.Transformer transformer)
        {
            transformer.withJoined(targetNode());
        }

        @Override
        protected void incrementCancel(ModelState.Transformer transformer)
        {
            transformer.incrementCancelledJoin();
        }

        protected Node targetNode()
        {
            return nodes[0];
        }

        @Override
        public SimulatedOperation started()
        {
            return new Join(nodes, sutActions, Status.STARTED, simulatedActions);
        }
    }

    public static class Leave extends SimulatedOperation
    {
        protected Leave(Node node)
        {
            this(new Node[]{ node }, null, Status.READY, null);
        }

        public Leave(Node[] nodes, Iterator<ClusterMetadata> sutActions, Status status, Transformations simulatedActions)
        {
            super(Type.LEAVE, nodes, sutActions, status, simulatedActions);
        }

        @Override
        public void create(CMSSut sut, SimulatedPlacements simulatedState, ModelState.Transformer transformer)
        {
            assert simulatedActions == null;

            Node toRemove = targetNode();
            Optional<UnbootstrapAndLeave> maybePlan = prepareLeave(sut, toRemove);
            if (!maybePlan.isPresent())
            {
                transformer.incrementRejected();
                return;
            }

            UnbootstrapAndLeave plan = maybePlan.get();
            sutActions = toIter(sut.service, plan.startLeave, plan.midLeave, plan.finishLeave);
            simulatedActions = PlacementSimulator.leave(simulatedState, toRemove);

            simulatedState = simulatedState.withStashed(simulatedActions);

            // we don't remove from bootstrapped nodes until the finish leave event executes
            transformer.addOperation(this)
                       .markLeaving(toRemove)
                       .updateSimulation(simulatedState);
        }

        @Override
        protected void finishOperation(ModelState.Transformer transformer)
        {
            transformer.withLeft(targetNode());
        }

        @Override
        protected void incrementCancel(ModelState.Transformer transformer)
        {
            transformer.incrementCancelledLeave();
        }

        protected Node targetNode()
        {
            return nodes[0];
        }

        @Override
        public SimulatedOperation started()
        {
            return new Leave(nodes, sutActions, Status.STARTED, simulatedActions);
        }
    }

    public static class Replace extends SimulatedOperation
    {
        protected Replace(Node toReplace, Node replacement)
        {
            this(new Node[]{ toReplace, replacement }, null, Status.READY, null);
        }

        public Replace(Node[] nodes, Iterator<ClusterMetadata> sutActions, Status status, Transformations simulatedActions)
        {
            super(Type.REPLACE, nodes, sutActions, status, simulatedActions);
        }

        @Override
        public void create(CMSSut sut, SimulatedPlacements simulatedState, ModelState.Transformer transformer)
        {
            assert simulatedActions == null;

            Node toReplace = nodes[0];
            Node replacement = nodes[1];

            Optional<BootstrapAndReplace> maybePlan = prepareReplace(sut, toReplace, replacement);
            if (!maybePlan.isPresent())
            {
                transformer.incrementRejected();
                return;
            }

            BootstrapAndReplace plan = maybePlan.get();
            sutActions = toIter(sut.service, plan.startReplace, plan.midReplace, plan.finishReplace);
            simulatedActions = PlacementSimulator.replace(simulatedState, nodes[0], nodes[1]);

            transformer.addOperation(this)
                       .updateSimulation(simulatedState);
        }

        @Override
        protected void finishOperation(ModelState.Transformer transformer)
        {
            transformer.withReplaced(nodes[0], nodes[1]);
        }

        @Override
        protected void incrementCancel(ModelState.Transformer transformer)
        {
            transformer.incrementCancelledReplace();
        }

        protected Node targetNode()
        {
            return nodes[1];
        }

        @Override
        public SimulatedOperation started()
        {
            return new Replace(nodes, sutActions, Status.STARTED, simulatedActions);
        }
    }

    public static class Move extends SimulatedOperation
    {
        protected Move(Node movingNode, Node newNode)
        {
            this(new Node[]{ newNode, movingNode }, null, Status.READY, null);
        }

        public Move(Node[] nodes, Iterator<ClusterMetadata> sutActions, Status status, Transformations simulatedActions)
        {
            super(Type.MOVE, nodes, sutActions, status, simulatedActions);
        }

        @Override
        public void create(CMSSut sut, SimulatedPlacements simulatedState, ModelState.Transformer transformer)
        {
            assert simulatedActions == null;
            Node newNode = nodes[0];
            Node movingNode = nodes[1];

            Optional<org.apache.cassandra.tcm.sequences.Move> maybePlan = prepareMove(sut, movingNode, newNode.longToken());
            if (!maybePlan.isPresent())
            {
                transformer.incrementRejected();
                return;
            }

            org.apache.cassandra.tcm.sequences.Move plan = maybePlan.get();
            sutActions = toIter(sut.service, plan.startMove, plan.midMove, plan.finishMove);
            simulatedActions = PlacementSimulator.move(simulatedState, moving(), newToken());

            // immediately execute the first step of move transformations, the splitting of existing ranges. This
            // is so that subsequent planned operations base their transformations on the split ranges.
            simulatedState = simulatedActions.advance(simulatedState);

            transformer.addOperation(this)
                       .updateSimulation(simulatedState)
                       .markMoving(moving());
        }

        @Override
        protected void finishOperation(ModelState.Transformer transformer)
        {
            transformer.withMoved(nodes[1], nodes[0]);
        }

        @Override
        protected void incrementCancel(ModelState.Transformer transformer)
        {
            transformer.incrementCancelledMove();
        }

        @Override
        protected Node targetNode()
        {
            return nodes[0];
        }

        private long newToken()
        {
            return nodes[0].token();
        }

        private Node moving()
        {
            return nodes[1];
        }

        @Override
        public SimulatedOperation started()
        {
            return new Move(nodes, sutActions, Status.STARTED, simulatedActions);
        }
    }

    private static Optional<BootstrapAndJoin> prepareJoin(CMSSut sut, Node node)
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

    public static Optional<org.apache.cassandra.tcm.sequences.Move> prepareMove(CMSSut sut, Node node, LongToken newToken)
    {
        try
        {
            ClusterMetadata metadata = sut.service.commit(new PrepareMove(node.nodeId(), Collections.singleton(newToken), sut.service.placementProvider(), false));
            return Optional.of((org.apache.cassandra.tcm.sequences.Move) metadata.inProgressSequences.get(node.nodeId()));
        }
        catch (Throwable t)
        {
            return Optional.empty();
        }
    }

    private static Optional<UnbootstrapAndLeave> prepareLeave(CMSSut sut, Node toRemove)
    {
        try
        {
            ClusterMetadata metadata = sut.service.commit(new PrepareLeave(toRemove.nodeId(),
                                                                           true,
                                                                           sut.service.placementProvider(),
                                                                           LeaveStreams.Kind.UNBOOTSTRAP));
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) metadata.inProgressSequences.get(toRemove.nodeId());
            return Optional.of(plan);
        }
        catch (Throwable e)
        {
            return Optional.empty();
        }
    }

    private static Optional<BootstrapAndReplace> prepareReplace(CMSSut sut, Node toReplace, Node replacement)
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