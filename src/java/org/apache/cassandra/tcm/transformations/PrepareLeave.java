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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.PlacementTransitionPlan;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class PrepareLeave implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareLeave.class);
    public static final Serializer serializer = new Serializer();

    private final NodeId nodeId;
    private final boolean force;
    private final PlacementProvider placementProvider;

    public PrepareLeave(NodeId nodeId, boolean force, PlacementProvider placementProvider)
    {
        this.nodeId = nodeId;
        this.force = force;
        this.placementProvider = placementProvider;
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_LEAVE;
    }

    public NodeId nodeId()
    {
        return nodeId;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        ClusterMetadata proposed = prev.transformer().proposeRemoveNode(nodeId).build().metadata;

        if (!force && !validateReplicationForDecommission(proposed))
            return new Rejected("Not enough live nodes to maintain replication factor after decomission.");

        if (proposed.directory.isEmpty())
            return new Rejected("No peers registered, at least local node should be");

        PlacementTransitionPlan transitionPlan = placementProvider.planForDecommission(prev,
                                                                                       nodeId,
                                                                                       prev.schema.getKeyspaces());

        LockedRanges.AffectedRanges rangesToLock = transitionPlan.affectedRanges();
        LockedRanges.Key alreadyLockedBy = prev.lockedRanges.intersects(rangesToLock);
        if (!alreadyLockedBy.equals(LockedRanges.NOT_LOCKED))
        {
            return new Rejected(String.format("Rejecting this plan as it interacts with a range locked by %s (locked: %s, new: %s)",
                                              alreadyLockedBy, prev.lockedRanges, rangesToLock));
        }

        PlacementDeltas startDelta = transitionPlan.addToWrites();
        PlacementDeltas midDelta = transitionPlan.moveReads();
        PlacementDeltas finishDelta = transitionPlan.removeFromWrites();

        LockedRanges.Key unlockKey = LockedRanges.keyFor(proposed.epoch);
        InetAddressAndPort leaving = prev.directory.endpoint(nodeId);

        StartLeave start = new StartLeave(nodeId, startDelta, unlockKey);
        MidLeave mid = new MidLeave(nodeId, midDelta, unlockKey);
        FinishLeave leave = new FinishLeave(nodeId, finishDelta, unlockKey);

        ProgressBarrier barrier = new ProgressBarrier(prev.nextEpoch(),
                                                      rangesToLock.toPeers(prev.placements, prev.directory),
                                                      false);
        UnbootstrapAndLeave plan = new UnbootstrapAndLeave(barrier,
                                                           unlockKey,
                                                           Kind.START_LEAVE,
                                                           start,
                                                           mid,
                                                           leave);

        // note: we throw away the state with the leaving node's tokens removed. It's only
        // used to produce the operation plan.
        ClusterMetadata.Transformer next = prev.transformer()
                                               .with(prev.lockedRanges.lock(unlockKey, rangesToLock))
                                               .with(prev.inProgressSequences.with(nodeId, plan));
        return success(next, rangesToLock);
    }

    private boolean validateReplicationForDecommission(ClusterMetadata proposed)
    {
        String dc = proposed.directory.location(nodeId).datacenter;
        int rf, numNodes;
        for (KeyspaceMetadata ksm : proposed.schema.getKeyspaces())
        {
            if (ksm.replicationStrategy instanceof NetworkTopologyStrategy)
            {
                NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) ksm.replicationStrategy;
                rf = strategy.getReplicationFactor(dc).allReplicas;
                numNodes = joinedNodeCount(proposed.directory, proposed.directory.allDatacenterEndpoints().get(dc));

                if (numNodes <= rf)
                {
                    logger.warn("Not enough live nodes to maintain replication factor for keyspace {}. " +
                                "Replication factor in {} is {}, live nodes = {}. " +
                                "Perform a forceful decommission to ignore.", ksm, dc, rf, numNodes);
                    return false;
                }
            }
            else if (ksm.params.replication.isMeta())
            {
                // TODO: usually we should not allow decommissioning of CMS node
                // from what i understand this is not necessarily decomissioning of the cms node; every node has this ks now
                continue;
            }
            else
            {
                numNodes = joinedNodeCount(proposed.directory, proposed.directory.allAddresses());
                rf = ksm.replicationStrategy.getReplicationFactor().allReplicas;
                if (numNodes <= rf)
                {
                    logger.warn("Not enough live nodes to maintain replication factor in keyspace "
                                + ksm + " (RF = " + rf + ", N = " + numNodes + ")."
                                + " Perform a forceful decommission to ignore.");
                    return false;
                }
            }
        }
        return true;
    }

    private static int joinedNodeCount(Directory directory, Collection<InetAddressAndPort> endpoints)
    {
        return (int)endpoints.stream().filter(i -> directory.peerState(i) == NodeState.JOINED).count();
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, PrepareLeave>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof PrepareLeave;
            PrepareLeave transformation = (PrepareLeave) t;
            NodeId.serializer.serialize(transformation.nodeId, out, version);
            out.writeBoolean(transformation.force);
        }

        public PrepareLeave deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId id = NodeId.serializer.deserialize(in, version);
            boolean force = in.readBoolean();

            return new PrepareLeave(id,
                                    force,
                                    ClusterMetadataService.instance().placementProvider());
        }

       public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof PrepareLeave;
            PrepareLeave transformation = (PrepareLeave) t;
            return NodeId.serializer.serializedSize(transformation.nodeId, version)
                   + TypeSizes.sizeof(transformation.force);
        }
    }

    public String toString()
    {
        return "PrepareLeave{" +
               "id=" + nodeId +
               ", force=" + force +
               '}';
    }

    public static class StartLeave extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public StartLeave(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.START_LEAVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer
                   .with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.MID_LEAVE)))
                   .withNodeState(nodeId, NodeState.LEAVING);
        }

        public static final class Serializer extends SerializerBase<StartLeave>
        {
            StartLeave construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new StartLeave(nodeId, delta, lockKey);
            }
        }
    }

    public static class MidLeave extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public MidLeave(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.MID_LEAVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.FINISH_LEAVE)));
        }

        public static final class Serializer extends SerializerBase<MidLeave>
        {
            MidLeave construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new MidLeave(nodeId, delta, lockKey);
            }
        }
    }

    public static class FinishLeave extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public FinishLeave(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, true);
        }

        @Override
        public Kind kind()
        {
            return Kind.FINISH_LEAVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.left(nodeId)
                              .with(prev.inProgressSequences.without(nodeId));
        }

        public static class Serializer extends SerializerBase<FinishLeave>
        {
            FinishLeave construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new FinishLeave(nodeId, delta, lockKey);
            }
        }
    }
}
