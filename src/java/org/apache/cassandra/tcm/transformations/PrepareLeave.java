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
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class PrepareLeave implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareLeave.class);
    public static final Serializer<PrepareLeave> serializer = new Serializer<PrepareLeave>()
    {
        @Override
        public PrepareLeave construct(NodeId leaving, boolean force, PlacementProvider placementProvider, LeaveStreams.Kind streamKind)
        {
            return new PrepareLeave(leaving, force, placementProvider, streamKind);
        }
    };

    protected final NodeId leaving;
    protected final boolean force;
    protected final PlacementProvider placementProvider;
    protected final LeaveStreams.Kind streamKind;

    public PrepareLeave(NodeId leaving, boolean force, PlacementProvider placementProvider, LeaveStreams.Kind streamKind)
    {
        this.leaving = leaving;
        this.force = force;
        this.placementProvider = placementProvider;
        this.streamKind = streamKind;
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_LEAVE;
    }

    public NodeId nodeId()
    {
        return leaving;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (prev.isCMSMember(prev.directory.endpoint(leaving)))
            return new Rejected(INVALID, String.format("Rejecting this plan as the node %s is still a part of CMS.", leaving));

        if (prev.directory.peerState(leaving) != NodeState.JOINED)
            return new Rejected(INVALID, String.format("Rejecting this plan as the node %s is in state %s", leaving, prev.directory.peerState(leaving)));

        ClusterMetadata proposed = prev.transformer().proposeRemoveNode(leaving).build().metadata;

        if (!force && !validateReplicationForDecommission(proposed))
            return new Rejected(INVALID, "Not enough live nodes to maintain replication factor after decommission.");

        if (proposed.directory.isEmpty())
            return new Rejected(INVALID, "No peers registered, at least local node should be");

        PlacementTransitionPlan transitionPlan = placementProvider.planForDecommission(prev,
                                                                                       leaving,
                                                                                       prev.schema.getKeyspaces());

        LockedRanges.AffectedRanges rangesToLock = transitionPlan.affectedRanges();
        LockedRanges.Key alreadyLockedBy = prev.lockedRanges.intersects(rangesToLock);
        if (!alreadyLockedBy.equals(LockedRanges.NOT_LOCKED))
        {
            return new Rejected(INVALID, String.format("Rejecting this plan as it interacts with a range locked by %s (locked: %s, new: %s)",
                                              alreadyLockedBy, prev.lockedRanges, rangesToLock));
        }

        PlacementDeltas startDelta = transitionPlan.addToWrites();
        PlacementDeltas midDelta = transitionPlan.moveReads();
        PlacementDeltas finishDelta = transitionPlan.removeFromWrites();
        transitionPlan.assertPreExistingWriteReplica(prev.placements);

        LockedRanges.Key unlockKey = LockedRanges.keyFor(proposed.epoch);

        StartLeave start = new StartLeave(leaving, startDelta, unlockKey);
        MidLeave mid = new MidLeave(leaving, midDelta, unlockKey);
        FinishLeave leave = new FinishLeave(leaving, finishDelta, unlockKey);

        UnbootstrapAndLeave plan = UnbootstrapAndLeave.newSequence(prev.nextEpoch(),
                                                                   unlockKey,
                                                                   start, mid, leave,
                                                                   streamKind.supplier.get());

        // note: we throw away the state with the leaving node's tokens removed. It's only
        // used to produce the operation plan.
        ClusterMetadata.Transformer next = prev.transformer()
                                               .with(prev.lockedRanges.lock(unlockKey, rangesToLock))
                                               .with(prev.inProgressSequences.with(leaving, plan));
        return Transformation.success(next, rangesToLock);
    }

    private boolean validateReplicationForDecommission(ClusterMetadata proposed)
    {
        String dc = proposed.directory.location(leaving).datacenter;
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
                // from what i understand this is not necessarily decommissioning of the cms node; every node has this ks now
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

    public static abstract class Serializer<T extends PrepareLeave> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            T transformation = (T) t;
            NodeId.serializer.serialize(transformation.leaving, out, version);
            out.writeBoolean(transformation.force);
            out.writeUTF(transformation.streamKind.toString());
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId id = NodeId.serializer.deserialize(in, version);
            boolean force = in.readBoolean();
            LeaveStreams.Kind streamsKind = LeaveStreams.Kind.valueOf(in.readUTF());

            return construct(id,
                             force,
                             ClusterMetadataService.instance().placementProvider(),
                             streamsKind);
        }

        public long serializedSize(Transformation t, Version version)
        {
            T transformation = (T) t;
            return NodeId.serializer.serializedSize(transformation.leaving, version)
                   + TypeSizes.sizeof(transformation.force)
                   + TypeSizes.sizeof(transformation.streamKind.toString());
        }

        public abstract T construct(NodeId leaving, boolean force, PlacementProvider placementProvider, LeaveStreams.Kind streamKind);

    }

    @Override
    public String toString()
    {
        return "PrepareLeave{" +
               "leaving=" + leaving +
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
                   .with(prev.inProgressSequences.with(nodeId, (UnbootstrapAndLeave plan) -> plan.advance(prev.nextEpoch())))
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
            return transformer.with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch())));
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
