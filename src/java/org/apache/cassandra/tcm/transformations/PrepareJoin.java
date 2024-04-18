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
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.PlacementTransitionPlan;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

/**
 * Create a plan for adding a new node and bootstrapping it, then start to execute that plan.
 * Creating the plan involves adding the joining node's tokens to the current tokenmap and generating the set of
 * required operations to be applied to existing data placements.
 * Specifically, this involves splitting the ranges containing the new node's tokens and adding the new node as a
 * write replica for those ranges. After the new node has completed streaming for bootstrap, it is made a read
 * replica for those ranges.
 *
 * For example, if we start with a (subset of a) ring where A has token 100, B has token 200 and C has token 300
 * with RF=2 then both the read and write placements will contain:
 *
 *  (0, 100]    : {A,B}
 *  (100, 200]  : {B,C}
 *
 * If we then begin bootstrap X with token 150, the first step is to range split the existing ranges in line without
 * changing any ownership. At this point, both the read and write placements will contain:
 *
 *  (0, 100]    : {A,B}
 *  (100, 150]  : {B,C}
 *  (150, 200]  : {B,C}
 *
 * Next, the new node is added to the write groups for the ranges it is acquiring. After this step, the read placement
 * is unchanged, while the write placement will contain:
 *
 *  (0, 100]    : {A,B,X}
 *  (100, 150]  : {B,C,X}
 *  (150, 200]  : {B,C}
 *
 * Once X completes bootstrapping, it is added to the corresponding read groups, replacing nodes which are no longer
 * replicas for those ranges. Now both the read and write placements will contain:
 *
 *  (0, 100]    : {A,X}
 *  (100, 150]  : {X,B}
 *  (150, 200]  : {B,C}
 *
 * Currently, we do not automatically clean up unowned ranges after bootstrap, so C will still hold data for
 * (100, 150] on disk even though it is no longer a replica for that range.
 */
public class PrepareJoin implements Transformation
{
    public static final Serializer<PrepareJoin> serializer = new Serializer<PrepareJoin>()
    {
        public PrepareJoin construct(NodeId nodeId, Set<Token> tokens, PlacementProvider placementProvider, boolean joinTokenRing, boolean streamData)
        {
            return new PrepareJoin(nodeId, tokens, placementProvider, joinTokenRing, streamData);
        }
    };

    protected final NodeId nodeId;
    protected final Set<Token> tokens;
    protected final PlacementProvider placementProvider;
    protected final boolean joinTokenRing;
    protected final boolean streamData;

    public PrepareJoin(NodeId nodeId,
                       Set<Token> tokens,
                       PlacementProvider placementProvider,
                       boolean joinTokenRing,
                       boolean streamData)
    {
        this.nodeId = nodeId;
        this.tokens = tokens;
        this.placementProvider = placementProvider;
        this.joinTokenRing = joinTokenRing;
        this.streamData = streamData;
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_JOIN;
    }

    public NodeId nodeId()
    {
        return nodeId;
    }

    private static final Set<NodeState> ALLOWED_STATES = ImmutableSet.of(NodeState.REGISTERED, NodeState.LEFT);

    @Override
    public Result execute(ClusterMetadata prev)
    {
        if (!ALLOWED_STATES.contains(prev.directory.peerState(nodeId)))
            return new Rejected(INVALID, String.format("Rejecting this plan as the node %s is in state %s",
                                                       nodeId, prev.directory.peerState(nodeId)));

        PlacementTransitionPlan transitionPlan = placementProvider.planForJoin(prev, nodeId, tokens, prev.schema.getKeyspaces());

        LockedRanges.AffectedRanges rangesToLock = transitionPlan.affectedRanges();
        LockedRanges.Key alreadyLockedBy = prev.lockedRanges.intersects(rangesToLock);
        if (!alreadyLockedBy.equals(LockedRanges.NOT_LOCKED))
        {
            return new Rejected(INVALID, String.format("Rejecting this plan as it interacts with a range locked by %s (locked: %s, new: %s)",
                                                       alreadyLockedBy, prev.lockedRanges, rangesToLock));
        }

        LockedRanges.Key lockKey = LockedRanges.keyFor(prev.nextEpoch());
        StartJoin startJoin = new StartJoin(nodeId, transitionPlan.addToWrites(), lockKey);
        MidJoin midJoin = new MidJoin(nodeId, transitionPlan.moveReads(), lockKey);
        FinishJoin finishJoin = new FinishJoin(nodeId, tokens, transitionPlan.removeFromWrites(), lockKey);

        BootstrapAndJoin plan = BootstrapAndJoin.newSequence(prev.nextEpoch(),
                                                             lockKey,
                                                             transitionPlan.toSplit,
                                                             startJoin, midJoin, finishJoin,
                                                             joinTokenRing, streamData);
        if (!prev.tokenMap.isEmpty())
        {
            Result res = assertPreExistingWriteReplica(prev.placements, transitionPlan);
            if (res != null)
                return res;
        }

        LockedRanges newLockedRanges = prev.lockedRanges.lock(lockKey, rangesToLock);
        DataPlacements startingPlacements = transitionPlan.toSplit.apply(prev.nextEpoch(), prev.placements);
        ClusterMetadata.Transformer proposed = prev.transformer()
                                                   .with(newLockedRanges)
                                                   .with(startingPlacements)
                                                   .with(prev.inProgressSequences.with(nodeId, plan));

        return Transformation.success(proposed, rangesToLock);
    }

    Result assertPreExistingWriteReplica(DataPlacements placements, PlacementTransitionPlan transitionPlan)
    {
        return PlacementTransitionPlan.assertPreExistingWriteReplica(placements, transitionPlan);
    }

    public static abstract class Serializer<T extends PrepareJoin> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            T transformation = (T) t;
            NodeId.serializer.serialize(transformation.nodeId, out, version);
            out.writeInt(transformation.tokens.size());
            for(Token token : transformation.tokens)
                Token.metadataSerializer.serialize(token, out, version);
            out.writeBoolean(transformation.joinTokenRing);
            out.writeBoolean(transformation.streamData);
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId id = NodeId.serializer.deserialize(in, version);
            int numTokens = in.readInt();
            Set<Token> tokens = new HashSet<>(numTokens);
            IPartitioner partitioner = ClusterMetadata.current().partitioner;
            for (int i = 0; i < numTokens; i++)
                tokens.add(Token.metadataSerializer.deserialize(in, partitioner, version));
            boolean joinTokenRing = in.readBoolean();
            boolean streamData = in.readBoolean();
            return construct(id, tokens, ClusterMetadataService.instance().placementProvider(), joinTokenRing, streamData);
        }

        public long serializedSize(Transformation t, Version version)
        {
            T transformation = (T) t;
            long size = NodeId.serializer.serializedSize(transformation.nodeId, version);
            size += TypeSizes.INT_SIZE;
            for (Token token : transformation.tokens)
                size += Token.metadataSerializer.serializedSize(token, version);
            size += TypeSizes.BOOL_SIZE * 2;
            return size;
        }

        public abstract T construct(NodeId nodeId,
                                    Set<Token> tokens,
                                    PlacementProvider placementProvider,
                                    boolean joinTokenRing,
                                    boolean streamData);

    }

    public String toString()
    {
        return "PrepareJoin{" +
               "nodeId=" + nodeId +
               ", tokens=" + tokens +
               ", joinTokenRing=" + joinTokenRing +
               ", streamData=" + streamData +
               '}';
    }

    public static class StartJoin extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public StartJoin(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.START_JOIN;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.withNodeState(nodeId, NodeState.BOOTSTRAPPING)
                              .with(prev.inProgressSequences.with(nodeId, (BootstrapAndJoin plan) -> plan.advance(prev.nextEpoch())));
        }

        public static final class Serializer extends SerializerBase<StartJoin>
        {
            StartJoin construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new StartJoin(nodeId, delta, lockKey);
            }
        }
    }

    public static class MidJoin extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public MidJoin(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.MID_JOIN;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.with(prev.inProgressSequences.with(nodeId, (BootstrapAndJoin plan) -> plan.advance(prev.nextEpoch())));
        }

        public static final class Serializer extends SerializerBase<MidJoin>
        {
            MidJoin construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new MidJoin(nodeId, delta, lockKey);
            }
        }
    }

    public static class FinishJoin extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();
        public final Set<Token> tokens;

        public FinishJoin(NodeId nodeId, Set<Token> tokens, PlacementDeltas delta, LockedRanges.Key unlockKey)
        {
            super(nodeId, delta, unlockKey, true);
            this.tokens = tokens;
        }

        @Override
        public Kind kind()
        {
            return Kind.FINISH_JOIN;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.join(nodeId)
                              .proposeToken(nodeId, tokens)
                              .addToRackAndDC(nodeId)
                              .with(prev.inProgressSequences.without(nodeId));
        }

        public static final class Serializer extends SerializerBase<FinishJoin>
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                super.serialize(t, out, version);
                Set<Token> tokens = ((FinishJoin)t).tokens;
                out.writeUnsignedVInt32(tokens.size());
                for (Token token : tokens)
                    Token.metadataSerializer.serialize(token, out, version);
            }

            @Override
            public FinishJoin deserialize(DataInputPlus in, Version version) throws IOException
            {
                NodeId nodeId = NodeId.serializer.deserialize(in, version);
                PlacementDeltas delta = PlacementDeltas.serializer.deserialize(in, version);
                LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
                int numTokens = in.readUnsignedVInt32();
                Set<Token> tokens = new HashSet<>();
                IPartitioner partitioner = ClusterMetadata.current().partitioner;
                for (int i = 0; i < numTokens; i++)
                    tokens.add(Token.metadataSerializer.deserialize(in, partitioner, version));
                return new FinishJoin(nodeId, tokens, delta, lockKey);
            }

            @Override
            public long serializedSize(Transformation t, Version version)
            {
                long size = super.serializedSize(t, version);
                Set<Token> tokens = ((FinishJoin)t).tokens;
                size += TypeSizes.sizeofUnsignedVInt(tokens.size());
                for (Token token : tokens)
                    size += Token.metadataSerializer.serializedSize(token, version);
                return size;
            }

            FinishJoin construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                throw new IllegalStateException();
            }
        }
    }
}