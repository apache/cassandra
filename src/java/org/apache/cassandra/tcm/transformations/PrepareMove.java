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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.PlacementTransitionPlan;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.Move;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class PrepareMove implements Transformation
{
    public static final Serializer<PrepareMove> serializer = new Serializer<PrepareMove>();

    protected final NodeId nodeId;
    protected final Set<Token> tokens;
    protected final PlacementProvider placementProvider;
    protected final boolean streamData;

    public PrepareMove(NodeId nodeId,
                       Set<Token> tokens,
                       PlacementProvider placementProvider,
                       boolean streamData)
    {
        this.nodeId = nodeId;
        this.tokens = tokens;
        this.placementProvider = placementProvider;
        this.streamData = streamData;
    }

    public String toString()
    {
        return "PrepareMove{" +
               "nodeId=" + nodeId +
               ", tokens=" + tokens +
               ", placementProvider=" + placementProvider +
               ", streamData=" + streamData +
               '}';
    }

    public Kind kind()
    {
        return Kind.PREPARE_MOVE;
    }

    public Result execute(ClusterMetadata prev)
    {
        PlacementTransitionPlan transitionPlan = placementProvider.planForMove(prev, nodeId, tokens, prev.schema.getKeyspaces());
        LockedRanges.AffectedRanges rangesToLock = transitionPlan.affectedRanges();
        LockedRanges.Key alreadyLockedBy = prev.lockedRanges.intersects(rangesToLock);

        if (!alreadyLockedBy.equals(LockedRanges.NOT_LOCKED))
        {
            return new Rejected(String.format("Rejecting this plan as it interacts with a range locked by %s (locked: %s, new: %s)",
                                              alreadyLockedBy, prev.lockedRanges, rangesToLock));
        }

        LockedRanges.Key lockKey = LockedRanges.keyFor(prev.nextEpoch());
        StartMove startMove = new StartMove(nodeId, transitionPlan.addToWrites(), lockKey);
        MidMove midMove = new MidMove(nodeId, transitionPlan.moveReads(), lockKey);
        FinishMove finishMove = new FinishMove(nodeId, tokens, transitionPlan.removeFromWrites(), lockKey);

        ProgressBarrier barrier = ProgressBarrier.immediate(rangesToLock.toPeers(prev.placements, prev.directory));
        Move sequence = new Move(tokens,
                                 barrier,
                                 lockKey,
                                 Kind.START_MOVE,
                                 transitionPlan.toSplit,
                                 startMove,
                                 midMove,
                                 finishMove,
                                 false);

        return success(prev.transformer()
                           .withNodeState(nodeId, NodeState.MOVING)
                           .proposeToken(nodeId, tokens)
                           .with(prev.lockedRanges.lock(lockKey, rangesToLock))
                           .with(transitionPlan.toSplit.apply(prev.placements))
                           .with(prev.inProgressSequences.with(nodeId, sequence)),
                       rangesToLock);
    }

    public static class Serializer<T extends PrepareMove> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            T transformation = (T) t;
            NodeId.serializer.serialize(transformation.nodeId, out, version);
            out.writeInt(transformation.tokens.size());
            for(Token token : transformation.tokens)
                Token.metadataSerializer.serialize(token, out, version);
            out.writeBoolean(transformation.streamData);
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId id = NodeId.serializer.deserialize(in, version);
            int numTokens = in.readInt();
            Set<Token> tokens = new HashSet<>(numTokens);
            for (int i=0;i<numTokens;i++)
                tokens.add(Token.metadataSerializer.deserialize(in, version));
            boolean streamData = in.readBoolean();
            return construct(id, tokens, ClusterMetadataService.instance().placementProvider(), streamData);
        }

        public long serializedSize(Transformation t, Version version)
        {
            T transformation = (T) t;
            long size = NodeId.serializer.serializedSize(transformation.nodeId, version);
            size += TypeSizes.INT_SIZE;
            for (Token token : transformation.tokens)
                size += Token.metadataSerializer.serializedSize(token, version);
            size += TypeSizes.BOOL_SIZE;
            return size;
        }

        public T construct(NodeId nodeId,
                           Set<Token> tokens,
                           PlacementProvider placementProvider,
                           boolean streamData)
        {
            return (T) new PrepareMove(nodeId, tokens, placementProvider, streamData);
        }

    }

    public static class StartMove extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        StartMove(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.START_MOVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.MID_MOVE)));
        }

        public static final class Serializer extends SerializerBase<PrepareMove.StartMove>
        {
            StartMove construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new PrepareMove.StartMove(nodeId, delta, lockKey);
            }
        }
    }

    public static class MidMove extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        MidMove(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.MID_MOVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.FINISH_MOVE)));
        }

        public static final class Serializer extends SerializerBase<PrepareMove.MidMove>
        {
            MidMove construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new PrepareMove.MidMove(nodeId, delta, lockKey);
            }
        }
    }

    public static class FinishMove extends ApplyPlacementDeltas
    {
        public static final Serializer serializer = new Serializer();

        public final Collection<Token> newTokens;
        FinishMove(NodeId nodeId, Collection<Token> newTokens, PlacementDeltas delta, LockedRanges.Key lockKey)
        {
            super(nodeId, delta, lockKey, true);
            this.newTokens = newTokens;
        }

        @Override
        public Kind kind()
        {
            return Kind.FINISH_MOVE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.unproposeTokens(nodeId)
                              .proposeToken(nodeId, newTokens)
                              .join(nodeId)
                              .with(prev.inProgressSequences.without(nodeId));
        }

        public static final class Serializer extends SerializerBase<PrepareMove.FinishMove>
        {
            @Override
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                super.serialize(t, out, version);
                Collection<Token> tokens = ((FinishMove)t).newTokens;
                out.writeUnsignedVInt32(tokens.size());
                for (Token token : tokens)
                    Token.metadataSerializer.serialize(token, out, version);
            }

            public FinishMove deserialize(DataInputPlus in, Version version) throws IOException
            {
                NodeId nodeId = NodeId.serializer.deserialize(in, version);
                PlacementDeltas delta = PlacementDeltas.serializer.deserialize(in, version);
                LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
                int numTokens = in.readUnsignedVInt32();
                List<Token> tokens = new ArrayList<>();
                for (int i = 0; i < numTokens; i++)
                    tokens.add(Token.metadataSerializer.deserialize(in, version));

                return new FinishMove(nodeId, tokens, delta, lockKey);
            }

            public long serializedSize(Transformation t, Version version)
            {
                long size = super.serializedSize(t, version);
                Collection<Token> tokens = ((FinishMove)t).newTokens;
                size += TypeSizes.sizeofUnsignedVInt(tokens.size());
                for (Token token : tokens)
                    size += Token.metadataSerializer.serializedSize(token, version);
                return size;
            }

            FinishMove construct(NodeId nodeId, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                throw new IllegalStateException();
            }
        }
    }
}
