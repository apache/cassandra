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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.tcm.sequences.BootstrapAndReplace;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ProgressBarrier;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.tcm.Transformation.Kind.START_REPLACE;

public class PrepareReplace implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareReplace.class);

    public static final Serializer serializer = new Serializer();

    private final NodeId replaced;
    private final NodeId replacement;
    private final PlacementProvider placementProvider;
    private final boolean joinTokenRing;
    private final boolean streamData;

    public PrepareReplace(NodeId replaced,
                          NodeId replacement,
                          PlacementProvider placementProvider,
                          boolean joinTokenRing,
                          boolean streamData)
    {
        this.replaced = replaced;
        this.replacement = replacement;
        this.placementProvider = placementProvider;
        this.joinTokenRing = joinTokenRing;
        this.streamData = streamData;
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_REPLACE;
    }

    public NodeId replacement()
    {
        return replacement;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        LockedRanges.Key unlockKey = LockedRanges.keyFor(prev.nextEpoch());
        LockedRanges lockedRanges = prev.lockedRanges;

        PlacementTransitionPlan transitionPlan = placementProvider.planForReplacement(prev,
                                                                                      replaced,
                                                                                      replacement,
                                                                                      prev.schema.getKeyspaces());
        PlacementDeltas.Builder addNewNodeToWrites = PlacementDeltas.builder();
        PlacementDeltas.Builder addNewNodeToReads = PlacementDeltas.builder();
        PlacementDeltas.Builder removeOldNodeFromWrites = PlacementDeltas.builder();

        // Only addition of the new node to the write groups is done as a consequence of the first transformation. Adding the new
        // node to the various read groups is deferred until the second transformation, after bootstrap. Also, track which ranges
        // are going to be affected by this operation (i.e. which will be the "pending" ranges for the new node. If the
        // plan is accepted those ranges will be locked to prevent other plans submitted later from interacting with the
        // same ranges.
        LockedRanges.AffectedRangesBuilder affectedRanges = LockedRanges.AffectedRanges.builder();
        transitionPlan.toMaximal.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
            addNewNodeToWrites.put(replication, delta.onlyWrites().onlyAdditions());
            addNewNodeToReads.put(replication, delta.onlyReads());
        });

        transitionPlan.toFinal.forEach((replication, delta) -> {
            delta.reads.additions.flattenValues().forEach(r -> affectedRanges.add(replication, r.range()));
            addNewNodeToReads.put(replication, delta.onlyReads());
            removeOldNodeFromWrites.put(replication, delta.onlyWrites().onlyRemovals());
        });

        LockedRanges.AffectedRanges rangesToLock = affectedRanges.build();
        LockedRanges.Key alreadyLockedBy = lockedRanges.intersects(rangesToLock);

        if (!alreadyLockedBy.equals(LockedRanges.NOT_LOCKED))
        {
            return new Rejected(String.format("Rejecting this plan as it interacts with a range locked by %s (locked: %s, new: %s)",
                                              alreadyLockedBy, lockedRanges, rangesToLock));
        }

        StartReplace start = new StartReplace(replaced, replacement, addNewNodeToWrites.build(), unlockKey);
        MidReplace mid = new MidReplace(replaced, replacement, addNewNodeToReads.build(), unlockKey);
        FinishReplace finish = new FinishReplace(replaced, replacement, removeOldNodeFromWrites.build(), unlockKey);

        Set<Token> tokens = new HashSet<>(prev.tokenMap.tokens(replaced));
        ImmutableSet<NodeId> toWatermark = rangesToLock.toPeers(prev.placements, prev.directory);
        ProgressBarrier barrier = new ProgressBarrier(prev.nextEpoch(), toWatermark, true);
        BootstrapAndReplace plan = new BootstrapAndReplace(barrier,
                                                           unlockKey,
                                                           START_REPLACE,
                                                           tokens,
                                                           start, mid, finish,
                                                           joinTokenRing,
                                                           streamData);




        LockedRanges newLockedRanges = lockedRanges.lock(unlockKey, rangesToLock);
        ClusterMetadata.Transformer proposed = prev.transformer()
                                                   .with(newLockedRanges)
                                                   .with(prev.inProgressSequences.with(replacement(), plan));
        logger.info("Node {} is replacing {}, tokens {}", prev.directory.endpoint(replacement), prev.directory.endpoint(replaced), prev.tokenMap.tokens(replaced));
        return success(proposed, rangesToLock);
    }

    public String toString()
    {
        return "PrepareReplace{" +
               "replaced=" + replaced+
               ", replacement=" + replacement +
               '}';
    }


    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, PrepareReplace>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof PrepareReplace;
            PrepareReplace transformation = (PrepareReplace) t;
            NodeId.serializer.serialize(transformation.replaced, out, version);
            NodeId.serializer.serialize(transformation.replacement, out, version);
            out.writeBoolean(transformation.joinTokenRing);
            out.writeBoolean(transformation.streamData);
        }

        public PrepareReplace deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId replaced = NodeId.serializer.deserialize(in, version);
            NodeId replacement = NodeId.serializer.deserialize(in, version);
            boolean joinTokenRing = in.readBoolean();
            boolean streamData = in.readBoolean();
            return new PrepareReplace(replaced,
                                      replacement,
                                      ClusterMetadataService.instance().placementProvider(),
                                      joinTokenRing,
                                      streamData);
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof PrepareReplace;
            PrepareReplace transformation = (PrepareReplace) t;
            return NodeId.serializer.serializedSize(transformation.replaced, version) +
                   NodeId.serializer.serializedSize(transformation.replacement, version) +
                   (TypeSizes.BOOL_SIZE * 2);
        }
    }

    public static abstract class BaseReplaceTransformation extends ApplyPlacementDeltas
    {
        protected final NodeId replaced;

        public BaseReplaceTransformation(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key unlockKey, boolean unlock)
        {
            super(replacement, delta, unlockKey, unlock);
            this.replaced = replaced;
        }

        public NodeId replacement()
        {
            return nodeId;
        }

        public NodeId replaced()
        {
            return replaced;
        }
    }

    // Analogous to ApplyPlacementDeltas.SerializerBase. The difference is that classes which extend
    // BaseReplaceTransformation have 2 NodeIds, whereas the equivalents from PrepareJoin and PrepareLeave only have a single id
    // TODO we can probably make the hierarchy of abstract/concrete transformations and associated serializers a bit less convoluted
    public abstract static class SerializerBase<T extends BaseReplaceTransformation> implements AsymmetricMetadataSerializer<Transformation, T>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            BaseReplaceTransformation change = (T) t;
            NodeId.serializer.serialize(change.replaced(), out, version);
            NodeId.serializer.serialize(change.replacement(), out, version);
            PlacementDeltas.serializer.serialize(change.delta, out, version);
            LockedRanges.Key.serializer.serialize(change.lockKey, out, version);
        }

        public T deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId replaced = NodeId.serializer.deserialize(in, version);
            NodeId replacement = NodeId.serializer.deserialize(in, version);
            PlacementDeltas delta = PlacementDeltas.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            return construct(replaced, replacement, delta, lockKey);
        }

        public long serializedSize(Transformation t, Version version)
        {
            BaseReplaceTransformation change = (T) t;

            return NodeId.serializer.serializedSize(change.replaced(), version) +
                   NodeId.serializer.serializedSize(change.replacement(), version) +
                   PlacementDeltas.serializer.serializedSize(change.delta, version) +
                   LockedRanges.Key.serializer.serializedSize(change.lockKey, version);
        }

        abstract T construct(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key lockKey);
    }

    //  This is functionally identical to StartJoin. They only exist as distinct transformations for clarity.
    public static class StartReplace extends BaseReplaceTransformation
    {
        public static final Serializer serializer = new Serializer();

        public StartReplace(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key unlockKey)
        {
            super(replaced, replacement, delta, unlockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.START_REPLACE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.withNodeState(replacement(), NodeState.BOOTSTRAPPING)
                              .with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.MID_REPLACE)));
        }

        @Override
        public String toString()
        {
            return "StartReplace{" +
                   ", delta=" + delta +
                   ", lockKey=" + lockKey +
                   ", unlock=" + unlock +
                   ", replaced=" + replaced +
                   ", replacement=" + replacement() +
                   '}';
        }

        public static final class Serializer extends PrepareReplace.SerializerBase<StartReplace>
        {
            @Override
            public StartReplace construct(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key unlockKey)
            {
                return new StartReplace(replaced, replacement, delta, unlockKey);
            }
        }
    }

    public static class MidReplace extends BaseReplaceTransformation
    {
        public static final Serializer serializer = new Serializer();

        public MidReplace(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key unlockKey)
        {
            super(replaced, replacement, delta, unlockKey, false);
        }

        @Override
        public Kind kind()
        {
            return Kind.MID_REPLACE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer
                   .with(prev.inProgressSequences.with(nodeId, (plan) -> plan.advance(prev.nextEpoch(), Kind.FINISH_REPLACE)));
        }

        @Override
        public String toString()
        {
            return "MidReplace{" +
                   ", delta=" + delta +
                   ", lockKey=" + lockKey +
                   ", unlock=" + unlock +
                   ", replaced=" + replaced +
                   ", replacement=" + replacement() +
                   '}';
        }

        public static final class Serializer extends PrepareReplace.SerializerBase<MidReplace>
        {
            @Override
            MidReplace construct(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new MidReplace(replaced, replacement, delta, lockKey);
            }
        }
    }

    public static class FinishReplace extends BaseReplaceTransformation
    {
        public static final Serializer serializer = new Serializer();

        public FinishReplace(NodeId replaced,
                             NodeId replacement,
                             PlacementDeltas delta,
                             LockedRanges.Key unlockKey)
        {
            super(replaced, replacement, delta, unlockKey, true);
        }

        @Override
        public Kind kind()
        {
            return Kind.FINISH_REPLACE;
        }

        @Override
        public ClusterMetadata.Transformer transform(ClusterMetadata prev, ClusterMetadata.Transformer transformer)
        {
            return transformer.replaced(replaced, replacement())
                              .with(prev.inProgressSequences.without(nodeId));
        }

        @Override
        public String toString()
        {
            return "FinishReplace{" +
                   "replaced=" + replaced +
                   ", replacement=" + replacement() +
                   ", delta=" + delta +
                   ", unlockKey=" + lockKey +
                   '}';
        }

        public static final class Serializer extends PrepareReplace.SerializerBase<FinishReplace>
        {
            @Override
            FinishReplace construct(NodeId replaced, NodeId replacement, PlacementDeltas delta, LockedRanges.Key lockKey)
            {
                return new FinishReplace(replaced, replacement, delta, lockKey);
            }
        }
    }
}