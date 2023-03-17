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

package org.apache.cassandra.tcm.sequences;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

public class Move implements InProgressSequence<Move>
{
    private static final Logger logger = LoggerFactory.getLogger(Move.class);
    public static final Serializer serializer = new Serializer();

    public final Collection<Token> tokens;
    public final ProgressBarrier barrier;
    public final LockedRanges.Key lockKey;
    public final Transformation.Kind next;

    public final PlacementDeltas toSplitRanges;
    public final PrepareMove.StartMove startMove;
    public final PrepareMove.MidMove midMove;
    public final PrepareMove.FinishMove finishMove;
    public final boolean streamData;


    public Move(Collection<Token> tokens,
                ProgressBarrier barrier,
                LockedRanges.Key lockKey,
                Transformation.Kind next,
                PlacementDeltas toSplitRanges,
                PrepareMove.StartMove startMove,
                PrepareMove.MidMove midMove,
                PrepareMove.FinishMove finishMove,
                boolean streamData)
    {
        this.tokens = tokens;
        this.barrier = barrier;
        this.lockKey = lockKey;
        this.next = next;
        this.toSplitRanges = toSplitRanges;
        this.startMove = startMove;
        this.midMove = midMove;
        this.finishMove = finishMove;

        this.streamData = streamData;
    }

    @Override
    public InProgressSequences.Kind kind()
    {
        return InProgressSequences.Kind.MOVE;
    }

    @Override
    public ProgressBarrier barrier()
    {
        return barrier;
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return next;
    }

    @Override
    public boolean executeNext()
    {
        switch (next)
        {
            case START_MOVE:
                try
                {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    logger.info("Moving {} from {} to {}.",
                                metadata.directory.endpoint(startMove.nodeId()),
                                metadata.tokenMap.tokens(startMove.nodeId()),
                                finishMove.newTokens);
                    ClusterMetadataService.instance().commit(startMove);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }
                break;
            case MID_MOVE:
                try
                {
                    logger.info("fetching new ranges and streaming old ranges");
                    StreamPlan streamPlan = new StreamPlan(StreamOperation.RELOCATION);
                    Keyspaces keyspaces = Schema.instance.getNonLocalStrategyKeyspaces();
                    Map<ReplicationParams, EndpointsByReplica> movementMap = movementMap(FailureDetector.instance,
                                                                                         ClusterMetadata.current().placements,
                                                                                         startMove.delta())
                                                                             .asMap();
                    for (KeyspaceMetadata ks : keyspaces)
                    {
                        ReplicationParams replicationParams = ks.params.replication;
                        if (replicationParams.isMeta())
                            continue;
                        EndpointsByReplica endpoints = movementMap.get(replicationParams);
                        for (Map.Entry<Replica, Replica> e : endpoints.flattenEntries())
                        {
                            Replica source = e.getKey();
                            Replica destination = e.getValue();
                            assert !source.endpoint().equals(destination.endpoint()) : String.format("Source should not be the same as destionation", source, destination);
                            if (source.isSelf())
                                streamPlan.transferRanges(destination.endpoint(), ks.name, RangesAtEndpoint.of(destination));
                            else if (destination.isSelf())
                                streamPlan.requestRanges(source.endpoint(), ks.name, RangesAtEndpoint.of(destination), RangesAtEndpoint.empty(destination.endpoint()));
                            else
                                throw new IllegalStateException("Node should be either source or desintation in the movement map " + endpoints);
                        }
                    }

                    streamPlan.execute().get();
                    StorageService.instance.repairPaxosForTopologyChange("move");
                }
                catch (InterruptedException e)
                {
                    return true;
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException("Unable to move", e);
                }

                try
                {
                    ClusterMetadataService.instance().commit(midMove);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }
                break;
            case FINISH_MOVE:
                try
                {
                    SystemKeyspace.updateTokens(tokens);
                    ClusterMetadataService.instance().commit(finishMove);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }

                break;
            default:
                throw new IllegalStateException("Can't proceed with join from " + next);
        }

        return true;
    }

    private MovementMap movementMap(IFailureDetector fd, DataPlacements placements, PlacementDeltas toStart)
    {
        MovementMap.Builder allMovements = MovementMap.builder();
        toStart.forEach((params, delta) -> {
            RangesByEndpoint targets = delta.writes.additions;
            PlacementForRange oldOwners = placements.get(params).reads;
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            targets.flattenValues().forEach(destination -> {
                EndpointsForRange candidates = oldOwners.forRange(destination.range());
                Optional<Replica> maybeSelf = candidates.stream().filter(Replica::isSelf).findFirst();
                if (maybeSelf.isPresent())
                {
                    movements.put(maybeSelf.get(), destination);
                    return;
                }

                assert destination.isSelf();
                for (Replica source : candidates)
                {
                    if ( fd.isAlive(source.endpoint()))
                    {
                        movements.put(source, destination);
                        return;
                    }
                }
                throw new IllegalStateException(String.format("No live sources for the range %s. Tried: %s",
                                                              destination.range(), oldOwners.forRange(destination.range())));
            });
            allMovements.put(params, movements.build());
        });

        return allMovements.build();
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;

        switch (next)
        {
            case FINISH_MOVE:
                placements = midMove.inverseDelta().apply(placements);
            case MID_MOVE:
                placements = startMove.inverseDelta().apply(placements);
            case START_MOVE:
                placements = toSplitRanges.invert().apply(placements);
                break;
            default:
                throw new IllegalStateException("Can't revert move from " + next);
        }

        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .withNodeState(startMove.nodeId(), NodeState.JOINED)
                       .with(placements)
                       .with(newLockedRanges);
    }

    public Move advance(Epoch waitForWatermark, Transformation.Kind next)
    {
        return new Move(tokens,
                        barrier().withNewEpoch(waitForWatermark), lockKey, next,
                        toSplitRanges, startMove, midMove, finishMove,
                        streamData);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, Move>
    {
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            Move plan = (Move) t;
            out.writeBoolean(plan.streamData);

            ProgressBarrier.serializer.serialize(plan.barrier(), out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            PlacementDeltas.serializer.serialize(plan.toSplitRanges, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);

            if (plan.next.ordinal() >= Transformation.Kind.START_MOVE.ordinal())
                PrepareMove.StartMove.serializer.serialize(plan.startMove, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.MID_MOVE.ordinal())
                PrepareMove.MidMove.serializer.serialize(plan.midMove, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.FINISH_MOVE.ordinal())
                PrepareMove.FinishMove.serializer.serialize(plan.finishMove, out, version);

            out.writeUnsignedVInt32(plan.tokens.size());
            for (Token token : plan.tokens)
                Token.metadataSerializer.serialize(token, out, version);
        }

        public Move deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean streamData = in.readBoolean();

            ProgressBarrier barrier = ProgressBarrier.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            PlacementDeltas toSplitRanges = PlacementDeltas.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];

            PrepareMove.StartMove startMove = null;
            if (next.ordinal() >= Transformation.Kind.START_MOVE.ordinal())
                startMove = PrepareMove.StartMove.serializer.deserialize(in, version);
            PrepareMove.MidMove midMove = null;
            if (next.ordinal() >= Transformation.Kind.MID_MOVE.ordinal())
                midMove = PrepareMove.MidMove.serializer.deserialize(in, version);
            PrepareMove.FinishMove finishMove = null;
            if (next.ordinal() >= Transformation.Kind.FINISH_MOVE.ordinal())
                finishMove = PrepareMove.FinishMove.serializer.deserialize(in, version);

            int numTokens = in.readUnsignedVInt32();
            List<Token> tokens = new ArrayList<>();
            for (int i = 0; i < numTokens; i++)
                tokens.add(Token.metadataSerializer.deserialize(in, version));
            return new Move(tokens, barrier, lockKey, next,
                            toSplitRanges, startMove, midMove, finishMove, streamData);
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            Move plan = (Move) t;
            long size = TypeSizes.BOOL_SIZE;

            size += ProgressBarrier.serializer.serializedSize(plan.barrier(), version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);
            size += PlacementDeltas.serializer.serializedSize(plan.toSplitRanges, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            if (plan.kind().ordinal() >= Transformation.Kind.START_MOVE.ordinal())
                size += PrepareMove.StartMove.serializer.serializedSize(plan.startMove, version);
            if (plan.kind().ordinal() >= Transformation.Kind.MID_MOVE.ordinal())
                size += PrepareMove.MidMove.serializer.serializedSize(plan.midMove, version);
            if (plan.kind().ordinal() >= Transformation.Kind.FINISH_MOVE.ordinal())
                size += PrepareMove.FinishMove.serializer.serializedSize(plan.finishMove, version);

            size += TypeSizes.sizeofUnsignedVInt(plan.tokens.size());
            for (Token token : plan.tokens)
                size += Token.metadataSerializer.serializedSize(token, version);
            return size;
        }
    }
}
