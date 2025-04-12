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
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
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
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_MOVE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_MOVE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_MOVE;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.MOVE;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;

public class Move extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(Move.class);
    public static final Serializer serializer = new Serializer();

    public final Collection<Token> tokens;
    public final LockedRanges.Key lockKey;
    public final PlacementDeltas toSplitRanges;
    public final PrepareMove.StartMove startMove;
    public final PrepareMove.MidMove midMove;
    public final PrepareMove.FinishMove finishMove;
    public final boolean streamData;
    public final Transformation.Kind next;

    public static Move newSequence(Epoch preparedAt,
                                   LockedRanges.Key lockKey,
                                   Collection<Token> tokens,
                                   PlacementDeltas toSplitRanges,
                                   PrepareMove.StartMove startMove,
                                   PrepareMove.MidMove midMove,
                                   PrepareMove.FinishMove finishMove,
                                   boolean streamData)
    {
        return new Move(preparedAt,
                        lockKey,
                        START_MOVE,
                        tokens,
                        toSplitRanges,
                        startMove, midMove, finishMove,
                        streamData);
    }

    /**
     * Used by factory method for external callers and by Serializer
     */
    @VisibleForTesting
    Move(Epoch latestModification,
         LockedRanges.Key lockKey,
         Transformation.Kind next,
         Collection<Token> tokens,
         PlacementDeltas toSplitRanges,
         PrepareMove.StartMove startMove,
         PrepareMove.MidMove midMove,
         PrepareMove.FinishMove finishMove,
         boolean streamData)
    {
        super(nextToIndex(next), latestModification);
        this.lockKey = lockKey;
        this.next = next;
        this.tokens = tokens;
        this.toSplitRanges = toSplitRanges;
        this.startMove = startMove;
        this.midMove = midMove;
        this.finishMove = finishMove;
        this.streamData = streamData;
    }

    /**
     * Used by advance to move forward in the sequence after execution
     */
    private Move(Move current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        this.lockKey = current.lockKey;
        this.tokens = current.tokens;
        this.toSplitRanges = current.toSplitRanges;
        this.startMove = current.startMove;
        this.midMove = current.midMove;
        this.finishMove = current.finishMove;
        this.streamData = current.streamData;
    }

    @Override
    public boolean finishDuringStartup()
    {
        return false;
    }

    @Override
    public Kind kind()
    {
        return MOVE;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return startMove.nodeId();
    }

    @Override
    public MetadataSerializer<? extends SequenceKey> keySerializer()
    {
        return NodeId.serializer;
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return indexToNext(idx);
    }

    @Override
    public Transformation.Result applyTo(ClusterMetadata metadata)
    {
        return applyMultipleTransformations(metadata, next, of(startMove, midMove, finishMove));
    }

    @Override
    public SequenceState executeNext()
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
                    return continuable();
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
                                                                                         toSplitRanges,
                                                                                         startMove.delta(),
                                                                                         midMove.delta(),
                                                                                         StorageService.useStrictConsistency)
                                                                             .asMap();

                    for (KeyspaceMetadata ks : keyspaces)
                    {
                        ReplicationParams replicationParams = ks.params.replication;
                        if (replicationParams.isMeta())
                            continue;
                        EndpointsByReplica endpoints = movementMap.get(replicationParams);
                        for (Map.Entry<Replica, Replica> e : endpoints.flattenEntries())
                        {
                            Replica destination = e.getKey();
                            Replica source = e.getValue();
                            logger.info("Stream source: {} destination: {}", source, destination);
                            assert !source.endpoint().equals(destination.endpoint()) : String.format("Source %s should not be the same as destionation %s", source, destination);
                            if (source.isSelf())
                                streamPlan.transferRanges(destination.endpoint(), ks.name, RangesAtEndpoint.of(destination));
                            else if (destination.isSelf())
                            {
                                if (destination.isFull())
                                    streamPlan.requestRanges(source.endpoint(), ks.name, RangesAtEndpoint.of(destination), RangesAtEndpoint.empty(destination.endpoint()));
                                else
                                    streamPlan.requestRanges(source.endpoint(), ks.name, RangesAtEndpoint.empty(destination.endpoint()), RangesAtEndpoint.of(destination));
                            }
                            else
                                throw new IllegalStateException("Node should be either source or destination in the movement map " + endpoints);
                        }
                    }

                    streamPlan.execute().get();
                    StorageService.instance.repairPaxosForTopologyChange("move");
                }
                catch (InterruptedException e)
                {
                    return continuable();
                }
                catch (ExecutionException e)
                {
                    StorageService.instance.markMoveFailed();
                    throw new RuntimeException("Unable to move", e);
                }
                catch (Exception e)
                {
                    StorageService.instance.markMoveFailed();
                    throw e;
                }

                try
                {
                    ClusterMetadataService.instance().commit(midMove);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            case FINISH_MOVE:
                ClusterMetadata metadata;
                try
                {
                    SystemKeyspace.updateLocalTokens(tokens);
                    metadata = ClusterMetadataService.instance().commit(finishMove);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                ClusterMetadataService.instance().ensureCMSPlacement(metadata);
                break;
            default:
                return error(new IllegalStateException("Can't proceed with join from " + next));
        }

        return continuable();
    }

    @Override
    public Move advance(Epoch waitForWatermark)
    {
        return new Move(this, waitForWatermark);
    }

    @Override
    public ProgressBarrier barrier()
    {
        if (next == START_MOVE)
            return ProgressBarrier.immediate();
        ClusterMetadata metadata = ClusterMetadata.current();
        return new ProgressBarrier(latestModification, metadata.directory.location(startMove.nodeId()), metadata.lockedRanges.locked.get(lockKey));
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;

        switch (next)
        {
            case FINISH_MOVE:
                placements = midMove.inverseDelta().apply(metadata.nextEpoch(), placements);
            case MID_MOVE:
                placements = startMove.inverseDelta().apply(metadata.nextEpoch(), placements);
            case START_MOVE:
                placements = toSplitRanges.invert().apply(metadata.nextEpoch(), placements);
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

    /**
     * Returns a mapping of destination -> source*, where the destination is the node that needs to stream from source
     *
     * there can be multiple sources for each destination
     */
    private static MovementMap movementMap(IFailureDetector fd, DataPlacements placements, PlacementDeltas toSplitRanges, PlacementDeltas toStart, PlacementDeltas midDeltas, boolean strictConsistency)
    {
        MovementMap.Builder allMovements = MovementMap.builder();
        toStart.forEach((params, delta) -> {
            RangesByEndpoint targets = delta.writes.additions;
            ReplicaGroups oldOwners = placements.get(params).reads;
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            targets.flattenValues().forEach(destination -> {
                SourceHolder sources = new SourceHolder(fd, destination, toSplitRanges.get(params), strictConsistency);
                AtomicBoolean needsRelaxedSources = new AtomicBoolean();
                // first, try to find strict sources for the ranges we need to stream - these are the ranges that
                // instances are losing.
                midDeltas.get(params).reads.removals.flattenValues().forEach(strictSource -> {
                    if (strictSource.range().equals(destination.range()) && !strictSource.endpoint().equals(destination.endpoint()))
                        if (!sources.addSource(strictSource))
                        {
                            if (!strictConsistency)
                                throw new IllegalStateException("Couldn't find any matching sufficient replica out of: " + strictSource + " -> " + destination);
                            needsRelaxedSources.set(true);
                        }
                });

                // if we are not running with strict consistency, try to find other sources for streaming
                if (needsRelaxedSources.get())
                {
                    for (Replica source : DatabaseDescriptor.getNodeProximity()
                                                            .sortedByProximity(FBUtilities.getBroadcastAddressAndPort(),
                                                                               oldOwners.forRange(destination.range()).get()))
                    {
                        if (fd.isAlive(source.endpoint()) && !source.endpoint().equals(destination.endpoint()))
                        {
                            if ((sources.fullSource == null && source.isFull()) ||
                                (sources.transientSource == null && source.isTransient()))
                                sources.addSource(source);
                        }
                    }
                }

                if (sources.fullSource == null && destination.isFull())
                    throw new IllegalStateException("Found no sources for "+destination);
                sources.addToMovements(destination, movements);
            });
            allMovements.put(params, movements.build());
        });

        return allMovements.build();
    }

    private static class SourceHolder
    {
        private final IFailureDetector fd;
        private final PlacementDeltas.PlacementDelta splitDelta;
        private final boolean strict;
        private Replica fullSource;
        private Replica transientSource;
        private final Replica destination;

        public SourceHolder(IFailureDetector fd, Replica destination, PlacementDeltas.PlacementDelta splitDelta, boolean strict)
        {
            this.fd = fd;
            this.splitDelta = splitDelta;
            this.strict = strict;
            this.destination = destination;
        }

        private boolean addSource(Replica source)
        {
            if (fd.isAlive(source.endpoint()))
            {
                if (source.isFull())
                {
                    assert fullSource == null;
                    fullSource = source;
                }
                else
                {
                    assert transientSource == null;
                    if (!destination.isSelf() && !source.isSelf())
                    {
                        // a transient replica is being removed, now, to be able to safely skip streaming from this
                        // replica we need to make sure it remains a replica for the range after the move has finished:
                        if (splitDelta.writes.additions.get(source.endpoint()).byRange().get(destination.range()) == null)
                        {
                            if (strict)
                                throw new IllegalStateException(String.format("Source %s for %s is not remaining as a replica after the move, can't do a consistent range movement, retry with that disabled", source, destination));
                            else
                                return false;
                        }
                        return true;
                    }
                    else
                    {
                        transientSource = source;
                    }
                }
                return true;
            }
            else if (strict)
                throw new IllegalStateException("Strict consistency requires the node losing the range to be UP but " + source + " is DOWN");
            return false;
        }

        private void addToMovements(Replica destination, EndpointsByReplica.Builder movements)
        {
            if (fullSource != null)
                movements.put(destination, fullSource);
            if (transientSource != null)
                movements.put(destination, transientSource);
        }
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case START_MOVE:
                return 0;
            case MID_MOVE:
                return 1;
            case FINISH_MOVE:
                return 2;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, MOVE));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return START_MOVE;
            case 1:
                return MID_MOVE;
            case 2:
                return FINISH_MOVE;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", index, MOVE));
        }
    }

    @Override
    public String toString()
    {
        return "Move{" +
               "latestModification=" + latestModification +
               ", tokens=" + tokens +
               ", lockKey=" + lockKey +
               ", toSplitRanges=" + toSplitRanges +
               ", startMove=" + startMove +
               ", midMove=" + midMove +
               ", finishMove=" + finishMove +
               ", streamData=" + streamData +
               ", next=" + next +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Move)) return false;
        Move move = (Move) o;
        return streamData == move.streamData &&
               next == move.next &&
               Objects.equals(latestModification, move.latestModification) &&
               Objects.equals(tokens, move.tokens) &&
               Objects.equals(lockKey, move.lockKey) &&
               Objects.equals(toSplitRanges, move.toSplitRanges) &&
               Objects.equals(startMove, move.startMove) &&
               Objects.equals(midMove, move.midMove) &&
               Objects.equals(finishMove, move.finishMove);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, tokens, lockKey, next, toSplitRanges, startMove, midMove, finishMove, streamData);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, Move>
    {
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            Move plan = (Move) t;
            out.writeBoolean(plan.streamData);

            Epoch.serializer.serialize(plan.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            PlacementDeltas.serializer.serialize(plan.toSplitRanges, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);

            PrepareMove.StartMove.serializer.serialize(plan.startMove, out, version);
            PrepareMove.MidMove.serializer.serialize(plan.midMove, out, version);
            PrepareMove.FinishMove.serializer.serialize(plan.finishMove, out, version);

            out.writeUnsignedVInt32(plan.tokens.size());
            for (Token token : plan.tokens)
                Token.metadataSerializer.serialize(token, out, version);
        }

        public Move deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean streamData = in.readBoolean();

            Epoch barrier = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            PlacementDeltas toSplitRanges = PlacementDeltas.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];

            PrepareMove.StartMove startMove = PrepareMove.StartMove.serializer.deserialize(in, version);
            PrepareMove.MidMove midMove = PrepareMove.MidMove.serializer.deserialize(in, version);
            PrepareMove.FinishMove finishMove = PrepareMove.FinishMove.serializer.deserialize(in, version);

            int numTokens = in.readUnsignedVInt32();
            Set<Token> tokens = new HashSet<>();
            IPartitioner partitioner = ClusterMetadata.current().partitioner;
            for (int i = 0; i < numTokens; i++)
                tokens.add(Token.metadataSerializer.deserialize(in, partitioner, version));
            return new Move(barrier, lockKey, next, tokens,
                            toSplitRanges, startMove, midMove, finishMove, streamData);
        }

        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            Move plan = (Move) t;
            long size = TypeSizes.BOOL_SIZE;

            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);
            size += PlacementDeltas.serializer.serializedSize(plan.toSplitRanges, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            size += PrepareMove.StartMove.serializer.serializedSize(plan.startMove, version);
            size += PrepareMove.MidMove.serializer.serializedSize(plan.midMove, version);
            size += PrepareMove.FinishMove.serializer.serializedSize(plan.finishMove, version);

            size += TypeSizes.sizeofUnsignedVInt(plan.tokens.size());
            for (Token token : plan.tokens)
                size += Token.metadataSerializer.serializedSize(token, version);
            return size;
        }
    }
}
