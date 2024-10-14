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
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.vint.VIntCoding;

import static com.google.common.collect.ImmutableList.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_JOIN;
import static org.apache.cassandra.tcm.Transformation.Kind.START_JOIN;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.JOIN;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;
import static org.apache.cassandra.tcm.sequences.SequenceState.halted;

public class BootstrapAndJoin extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapAndJoin.class);
    public static final Serializer serializer = new Serializer();

    public final LockedRanges.Key lockKey;
    public final PlacementDeltas toSplitRanges;
    public final PrepareJoin.StartJoin startJoin;
    public final PrepareJoin.MidJoin midJoin;
    public final PrepareJoin.FinishJoin finishJoin;
    public final Transformation.Kind next;

    public final boolean finishJoiningRing;
    public final boolean streamData;

    public static BootstrapAndJoin newSequence(Epoch preparedAt,
                                               LockedRanges.Key lockKey,
                                               PlacementDeltas toSplitRanges,
                                               PrepareJoin.StartJoin startJoin,
                                               PrepareJoin.MidJoin midJoin,
                                               PrepareJoin.FinishJoin finishJoin,
                                               boolean finishJoiningRing,
                                               boolean streamData)
    {
        return new BootstrapAndJoin(preparedAt,
                                    lockKey,
                                    toSplitRanges,
                                    START_JOIN,
                                    startJoin, midJoin, finishJoin,
                                    finishJoiningRing, streamData);
    }

    /**
     * Used by factory method for external callers and by Serializer
     */
    @VisibleForTesting
    BootstrapAndJoin(Epoch latestModification,
                     LockedRanges.Key lockKey,
                     PlacementDeltas toSplitRanges,
                     Transformation.Kind next,
                     PrepareJoin.StartJoin startJoin,
                     PrepareJoin.MidJoin midJoin,
                     PrepareJoin.FinishJoin finishJoin,
                     boolean finishJoiningRing,
                     boolean streamData)
    {
        super(nextToIndex(next), latestModification);
        this.lockKey = lockKey;
        this.toSplitRanges = toSplitRanges;
        this.next = next;
        this.startJoin = startJoin;
        this.midJoin = midJoin;
        this.finishJoin = finishJoin;
        this.finishJoiningRing = finishJoiningRing;
        this.streamData = streamData;
    }

    /**
     * Used by advance to move forward in the sequence after execution
     */
    private BootstrapAndJoin(BootstrapAndJoin current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        this.lockKey = current.lockKey;
        this.toSplitRanges = current.toSplitRanges;
        this.startJoin = current.startJoin;
        this.midJoin = current.midJoin;
        this.finishJoin = current.finishJoin;
        this.finishJoiningRing = current.finishJoiningRing;
        this.streamData = current.streamData;
    }

    @Override
    public Kind kind()
    {
        return JOIN;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return startJoin.nodeId();
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
        return applyMultipleTransformations(metadata, next, of(startJoin, midJoin, finishJoin));
    }

    @Override
    public SequenceState executeNext()
    {
        switch (next)
        {
            case START_JOIN:
                try
                {
                    SystemKeyspace.updateLocalTokens(finishJoin.tokens);
                    ClusterMetadataService.instance().commit(startJoin);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Exception committing startJoin", e);
                    return continuable();
                }

                break;
            case MID_JOIN:
                try
                {
                    Collection<Token> bootstrapTokens = SystemKeyspace.getSavedTokens();
                    ClusterMetadata metadata = ClusterMetadata.current();
                    Pair<MovementMap, MovementMap> movements = getMovementMaps(metadata);
                    MovementMap movementMap = movements.left;
                    MovementMap strictMovementMap = movements.right;
                    if (streamData)
                    {
                        boolean dataAvailable = bootstrap(bootstrapTokens,
                                                          StorageService.INDEFINITE,
                                                          ClusterMetadata.current(),
                                                          null,
                                                          movementMap,
                                                          strictMovementMap);

                        if (!dataAvailable)
                        {
                            logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. " +
                                        "For more, see `nodetool help bootstrap`. {}", SystemKeyspace.getBootstrapState());
                            return halted();
                        }
                        SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                    }
                    else
                    {
                        // The node may have previously been started in write survey mode and may or may not have
                        // performed initial streaming (i.e. auto_bootstap: false). When an operator then manually joins
                        // it (or it bounces and comes up without the system property), it will hit this condition.
                        // If during the initial startup no streaming was performed then bootstrap state is not
                        // COMPLETED and so we log the message about skipping data streaming. Alternatively, if
                        // streaming was done before entering write survey mode, the bootstrap is COMPLETE and so no
                        // need to log.
                        // The ability to join without bootstrapping, especially when combined with write survey mode
                        // is probably a mis-feature and serious consideration should be given to removing it.
                        if (!SystemKeyspace.bootstrapComplete())
                            logger.info("Skipping data streaming for join");
                    }

                    if (finishJoiningRing)
                    {
                        StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                                     .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.keyspace.getName()))
                                     .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));
                        ClusterMetadataService.instance().commit(midJoin);
                    }
                    else
                    {
                        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
                        return halted();
                    }
                }
                catch (IllegalStateException e)
                {
                    logger.error("Can't complete bootstrap", e);
                    return error(e);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.info("Exception committing midJoin", e);
                    return halted();
                }

                break;
            case FINISH_JOIN:
                ClusterMetadata metadata;
                try
                {
                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                    metadata = ClusterMetadataService.instance().commit(finishJoin);
                    StorageService.instance.clearTransientMode();
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Exception committing finishJoin", e);
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
    public BootstrapAndJoin advance(Epoch waitForWatermark)
    {
        return new BootstrapAndJoin(this, waitForWatermark);
    }

    @Override
    public ProgressBarrier barrier()
    {
        // There is no requirement to wait for peers to sync before starting the sequence
        if (next == START_JOIN)
            return ProgressBarrier.immediate();
        ClusterMetadata metadata = ClusterMetadata.current();
        return new ProgressBarrier(latestModification, metadata.directory.location(startJoin.nodeId()), metadata.lockedRanges.locked.get(lockKey));
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_JOIN and START_JOIN, then merge the ranges split by PrepareJoin
            case FINISH_JOIN:
                placements = midJoin.inverseDelta().apply(metadata.nextEpoch(), placements);
            case MID_JOIN:
                placements = startJoin.inverseDelta().apply(metadata.nextEpoch(), placements);
            case START_JOIN:
                placements = toSplitRanges.invert().apply(metadata.nextEpoch(), placements);
                break;
            default:
                throw new IllegalStateException("Can't revert join from " + next);
        }
        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .withNodeState(startJoin.nodeId(), NodeState.REGISTERED)
                       .with(placements)
                       .with(newLockedRanges);
    }

    public BootstrapAndJoin finishJoiningRing()
    {
        return new BootstrapAndJoin(latestModification, lockKey, toSplitRanges,
                                    next, startJoin, midJoin, finishJoin,
                                    true, false);
    }

    @VisibleForTesting
    public Pair<MovementMap, MovementMap> getMovementMaps(ClusterMetadata metadata)
    {
        MovementMap movementMap = movementMap(metadata.directory.endpoint(startJoin.nodeId()), metadata.placements, startJoin.delta());
        MovementMap strictMovementMap = toStrict(movementMap, finishJoin.delta());
        return Pair.create(movementMap, strictMovementMap);
    }

    // TODO this is reused by BootstrapAndReplace, should we move it somewhere common?
    public static boolean bootstrap(final Collection<Token> tokens,
                                    long bootstrapTimeoutMillis,
                                    ClusterMetadata metadata,
                                    InetAddressAndPort beingReplaced,
                                    MovementMap movements,
                                    MovementMap strictMovements)
    {
        SystemKeyspace.updateLocalTokens(tokens);
        assert beingReplaced == null || strictMovements == null : "Can't have strict movements during replacements";

        if (CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS.getBoolean())
        {
            logger.info("Resetting bootstrap progress to start fresh");
            SystemKeyspace.resetAvailableStreamedRanges();
        }

        StorageService.instance.repairPaxosForTopologyChange("bootstrap");
        Future<StreamState> bootstrapStream = StorageService.instance.startBootstrap(metadata, beingReplaced, movements, strictMovements);
        try
        {
            if (bootstrapTimeoutMillis > 0)
                bootstrapStream.get(bootstrapTimeoutMillis, MILLISECONDS);
            else
                bootstrapStream.get();
            StorageService.instance.markViewsAsBuilt();
            StorageService.instance.clearOngoingBootstrap();
            logger.info("Bootstrap completed for tokens {}", tokens);
            return true;
        }
        catch (Throwable e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.", e);
            return false;
        }
    }

    private static MovementMap movementMap(InetAddressAndPort joining, DataPlacements placements, PlacementDeltas startDelta)
    {
        MovementMap.Builder movementMapBuilder = MovementMap.builder();
        // we need all original placements for the ranges to stream - after initial split these new ranges exist in placements
        // startDelta write additions contains the ranges we need to stream
        startDelta.forEach((params, delta) -> {
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            DataPlacement oldPlacement = placements.get(params);
            delta.writes.additions.flattenValues().forEach((destination) -> {
                assert destination.endpoint().equals(joining);
                oldPlacement.reads.forRange(destination.range())
                                  .get()
                                  .stream()
                                  .forEach(source -> movements.put(destination, source));
            });
            movementMapBuilder.put(params, movements.build());
        });
        return movementMapBuilder.build();
    }

    private static MovementMap toStrict(MovementMap completeMovementMap, PlacementDeltas finishDelta)
    {
        MovementMap.Builder movementMapBuilder = MovementMap.builder();
        completeMovementMap.forEach((params, byreplica) -> {
            Set<Replica> strictCandidates = Iterables.toSet(finishDelta.get(params).writes.removals.flattenValues());
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            for (Replica destination : byreplica.keySet())
            {
                byreplica.get(destination).forEach((source) -> {
                    if (strictCandidates.contains(source))
                        movements.put(destination, source);
                });
            }
            movementMapBuilder.put(params, movements.build());
        });
        return movementMapBuilder.build();
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case START_JOIN:
                return 0;
            case MID_JOIN:
                return 1;
            case FINISH_JOIN:
                return 2;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, JOIN));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return START_JOIN;
            case 1:
                return MID_JOIN;
            case 2:
                return FINISH_JOIN;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", index, JOIN));
        }
    }

    public String toString()
    {
        return "BootstrapAndJoinPlan{" +
               "barrier=" + latestModification +
               ", lockKey=" + lockKey +
               ", toSplitRanges=" + toSplitRanges +
               ", startJoin=" + startJoin +
               ", midJoin=" + midJoin +
               ", finishJoin=" + finishJoin +
               ", next=" + next +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BootstrapAndJoin that = (BootstrapAndJoin) o;
        return finishJoiningRing == that.finishJoiningRing &&
               streamData == that.streamData &&
               next == that.next &&
               Objects.equals(latestModification, that.latestModification) &&
               Objects.equals(lockKey, that.lockKey) &&
               Objects.equals(toSplitRanges, that.toSplitRanges) &&
               Objects.equals(startJoin, that.startJoin) &&
               Objects.equals(midJoin, that.midJoin) &&
               Objects.equals(finishJoin, that.finishJoin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, lockKey, toSplitRanges, startJoin, midJoin, finishJoin, next, finishJoiningRing, streamData);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, BootstrapAndJoin>
    {
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            BootstrapAndJoin plan = (BootstrapAndJoin) t;
            out.writeBoolean(plan.finishJoiningRing);
            out.writeBoolean(plan.streamData);

            Epoch.serializer.serialize(plan.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            PlacementDeltas.serializer.serialize(plan.toSplitRanges, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);

            PrepareJoin.StartJoin.serializer.serialize(plan.startJoin, out, version);
            PrepareJoin.MidJoin.serializer.serialize(plan.midJoin, out, version);
            PrepareJoin.FinishJoin.serializer.serialize(plan.finishJoin, out, version);
        }

        public BootstrapAndJoin deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean finishJoiningRing = in.readBoolean();
            boolean streamData = in.readBoolean();

            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            PlacementDeltas toSplitRanges = PlacementDeltas.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareJoin.StartJoin startJoin = PrepareJoin.StartJoin.serializer.deserialize(in, version);
            PrepareJoin.MidJoin midJoin = PrepareJoin.MidJoin.serializer.deserialize(in, version);
            PrepareJoin.FinishJoin finishJoin = PrepareJoin.FinishJoin.serializer.deserialize(in, version);

            return new BootstrapAndJoin(lastModified, lockKey, toSplitRanges, next, startJoin, midJoin, finishJoin, finishJoiningRing, streamData);
        }

        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            BootstrapAndJoin plan = (BootstrapAndJoin) t;
            long size = (TypeSizes.BOOL_SIZE * 2);

            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);
            size += PlacementDeltas.serializer.serializedSize(plan.toSplitRanges, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            size += PrepareJoin.StartJoin.serializer.serializedSize(plan.startJoin, version);
            size += PrepareJoin.MidJoin.serializer.serializedSize(plan.midJoin, version);
            size += PrepareJoin.FinishJoin.serializer.serializedSize(plan.finishJoin, version);

            return size;
        }
    }
}
