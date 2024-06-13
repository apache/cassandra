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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
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
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.vint.VIntCoding;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_REPLACE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_REPLACE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_REPLACE;
import static org.apache.cassandra.tcm.sequences.BootstrapAndJoin.bootstrap;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.REPLACE;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;
import static org.apache.cassandra.tcm.sequences.SequenceState.halted;

public class BootstrapAndReplace extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapAndReplace.class);
    public static final Serializer serializer = new Serializer();

    public final LockedRanges.Key lockKey;
    public final Set<Token> bootstrapTokens;
    public final PrepareReplace.StartReplace startReplace;
    public final PrepareReplace.MidReplace midReplace;
    public final PrepareReplace.FinishReplace finishReplace;
    public final Transformation.Kind next;

    public final boolean finishJoiningRing;
    public final boolean streamData;

    public static BootstrapAndReplace newSequence(Epoch preparedAt,
                                                  LockedRanges.Key lockKey,
                                                  Set<Token> bootstrapTokens,
                                                  PrepareReplace.StartReplace startReplace,
                                                  PrepareReplace.MidReplace midReplace,
                                                  PrepareReplace.FinishReplace finishReplace,
                                                  boolean finishJoiningRing,
                                                  boolean streamData)
    {
        return new BootstrapAndReplace(preparedAt,
                                       lockKey,
                                       bootstrapTokens,
                                       START_REPLACE,
                                       startReplace, midReplace, finishReplace,
                                       finishJoiningRing, streamData);
    }

    /**
     * Used by factory method for external callers and by Serializer
     */
    @VisibleForTesting
    BootstrapAndReplace(Epoch latestModification,
                        LockedRanges.Key lockKey,
                        Set<Token> bootstrapTokens,
                        Transformation.Kind next,
                        PrepareReplace.StartReplace startReplace,
                        PrepareReplace.MidReplace midReplace,
                        PrepareReplace.FinishReplace finishReplace,
                        boolean finishJoiningRing,
                        boolean streamData)
    {
        super(nextToIndex(next), latestModification);
        this.lockKey = lockKey;
        this.bootstrapTokens = bootstrapTokens;
        this.next = next;
        this.startReplace = startReplace;
        this.midReplace = midReplace;
        this.finishReplace = finishReplace;
        this.finishJoiningRing = finishJoiningRing;
        this.streamData = streamData;
    }

    /**
     * Used by advance to move forward in the sequence after execution
     */
    private BootstrapAndReplace(BootstrapAndReplace current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        this.lockKey = current.lockKey;
        this.bootstrapTokens = current.bootstrapTokens;
        this.startReplace = current.startReplace;
        this.midReplace = current.midReplace;
        this.finishReplace = current.finishReplace;
        this.finishJoiningRing = current.finishJoiningRing;
        this.streamData = current.streamData;
    }

    @Override
    public Kind kind()
    {
        return REPLACE;
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return startReplace.nodeId();
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
        return applyMultipleTransformations(metadata, next, of(startReplace, midReplace, finishReplace));
    }

    @Override
    public SequenceState executeNext()
    {
        switch (next)
        {
            // Last transformation is register, so we move to PrepareReplace
            case START_REPLACE:
                try
                {
                    ClusterMetadataService.instance().commit(startReplace);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Got exception committing startReplace", e);
                    return continuable();
                }
                break;
            case MID_REPLACE:
                try
                {
                    ClusterMetadata metadata = ClusterMetadata.current();

                    if (streamData)
                    {
                        MovementMap movements = movementMap(metadata.directory.endpoint(startReplace.replaced()), startReplace.delta());
                        boolean dataAvailable = bootstrap(bootstrapTokens,
                                                          StorageService.INDEFINITE,
                                                          metadata,
                                                          metadata.directory.endpoint(startReplace.replaced()),
                                                          movements,
                                                          null); // no potential for strict movements when replacing

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
                        ClusterMetadataService.instance().commit(midReplace);
                    }
                    else
                    {
                        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
                        return halted();
                    }
                }
                catch (IllegalStateException e)
                {
                    logger.error("Can't complete replacement", e);
                    return error(e);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Got exception committing midReplace", e);
                    return halted();
                }
                break;
            case FINISH_REPLACE:
                ClusterMetadata metadata;
                try
                {
                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                    metadata = ClusterMetadataService.instance().commit(finishReplace);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Got exception committing finishReplace", e);
                    return halted();
                }
                ClusterMetadataService.instance().ensureCMSPlacement(metadata);

                break;
            default:
                return error(new IllegalStateException("Can't proceed with replacement from " + next));
        }

        return continuable();
    }

    @Override
    public BootstrapAndReplace advance(Epoch waitFor)
    {
        return new BootstrapAndReplace(this, waitFor);
    }

    @Override
    public ProgressBarrier barrier()
    {
        // There is no requirement to wait for peers to sync before starting the sequence
        if (next == START_REPLACE)
            return ProgressBarrier.immediate();
        ClusterMetadata metadata = ClusterMetadata.current();
        InetAddressAndPort replaced = metadata.directory.getNodeAddresses(startReplace.replaced()).broadcastAddress;
        return new ProgressBarrier(latestModification, metadata.directory.location(startReplace.nodeId()), metadata.lockedRanges.locked.get(lockKey), e -> !e.equals(replaced));
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_REPLACE and START_REPLACE, but PREPARE_REPLACE doesn't affect placements
            case FINISH_REPLACE:
                placements = midReplace.inverseDelta().apply(metadata.nextEpoch(), placements);
            case MID_REPLACE:
            case START_REPLACE:
                placements = startReplace.inverseDelta().apply(metadata.nextEpoch(), placements);
                break;
            default:
                throw new IllegalStateException("Can't revert replacement from " + next);
        }

        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .withNodeState(startReplace.replacement(), NodeState.REGISTERED)
                       .with(placements)
                       .with(newLockedRanges);
    }

    public BootstrapAndReplace finishJoiningRing()
    {
        return new BootstrapAndReplace(latestModification, lockKey, bootstrapTokens,
                                       next, startReplace, midReplace, finishReplace,
                                       true, false);
    }

    /**
     * startDelta.writes.additions contains the ranges we need to stream
     * for each of those ranges, add all possible endpoints (except for the replica we're replacing) to the movement map
     *
     * keys in the map are the ranges the replacement node needs to stream, values are the potential endpoints.
     */
    private static MovementMap movementMap(InetAddressAndPort beingReplaced, PlacementDeltas startDelta)
    {
        MovementMap.Builder movementMapBuilder = MovementMap.builder();
        DataPlacements placements = ClusterMetadata.current().placements;
        startDelta.forEach((params, delta) -> {
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            DataPlacement originalPlacements = placements.get(params);
            delta.writes.additions.flattenValues().forEach((destination) -> {
                originalPlacements.reads.forRange(destination.range())
                                        .get().stream()
                                        .filter(r -> !r.endpoint().equals(beingReplaced))
                                        .forEach(source -> movements.put(destination, source));
            });
            movementMapBuilder.put(params, movements.build());
        });
        return movementMapBuilder.build();
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case START_REPLACE:
                return 0;
            case MID_REPLACE:
                return 1;
            case FINISH_REPLACE:
                return 2;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, REPLACE));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return START_REPLACE;
            case 1:
                return MID_REPLACE;
            case 2:
                return FINISH_REPLACE;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", index, REPLACE));
        }
    }

    public String toString()
    {
        return "BootstrapAndReplacePlan{" +
               "barrier=" + barrier() +
               "lockKey=" + lockKey +
               ", bootstrapTokens=" + bootstrapTokens +
               ", startReplace=" + startReplace +
               ", midReplace=" + midReplace +
               ", finishReplace=" + finishReplace +
               ", next=" + next +
               ", finishJoiningRing=" + finishJoiningRing +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BootstrapAndReplace that = (BootstrapAndReplace) o;
        return finishJoiningRing == that.finishJoiningRing &&
               streamData == that.streamData &&
               next == that.next &&
               Objects.equals(latestModification, that.latestModification) &&
               Objects.equals(lockKey, that.lockKey) &&
               Objects.equals(bootstrapTokens, that.bootstrapTokens) &&
               Objects.equals(startReplace, that.startReplace) &&
               Objects.equals(midReplace, that.midReplace) &&
               Objects.equals(finishReplace, that.finishReplace);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, lockKey, bootstrapTokens, startReplace, midReplace, finishReplace, next, finishJoiningRing, streamData);
    }

    public static void checkUnsafeReplace(boolean shouldBootstrap)
    {
        if (!shouldBootstrap && !CassandraRelevantProperties.ALLOW_UNSAFE_REPLACE.getBoolean())
        {
            throw new RuntimeException("Replacing a node without bootstrapping risks invalidating consistency " +
                                       "guarantees as the expected data may not be present until repair is run. " +
                                       "To perform this operation, please restart with " +
                                       "-Dcassandra.allow_unsafe_replace=true");
        }

    }

    public static void gossipStateToHibernate(ClusterMetadata metadata, NodeId nodeId)
    {
        // order is important here, the gossiper can fire in between adding these two states.  It's ok to send TOKENS without STATUS, but *not* vice versa.
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>();
        VersionedValue.VersionedValueFactory valueFactory = StorageService.instance.valueFactory;
        states.add(Pair.create(ApplicationState.TOKENS, valueFactory.tokens(metadata.tokenMap.tokens(nodeId))));
        states.add(Pair.create(ApplicationState.STATUS_WITH_PORT, valueFactory.hibernate(true)));
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.hibernate(true)));
        Gossiper.instance.addLocalApplicationStates(states);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, BootstrapAndReplace>
    {
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            BootstrapAndReplace plan = (BootstrapAndReplace) t;
            out.writeBoolean(plan.finishJoiningRing);
            out.writeBoolean(plan.streamData);

            out.writeUnsignedVInt32(plan.bootstrapTokens.size());
            for (Token token : plan.bootstrapTokens)
                Token.metadataSerializer.serialize(token, out, version);

            Epoch.serializer.serialize(plan.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);

            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            PrepareReplace.StartReplace.serializer.serialize(plan.startReplace, out, version);
            PrepareReplace.MidReplace.serializer.serialize(plan.midReplace, out, version);
            PrepareReplace.FinishReplace.serializer.serialize(plan.finishReplace, out, version);
        }

        public BootstrapAndReplace deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean finishJoiningRing = in.readBoolean();
            boolean streamData = in.readBoolean();

            Set<Token> tokens = new HashSet<>();
            int tokenCount = VIntCoding.readUnsignedVInt32(in);
            IPartitioner partitioner = ClusterMetadata.current().partitioner;
            for (int i = 0; i < tokenCount; i++)
                tokens.add(Token.metadataSerializer.deserialize(in, partitioner, version));

            Epoch barrier = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareReplace.StartReplace startReplace = PrepareReplace.StartReplace.serializer.deserialize(in, version);
            PrepareReplace.MidReplace midReplace = PrepareReplace.MidReplace.serializer.deserialize(in, version);
            PrepareReplace.FinishReplace finishReplace = PrepareReplace.FinishReplace.serializer.deserialize(in, version);

            return new BootstrapAndReplace(barrier, lockKey, tokens, next, startReplace, midReplace, finishReplace, finishJoiningRing, streamData);
        }

        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            BootstrapAndReplace plan = (BootstrapAndReplace) t;
            long size = (TypeSizes.BOOL_SIZE * 2);

            size += VIntCoding.computeVIntSize(plan.bootstrapTokens.size());
            for (Token token : plan.bootstrapTokens)
                size += Token.metadataSerializer.serializedSize(token, version);

            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            size += PrepareReplace.StartReplace.serializer.serializedSize(plan.startReplace, version);
            size += PrepareReplace.MidReplace.serializer.serializedSize(plan.midReplace, version);
            size += PrepareReplace.FinishReplace.serializer.serializedSize(plan.finishReplace, version);

            return size;
        }
    }
}
