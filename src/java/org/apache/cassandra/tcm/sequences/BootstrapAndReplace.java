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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.sequences.BootstrapAndJoin.bootstrap;

public class BootstrapAndReplace implements InProgressSequence<BootstrapAndReplace>
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapAndReplace.class);
    public static final Serializer serializer = new Serializer();

    public final Epoch latestModification;
    public final LockedRanges.Key lockKey;
    public final Set<Token> bootstrapTokens;
    public final PrepareReplace.StartReplace startReplace;
    public final PrepareReplace.MidReplace midReplace;
    public final PrepareReplace.FinishReplace finishReplace;
    public final Transformation.Kind next;

    public final boolean finishJoiningRing;
    public final boolean streamData;

    public BootstrapAndReplace(Epoch latestModification,
                               LockedRanges.Key lockKey,
                               Transformation.Kind next,
                               Set<Token> bootstrapTokens,
                               PrepareReplace.StartReplace startReplace,
                               PrepareReplace.MidReplace midReplace,
                               PrepareReplace.FinishReplace finishReplace,
                               boolean finishJoiningRing,
                               boolean streamData)
    {
        this.latestModification = latestModification;
        this.lockKey = lockKey;
        this.bootstrapTokens = bootstrapTokens;
        this.startReplace = startReplace;
        this.midReplace = midReplace;
        this.finishReplace = finishReplace;
        this.next = next;
        this.finishJoiningRing = finishJoiningRing;
        this.streamData = streamData;
    }

    public BootstrapAndReplace advance(Epoch waitFor, Transformation.Kind next)
    {
        return new BootstrapAndReplace(waitFor,
                                       lockKey,
                                       next,
                                       bootstrapTokens,
                                       startReplace, midReplace, finishReplace,
                                       finishJoiningRing,
                                       streamData);
    }

    public InProgressSequences.Kind kind()
    {
        return InProgressSequences.Kind.REPLACE;
    }

    public ProgressBarrier barrier()
    {
        InetAddressAndPort replaced = ClusterMetadata.current().directory.getNodeAddresses(startReplace.replaced()).broadcastAddress;
        return new ProgressBarrier(latestModification, ClusterMetadata.current().lockedRanges.locked.get(lockKey), e -> !e.equals(replaced));
    }

    public Transformation.Kind nextStep()
    {
        return next;
    }

    @Override
    public boolean executeNext()
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
                    return true;
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
                            return false;
                        }
                    }

                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                    StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                                 .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.getKeyspaceName()))
                                 .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));

                    ClusterMetadataService.instance().commit(midReplace);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Got exception committing midReplace", e);
                    return false;
                }
                break;
            case FINISH_REPLACE:
                try
                {
                    if (!finishJoiningRing)
                    {
                        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
                        return false;
                    }
                    ClusterMetadataService.instance().commit(finishReplace);
                }
                catch (Throwable e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.warn("Got exception committing finishReplace", e);
                    return false;
                }
                break;
            default:
                throw new IllegalStateException("Can't proceed with replacement from " + next);
        }

        return true;
    }

    /**
     * startDelta.writes.additions contains the ranges we need to stream
     * for each of those ranges, add all possible endpoints (except for the replica we're replacing) to the movement map
     *
     * keys in the map are the ranges the replacement node needs to stream, values are the potential endpoints.
     */
    private MovementMap movementMap(InetAddressAndPort beingReplaced, PlacementDeltas startDelta)
    {
        MovementMap.Builder movementMapBuilder = MovementMap.builder();
        DataPlacements placements = ClusterMetadata.current().placements;
        startDelta.forEach((params, delta) -> {
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            DataPlacement originalPlacements = placements.get(params);
            delta.writes.additions.flattenValues().forEach((destination) -> {
                originalPlacements.reads.forRange(destination.range()).stream()
                                        .filter(r -> !r.endpoint().equals(beingReplaced))
                                        .forEach(source -> movements.put(destination, source));
            });
            movementMapBuilder.put(params, movements.build());
        });
        return movementMapBuilder.build();
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_REPLACE and START_REPLACE, but PREPARE_REPLACE doesn't affect placements
            case FINISH_REPLACE:
                placements = midReplace.inverseDelta().apply(placements);
            case MID_REPLACE:
            case START_REPLACE:
                placements = startReplace.inverseDelta().apply(placements);
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
               Objects.equals(latestModification, that.latestModification) &&
               Objects.equals(lockKey, that.lockKey) &&
               Objects.equals(bootstrapTokens, that.bootstrapTokens) &&
               Objects.equals(startReplace, that.startReplace) &&
               Objects.equals(midReplace, that.midReplace) &&
               Objects.equals(finishReplace, that.finishReplace) && next == that.next;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, lockKey, bootstrapTokens, startReplace, midReplace, finishReplace, next, finishJoiningRing, streamData);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, BootstrapAndReplace>
    {
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
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
            if (plan.next.ordinal() >= Transformation.Kind.START_REPLACE.ordinal())
                PrepareReplace.StartReplace.serializer.serialize(plan.startReplace, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.MID_REPLACE.ordinal())
                PrepareReplace.MidReplace.serializer.serialize(plan.midReplace, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.FINISH_REPLACE.ordinal())
                PrepareReplace.FinishReplace.serializer.serialize(plan.finishReplace, out, version);
        }

        public BootstrapAndReplace deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean finishJoiningRing = in.readBoolean();
            boolean streamData = in.readBoolean();

            Set<Token> tokens = new HashSet<>();
            int tokenCount = VIntCoding.readUnsignedVInt32(in);
            for (int i = 0; i < tokenCount; i++)
                tokens.add(Token.metadataSerializer.deserialize(in, version));

            Epoch barrier = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareReplace.StartReplace startReplace = null;
            if (next.ordinal() >= Transformation.Kind.START_REPLACE.ordinal())
                startReplace = PrepareReplace.StartReplace.serializer.deserialize(in, version);
            PrepareReplace.MidReplace midReplace = null;
            if (next.ordinal() >= Transformation.Kind.MID_REPLACE.ordinal())
                midReplace = PrepareReplace.MidReplace.serializer.deserialize(in, version);
            PrepareReplace.FinishReplace finishReplace = null;
            if (next.ordinal() >= Transformation.Kind.FINISH_REPLACE.ordinal())
                finishReplace = PrepareReplace.FinishReplace.serializer.deserialize(in, version);

            return new BootstrapAndReplace(barrier, lockKey, next, tokens, startReplace, midReplace, finishReplace, finishJoiningRing, streamData);
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            BootstrapAndReplace plan = (BootstrapAndReplace) t;
            long size = (TypeSizes.BOOL_SIZE * 2);

            size += VIntCoding.computeVIntSize(plan.bootstrapTokens.size());
            for (Token token : plan.bootstrapTokens)
                size += Token.metadataSerializer.serializedSize(token, version);

            size += Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            if (plan.kind().ordinal() >= Transformation.Kind.START_REPLACE.ordinal())
                size += PrepareReplace.StartReplace.serializer.serializedSize(plan.startReplace, version);
            if (plan.kind().ordinal() >= Transformation.Kind.MID_REPLACE.ordinal())
                size += PrepareReplace.MidReplace.serializer.serializedSize(plan.midReplace, version);
            if (plan.kind().ordinal() >= Transformation.Kind.FINISH_REPLACE.ordinal())
                size += PrepareReplace.FinishReplace.serializer.serializedSize(plan.finishReplace, version);

            return size;
        }
    }
}
