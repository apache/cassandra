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
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BootstrapAndJoin implements InProgressSequence<BootstrapAndJoin>
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapAndJoin.class);
    public static final Serializer serializer = new Serializer();

    public final ProgressBarrier barrier;
    public final LockedRanges.Key lockKey;
    public final PlacementDeltas toSplitRanges;
    public final PrepareJoin.StartJoin startJoin;
    public final PrepareJoin.MidJoin midJoin;
    public final PrepareJoin.FinishJoin finishJoin;
    public final Transformation.Kind next;

    public final boolean finishJoiningRing;
    public final boolean streamData;

    public BootstrapAndJoin(ProgressBarrier barrier,
                            LockedRanges.Key lockKey,
                            Transformation.Kind next,
                            PlacementDeltas toSplitRanges,
                            PrepareJoin.StartJoin startJoin,
                            PrepareJoin.MidJoin midJoin,
                            PrepareJoin.FinishJoin finishJoin,
                            boolean finishJoiningRing,
                            boolean streamData)
    {
        this.barrier = barrier;
        this.lockKey = lockKey;
        this.next = next;

        this.toSplitRanges = toSplitRanges;
        this.startJoin = startJoin;
        this.midJoin = midJoin;
        this.finishJoin = finishJoin;

        this.finishJoiningRing = finishJoiningRing;
        this.streamData = streamData;
    }

    // TODO this is reused by BootstrapAndReplace, should we move it somewhere common?
    public static boolean bootstrap(final Collection<Token> tokens,
                                    long bootstrapTimeoutMillis,
                                    ClusterMetadata metadata,
                                    InetAddressAndPort replacingEndpoint){
        SystemKeyspace.updateTokens(tokens); // DON'T use setToken, that makes us part of the ring locally which is incorrect until we are done bootstrapping

        if (CassandraRelevantProperties.RESET_BOOTSTRAP_PROGRESS.getBoolean())
        {
            logger.info("Resetting bootstrap progress to start fresh");
            SystemKeyspace.resetAvailableStreamedRanges();
        }

//        Future<StreamState> bootstrapStream = StorageService.instance.startBootstrap(tokens, metadata, replacingEndpoint);
        Future<StreamState> bootstrapStream = ImmediateFuture.failure(new UnsupportedOperationException("Not implemented"));
        try
        {
            if (bootstrapTimeoutMillis > 0)
                bootstrapStream.get(bootstrapTimeoutMillis, MILLISECONDS);
            else
                bootstrapStream.get();
            StorageService.instance.markViewsAsBuilt();
            logger.info("Bootstrap completed for tokens {}", tokens);
            return true;
        }
        catch (Throwable e)
        {
            logger.error("Error while waiting on bootstrap to complete. Bootstrap will have to be restarted.", e);
            return false;
        }
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
    public BootstrapAndJoin advance(Epoch waitForWatermark, Transformation.Kind next)
    {
        return new BootstrapAndJoin(barrier().withNewEpoch(waitForWatermark), lockKey, next,
                                    toSplitRanges, startJoin, midJoin, finishJoin,
                                    finishJoiningRing, streamData);
    }


    @Override
    public InProgressSequences.Kind kind()
    {
        return InProgressSequences.Kind.JOIN;
    }

    @Override
    public boolean executeNext()
    {
        switch (next)
        {
            case START_JOIN:
                try
                {
                    NodeId nodeId = ClusterMetadata.current().myNodeId();
                    SystemKeyspace.updateTokens(ClusterMetadata.current().tokenMap.tokens(nodeId));

                    ClusterMetadataService.instance().commit(startJoin);
                }
                catch (Throwable e)
                {
                    return true;
                }

                break;
            case MID_JOIN:
                try
                {
                    Collection<Token> bootstrapTokens = SystemKeyspace.getSavedTokens();

                    if (streamData)
                    {
                        boolean dataAvailable = bootstrap(bootstrapTokens,
                                                          StorageService.INDEFINITE,
                                                          ClusterMetadata.current(),
                                                          null);

                        if (!dataAvailable)
                        {
                            logger.warn("Some data streaming failed. Use nodetool to check bootstrap state and resume. " +
                                        "For more, see `nodetool help bootstrap`. {}", SystemKeyspace.getBootstrapState());
                            return false;
                        }
                    }

                    SystemKeyspace.setBootstrapState(SystemKeyspace.BootstrapState.COMPLETED);
                    StreamSupport.stream(ColumnFamilyStore.all().spliterator(), false)
                                 .filter(cfs -> Schema.instance.getUserKeyspaces().names().contains(cfs.keyspace.getName()))
                                 .forEach(cfs -> cfs.indexManager.executePreJoinTasksBlocking(true));

                    ClusterMetadataService.instance().commit(midJoin);
                }
                catch (Throwable e)
                {
                    return true;
                }

                break;
            case FINISH_JOIN:
                try
                {
                    if (!finishJoiningRing)
                    {
                        logger.info("Startup complete, but write survey mode is active, not becoming an active ring member. Use JMX (StorageService->joinRing()) to finalize ring joining.");
                        return false;
                    }

                    ClusterMetadataService.instance().commit(finishJoin);
                }
                catch (Throwable e)
                {
                    return true;
                }
                break;
            default:
                throw new IllegalStateException("Can't proceed with join from " + next);
        }
        return true;
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_JOIN and START_JOIN, then merge the ranges split by PrepareJoin
            case FINISH_JOIN:
                placements = midJoin.inverseDelta().apply(placements);
            case MID_JOIN:
                placements = startJoin.inverseDelta().apply(placements);
            case START_JOIN:
                placements = toSplitRanges.invert().apply(placements);
                break;
            default:
                throw new IllegalStateException("Can't revert join from " + next);
        }
        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .unproposeTokens(startJoin.nodeId())
                       .withNodeState(startJoin.nodeId(), NodeState.REGISTERED)
                       .with(placements)
                       .with(newLockedRanges);
    }

    public BootstrapAndJoin finishJoiningRing()
    {
        return new BootstrapAndJoin(barrier(), lockKey, next, toSplitRanges, startJoin, midJoin, finishJoin,
                                    true, streamData);
    }

    public String toString()
    {
        return "BootstrapAndJoinPlan{" +
               "barrier=" + barrier +
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
               Objects.equals(barrier, that.barrier) &&
               Objects.equals(lockKey, that.lockKey) &&
               Objects.equals(toSplitRanges, that.toSplitRanges) &&
               Objects.equals(startJoin, that.startJoin) &&
               Objects.equals(midJoin, that.midJoin) &&
               Objects.equals(finishJoin, that.finishJoin) &&
               next == that.next;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(barrier, lockKey, toSplitRanges, startJoin, midJoin, finishJoin, next, finishJoiningRing, streamData);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, BootstrapAndJoin>
    {
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            BootstrapAndJoin plan = (BootstrapAndJoin) t;
            out.writeBoolean(plan.finishJoiningRing);
            out.writeBoolean(plan.streamData);

            ProgressBarrier.serializer.serialize(plan.barrier(), out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            PlacementDeltas.serializer.serialize(plan.toSplitRanges, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);

            if (plan.next.ordinal() >= Transformation.Kind.START_JOIN.ordinal())
                PrepareJoin.StartJoin.serializer.serialize(plan.startJoin, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.MID_JOIN.ordinal())
                PrepareJoin.MidJoin.serializer.serialize(plan.midJoin, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.FINISH_JOIN.ordinal())
                PrepareJoin.FinishJoin.serializer.serialize(plan.finishJoin, out, version);
        }

        public BootstrapAndJoin deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean finishJoiningRing = in.readBoolean();
            boolean streamData = in.readBoolean();

            ProgressBarrier barrier = ProgressBarrier.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);
            PlacementDeltas toSplitRanges = PlacementDeltas.serializer.deserialize(in, version);
            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareJoin.StartJoin startJoin = null;
            if (next.ordinal() >= Transformation.Kind.START_JOIN.ordinal())
                startJoin = PrepareJoin.StartJoin.serializer.deserialize(in, version);
            PrepareJoin.MidJoin midJoin = null;
            if (next.ordinal() >= Transformation.Kind.MID_JOIN.ordinal())
                midJoin = PrepareJoin.MidJoin.serializer.deserialize(in, version);
            PrepareJoin.FinishJoin finishJoin = null;
            if (next.ordinal() >= Transformation.Kind.FINISH_JOIN.ordinal())
                finishJoin = PrepareJoin.FinishJoin.serializer.deserialize(in, version);

            return new BootstrapAndJoin(barrier, lockKey, next, toSplitRanges, startJoin, midJoin, finishJoin, finishJoiningRing, streamData);
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            BootstrapAndJoin plan = (BootstrapAndJoin) t;
            long size = (TypeSizes.BOOL_SIZE * 2);

            size += ProgressBarrier.serializer.serializedSize(plan.barrier(), version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);
            size += PlacementDeltas.serializer.serializedSize(plan.toSplitRanges, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            if (plan.kind().ordinal() >= Transformation.Kind.START_JOIN.ordinal())
                size += PrepareJoin.StartJoin.serializer.serializedSize(plan.startJoin, version);
            if (plan.kind().ordinal() >= Transformation.Kind.MID_JOIN.ordinal())
                size += PrepareJoin.MidJoin.serializer.serializedSize(plan.midJoin, version);
            if (plan.kind().ordinal() >= Transformation.Kind.FINISH_JOIN.ordinal())
                size += PrepareJoin.FinishJoin.serializer.serializedSize(plan.finishJoin, version);

            return size;
        }
    }
}
