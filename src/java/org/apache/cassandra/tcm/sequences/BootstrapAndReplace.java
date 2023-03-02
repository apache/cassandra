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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.sequences.BootstrapAndJoin.bootstrap;

public class BootstrapAndReplace implements InProgressSequence<BootstrapAndReplace>
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrapAndReplace.class);
    public static final Serializer serializer = new Serializer();

    public final ProgressBarrier barrier;
    public final LockedRanges.Key lockKey;
    public final Set<Token> bootstrapTokens;
    public final PrepareReplace.StartReplace startReplace;
    public final PrepareReplace.MidReplace midReplace;
    public final PrepareReplace.FinishReplace finishReplace;
    public final Transformation.Kind next;

    public final boolean finishJoiningRing;
    public final boolean streamData;

    public BootstrapAndReplace(ProgressBarrier barrier,
                               LockedRanges.Key lockKey,
                               Transformation.Kind next,
                               Set<Token> bootstrapTokens,
                               PrepareReplace.StartReplace startReplace,
                               PrepareReplace.MidReplace midReplace,
                               PrepareReplace.FinishReplace finishReplace,
                               boolean finishJoiningRing,
                               boolean streamData)
    {
        this.barrier = barrier;
        this.lockKey = lockKey;
        this.bootstrapTokens = bootstrapTokens;
        this.startReplace = startReplace;
        this.midReplace = midReplace;
        this.finishReplace = finishReplace;
        this.next = next;
        this.finishJoiningRing = finishJoiningRing;
        this.streamData = streamData;
    }

    public BootstrapAndReplace advance(Epoch waitForWatermark, Transformation.Kind next)
    {
        return new BootstrapAndReplace(barrier.withNewEpoch(waitForWatermark),
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
        return barrier;
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
                    return true;
                }

            case MID_REPLACE:
                try
                {
                    ClusterMetadata metadata = ClusterMetadata.current();

                    if (streamData)
                    {
                        boolean dataAvailable = bootstrap(bootstrapTokens,
                                                          StorageService.INDEFINITE,
                                                          metadata,
                                                          // TODO: do we need replaced or replacement here?
                                                          metadata.directory.endpoint(startReplace.replaced()));

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

                    ClusterMetadataService.instance().commit(midReplace);
                }
                catch (Throwable e)
                {
                    return true;
                }
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
                    return true;
                }
                break;
            default:
                throw new IllegalStateException("Can't proceed with replacement from " + next);
        }

        return true;
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
               Objects.equals(barrier, that.barrier) &&
               Objects.equals(lockKey, that.lockKey) &&
               Objects.equals(bootstrapTokens, that.bootstrapTokens) &&
               Objects.equals(startReplace, that.startReplace) &&
               Objects.equals(midReplace, that.midReplace) &&
               Objects.equals(finishReplace, that.finishReplace) && next == that.next;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(barrier, lockKey, bootstrapTokens, startReplace, midReplace, finishReplace, next, finishJoiningRing, streamData);
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

            ProgressBarrier.serializer.serialize(t.barrier(), out, version);
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

            ProgressBarrier barrier = ProgressBarrier.serializer.deserialize(in, version);
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

            size += ProgressBarrier.serializer.serializedSize(plan.barrier(), version);
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
