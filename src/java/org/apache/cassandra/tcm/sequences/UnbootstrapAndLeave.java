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
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.cassandra.tcm.Transformation.Kind.FINISH_LEAVE;
import static org.apache.cassandra.tcm.Transformation.Kind.MID_LEAVE;
import static org.apache.cassandra.tcm.Transformation.Kind.START_LEAVE;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.LEAVE;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.error;

public class UnbootstrapAndLeave extends MultiStepOperation<Epoch>
{
    private static final Logger logger = LoggerFactory.getLogger(UnbootstrapAndLeave.class);
    public static final Serializer serializer = new Serializer();

    public final LockedRanges.Key lockKey;
    public final Transformation.Kind next;

    public final PrepareLeave.StartLeave startLeave;
    public final PrepareLeave.MidLeave midLeave;
    public final PrepareLeave.FinishLeave finishLeave;
    private final LeaveStreams streams;

    public static UnbootstrapAndLeave newSequence(Epoch preparedAt,
                                                  LockedRanges.Key lockKey,
                                                  PrepareLeave.StartLeave startLeave,
                                                  PrepareLeave.MidLeave midLeave,
                                                  PrepareLeave.FinishLeave finishLeave,
                                                  LeaveStreams streams)
    {
        return new UnbootstrapAndLeave(preparedAt,
                                       lockKey,
                                       START_LEAVE,
                                       startLeave, midLeave, finishLeave,
                                       streams);
    }

    /**
     * Used by factory method for external callers and by Serializer
     */
    @VisibleForTesting
    UnbootstrapAndLeave(Epoch latestModification,
                               LockedRanges.Key lockKey,
                               Transformation.Kind next,
                               PrepareLeave.StartLeave startLeave,
                               PrepareLeave.MidLeave midLeave,
                               PrepareLeave.FinishLeave finishLeave,
                               LeaveStreams streams)
    {
        super(nextToIndex(next), latestModification);
        this.lockKey = lockKey;
        this.next = next;
        this.startLeave = startLeave;
        this.midLeave = midLeave;
        this.finishLeave = finishLeave;
        this.streams = streams;
    }

    /**
     * Used by advance to move forward in the sequence after execution
     */
    private UnbootstrapAndLeave(UnbootstrapAndLeave current, Epoch latestModification)
    {
        super(current.idx + 1, latestModification);
        this.next = indexToNext(current.idx + 1);
        this.lockKey = current.lockKey;
        this.startLeave = current.startLeave;
        this.midLeave = current.midLeave;
        this.finishLeave = current.finishLeave;
        this.streams = current.streams;
    }

    @Override
    public Kind kind()
    {
        switch (streams.kind())
        {
            case UNBOOTSTRAP:
                return LEAVE;
            case REMOVENODE:
                return MultiStepOperation.Kind.REMOVE;
            default:
                throw new IllegalStateException("Invalid stream kind: "+streams.kind());
        }
    }

    @Override
    protected SequenceKey sequenceKey()
    {
        return startLeave.nodeId();
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
        return applyMultipleTransformations(metadata, next, of(startLeave, midLeave, finishLeave));
    }

    @Override
    public SequenceState executeNext()
    {
        switch (next)
        {
            case START_LEAVE:
                try
                {
                    DatabaseDescriptor.getSeverityDuringDecommission().ifPresent(DynamicEndpointSnitch::addSeverity);
                    ClusterMetadataService.instance().commit(startLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            case MID_LEAVE:
                try
                {
                    streams.execute(startLeave.nodeId(),
                                    startLeave.delta(),
                                    midLeave.delta(),
                                    finishLeave.delta());
                    ClusterMetadataService.instance().commit(midLeave);
                }
                catch (ExecutionException e)
                {
                    StorageService.instance.markDecommissionFailed();
                    JVMStabilityInspector.inspectThrowable(e);
                    logger.error("Error while decommissioning node: {}", e.getCause().getMessage());
                    throw new RuntimeException("Error while decommissioning node: " + e.getCause().getMessage());
                }
                catch (Throwable t)
                {
                    logger.warn("Error committing midLeave", t);
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            case FINISH_LEAVE:
                try
                {
                    ClusterMetadataService.instance().commit(finishLeave);
                    StorageService.instance.clearTransientMode();
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return continuable();
                }
                break;
            default:
                return error(new IllegalStateException("Can't proceed with leave from " + next));
        }

        return continuable();
    }

    @Override
    public UnbootstrapAndLeave advance(Epoch waitUntilAcknowledged)
    {
        return new UnbootstrapAndLeave(this, waitUntilAcknowledged);
    }

    @Override
    public ProgressBarrier barrier()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        LockedRanges.AffectedRanges affectedRanges = metadata.lockedRanges.locked.get(lockKey);
        Location location = metadata.directory.location(startLeave.nodeId());
        if (kind() == MultiStepOperation.Kind.REMOVE)
            return new ProgressBarrier(latestModification, location, affectedRanges, (e) -> !e.equals(metadata.directory.endpoint(startLeave.nodeId())));
        else
            return new ProgressBarrier(latestModification, location, affectedRanges);
    }

    @Override
    public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)
    {
        DataPlacements placements = metadata.placements;
        switch (next)
        {
            // need to undo MID_LEAVE and START_LEAVE, but PrepareLeave doesn't affect placement
            case FINISH_LEAVE:
                placements = midLeave.inverseDelta().apply(metadata.nextEpoch(), placements);
            case MID_LEAVE:
            case START_LEAVE:
                placements = startLeave.inverseDelta().apply(metadata.nextEpoch(), placements);
                break;
            default:
                throw new IllegalStateException("Can't revert leave from " + next);
        }
        LockedRanges newLockedRanges = metadata.lockedRanges.unlock(lockKey);
        return metadata.transformer()
                       .with(placements)
                       .with(newLockedRanges)
                       .withNodeState(startLeave.nodeId(), NodeState.JOINED);
    }

    @Override
    public String status()
    {
        // Overridden to maintain compatibility with nodetool removenode output
        return String.format("step: %s, streams: %s", next, streams.status());
    }

    private static int nextToIndex(Transformation.Kind next)
    {
        switch (next)
        {
            case START_LEAVE:
                return 0;
            case MID_LEAVE:
                return 1;
            case FINISH_LEAVE:
                return 2;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", next, LEAVE));
        }
    }

    private static Transformation.Kind indexToNext(int index)
    {
        switch (index)
        {
            case 0:
                return START_LEAVE;
            case 1:
                return MID_LEAVE;
            case 2:
                return FINISH_LEAVE;
            default:
                throw new IllegalStateException(String.format("Step %s is invalid for sequence %s ", index, LEAVE));
        }
    }

    @Override
    public boolean finishDuringStartup()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return "UnbootstrapAndLeavePlan{" +
               "lastModified=" + latestModification +
               ", lockKey=" + lockKey +
               ", startLeave=" + startLeave +
               ", midLeave=" + midLeave +
               ", finishLeave=" + finishLeave +
               ", next=" + next +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnbootstrapAndLeave that = (UnbootstrapAndLeave) o;
        return next == that.next &&
               Objects.equals(startLeave, that.startLeave) &&
               Objects.equals(midLeave, that.midLeave) &&
               Objects.equals(finishLeave, that.finishLeave) &&
               Objects.equals(latestModification, that.latestModification) &&
               Objects.equals(lockKey, that.lockKey);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startLeave, midLeave, finishLeave, latestModification, lockKey, next);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, UnbootstrapAndLeave>
    {
        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;

            Epoch.serializer.serialize(plan.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);
            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            VIntCoding.writeUnsignedVInt32(plan.streams.kind().ordinal(), out);

            PrepareLeave.StartLeave.serializer.serialize(plan.startLeave, out, version);
            PrepareLeave.MidLeave.serializer.serialize(plan.midLeave, out, version);
            PrepareLeave.FinishLeave.serializer.serialize(plan.finishLeave, out, version);
        }

        public UnbootstrapAndLeave deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch barrier = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            LeaveStreams.Kind streamKind = LeaveStreams.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareLeave.StartLeave startLeave = PrepareLeave.StartLeave.serializer.deserialize(in, version);
            PrepareLeave.MidLeave midLeave = PrepareLeave.MidLeave.serializer.deserialize(in, version);
            PrepareLeave.FinishLeave finishLeave = PrepareLeave.FinishLeave.serializer.deserialize(in, version);

            return new UnbootstrapAndLeave(barrier, lockKey, next,
                                           startLeave, midLeave, finishLeave,
                                           streamKind.supplier.get());
        }

        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;
            long size = Epoch.serializer.serializedSize(plan.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());
            size += VIntCoding.computeVIntSize(plan.streams.kind().ordinal());
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.startLeave, version);
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.midLeave, version);
            size += PrepareLeave.StartLeave.serializer.serializedSize(plan.finishLeave, version);
            return size;
        }
    }
}
