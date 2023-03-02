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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.SystemStrategy;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.vint.VIntCoding;

public class UnbootstrapAndLeave implements InProgressSequence<UnbootstrapAndLeave>
{
    private static final Logger logger = LoggerFactory.getLogger(UnbootstrapAndLeave.class);
    public static final Serializer serializer = new Serializer();

    public final PrepareLeave.StartLeave startLeave;
    public final PrepareLeave.MidLeave midLeave;
    public final PrepareLeave.FinishLeave finishLeave;

    public final ProgressBarrier barrier;
    public final LockedRanges.Key lockKey;
    public final Transformation.Kind next;

    public UnbootstrapAndLeave(ProgressBarrier barrier,
                               LockedRanges.Key lockKey,
                               Transformation.Kind next,
                               PrepareLeave.StartLeave startLeave,
                               PrepareLeave.MidLeave midLeave,
                               PrepareLeave.FinishLeave finishLeave)
    {
        this.barrier = barrier;
        this.lockKey = lockKey;
        this.next = next;
        this.startLeave = startLeave;
        this.midLeave = midLeave;
        this.finishLeave = finishLeave;
    }

    public UnbootstrapAndLeave advance(Epoch waitForWatermark, Transformation.Kind next)
    {
        return new UnbootstrapAndLeave(barrier().withNewEpoch(waitForWatermark), lockKey, next,
                                       startLeave, midLeave, finishLeave);
    }

    public ProgressBarrier barrier()
    {
        return barrier;
    }

    public Transformation.Kind nextStep()
    {
        return next;
    }

    public InProgressSequences.Kind kind()
    {
        return InProgressSequences.Kind.LEAVE;
    }

    public boolean executeNext()
    {
        switch (next)
        {
            case START_LEAVE:
                try
                {
                    ClusterMetadataService.instance().commit(startLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }
                break;
            case MID_LEAVE:
                try
                {
                    unbootstrap(Schema.instance.getNonLocalStrategyKeyspaces(),
                                movementMap(ClusterMetadata.current().directory.endpoint(startLeave.nodeId()),
                                            startLeave.delta(), finishLeave.delta()));
                }
                catch (InterruptedException e)
                {
                    return true;
                }
                catch (ExecutionException e)
                {
                    JVMStabilityInspector.inspectThrowable(e);
                    throw new RuntimeException("Unable to unbootstrap", e);
                }

                try
                {
                    ClusterMetadataService.instance().commit(midLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }
                break;
            case FINISH_LEAVE:
                try
                {
                    ClusterMetadataService.instance().commit(finishLeave);
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    return true;
                }
//                StorageService.instance.finishLeaving();
                break;
            default:
                throw new IllegalStateException("Can't proceed with leave from " + next);
        }

        return true;
    }

    private MovementMap movementMap(InetAddressAndPort leaving, PlacementDeltas startDelta, PlacementDeltas finishDelta)
    {
        MovementMap.Builder allMovements = MovementMap.builder();
        // map of src->dest movements, keyed by replication settings. During unbootstrap, this will be used to construct
        // a stream plan for each keyspace, based on their replication params.
        finishDelta.forEach((params, delta) -> {
            // no streaming for LocalStrategy and friends
            if (SystemStrategy.class.isAssignableFrom(params.klass))
                return;

            // first identify ranges to be migrated off the leaving node
            Map<Range<Token>, Replica> oldReplicas = delta.writes.removals.get(leaving).byRange();

            // next go through the additions to the write groups that will be applied during the
            // first step of the plan. These represent the ranges moving to new replicas so in
            // order to construct a streaming plan we can match these up with the corresponding
            // removals to produce a src->dest mapping.
            EndpointsByReplica.Builder movements = new EndpointsByReplica.Builder();
            RangesByEndpoint startWriteAdditions = startDelta.get(params).writes.additions;
            startWriteAdditions.flattenValues()
                               .forEach(newReplica -> {
                                   movements.put(oldReplicas.get(newReplica.range()), newReplica);
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
            // need to undo MID_LEAVE and START_LEAVE, but PrepareLeave doesn't affect placement
            case FINISH_LEAVE:
                placements = midLeave.inverseDelta().apply(placements);
            case MID_LEAVE:
            case START_LEAVE:
                placements = startLeave.inverseDelta().apply(placements);
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

    private static void unbootstrap(Keyspaces keyspaces, MovementMap movements) throws ExecutionException, InterruptedException
    {
        Supplier<Future<StreamState>> startStreaming = prepareUnbootstrapStreaming(keyspaces, movements);

        logger.info("replaying batch log and streaming data to other nodes");
        // Start with BatchLog replay, which may create hints but no writes since this is no longer a valid endpoint.
        Future<?> batchlogReplay = BatchlogManager.instance.startBatchlogReplay();
        Future<StreamState> streamSuccess = startStreaming.get();

        // Wait for batch log to complete before streaming hints.
        logger.debug("waiting for batch log processing.");
        batchlogReplay.get();

        logger.info("streaming hints to other nodes");

        Future<?> hintsSuccess = StorageService.instance.streamHints();

        // wait for the transfer runnables to signal the latch.
        logger.debug("waiting for stream acks.");
        streamSuccess.get();
        hintsSuccess.get();
        logger.debug("stream acks all received.");
    }

    private static Supplier<Future<StreamState>> prepareUnbootstrapStreaming(Keyspaces keyspaces,
                                                                             MovementMap movements)
    {
        // PrepareLeave transformation gives us a map of range movements for unbootstrap, keyed on replication settings.
        // The movements themselves are maps of leavingReplica -> newReplica(s). Here we just "inflate" the outer
        // map to a set of movements per-keyspace, duplicating where keyspaces share the same replication params
        Map<String, EndpointsByReplica> byKeyspace =
        keyspaces.stream()
                 .collect(Collectors.toMap(k -> k.name,
                                           k -> movements.get(k.params.replication)));

        return () -> StorageService.instance.streamRanges(byKeyspace);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UnbootstrapAndLeave that = (UnbootstrapAndLeave) o;
        return Objects.equals(startLeave, that.startLeave) &&
               Objects.equals(midLeave, that.midLeave) &&
               Objects.equals(finishLeave, that.finishLeave) &&
               Objects.equals(barrier, that.barrier) &&
               Objects.equals(lockKey, that.lockKey) &&
               next == that.next;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startLeave, midLeave, finishLeave, barrier, lockKey, next);
    }

    @Override
    public String toString()
    {
        return "UnbootstrapAndLeavePlan{" +
               "barrier=" + barrier +
               ", lockKey=" + lockKey +
               ", startLeave=" + startLeave +
               ", midLeave=" + midLeave +
               ", finishLeave=" + finishLeave +
               ", next=" + next +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, UnbootstrapAndLeave>
    {
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;

            ProgressBarrier.serializer.serialize(t.barrier(), out, version);
            LockedRanges.Key.serializer.serialize(plan.lockKey, out, version);

            VIntCoding.writeUnsignedVInt32(plan.next.ordinal(), out);
            if (plan.next.ordinal() >= Transformation.Kind.START_LEAVE.ordinal())
                PrepareLeave.StartLeave.serializer.serialize(plan.startLeave, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.MID_LEAVE.ordinal())
                PrepareLeave.MidLeave.serializer.serialize(plan.midLeave, out, version);
            if (plan.next.ordinal() >= Transformation.Kind.FINISH_LEAVE.ordinal())
                PrepareLeave.FinishLeave.serializer.serialize(plan.finishLeave, out, version);
        }

        public UnbootstrapAndLeave deserialize(DataInputPlus in, Version version) throws IOException
        {
            ProgressBarrier barrier = ProgressBarrier.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            Transformation.Kind next = Transformation.Kind.values()[VIntCoding.readUnsignedVInt32(in)];
            PrepareLeave.StartLeave startLeave = null;
            if (next.ordinal() >= Transformation.Kind.START_LEAVE.ordinal())
                startLeave = PrepareLeave.StartLeave.serializer.deserialize(in, version);
            PrepareLeave.MidLeave midLeave = null;
            if (next.ordinal() >= Transformation.Kind.MID_LEAVE.ordinal())
                midLeave = PrepareLeave.MidLeave.serializer.deserialize(in, version);
            PrepareLeave.FinishLeave finishLeave = null;
            if (next.ordinal() >= Transformation.Kind.FINISH_LEAVE.ordinal())
                finishLeave = PrepareLeave.FinishLeave.serializer.deserialize(in, version);

            return new UnbootstrapAndLeave(barrier, lockKey, next,
                                           startLeave, midLeave, finishLeave);
        }

        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            UnbootstrapAndLeave plan = (UnbootstrapAndLeave) t;
            long size = ProgressBarrier.serializer.serializedSize(plan.barrier(), version);
            size += LockedRanges.Key.serializer.serializedSize(plan.lockKey, version);

            size += VIntCoding.computeVIntSize(plan.kind().ordinal());

            if (plan.next.ordinal() >= Transformation.Kind.START_LEAVE.ordinal())
                size += PrepareLeave.StartLeave.serializer.serializedSize(plan.startLeave, version);
            if (plan.next.ordinal() >= Transformation.Kind.MID_LEAVE.ordinal())
                size += PrepareLeave.StartLeave.serializer.serializedSize(plan.midLeave, version);
            if (plan.next.ordinal() >= Transformation.Kind.FINISH_LEAVE.ordinal())
                size += PrepareLeave.StartLeave.serializer.serializedSize(plan.finishLeave, version);
            return size;
        }
    }
}
