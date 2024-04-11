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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.locator.MetaStrategy.entireRange;
import static org.apache.cassandra.tcm.MultiStepOperation.Kind.RECONFIGURE_CMS;

/**
 * A step in a CMS Reconfiguration sequence. This may represent the addition of a new CMS member or the removal of an
 * existing one. Member additions are actually further decomposed into a pair of distinct steps: the first adds the
 * node as a passive member of the CMS that only receives committed log updates, while the second enables it to begin
 * participating in reads and in quorums for commit. Each of these two steps will be implemented by an instance of this
 * class. Removing a member is more straightforward and so is done in a single step.
 * See the {@link #startAdd}, {@link #finishAdd} and {@link #executeRemove} emove} methods.
 */
public class AdvanceCMSReconfiguration implements Transformation
{
    public static final Serializer serializer = new Serializer();

    // Identifies the position this instance represents in a sequence to reconfigure the CMS. Such sequences are dynamic
    // and only contain a single element at any one time. Logically sequences of this specific type comprise multiple
    // steps, which are created anew when we advance from step to step.
    public final int sequenceIndex;
    // Identifies the epoch enacted by the preceding step in this reconfiguration sequence. Used to construct a
    // ProgressBarrier when stepping through the sequence. Initialising a completely new sequence is a special case here
    // as there is no preceding epoch, so the factory method in ReconfigureCMS which does this will supply Epoch.EMPTY
    // which results in ProgressBarrier.immediate()
    public final Epoch latestModification;
    public final LockedRanges.Key lockKey;

    public final PrepareCMSReconfiguration.Diff diff;
    public final ReconfigureCMS.ActiveTransition activeTransition;

    public AdvanceCMSReconfiguration(int sequenceIndex,
                                     Epoch latestModification,
                                     LockedRanges.Key lockKey,
                                     PrepareCMSReconfiguration.Diff diff,
                                     ReconfigureCMS.ActiveTransition active)
    {
        this.sequenceIndex = sequenceIndex;
        this.latestModification = latestModification;
        this.lockKey = lockKey;
        this.diff = diff;
        this.activeTransition = active;
    }

    @Override
    public Kind kind()
    {
        return Kind.ADVANCE_CMS_RECONFIGURATION;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        InProgressSequences sequences = prev.inProgressSequences;
        MultiStepOperation<?> sequence = sequences.get(ReconfigureCMS.SequenceKey.instance);

        if (sequence == null)
            return new Transformation.Rejected(INVALID, "Can't advance CMS Reconfiguration as it is not present in current metadata");

        if (sequence.kind() != RECONFIGURE_CMS)
            return new Transformation.Rejected(INVALID, "Can't advance CMS Reconfiguraton as in incompatible sequence was detected: " + sequence.kind());

        ReconfigureCMS reconfigureCMS = (ReconfigureCMS) sequence;
        if (reconfigureCMS.next.sequenceIndex != sequenceIndex)
            return new Transformation.Rejected(INVALID, String.format("This transformation (%d) has already been applied. Expected: %d", sequenceIndex, reconfigureCMS.next.sequenceIndex));

        // An active transition means that the preceding step in this sequences began adding a new member
        if (activeTransition == null)
        {
            // Execute additions before removals to avoid shrinking the CMS to the extent that we cannot then expand it
            if (!diff.additions.isEmpty())
            {
                return startAdd(prev, reconfigureCMS);
            }
            // Any additions have already been completed, start removing the CMS members specified by the diff
            else if (!diff.removals.isEmpty())
            {
                return executeRemove(prev, reconfigureCMS);
            }
            // All additions and removals in the reconfiguration sequence have completed, the final step is to remove
            // the sequence itselt from ClusterMetadata and release the lock
            else
            {
                return Transformation.success(prev.transformer()
                                                  .with(prev.inProgressSequences.without(ReconfigureCMS.SequenceKey.instance))
                                                  .with(prev.lockedRanges.unlock(lockKey)),
                                              MetaStrategy.affectedRanges(prev));
            }
        }
        else
        {
            // A 2 step member addition is in progress, so complete it
            return finishAdd(prev, reconfigureCMS, activeTransition.nodeId);
        }
    }

    /**
     * Execute the transformation to begin adding a CMS member.
     * Takes the node to be added from the diff and makes it a write replica of the CMS.
     * Identifies the sources for streaming to it, which the reconfiguration sequence will initiate before attempting
     * to execute the next step.
     * Advances the sequence by constructing the next step and updating the stored sequences.
     * @param prev
     * @param sequence
     * @return
     * @throws Transformation.RejectedTransformationException
     */
    private Transformation.Result startAdd(ClusterMetadata prev, ReconfigureCMS sequence)
    {
        // Pop the next node to be added from the list diff.additions
        NodeId addition = diff.additions.get(0);
        InetAddressAndPort endpoint = prev.directory.endpoint(addition);
        Replica replica = new Replica(endpoint, entireRange, true);
        List<NodeId> newAdditions = new ArrayList<>(diff.additions.subList(1, diff.additions.size()));

        // Check that the candidate is not already a CMS member
        ReplicationParams metaParams = ReplicationParams.meta(prev);
        RangesByEndpoint readReplicas = prev.placements.get(metaParams).reads.byEndpoint();
        RangesByEndpoint writeReplicas = prev.placements.get(metaParams).writes.byEndpoint();
        if (readReplicas.containsKey(endpoint) || writeReplicas.containsKey(endpoint))
            return new Transformation.Rejected(INVALID, "Endpoint is already a member of CMS");


        ClusterMetadata.Transformer transformer = prev.transformer();
        // Add the candidate as a write replica
        DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild()
                                                       .withWriteReplica(prev.nextEpoch(), replica);
        transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build());

        // Construct a set of sources for the new member to stream log tables from (essentially this is the existing members)
        Set<InetAddressAndPort> streamCandidates = new HashSet<>();
        for (Replica r : prev.placements.get(metaParams).reads.byEndpoint().flattenValues())
        {
            if (!replica.equals(r))
                streamCandidates.add(r.endpoint());
        }

        // Set up the next step in the sequence. This encapsulates the entire state of the reconfiguration sequence,
        // including the remaining add/remove operations and the streaming that needs to be done by the joining node
        AdvanceCMSReconfiguration next = next(prev.nextEpoch(),
                                              newAdditions,
                                              diff.removals,
                                              new ReconfigureCMS.ActiveTransition(addition, streamCandidates));
        // Create a new sequence instance with the next step to reflect that the state has progressed.
        ReconfigureCMS advanced = sequence.advance(next);
        // Finally, replace the existing reconfiguration sequence with this updated one.
        transformer.with(prev.inProgressSequences.with(ReconfigureCMS.SequenceKey.instance, (ReconfigureCMS old) -> advanced));
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    /**
     * Execute the transformation to finish adding a CMS member.
     * Takes the node currently being added, which was obtained from the sequence's ActiveTransition and makes it a
     * full (read/write) replica of the CMS.
     * Advances the sequence by constructing the next step and updating the stored sequences.
     * @param prev
     * @param sequence
     * @param addition
     * @return
     * @throws Transformation.RejectedTransformationException
     */
    private Transformation.Result finishAdd(ClusterMetadata prev, ReconfigureCMS sequence, NodeId addition)
    {
        // Add the new member as a full read replica, able to participate in quorums for log updates
        ReplicationParams metaParams = ReplicationParams.meta(prev);
        InetAddressAndPort endpoint = prev.directory.endpoint(addition);
        Replica replica = new Replica(endpoint, entireRange, true);
        ClusterMetadata.Transformer transformer = prev.transformer();
        DataPlacement.Builder builder = prev.placements.get(metaParams)
                                                       .unbuild()
                                                       .withReadReplica(prev.nextEpoch(), replica);
        transformer = transformer.with(prev.placements.unbuild().with(metaParams, builder.build()).build());

        // Set up the next step in the sequence. This encapsulates the entire state of the reconfiguration sequence,
        // which includes the remaining add/remove operations
        AdvanceCMSReconfiguration next = next(prev.nextEpoch(), diff.additions, diff.removals, null);
        // Create a new sequence instance with the next step to reflect that the state has progressed.
        ReconfigureCMS advanced = sequence.advance(next);
        // Finally, replace the existing reconfiguration sequence with this updated one.
        transformer.with(prev.inProgressSequences.with(ReconfigureCMS.SequenceKey.instance, (ReconfigureCMS old) -> advanced));
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    /**
     * Execute the transformation to remove a CMS member.
     * Takes the node to be removed from the diff and removes it from the read/write replicas of the CMS.
     * Advances the sequence by constructing the next step and updating the stored sequences.
     */
    private Transformation.Result executeRemove(ClusterMetadata prev, ReconfigureCMS sequence)
    {
        // Pop the next member to be removed from the list diff.removals
        NodeId removal = diff.removals.get(0);
        List<NodeId> newRemovals = new ArrayList<>(diff.removals.subList(1, diff.removals.size()));

        // Check that the candidate is actually a CMS member
        ClusterMetadata.Transformer transformer = prev.transformer();
        InetAddressAndPort endpoint = prev.directory.endpoint(removal);
        Replica replica = new Replica(endpoint, entireRange, true);
        ReplicationParams metaParams = ReplicationParams.meta(prev);
        if (!prev.fullCMSMembers().contains(endpoint))
            return new Transformation.Rejected(INVALID, String.format("%s is not currently a CMS member, cannot remove it", endpoint));

        // Check that the candidate is not the only CMS member
        DataPlacement.Builder builder = prev.placements.get(metaParams).unbuild();
        builder.reads.withoutReplica(prev.nextEpoch(), replica);
        builder.writes.withoutReplica(prev.nextEpoch(), replica);
        DataPlacement proposed = builder.build();
        if (proposed.reads.byEndpoint().isEmpty() || proposed.writes.byEndpoint().isEmpty())
            return new Transformation.Rejected(INVALID, String.format("Removing %s will leave no nodes in CMS", endpoint));

        // Actually remove the candidate
        transformer = transformer.with(prev.placements.unbuild().with(metaParams, proposed).build());

        // Set up the next step in the sequence. This encapsulates the entire state of the reconfiguration sequence,
        // which includes the remaining add/remove operations
        AdvanceCMSReconfiguration next = next(prev.nextEpoch(), diff.additions, newRemovals, null);
        // Create a new sequence instance with the next step to reflect that the state has progressed.
        ReconfigureCMS advanced = sequence.advance(next);
        // Finally, replace the existing reconfiguration sequence with this updated one.
        transformer.with(prev.inProgressSequences.with(ReconfigureCMS.SequenceKey.instance, (ReconfigureCMS old) -> advanced));
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    private AdvanceCMSReconfiguration next(Epoch latestModification,
                                           List<NodeId> additions,
                                           List<NodeId> removals,
                                           ReconfigureCMS.ActiveTransition active)
    {
        return new AdvanceCMSReconfiguration(sequenceIndex + 1,
                                             latestModification,
                                             lockKey,
                                             new PrepareCMSReconfiguration.Diff(additions, removals),
                                             active);
    }

    public boolean isLast()
    {
        if (!diff.additions.isEmpty())
            return false;
        if (!diff.removals.isEmpty())
            return false;
        if (activeTransition != null)
            return false;

        return true;
    }

    public String toString()
    {
        String current;
        if (activeTransition == null)
        {
            if (!diff.additions.isEmpty())
            {
                NodeId addition = diff.additions.get(0);
                current = "StartAddToCMS(" + addition + ")";
            }
            else if (!diff.removals.isEmpty())
            {
                NodeId removal = diff.removals.get(0);
                current = "RemoveFromCMS(" + removal + ")";
            }
            else
            {
                current = "FinishReconfiguration()";
            }
        }
        else
        {
            current = "FinishCMSReconfiguration()";
        }
        return "AdvanceCMSReconfiguration{" +
               "idx=" + sequenceIndex +
               ", current=" + current +
               ", diff=" + diff +
               ", activeTransition=" + activeTransition +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceCMSReconfiguration>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceCMSReconfiguration transformation = (AdvanceCMSReconfiguration) t;
            out.writeUnsignedVInt32(transformation.sequenceIndex);
            Epoch.serializer.serialize(transformation.latestModification, out, version);
            LockedRanges.Key.serializer.serialize(transformation.lockKey, out, version);

            PrepareCMSReconfiguration.Diff.serializer.serialize(transformation.diff, out, version);

            out.writeBoolean(transformation.activeTransition != null);
            if (transformation.activeTransition != null)
            {
                ReconfigureCMS.ActiveTransition activeTransition = transformation.activeTransition;
                NodeId.serializer.serialize(activeTransition.nodeId, out, version);
                out.writeInt(activeTransition.streamCandidates.size());
                for (InetAddressAndPort e : activeTransition.streamCandidates)
                    InetAddressAndPort.MetadataSerializer.serializer.serialize(e, out, version);
            }
        }

        public AdvanceCMSReconfiguration deserialize(DataInputPlus in, Version version) throws IOException
        {
            int idx = in.readUnsignedVInt32();
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            LockedRanges.Key lockKey = LockedRanges.Key.serializer.deserialize(in, version);

            PrepareCMSReconfiguration.Diff diff = PrepareCMSReconfiguration.Diff.serializer.deserialize(in, version);

            boolean hasActiveTransition = in.readBoolean();
            ReconfigureCMS.ActiveTransition activeTransition = null;
            if (hasActiveTransition)
            {
                NodeId nodeId = NodeId.serializer.deserialize(in, version);
                int streamCandidatesCount = in.readInt();
                Set<InetAddressAndPort> streamCandidates = new HashSet<>();
                for (int i = 0; i < streamCandidatesCount; i++)
                    streamCandidates.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
                activeTransition = new ReconfigureCMS.ActiveTransition(nodeId, streamCandidates);
            }

            return new AdvanceCMSReconfiguration(idx, lastModified, lockKey, diff, activeTransition);
        }

        public long serializedSize(Transformation t, Version version)
        {
            AdvanceCMSReconfiguration transformation = (AdvanceCMSReconfiguration) t;
            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(transformation.sequenceIndex);
            size += Epoch.serializer.serializedSize(transformation.latestModification, version);
            size += LockedRanges.Key.serializer.serializedSize(transformation.lockKey, version);
            size += PrepareCMSReconfiguration.Diff.serializer.serializedSize(transformation.diff, version);

            size += TypeSizes.BOOL_SIZE;
            if (transformation.activeTransition != null)
            {
                ReconfigureCMS.ActiveTransition activeTransition = transformation.activeTransition;
                size += NodeId.serializer.serializedSize(activeTransition.nodeId, version);
                size += TypeSizes.INT_SIZE;
                for (InetAddressAndPort e : activeTransition.streamCandidates)
                    size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(e, version);
            }

            return size;
        }
    }

}
