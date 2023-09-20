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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.FinishAddToCMS;
import org.apache.cassandra.tcm.transformations.cms.StartAddToCMS;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.sequences.InProgressSequences.Kind.JOIN_OWNERSHIP_GROUP;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.transformations.cms.EntireRange.entireRange;

/**
 * Add this or another node as a member of CMS.
 */
public class AddToCMS extends InProgressSequence<AddToCMS>
{
    private static final Logger logger = LoggerFactory.getLogger(AddToCMS.class);
    public static Serializer serializer = new Serializer();

    private final Epoch latestModification;
    private final NodeId toAdd;
    private final List<InetAddressAndPort> streamCandidates;
    private final FinishAddToCMS finishJoin;

    public static void initiate()
    {
        initiate(ClusterMetadata.current().myNodeId(), FBUtilities.getBroadcastAddressAndPort());
    }

    public static void initiate(NodeId nodeId, InetAddressAndPort addr)
    {
        InProgressSequence<?> sequence = ClusterMetadataService.instance()
                                                               .commit(new StartAddToCMS(addr),
                                                                       (metadata) -> metadata.inProgressSequences.get(nodeId),
                                                                       (metadata, code, reason) -> {
                                                                           InProgressSequence<?> sequence_ = metadata.inProgressSequences.get(nodeId);

                                                                           if (sequence_ == null)
                                                                           {
                                                                               throw new IllegalStateException("Can't join ownership group: " + reason);
                                                                           }

                                                                           // We might have discovered a startup sequence we ourselves committed but got no response for
                                                                           if (sequence_.kind() != JOIN_OWNERSHIP_GROUP)
                                                                           {
                                                                               throw new IllegalStateException(String.format("Following accepted initiation of node to CMS, " +
                                                                                                                             "an incorrect sequence %s was found in progress. %s ",
                                                                                                                             sequence_.kind(), sequence_));
                                                                           }

                                                                           return metadata.inProgressSequences.get(nodeId);
                                                                       });

        InProgressSequences.resume(sequence);
        repairPaxosTopology();
    }

    public AddToCMS(Epoch latestModification, NodeId toAdd, List<InetAddressAndPort> streamCandidates, FinishAddToCMS join)
    {
        this.toAdd = toAdd;
        this.latestModification = latestModification;
        this.streamCandidates = streamCandidates;
        this.finishJoin = join;
    }

    @Override
    public ProgressBarrier barrier()
    {
        return ProgressBarrier.immediate();
    }

    @Override
    public Transformation.Kind nextStep()
    {
        return finishJoin.kind();
    }

    private void streamRanges() throws ExecutionException, InterruptedException
    {
        StreamPlan streamPlan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, true, null, PreviewKind.NONE);
        // Current node is the streaming target. We can pick any other live CMS node as a streaming source
        if (finishJoin.getEndpoint().equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            Optional<InetAddressAndPort> streamingSource = streamCandidates.stream().filter(FailureDetector.instance::isAlive).findFirst();
            if (!streamingSource.isPresent())
                throw new IllegalStateException(String.format("Can not start range streaming as all candidates (%s) are down", streamCandidates));
            streamPlan.requestRanges(streamingSource.get(),
                                     SchemaConstants.METADATA_KEYSPACE_NAME,
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).add(finishJoin.replicaForStreaming()).build(),
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).build(),
                                     DistributedMetadataLogKeyspace.TABLE_NAME);
        }
        else
        {
            streamPlan.transferRanges(finishJoin.getEndpoint(),
                                      SchemaConstants.METADATA_KEYSPACE_NAME,
                                      new RangesAtEndpoint.Builder(finishJoin.replicaForStreaming().endpoint()).add(finishJoin.replicaForStreaming()).build(),
                                      DistributedMetadataLogKeyspace.TABLE_NAME);
        }
        streamPlan.execute().get();
    }

    private static void repairPaxosTopology()
    {
        Retry.Backoff retry = new Retry.Backoff(TCMMetrics.instance.repairPaxosTopologyRetries);
        List<Supplier<Future<?>>> remaining = ActiveRepairService.instance.repairPaxosForTopologyChangeAsync(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                                                                             Collections.singletonList(entireRange),
                                                                                                             "bootstrap");

        while (!retry.reachedMax())
        {
            Map<Supplier<Future<?>>, Future<?>> tasks = new HashMap<>();
            for (Supplier<Future<?>> supplier : remaining)
                tasks.put(supplier, supplier.get());
            remaining.clear();
            logger.info("Performing paxos topology repair on:", remaining);

            for (Map.Entry<Supplier<Future<?>>, Future<?>> e : tasks.entrySet())
            {
                try
                {
                    e.getValue().get();
                }
                catch (ExecutionException t)
                {
                    logger.error("Caught an exception while repairing paxos topology.", e);
                    remaining.add(e.getKey());
                }
                catch (InterruptedException t)
                {
                    return;
                }
            }

            if (remaining.isEmpty())
                return;

            retry.maybeSleep();
        }
        logger.error(String.format("Added node as a CMS, but failed to repair paxos topology after this operation."));
    }

    @Override
    public SequenceState executeNext()
    {
        try
        {
            streamRanges();
            commit(finishJoin);
        }
        catch (Throwable t)
        {
            logger.error("Could not finish adding the node to the metadata ownership group", t);
        }

        return continuable();
    }

    @Override
    public AddToCMS advance(Epoch waitForWatermark)
    {
        throw new NoSuchElementException();
    }

    @Override
    protected Transformation.Kind stepFollowing(Transformation.Kind kind)
    {
        if (kind == null)
            return null;

        switch (kind)
        {
            case START_ADD_TO_CMS:
                return Transformation.Kind.FINISH_ADD_TO_CMS;
            case FINISH_ADD_TO_CMS:
                return null;
            default:
                throw new IllegalStateException(String.format("Step %s is not a part of %s sequence", kind, kind()));
        }
    }

    @Override
    protected NodeId nodeId()
    {
        return toAdd;
    }

    @Override
    public InProgressSequences.Kind kind()
    {
        return JOIN_OWNERSHIP_GROUP;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddToCMS addMember = (AddToCMS) o;
        return Objects.equals(latestModification, addMember.latestModification) &&
               Objects.equals(streamCandidates, addMember.streamCandidates) &&
               Objects.equals(finishJoin, addMember.finishJoin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(latestModification, streamCandidates, finishJoin);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, AddToCMS>
    {
        @Override
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            AddToCMS seq = (AddToCMS) t;
            NodeId.serializer.serialize(seq.toAdd,out, version);
            Epoch.serializer.serialize(seq.latestModification, out, version);
            FinishAddToCMS.serializer.serialize(seq.finishJoin, out, version);
            out.writeInt(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(ep, out, version);
        }

        @Override
        public AddToCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            NodeId nodeId = NodeId.serializer.deserialize(in, version);
            Epoch barrier = Epoch.serializer.deserialize(in, version);
            FinishAddToCMS finish = FinishAddToCMS.serializer.deserialize(in, version);
            int streamCandidatesSize = in.readInt();
            List<InetAddressAndPort> streamCandidates = new ArrayList<>();

            for (int i = 0; i < streamCandidatesSize; i++)
                streamCandidates.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
            return new AddToCMS(barrier, nodeId, streamCandidates, finish);
        }

        @Override
        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            AddToCMS seq = (AddToCMS) t;
            long size = NodeId.serializer.serializedSize(seq.toAdd, version);
            size += Epoch.serializer.serializedSize(seq.latestModification, version);
            size += FinishAddToCMS.serializer.serializedSize(seq.finishJoin, version);
            size += sizeof(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(ep, version);
            return size;
        }
    }
}
