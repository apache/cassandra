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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
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
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.FinishAddToCMS;
import org.apache.cassandra.tcm.transformations.cms.StartAddToCMS;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.sequences.InProgressSequences.Kind.JOIN_OWNERSHIP_GROUP;
import static org.apache.cassandra.tcm.transformations.cms.EntireRange.*;

/**
 * Add this or another node as a member of CMS.
 */
public class AddToCMS implements InProgressSequence<AddToCMS>
{
    private static final Logger logger = LoggerFactory.getLogger(AddToCMS.class);
    public static Serializer serializer = new Serializer();

    private final ProgressBarrier barrier;
    private final List<InetAddressAndPort> streamCandidates;
    private final FinishAddToCMS finishJoin;

    public static void initiate()
    {
        initiate(ClusterMetadata.current().myNodeId(), FBUtilities.getBroadcastAddressAndPort());
    }

    public static void initiate(NodeId nodeId, InetAddressAndPort addr)
    {
        InProgressSequence<?> continuation = ClusterMetadataService.instance()
                                                                   .commit(new StartAddToCMS(addr),
                                                                           (metadata) -> !metadata.inProgressSequences.contains(nodeId),
                                                                           (metadata) -> metadata.inProgressSequences.get(nodeId),
                                                                           (metadata, reason) -> {
                                                                               throw new IllegalStateException("Can't join ownership group: " + reason);
                                                                           });
        if (continuation.kind() != JOIN_OWNERSHIP_GROUP)
            throw new IllegalStateException(String.format("Following accepted initiation of node to CMS, " +
                                                          "an incorrect sequence %s was found in progress. %s ",
                                            continuation.kind(), continuation));
        continuation.executeNext();
    }

    public AddToCMS(ProgressBarrier barrier, List<InetAddressAndPort> streamCandidates, FinishAddToCMS join)
    {
        this.barrier = barrier;
        this.streamCandidates = streamCandidates;
        this.finishJoin = join;
    }

    @Override
    public ProgressBarrier barrier()
    {
        return barrier;
    }

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

    private void finishJoin()
    {
        NodeId self = ClusterMetadata.current().myNodeId();
        ClusterMetadataService.instance().commit(finishJoin,
                                                 (ClusterMetadata metadata) -> metadata.inProgressSequences.contains(self),
                                                 (ClusterMetadata metadata) -> null,
                                                 (ClusterMetadata metadata, String error) -> {
                                                     throw new IllegalStateException(String.format("Could not finish join due to \"%s\". Next transformation in sequence: %s.",
                                                                                                   error,
                                                                                                   metadata.inProgressSequences.contains(self) ? metadata.inProgressSequences.get(self).nextStep() : null));
                                                 });
    }

    private void repairPaxosTopology() throws ExecutionException, InterruptedException
    {
        ActiveRepairService.instance.repairPaxosForTopologyChange(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                                  Collections.singletonList(entireRange),
                                                                  "bootstrap")
                                    .get();
    }

    public boolean executeNext()
    {
        try
        {
            streamRanges();
            finishJoin();
            repairPaxosTopology();
        }
        catch (Throwable t)
        {
            logger.error("Could not finish adding the node to the metadata ownership group", t);
            throw new RuntimeException(t);
        }

        return false;
    }

    public AddToCMS advance(Epoch waitForWatermark, Transformation.Kind next)
    {
        throw new NoSuchElementException();
    }

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
        return Objects.equals(barrier, addMember.barrier) &&
               Objects.equals(streamCandidates, addMember.streamCandidates) &&
               Objects.equals(finishJoin, addMember.finishJoin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(barrier, streamCandidates, finishJoin);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<InProgressSequence<?>, AddToCMS>
    {
        @Override
        public void serialize(InProgressSequence<?> t, DataOutputPlus out, Version version) throws IOException
        {
            AddToCMS seq = (AddToCMS) t;
            ProgressBarrier.serializer.serialize(t.barrier(), out, version);
            FinishAddToCMS.serializer.serialize(seq.finishJoin, out, version);
            out.writeInt(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                InetAddressAndPort.MetadataSerializer.serializer.serialize(ep, out, version);
        }

        @Override
        public AddToCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            ProgressBarrier barrier = ProgressBarrier.serializer.deserialize(in, version);
            FinishAddToCMS finish = FinishAddToCMS.serializer.deserialize(in, version);
            int streamCandidatesSize = in.readInt();
            List<InetAddressAndPort> streamCandidates = new ArrayList<>();

            for (int i = 0; i < streamCandidatesSize; i++)
                streamCandidates.add(InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version));
            return new AddToCMS(barrier, streamCandidates, finish);
        }

        @Override
        public long serializedSize(InProgressSequence<?> t, Version version)
        {
            AddToCMS seq = (AddToCMS) t;
            long size = ProgressBarrier.serializer.serializedSize(t.barrier(), version);
            size += FinishAddToCMS.serializer.serializedSize(seq.finishJoin, version);
            size += sizeof(seq.streamCandidates.size());
            for (InetAddressAndPort ep : seq.streamCandidates)
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(ep, version);
            return size;
        }
    }
}
