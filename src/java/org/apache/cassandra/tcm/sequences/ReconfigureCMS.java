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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.DataMovement;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.MovementMap;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.cms.AdvanceCMSReconfiguration;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.streaming.StreamOperation.RESTORE_REPLICA_COUNT;
import static org.apache.cassandra.locator.MetaStrategy.entireRange;

public class ReconfigureCMS extends MultiStepOperation<AdvanceCMSReconfiguration>
{
    public static final Serializer serializer = new Serializer();
    private static final Logger logger = LoggerFactory.getLogger(ReconfigureCMS.class);

    /**
     * We store the state (lock key, diff, active transition, position in the logical sequence, latest epoch enacted
     * as part of this sequence) in the singleton transformation itself. This simplifies access to that state by the
     * transformation and makes its representation in `Enacted` log messages and the virtual log table more useful.
     */
    public final AdvanceCMSReconfiguration next;

    /**
     * Factory method, called when intiating a sequence to reconfigure the membership of the CMS. Supplies Epoch.EMPTY
     * as the progress barrier condition as an entirely new reconfiguration sequence has no prerequisite.
     * @param lockKey token which prevents intersecting operations being run concurrently. Due to the scope and nature
     *                of this particular operation this key always covers the entire cluster, effectively preventing
     *                multiple CMS reconfigurations from being prepared concurrently. It is stored on the sequence
     *                itself so that it can be released when the sequence completes or is cancelled.
     * @param diff The set of add member / remove member operations that must be executed to transform the CMS
     *             membership between the initial and desired states.
     */
    public static ReconfigureCMS newSequence(LockedRanges.Key lockKey, PrepareCMSReconfiguration.Diff diff)
    {
        return new ReconfigureCMS(new AdvanceCMSReconfiguration(0, Epoch.EMPTY, lockKey, diff, null));
    }

    /**
     * Called by the factory method and deserializer.
     * The supplied transformation represents the next step in the logical sequence.
     * @param next step to be executed next
     */
    private ReconfigureCMS(AdvanceCMSReconfiguration next)
    {
        super(next.sequenceIndex, next.latestModification);
        this.next = next;
    }
    @Override
    public Kind kind()
    {
        return MultiStepOperation.Kind.RECONFIGURE_CMS;
    }
    @Override
    protected SequenceKey sequenceKey()
    {
        return SequenceKey.instance;
    }

    @Override
    public MetadataSerializer<? extends MultiStepOperation.SequenceKey> keySerializer()
    {
        return SequenceKey.serializer;
    }

    @Override public Transformation.Kind nextStep()
    {
        return next.kind();
    }

    @Override
    public Transformation.Result applyTo(ClusterMetadata metadata)
    {
        MultiStepOperation<?> sequence = metadata.inProgressSequences.get(SequenceKey.instance);
        if (sequence.kind() != MultiStepOperation.Kind.RECONFIGURE_CMS)
            throw new IllegalStateException(String.format("Can not apply in-progress sequence, since its kind is %s, but not %s", sequence.kind(), MultiStepOperation.Kind.RECONFIGURE_CMS));
        Epoch lastModifiedEpoch = metadata.epoch;
        ImmutableSet.Builder<MetadataKey> modifiedKeys = ImmutableSet.builder();
        while (metadata.inProgressSequences.contains(SequenceKey.instance))
        {
            ReconfigureCMS transitionCMS = (ReconfigureCMS) metadata.inProgressSequences.get(SequenceKey.instance);
            Transformation.Result result = transitionCMS.next.execute(metadata);
            assert result.isSuccess();
            metadata = result.success().metadata.forceEpoch(lastModifiedEpoch);
            modifiedKeys.addAll(result.success().affectedMetadata);
        }
        return new Transformation.Success(metadata.forceEpoch(lastModifiedEpoch.nextEpoch()), LockedRanges.AffectedRanges.EMPTY, modifiedKeys.build());
    }

    @Override
    public SequenceState executeNext()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        MultiStepOperation<?> sequence = metadata.inProgressSequences.get(SequenceKey.instance);
        if (sequence.kind() != MultiStepOperation.Kind.RECONFIGURE_CMS)
            throw new IllegalStateException(String.format("Can not advance in-progress sequence, since its kind is %s, but not %s", sequence.kind(), MultiStepOperation.Kind.RECONFIGURE_CMS));

        ReconfigureCMS transitionCMS = (ReconfigureCMS) sequence;
        try
        {
            if (transitionCMS.next.activeTransition != null)
            {
                // An active transition represents a joining member which has been added as a write replica, but must
                // stream up to date distributed log tables before being able to serve reads & participate in quorums.
                // If this is the case, do that streaming now.
                ActiveTransition activeTransition = transitionCMS.next.activeTransition;
                InetAddressAndPort endpoint = metadata.directory.endpoint(activeTransition.nodeId);
                Replica replica = new Replica(endpoint, entireRange, true);
                streamRanges(replica, activeTransition.streamCandidates);
            }
            // Commit the next step in the sequence
            ClusterMetadataService.instance().commit(transitionCMS.next);
            return SequenceState.continuable();
        }
        catch (Throwable t)
        {
            String message = "Some data streaming failed. Use nodetool to check CMS reconfiguration state and resume. " +
                             "For more, see `nodetool help cms reconfigure`.";
            logger.warn(message);
            return SequenceState.error(new RuntimeException(message));
        }
    }

    @Override
    public ReconfigureCMS advance(AdvanceCMSReconfiguration next)
    {
        return new ReconfigureCMS(next);
    }

    @Override
    public ProgressBarrier barrier()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        return new ProgressBarrier(latestModification,
                                   metadata.directory.location(metadata.myNodeId()),
                                   MetaStrategy.affectedRanges(metadata));
    }

    public static void maybeReconfigureCMS(ClusterMetadata metadata, InetAddressAndPort toRemove)
    {
        if (!metadata.fullCMSMembers().contains(toRemove))
            return;
        Set<NodeId> downNodes = new HashSet<>();
        for (InetAddressAndPort ep : metadata.directory.allJoinedEndpoints())
            if (!FailureDetector.instance.isAlive(ep))
                downNodes.add(metadata.directory.peerId(ep));

        PrepareCMSReconfiguration.Simple transformation = new PrepareCMSReconfiguration.Simple(metadata.directory.peerId(toRemove), downNodes);
        transformation.verify(metadata);
        // We can force removal from the CMS as it doesn't alter the size of the service
        ClusterMetadataService.instance().commit(transformation);

        InProgressSequences.finishInProgressSequences(SequenceKey.instance);
        if (ClusterMetadata.current().isCMSMember(toRemove))
            throw new IllegalStateException(String.format("Could not remove %s from CMS", toRemove));
    }

    private static void initiateRemoteStreaming(Replica replicaForStreaming, Set<InetAddressAndPort> streamCandidates)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        EndpointsForRange.Builder efr = EndpointsForRange.builder(entireRange);
        streamCandidates.forEach(addr -> efr.add(new Replica(addr, entireRange, true)));

        MovementMap movements = MovementMap.builder().put(ReplicationParams.meta(metadata),
                                                          new EndpointsByReplica(Collections.singletonMap(replicaForStreaming, efr.build())))
                                           .build();

        String operationId = replicaForStreaming.toString();
        DataMovements.ResponseTracker responseTracker = DataMovements.instance.registerMovements(RESTORE_REPLICA_COUNT, operationId, movements);
        movements.byEndpoint().forEach((ep, epMovements) -> {
            DataMovement msg = new DataMovement(operationId, RESTORE_REPLICA_COUNT.name(), epMovements);
            MessagingService.instance().sendWithCallback(Message.out(Verb.INITIATE_DATA_MOVEMENTS_REQ, msg), ep, response -> {
                logger.debug("Endpoint {} starting streams {}", response.from(), epMovements);
            });
        });

        try
        {
            responseTracker.await();
        }
        finally
        {
            DataMovements.instance.unregisterMovements(RESTORE_REPLICA_COUNT, operationId);
        }
    }

    public static void streamRanges(Replica replicaForStreaming, Set<InetAddressAndPort> streamCandidates) throws ExecutionException, InterruptedException
    {
        InetAddressAndPort endpoint = replicaForStreaming.endpoint();

        // Current node is the streaming target. We can pick any other live CMS node as a streaming source
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
        {
            StreamPlan streamPlan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, true, null, PreviewKind.NONE);
            Optional<InetAddressAndPort> streamingSource = streamCandidates.stream().filter(FailureDetector.instance::isAlive).findFirst();
            if (!streamingSource.isPresent())
                throw new IllegalStateException(String.format("Can not start range streaming as all candidates (%s) are down", streamCandidates));
            streamPlan.requestRanges(streamingSource.get(),
                                     SchemaConstants.METADATA_KEYSPACE_NAME,
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).add(replicaForStreaming).build(),
                                     new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).build(),
                                     DistributedMetadataLogKeyspace.TABLE_NAME);
            streamPlan.execute().get();
        }
        // Current node is a live CMS node, therefore the streaming source
        else if (streamCandidates.contains(FBUtilities.getBroadcastAddressAndPort()))
        {
            StreamPlan streamPlan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, true, null, PreviewKind.NONE);
            streamPlan.transferRanges(endpoint,
                                      SchemaConstants.METADATA_KEYSPACE_NAME,
                                      new RangesAtEndpoint.Builder(replicaForStreaming.endpoint()).add(replicaForStreaming).build(),
                                      DistributedMetadataLogKeyspace.TABLE_NAME);
            streamPlan.execute().get();
        }
        // We are neither a target, nor a source, so initiate streaming on the target
        else
        {
            initiateRemoteStreaming(replicaForStreaming, streamCandidates);
        }

    }

    @Override
    public String toString()
    {
        return "ReconfigureCMS{" +
               "next=" + next +
               ", idx=" + idx +
               ", latestModification=" + latestModification +
               '}';
    }

    static void repairPaxosTopology()
    {
        Retry.Backoff retry = new Retry.Backoff(TCMMetrics.instance.repairPaxosTopologyRetries);

        // The system.paxos table is what we're actually repairing and that uses the system configured partitioner
        // so although we use MetaStrategy.entireRange for streaming between CMS members, we don't use it here
        Range<Token> entirePaxosRange = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                    DatabaseDescriptor.getPartitioner().getMinimumToken());
        List<Supplier<Future<?>>> remaining = ActiveRepairService.instance().repairPaxosForTopologyChangeAsync(SchemaConstants.METADATA_KEYSPACE_NAME,
                                                                                                               Collections.singletonList(entirePaxosRange),
                                                                                                               "bootstrap");

        while (!retry.reachedMax())
        {
            Map<Supplier<Future<?>>, Future<?>> tasks = new HashMap<>();
            for (Supplier<Future<?>> supplier : remaining)
                tasks.put(supplier, supplier.get());
            remaining.clear();
            logger.info("Performing paxos topology repair on: {}", remaining);

            for (Map.Entry<Supplier<Future<?>>, Future<?>> e : tasks.entrySet())
            {
                try
                {
                    e.getValue().get();
                }
                catch (ExecutionException t)
                {
                    logger.error("Caught an exception while repairing paxos topology.", t);
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
        logger.error("Added node as a CMS, but failed to repair paxos topology after this operation.");
    }

    public static class ActiveTransition
    {
        public final NodeId nodeId;
        public final Set<InetAddressAndPort> streamCandidates;

        public ActiveTransition(NodeId nodeId, Set<InetAddressAndPort> streamCandidates)
        {
            this.nodeId = nodeId;
            this.streamCandidates = Collections.unmodifiableSet(streamCandidates);
        }

        @Override
        public String toString()
        {
            return "ActiveTransition{" +
                   "nodeId=" + nodeId +
                   ", streamCandidates=" + streamCandidates +
                   '}';
        }
    }

    public static class Serializer implements AsymmetricMetadataSerializer<MultiStepOperation<?>, ReconfigureCMS>
    {

        public void serialize(MultiStepOperation<?> t, DataOutputPlus out, Version version) throws IOException
        {
            ReconfigureCMS transformation = (ReconfigureCMS) t;
            AdvanceCMSReconfiguration.serializer.serialize(transformation.next, out, version);
        }

        public ReconfigureCMS deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ReconfigureCMS(AdvanceCMSReconfiguration.serializer.deserialize(in, version));
        }

        public long serializedSize(MultiStepOperation<?> t, Version version)
        {
            ReconfigureCMS transformation = (ReconfigureCMS) t;
            return AdvanceCMSReconfiguration.serializer.serializedSize(transformation.next, version);
        }
    }

    public static class SequenceKey implements MultiStepOperation.SequenceKey
    {
        public static SequenceKey instance = new SequenceKey();
        public static Serializer serializer = new Serializer();

        private SequenceKey(){}

        @Override
        public String toString()
        {
            return "Reconfigure CMS";
        }

        public static class Serializer implements MetadataSerializer<SequenceKey>
        {

            public void serialize(SequenceKey t, DataOutputPlus out, Version version) throws IOException
            {
                // not actually serialized at only one reconfiguration sequence
                // is permitted at a time so the key is a constant
            }

            public SequenceKey deserialize(DataInputPlus in, Version version) throws IOException
            {
                return instance;
            }

            public long serializedSize(SequenceKey t, Version version)
            {
                return 0;
            }
        }
    }
}
