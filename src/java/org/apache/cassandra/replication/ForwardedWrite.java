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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * Handles tracked writes where the coordinator is NOT a replica for the write.
 *
 * <p>For a forwarded write there are 2 nodes involved in coordination: a coordinator and a leader. The coordinator is
 * the node that the client is communicating with, and the leader is the mutation replica that is handling the mutation
 * tracking for that write.
 *
 * <h2>Request/Response Flow</h2>
 *
 * <h3>Regular Writes (Mutation)</h3>
 *
 * <pre>
 * Client                Coordinator           Leader Replica        Other Replicas
 *   |                        |                       |                     |
 *   |---Write Request------->|                       |                     |
 *   |                        |                       |                     |
 *   |                        |--FORWARD_WRITE_REQ--->|                     |
 *   |                        |   (Mutation w/o ID)   |                     |
 *   |                        |                       |                     |
 *   |                        |                       |--Assign MutationId  |
 *   |                        |                       |                     |
 *   |                        |                       |--Apply locally----->|
 *   |                        |                       |   (with ID)         |
 *   |                        |                       |                     |
 *   |                        |                       |--MUTATION_REQ------>|
 *   |                        |                       |   (Mutation w/ ID + |
 *   |                        |                       |    CoordinatorAck)  |
 *   |                        |                       |                     |
 *   |                        |                       |                     |--Apply locally
 *   |                        |                       |                     |
 *   |                        |<------MUTATION_RSP (for CL)-----------------|
 *   |                        |                       |<-MUTATION_RSP (tracking)-|
 *   |<--Write Response-------|                       |                     |
 *   |  (when CL satisfied)   |                       |                     |
 *   |                        |                       |--Mark witnessed---->|
 * </pre>
 *
 * <p><b>Key Points:</b>
 * <ul>
 *   <li>Coordinator selects a leader replica based on proximity and liveness</li>
 *   <li>Coordinator sends mutation WITHOUT an ID to the leader</li>
 *   <li>Leader assigns a MutationId using {@link MutationTrackingService#nextMutationId}</li>
 *   <li>Leader applies mutation locally and forwards to other replicas</li>
 *   <li>All replicas respond to coordinator for consistency level (using CoordinatorAckInfo)</li>
 *   <li>Other replicas also respond to leader for mutation tracking/witnessing</li>
 *   <li>Coordinator waits for CL responses before responding to client</li>
 * </ul>
 *
 * <h3>Counter Writes (CounterMutation)</h3>
 *
 * <pre>
 * Client                Coordinator           Leader Replica        Other Replicas
 *   |                        |                       |                     |
 *   |---Counter Write------->|                       |                     |
 *   |                        |                       |                     |
 *   |                        |--COUNTER_MUTATION_REQ>|                     |
 *   |                        |  (CounterMutation w/o |                     |
 *   |                        |   ID)                 |                     |
 *   |                        |                       |                     |
 *   |                        |                       |--Assign MutationId  |
 *   |                        |                       |                     |
 *   |                        |                       |--Apply counter----->|
 *   |                        |                       |  mutation (converts |
 *   |                        |                       |  CounterMutation to |
 *   |                        |                       |  Mutation w/ ID)    |
 *   |                        |                       |                     |
 *   |                        |                       |--MUTATION_REQ------>|
 *   |                        |                       |  (Mutation result + |
 *   |                        |                       |   CoordinatorAck)   |
 *   |                        |                       |                     |
 *   |                        |                       |                     |--Apply locally
 *   |                        |                       |                     |
 *   |                        |<------MUTATION_RSP (for CL)-----------------|
 *   |                        |                       |<-MUTATION_RSP (tracking)-|
 *   |<--Write Response-------|                       |                     |
 *   |  (when CL satisfied)   |                       |                     |
 *   |                        |                       |--Mark witnessed---->|
 * </pre>
 *
 * <p><b>Key Points:</b>
 * <ul>
 *   <li>Coordinator selects a counter leader replica (prefers local DC)</li>
 *   <li>Coordinator sends CounterMutation WITHOUT an ID to the leader</li>
 *   <li>Leader assigns a MutationId using {@link MutationTrackingService#nextMutationId}</li>
 *   <li>Leader applies counter mutation locally, which converts CounterMutation to a regular Mutation</li>
 *   <li>Leader forwards the resulting Mutation (NOT CounterMutation) to other replicas</li>
 *   <li>All replicas respond to coordinator for consistency level (using CoordinatorAckInfo)</li>
 *   <li>Other replicas also respond to leader for mutation tracking/witnessing</li>
 *   <li>Coordinator waits for CL responses before responding to client</li>
 * </ul>
 *
 * @see TrackedWriteRequest for the flow when coordinator IS a replica
 * @see MutationTrackingService for mutation ID assignment and witnessing
 */
public class ForwardedWrite
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWrite.class);

    public static class MutationRequest
    {
        private final Mutation mutation;

        /**
         * We encode the two sets of nodes separately to make sure that the leader
         * contacts the same replicas that the coordinator set up its callbacks for.
         */
        private final Participants liveReplicas;
        private final Participants downReplicas;

        /**
         * Epoch of the {@link ReplicaPlan.ForWrite} the coordinator built
         * {@link #liveReplicas} and {@link #downReplicas} from.
         */
        private final Epoch planEpoch;

        public MutationRequest(Mutation mutation, Participants liveReplicas, Participants downReplicas, Epoch planEpoch)
        {
            Preconditions.checkArgument(mutation.id().isNone());
            this.mutation = mutation;
            this.liveReplicas = liveReplicas;
            this.downReplicas = downReplicas;
            this.planEpoch = planEpoch;
        }

        public DecoratedKey key()
        {
            return mutation.key();
        }

        public void applyLocallyAndForwardToReplicas(ClusterMetadata cm, VersionedEndpoints.ForToken writePlacements, CoordinatorAckInfo ackTo)
        {
            Preconditions.checkState(ackTo != null);
            Preconditions.checkArgument(mutation.id().isNone());

            String keyspaceName = mutation.getKeyspaceName();
            Token token = mutation.key().getToken();
            String localDataCenter = cm.locator.local().datacenter;

            /*
             * nextMutationId() always allocates from the most recent Shard for the token, so it's possible
             * for the topology to change *right after* the verb handler successfully validates coordinator/leader
             * epochs matching; if Shard's participants no longer match the recepients calculated on
             * the coordinator, we need to abort here and now.
             */
            MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, token);

            Mutation mutation;
            LeaderCallback handler;

            // this DC replicas
            List<Replica> localDCReplicas = null;

            // extra-DC, grouped by DC
            Map<String, List<Replica>> remoteDCReplicas = null;

            // only need to create a Message for non-local writes
            Message<Mutation> message = null;

            try
            {
                Participants shardParticipants = MutationTrackingService.instance().getLogParticipants(id.asLogId());
                Participants liveAndDownParticipants = Participants.merge(liveReplicas, downReplicas);
                if (!shardParticipants.equals(liveAndDownParticipants))
                {
                    TCMMetrics.instance.coordinatorBehindPlacements.mark();
                    String msg =
                        format("Mutation id %s: shard participants %s disagree with plan replicas %s; coordinator must refresh and retry",
                               id, shardParticipants, liveAndDownParticipants);
                    throw new CoordinatorBehindException(msg);
                }

                mutation = this.mutation.withMutationId(id);

                // Do not wait for handler completion, since the coordinator is already waiting and we don't want to block the stage
                handler = new LeaderCallback(id, ackTo);

                boolean applyLocally = false;

                // Expensive, but easier to work with Replica than InetAddressAndPort for now
                Int2ObjectHashMap<Replica> replicas = new Int2ObjectHashMap<>(liveReplicas.size(), 0.65f);
                EndpointsForToken endpoints = writePlacements.get();
                for (Replica replica : endpoints)
                    replicas.put(cm.directory.peerId(replica.endpoint()).id(), replica);

                // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
                // computation is not synchronized however, and we will potentially call that method concurrently for each
                // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
                // may result in multiple computations, making the caching optimization moot). So forcing the serialization
                // here to make sure it's already cached/computed when it's concurrently used later.
                // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
                // the current version, but it's just an optimization, and we're ok not optimizing for mixed-version clusters.
                Mutation.serializer.prepareSerializedBuffer(mutation, MessagingService.current_version);

                for (int i = 0; i < liveReplicas.size(); i++)
                {
                    int nodeId = liveReplicas.get(i);

                    if (cm.myNodeId().id() == nodeId)
                    {
                        applyLocally = true;
                        continue;
                    }

                    if (message == null)
                        message = Message.builder(MUTATION_REQ, mutation)
                                         .withRequestTime(handler.getRequestTime())
                                         .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                         .withParam(ParamType.COORDINATOR_ACK_INFO, ackTo)
                                         .build();

                    Replica replica = replicas.get(nodeId);
                    String dc = cm.locator.location(replica.endpoint()).datacenter;

                    if (localDataCenter.equals(dc))
                    {
                        if (localDCReplicas == null)
                            localDCReplicas = new ArrayList<>();
                        localDCReplicas.add(replica);
                    }
                    else
                    {
                        if (remoteDCReplicas == null)
                            remoteDCReplicas = new HashMap<>();
                        remoteDCReplicas.computeIfAbsent(dc, ignore -> new ArrayList<>(3)) // most DCs will have <= 3 replicas
                                        .add(replica);
                    }
                }

                Preconditions.checkState(applyLocally); // the leader is always a replica
            }
            catch (Throwable t)
            {
                MutationTrackingService.instance().completeLocalWrite(id);
                throw t;
            }

            TrackedWriteRequest.applyMutationLocally(mutation, handler);

            if (localDCReplicas != null)
                for (Replica replica : localDCReplicas)
                    MessagingService.instance().sendWithCallback(message, replica.endpoint(), handler);

            if (remoteDCReplicas != null)
            {
                // for each datacenter, send the message to one node to relay the write to other replicas
                for (List<Replica> dcReplicas : remoteDCReplicas.values())
                    TrackedWriteRequest.sendMessagesToRemoteDC(message, EndpointsForToken.copyOf(mutation.key().getToken(), dcReplicas), handler, ackTo);
            }
        }
    }

    public static AbstractWriteResponseHandler<Object> forwardMutation(Mutation mutation, ReplicaPlan.ForWrite plan, AbstractReplicationStrategy strategy, Dispatcher.RequestTime requestTime)
    {
        return forwardMutationInternal(mutation, plan, strategy, requestTime, null);
    }

    private static AbstractWriteResponseHandler<Object> forwardMutationInternal(
        Mutation mutation, ReplicaPlan.ForWrite plan, AbstractReplicationStrategy strategy,
        Dispatcher.RequestTime requestTime, Consumer<AbstractWriteResponseHandler<?>> onComplete)
    {
        // find leader
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        ClusterMetadata cm = ClusterMetadata.current();
        Token token = mutation.key().getToken();
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        EndpointsForToken endpoints = cm.placements.get(keyspace.getMetadata().params.replication).writes.forToken(token).get();
        logger.trace("Finding best leader from replicas {}", endpoints);

        // TODO (consider?) should match ReplicaPlans.findCounterLeaderReplica, including DC-local priority, current health, severity, etc.
        Replica leader = null;
        for (Replica replica : proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), endpoints))
            if (plan.isAlive(replica))
                leader = replica;
        Preconditions.checkState(leader != null, "Could not find leader for %s", mutation);

        // create callback and forward to leader
        logger.trace("Selected {} as leader for mutation with key {}", leader.endpoint(), mutation.key());

        AbstractWriteResponseHandler<Object> handler =
            strategy.getWriteResponseHandler(plan, onComplete, WriteType.SIMPLE, null, requestTime);

        HashSet<Replica> liveReplicas = Sets.newHashSetWithExpectedSize(endpoints.size());
        HashSet<Replica> downReplicas = Sets.newHashSetWithExpectedSize(endpoints.size());
        for (Replica replica : endpoints)
            (plan.isAlive(replica) ? liveReplicas : downReplicas).add(replica);

        MutationRequest request =
            new MutationRequest(mutation,
                                Participants.fromReplicas(liveReplicas, cm),
                                Participants.fromReplicas(downReplicas, cm),
                                plan.epoch());

        Message<MutationRequest> toLeader =
            Message.outWithFlags(Verb.MT_FORWARD_WRITE_REQ,
                                 request,
                                 requestTime,
                                 Collections.singletonList(MessageFlag.CALL_BACK_ON_FAILURE));

        // add callbacks for replicas to respond directly to coordinator
        for (Replica replica : liveReplicas)
        {
            logger.trace("Adding forwarding callback for response from {} id {}", replica, toLeader.id());
            MessagingService.instance().callbacks.addWithExpiration(handler, toLeader, replica);
        }

        for (Replica replica : downReplicas)
            handler.expired();

        MessagingService.instance().send(toLeader, leader.endpoint());
        return handler;
    }

    /**
     * Forward a tracked counter mutation to a replica leader for processing.
     * The leader will apply the counter mutation, assign a mutation ID, and replicate to other replicas.
     */
    public static AbstractWriteResponseHandler<Object> forwardCounterMutation(CounterMutation counterMutation,
                                                                              ReplicaPlan.ForWrite plan,
                                                                              AbstractReplicationStrategy strategy,
                                                                              Dispatcher.RequestTime requestTime)
    {
        return forwardCounterMutationInternal(counterMutation, plan, strategy, requestTime, null);
    }

    private static AbstractWriteResponseHandler<Object> forwardCounterMutationInternal(
        CounterMutation counterMutation, ReplicaPlan.ForWrite plan,
        AbstractReplicationStrategy strategy, Dispatcher.RequestTime requestTime,
        Consumer<AbstractWriteResponseHandler<?>> onComplete)
    {
        Preconditions.checkArgument(counterMutation.id().isNone(), "CounterMutation should not have an ID when forwarding");

        ClusterMetadata cm = ClusterMetadata.current();
        String localDataCenter = DatabaseDescriptor.getLocator().local().datacenter;

        // Find the leader replica - prefer local DC replicas for counters
        Replica leader;
        try
        {
            leader = ReplicaPlans.findCounterLeaderReplica(cm, counterMutation.getKeyspaceName(),
                                                           counterMutation.key(),
                                                           localDataCenter,
                                                           counterMutation.consistency());
        }
        catch (Exception e)
        {
            logger.error("Failed to find counter leader replica for tracked write", e);
            throw e;
        }

        Preconditions.checkState(!leader.isSelf(), "Leader should not be self when forwarding counter mutation");
        logger.trace("Forwarding tracked counter mutation to leader replica {}", leader);

        // Create response handler for all replicas
        AbstractWriteResponseHandler<Object> handler =
            strategy.getWriteResponseHandler(plan, onComplete, WriteType.COUNTER, null, requestTime);

        // Add callbacks for all live replicas to respond directly to coordinator
        Message<CounterMutation> forwardMessage =
            Message.outWithFlags(Verb.COUNTER_MUTATION_REQ,
                                 counterMutation,
                                 requestTime,
                                 Collections.singletonList(MessageFlag.CALL_BACK_ON_FAILURE));

        // TODO (expected): the view of what nodes are alive and what aren't may be different between
        //      the coordinator and the leader, which can result in timing out and/or valid responses
        //      being ignored by the coordinator (if the coordinator think they were down and expires
        //      them here, but the leader sees them as up (correctly) and forward the request there)
        for (Replica replica : plan.contacts())
        {
            if (plan.isAlive(replica))
            {
                logger.trace("Adding forwarding callback for tracked counter response from {} id {}", replica, forwardMessage.id());
                MessagingService.instance().callbacks.addWithExpiration(handler, forwardMessage, replica);
            }
            else
            {
                handler.expired();
            }
        }

        // Send the counter mutation to the leader
        MessagingService.instance().send(forwardMessage, leader.endpoint());

        return handler;
    }

    /**
     * Forward a mutation to a replica leader for processing.
     * Dispatches to the appropriate method based on mutation type.
     *
     * @param mutation    the mutation to forward (can be Mutation or CounterMutation)
     * @param plan        the replica plan
     * @param strategy    the replication strategy
     * @param requestTime the request time
     * @return the write response handler
     */
    public static AbstractWriteResponseHandler<Object> forward(IMutation mutation,
                                                               ReplicaPlan.ForWrite plan,
                                                               AbstractReplicationStrategy strategy,
                                                               Dispatcher.RequestTime requestTime)
    {
        if (mutation instanceof CounterMutation)
            return forwardCounterMutation((CounterMutation) mutation, plan, strategy, requestTime);
        else
            return forwardMutation((Mutation) mutation, plan, strategy, requestTime);
    }

    /**
     * Forward a mutation to a replica leader for processing.
     * Dispatches to the appropriate method based on mutation type.
     *
     * <p>Like {@link #forward(IMutation, ReplicaPlan.ForWrite, AbstractReplicationStrategy, Dispatcher.RequestTime)},
     * but wires a completion callback on the handler before any messages are sent, avoiding races where the handler
     * is signaled before the caller can observe it.
     *
     * @param mutation    the mutation to forward (can be Mutation or CounterMutation)
     * @param plan        the replica plan
     * @param strategy    the replication strategy
     * @param requestTime the request time
     * @param onComplete  callback invoked when the write response handler completes
     * @return the write response handler
     */
    static AbstractWriteResponseHandler<?> forward(IMutation mutation,
                                                   ReplicaPlan.ForWrite plan,
                                                   AbstractReplicationStrategy strategy,
                                                   Dispatcher.RequestTime requestTime,
                                                   Consumer<AbstractWriteResponseHandler<?>> onComplete)
    {
        if (mutation instanceof CounterMutation)
            return forwardCounterMutationInternal((CounterMutation) mutation, plan, strategy, requestTime, onComplete);
        else
            return forwardMutationInternal((Mutation) mutation, plan, strategy, requestTime, onComplete);
    }

    /**
     * Apply a forwarded tracked counter mutation on the leader replica.
     * Called by CounterMutationVerbHandler when receiving a forwarded counter write.
     * <p>
     * This method:
     * 1. Creates CoordinatorAckInfo from the incoming message
     * 2. Creates a LeaderCallback to track responses from replicas
     * 3. Applies counter mutation locally with generated mutation ID
     * 4. Forwards result (Mutation not CounterMutation) to other replicas with CoordinatorAckInfo and LeaderCallback
     * 5. Sends leader's response back to coordinator
     *
     * @param counterMutation the counter mutation to apply
     * @param message the original message (contains coordinator address and message ID)
     */
    public static void applyForwardedCounterMutation(CounterMutation counterMutation, Message<CounterMutation> message)
    {
        CoordinatorAckInfo coordinatorAckInfo = CoordinatorAckInfo.toCoordinator(message.from(), message.id());

        String keyspaceName = counterMutation.getKeyspaceName();
        Token token = counterMutation.key().getToken();
        Keyspace ks = Keyspace.open(keyspaceName);
        ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(ks, counterMutation.consistency(), token, ReplicaPlans.writeAll);

        MutationId id = MutationTrackingService.instance().nextMutationId(keyspaceName, token);

        logger.trace("Forwarded counter mutation {}: applying locally with ID and forwarding to other replicas", id);

        // Create LeaderCallback to track when replicas respond, allowing the leader
        // to mark the mutation ID as witnessed on each replica proactively
        LeaderCallback leaderCallback = new LeaderCallback(id, coordinatorAckInfo);

        // Apply counter mutation with ID to get result
        Mutation result;
        try
        {
            result = counterMutation.applyCounterMutation(id);
        }
        catch (Throwable t)
        {
            MutationTrackingService.instance().completeLocalWrite(id);
            throw t;
        }

        // Apply locally using the leader callback
        TrackedWriteRequest.applyMutationLocally(result, leaderCallback);

        // Send result to other replicas with CoordinatorAckInfo and LeaderCallback
        // Replicas will respond to both the leader (for witnessing) and the coordinator (for CL)
        TrackedWriteRequest.sendToReplicas(result, plan, leaderCallback, coordinatorAckInfo);

        logger.trace("Tracked counter mutation {} processed, local application and replication initiated", id);
    }

    // TODO (expected): depending on when topology change support lands, bump MT version,
    //      and add support for the old one
    public static final VersionedSerializer<MutationRequest> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(MutationRequest request, DataOutputPlus out, Version version) throws IOException
        {
            Mutation.serializer.serialize(request.mutation, out, version.messagingVersion());
            Participants.serializer.serialize(request.liveReplicas, out);
            Participants.serializer.serialize(request.downReplicas, out);
            Epoch.serializer.serialize(request.planEpoch, out);
        }

        @Override
        public MutationRequest deserialize(DataInputPlus in, Version version) throws IOException
        {
            Mutation mutation = Mutation.serializer.deserialize(in, version.messagingVersion());
            Participants liveReplicas = Participants.serializer.deserialize(in);
            Participants downReplicas = Participants.serializer.deserialize(in);
            Epoch planEpoch = Epoch.serializer.deserialize(in);
            return new MutationRequest(mutation, liveReplicas, downReplicas, planEpoch);
        }

        @Override
        public long serializedSize(MutationRequest request, Version version)
        {
            long size = Mutation.serializer.serializedSize(request.mutation, version.messagingVersion());
            size += Participants.serializer.serializedSize(request.liveReplicas);
            size += Participants.serializer.serializedSize(request.downReplicas);
            size += Epoch.serializer.serializedSize(request.planEpoch);
            return size;
        }
    };

    public static final IVerbHandler<MutationRequest> verbHandler = incoming ->
    {
        MutationTrackingService.ensureEnabled();

        if (approxTime.now() > incoming.expiresAtNanos())
        {
            Tracing.trace("Discarding mutation from {} (timed out)", incoming.from());
            MessagingService.instance().metrics.recordDroppedMessage(incoming, incoming.elapsedSinceCreated(NANOSECONDS), NANOSECONDS);
            return;
        }

        logger.trace("Received incoming ForwardedWriteRequest {} id {}", incoming, incoming.id());
        MutationRequest request = incoming.payload;

        ClusterMetadata metadata = ClusterMetadata.current();

        Epoch planEpoch = request.planEpoch;
        String keyspace = request.mutation.getKeyspaceName();
        DecoratedKey key = request.mutation.key();

        VersionedEndpoints.ForToken writePlacements = writePlacements(metadata, keyspace, key);

        /*
         * Reconcile leader's metadata epoch with the coordinator's to ensure that the replicas
         * calculated on the coordinator are the same as replicas calculated on the leader; this is
         * necessary because replicas will reply directly to the coordinator, and it better always be the same
         * replicas that the coordinator has set up its callbacks for (and a topology change can make it not so).
         *
         * This is the first check out of necessary two: the second one is performed after allocating the mutation id,
         * to ensure that the most recent shard - the one from which the mutation id will be allocated - has the same
         * set of participants that the recepients the coordinator has calculated.
         */

        if (planEpoch.isAfter(metadata.epoch))
        {
            if (!writePlacements.get().containsSelf())
            {
                metadata = ClusterMetadataService.instance().fetchLogFromPeerOrCMS(metadata, incoming.from(), planEpoch);
                writePlacements = writePlacements(metadata, keyspace, key);
            }
            else
            {
                ClusterMetadataService.instance().fetchLogFromPeerOrCMSAsync(metadata, incoming.from(), planEpoch); // async
            }
        }

        if (!writePlacements.get().containsSelf())
        {
            StorageService.instance.incOutOfRangeOperationCount();
            Keyspace.open(keyspace).metric.outOfRangeTokenWrites.inc();
            throw InvalidRoutingException.forWrite(incoming.from(), key.getToken(), metadata.epoch, request.mutation);
        }

        if (writePlacements.lastModified().isAfter(planEpoch))
        {
            TCMMetrics.instance.coordinatorBehindPlacements.mark();
            String msg =
                format("Routing is correct, but coordinator needs to catch-up at least to epoch %s to maintain consistency. " +
                       "Current coordinator epoch is %s", writePlacements.lastModified(), planEpoch);
            throw new CoordinatorBehindException(msg);
        }

        // at this stage it's possible that local (leader) writePlacements.lastModified() is *before* coordinator's
        // planEpoch and this may be completely fine, or not, so we must compare actual replica sets.
        Participants liveAndDownParticipants = Participants.merge(request.liveReplicas, request.downReplicas);
        if (!liveAndDownParticipants.matches(writePlacements.get(), metadata))
        {
            String msg =
                format("Leader is possibly behind coordinator for tracked write; live: %s, down: %s, placements: %s",
                       request.liveReplicas, request.downReplicas, writePlacements.get());
            throw new CoordinatorBehindException(msg);
        }

        CoordinatorAckInfo ackTo = CoordinatorAckInfo.toCoordinator(incoming.from(), incoming.id());
        request.applyLocallyAndForwardToReplicas(metadata, writePlacements, ackTo);
    };

    private static VersionedEndpoints.ForToken writePlacements(ClusterMetadata metadata, String keyspace, DecoratedKey key)
    {
        return metadata.placements.get(metadata.schema.getKeyspace(keyspace).getMetadata().params.replication).writes.forToken(key.getToken());
    }

    // Leader just needs to acknowledge propagation for its own log, not for client consistency level
    // See org.apache.cassandra.service.TrackedWriteResponseHandler.onResponse, this class should probably merge with that one
    public static class LeaderCallback implements RequestCallback<NoPayload>
    {
        private final MutationId id;
        private final CoordinatorAckInfo ackTo;
        private final Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();

        public LeaderCallback(MutationId id, CoordinatorAckInfo ackTo)
        {
            this.id = id;
            this.ackTo = ackTo;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            // Local mutations are witnessed from Keyspace.applyInternalTracked
            if (msg != null)
                MutationTrackingService.instance().receivedWriteResponse(id, msg.from());

            // Local write needs to be ack'd to coordinator
            if (msg == null && ackTo != null)
            {
                Message<NoPayload> message = Message.builder(Verb.MUTATION_RSP, NoPayload.noPayload)
                                                    .from(FBUtilities.getBroadcastAddressAndPort())
                                                    .withId(ackTo.id)
                                                    .build();
                MessagingService.instance().send(message, ackTo.coordinator);
            }
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failure)
        {
            logger.error("Got failure from {} reason {}", from, failure.reason);
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        public Dispatcher.RequestTime getRequestTime()
        {
            return requestTime;
        }
    }

    public static class CoordinatorAckInfo
    {
        public static IVersionedSerializer<CoordinatorAckInfo> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(CoordinatorAckInfo ackTo, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(ackTo.coordinator, out, version);
                out.writeLong(ackTo.id);
            }

            @Override
            public CoordinatorAckInfo deserialize(DataInputPlus in, int version) throws IOException
            {
                InetAddressAndPort coordinator = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                long id = in.readLong();
                return new CoordinatorAckInfo(coordinator, id);
            }

            @Override
            public long serializedSize(CoordinatorAckInfo ackTo, int version)
            {
                long size = 0;
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(ackTo.coordinator, version);
                size += TypeSizes.LONG_SIZE;
                return size;
            }
        };

        public final InetAddressAndPort coordinator;
        public final long id;

        private CoordinatorAckInfo(InetAddressAndPort coordinator, long id)
        {
            this.coordinator = coordinator;
            this.id = id;
        }

        private static CoordinatorAckInfo toCoordinator(InetAddressAndPort coordinator, long messageId)
        {
            return new CoordinatorAckInfo(coordinator, messageId);
        }
    }
}
