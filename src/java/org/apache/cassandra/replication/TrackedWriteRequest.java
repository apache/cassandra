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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntHashSet;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.TrackedWriteResponseHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.writeMetrics;
import static org.apache.cassandra.net.Verb.COUNTER_MUTATION_REQ;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;

/**
 * Handles tracked writes where the coordinator IS a replica for the write.
 *
 * <p>When the coordinator is a replica, it acts as the leader and directly assigns the MutationId,
 * applies the mutation locally, and forwards to other replicas. This avoids the extra network hop
 * compared to {@link ForwardedWrite}.
 *
 * <h2>Request/Response Flow</h2>
 *
 * <h3>Regular Writes (Mutation)</h3>
 *
 * <pre>
 * Client            Coordinator/Leader         Other Replicas
 *   |                      |                          |
 *   |---Write Request----->|                          |
 *   |                      |                          |
 *   |                      |--Assign MutationId       |
 *   |                      |                          |
 *   |                      |--Apply locally---------->|
 *   |                      |   (with ID)              |
 *   |                      |                          |
 *   |                      |--MUTATION_REQ----------->|
 *   |                      |   (Mutation w/ ID)       |
 *   |                      |                          |
 *   |                      |                          |--Apply locally
 *   |                      |                          |
 *   |                      |<------MUTATION_RSP-------|
 *   |                      |                          |
 *   |<--Write Response-----|                          |
 *   |  (when CL satisfied) |                          |
 *   |                      |--Mark witnessed--------->|
 * </pre>
 *
 * <p><b>Key Points:</b>
 * <ul>
 *   <li>Coordinator is a replica, so it acts as the leader</li>
 *   <li>Coordinator assigns a MutationId using {@link MutationTrackingService#nextMutationId}</li>
 *   <li>Coordinator applies mutation locally before sending to other replicas</li>
 *   <li>Coordinator sends mutation WITH ID to other replicas</li>
 *   <li>Replicas respond directly to coordinator for consistency level</li>
 *   <li>Coordinator tracks witnessing for all replicas including itself</li>
 *   <li>Client response sent when CL is satisfied</li>
 * </ul>
 *
 * <h3>Counter Writes (CounterMutation)</h3>
 *
 * <pre>
 * Client            Coordinator/Leader         Other Replicas
 *   |                      |                          |
 *   |---Counter Write----->|                          |
 *   |                      |                          |
 *   |                      |--Assign MutationId       |
 *   |                      |                          |
 *   |                      |--Apply counter---------->|
 *   |                      |  mutation (converts      |
 *   |                      |  CounterMutation to      |
 *   |                      |  Mutation w/ ID)         |
 *   |                      |                          |
 *   |                      |--MUTATION_REQ----------->|
 *   |                      |  (Mutation result)       |
 *   |                      |                          |
 *   |                      |                          |--Apply locally
 *   |                      |                          |
 *   |                      |<------MUTATION_RSP-------|
 *   |                      |                          |
 *   |<--Write Response-----|                          |
 *   |  (when CL satisfied) |                          |
 *   |                      |--Mark witnessed--------->|
 * </pre>
 *
 * <p><b>Key Points:</b>
 * <ul>
 *   <li>Coordinator is a replica, so it acts as the leader</li>
 *   <li>Coordinator assigns a MutationId using {@link MutationTrackingService#nextMutationId}</li>
 *   <li>Coordinator applies counter mutation locally, which converts CounterMutation to regular Mutation</li>
 *   <li>Coordinator sends the resulting Mutation (NOT CounterMutation) to other replicas</li>
 *   <li>Replicas respond directly to coordinator for consistency level</li>
 *   <li>Coordinator tracks witnessing for all replicas including itself</li>
 *   <li>Client response sent when CL is satisfied</li>
 * </ul>
 *
 * @see ForwardedWrite for the flow when coordinator is NOT a replica
 * @see MutationTrackingService for mutation ID assignment and witnessing
 */
public class TrackedWriteRequest
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedWriteRequest.class);

    /**
     * Coordinate write of a tracked mutation. Assumes the replica is a coordinator.
     *
     * @param mutation the mutation to be applied
     * @param consistencyLevel the consistency level for the write operation
     * @param requestTime object holding times when request got enqueued and started execution
     */
    public static AbstractWriteResponseHandler<?> perform(
        IMutation mutation, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        Tracing.trace("Determining replicas for mutation");

        Preconditions.checkArgument(mutation.id().isNone());
        String keyspaceName = mutation.getKeyspaceName();
        Keyspace keyspace = Keyspace.open(keyspaceName);
        Token token = mutation.key().getToken();

        ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(keyspace, consistencyLevel, token, ReplicaPlans.writeAll);
        AbstractReplicationStrategy rs = plan.replicationStrategy();

        if (plan.lookup(FBUtilities.getBroadcastAddressAndPort()) == null)
        {
            logger.trace("Remote tracked request {} {}", mutation, plan);
            writeMetrics.remoteRequests.mark();
            return ForwardedWrite.forward(mutation, plan, rs, requestTime);
        }

        logger.trace("Local tracked request {} {}", mutation, plan);
        writeMetrics.localRequests.mark();

        MutationId id = MutationTrackingService.instance.nextMutationId(keyspaceName, token);
        mutation = mutation.withMutationId(id);

        if (logger.isTraceEnabled())
        {
            logger.trace("Write replication plan for mutation {}: live={}, pending={}, all={}",
                         id, plan.live(), plan.pending(), plan.contacts());
        }

        final TrackedWriteResponseHandler handler;
        if (mutation instanceof CounterMutation)
        {
            handler = TrackedWriteResponseHandler.wrap(rs.getWriteResponseHandler(plan, null, WriteType.COUNTER, null, requestTime), id);
            applyCounterMutationLocally((CounterMutation) mutation, plan, handler);
        }
        else
        {
            handler = TrackedWriteResponseHandler.wrap(rs.getWriteResponseHandler(plan, null, WriteType.SIMPLE, null, requestTime), id);
            applyLocallyAndSendToReplicas((Mutation) mutation, plan, handler);
        }
        return handler;
    }

    public static void applyLocallyAndSendToReplicas(Mutation mutation, ReplicaPlan.ForWrite plan, TrackedWriteResponseHandler handler)
    {
        applyMutationLocally(mutation, handler);
        sendToReplicas(mutation, plan, handler, null);
    }

    /**
     * Sends a mutation to all replicas.
     * Handles grouping replicas by DC, sending messages, and tracking remote replicas.
     *
     * @param mutation the mutation with assigned ID to send to replicas
     * @param plan the replica plan
     * @param handler the response handler (can be TrackedWriteResponseHandler or LeaderCallback)
     * @param coordinatorAckInfo optional coordinator info for forwarded writes (null for local coordinator)
     */
    public static void sendToReplicas(Mutation mutation,
                                      ReplicaPlan.ForWrite plan,
                                      RequestCallback<NoPayload> handler,
                                      ForwardedWrite.CoordinatorAckInfo coordinatorAckInfo)
    {
        Preconditions.checkArgument(handler instanceof TrackedWriteResponseHandler || handler instanceof ForwardedWrite.LeaderCallback,
                                    "Handler must be TrackedWriteResponseHandler or LeaderCallback");

        String localDataCenter = DatabaseDescriptor.getLocator().local().datacenter;

        // this DC replicas
        List<Replica> localDCReplicas = null;

        // extra-DC, grouped by DC
        Map<String, List<Replica>> remoteDCReplicas = null;

        // create a Message for non-local writes
        Message<Mutation> message = null;

        // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
        // computation is not synchronized however, and we will potentially call that method concurrently for each
        // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
        // may result in multiple computations, making the caching optimization moot). So forcing the serialization
        // here to make sure it's already cached/computed when it's concurrently used later.
        // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
        // the current version, but it's just an optimization, and we're ok not optimizing for mixed-version clusters.
        Mutation.serializer.prepareSerializedBuffer(mutation, MessagingService.current_version);

        // Extract request time from handler
        Dispatcher.RequestTime requestTime = getRequestTime(handler);

        boolean foundSelf = false;
        for (Replica destination : plan.contacts())
        {
            if (!plan.isAlive(destination))
            {
                logger.trace("Skipping dead replica {} for mutation {}", destination, mutation.id());
                // Only call expired() for AbstractWriteResponseHandler (not for LeaderCallback)
                if (handler instanceof AbstractWriteResponseHandler)
                    ((AbstractWriteResponseHandler<?>) handler).expired(); // immediately mark the response as expired since the request will not be sent
                continue;
            }

            if (destination.isSelf())
            {
                foundSelf = true; // Mutation was already applied locally
                continue;
            }

            if (message == null)
            {
                Message.Builder<Mutation> builder = Message.builder(MUTATION_REQ, mutation)
                                                           .withRequestTime(requestTime)
                                                           .withFlag(MessageFlag.CALL_BACK_ON_FAILURE);

                // If this is a forwarded write, include coordinator ack info so replicas
                // know to respond to the original coordinator, not this leader
                if (coordinatorAckInfo != null)
                    builder.withParam(ParamType.COORDINATOR_ACK_INFO, coordinatorAckInfo);

                message = builder.build();
            }

            String dc = DatabaseDescriptor.getLocator().location(destination.endpoint()).datacenter;

            if (localDataCenter.equals(dc))
            {
                if (localDCReplicas == null)
                    localDCReplicas = new ArrayList<>(plan.contacts().size());
                localDCReplicas.add(destination);
            }
            else
            {
                if (remoteDCReplicas == null)
                    remoteDCReplicas = new HashMap<>();
                remoteDCReplicas.computeIfAbsent(dc, ignore -> new ArrayList<>(3)) // most DCs will have <= 3 replicas
                                .add(destination);
            }
        }

        Preconditions.checkState(foundSelf, "Coordinator must be a replica");

        IntHashSet remoteReplicas = null;
        if (localDCReplicas != null || remoteDCReplicas != null)
            remoteReplicas = new IntHashSet();

        if (localDCReplicas != null)
        {
            for (Replica replica : localDCReplicas)
            {
                logger.trace("Sending mutation {} to local replica {}", mutation.id(), replica);
                // Use appropriate send method based on handler type
                if (handler instanceof AbstractWriteResponseHandler)
                    MessagingService.instance().sendWriteWithCallback(message, replica, (AbstractWriteResponseHandler<?>) handler);
                else
                    MessagingService.instance().sendWithCallback(message, replica.endpoint(), handler);

                remoteReplicas.add(ClusterMetadata.current().directory.peerId(replica.endpoint()).id());
            }
        }

        if (remoteDCReplicas != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            for (List<Replica> dcReplicas : remoteDCReplicas.values())
            {
                logger.trace("Sending mutation {} to remote dc replicas {}", mutation.id(), dcReplicas);
                sendMessagesToRemoteDC(message, EndpointsForToken.copyOf(mutation.key().getToken(), dcReplicas), handler, coordinatorAckInfo);
                for (Replica replica : dcReplicas)
                    remoteReplicas.add(ClusterMetadata.current().directory.peerId(replica.endpoint()).id());
            }
        }

        if (remoteReplicas != null)
            MutationTrackingService.instance.sentWriteRequest(mutation, remoteReplicas);
    }

    /*
     * Send the message to the first replica of targets, and have it forward the message to others in its DC
     */
    static void sendMessagesToRemoteDC(Message<? extends IMutation> message,
                                       EndpointsForToken targets,
                                       RequestCallback<NoPayload> handler,
                                       ForwardedWrite.CoordinatorAckInfo ackTo)
    {
        final Replica target;

        if (targets.size() > 1)
        {
            target = pickReplica(targets);
            EndpointsForToken forwardToReplicas = targets.filter(r -> r != target, targets.size());

            for (Replica replica : forwardToReplicas)
            {
                if (handler instanceof TrackedWriteResponseHandler)
                    MessagingService.instance().callbacks.addWithExpiration((TrackedWriteResponseHandler) handler, message, replica);
                else if (handler instanceof ForwardedWrite.LeaderCallback)
                    MessagingService.instance().callbacks.addWithExpiration(handler, message, replica.endpoint());
                else
                    throw new IllegalStateException();
                logger.trace("Adding FWD message to {}@{}", message.id(), replica);
            }

            // starting with 4.0, use the same message id for all replicas
            long[] messageIds = new long[forwardToReplicas.size()];
            Arrays.fill(messageIds, message.id());

            message = message.withForwardTo(new ForwardingInfo(forwardToReplicas.endpointList(), messageIds));
        }
        else
        {
            target = targets.get(0);
        }
        if (ackTo != null)
            message = message.withParam(ParamType.COORDINATOR_ACK_INFO, ackTo);

        Tracing.trace("Sending mutation to remote replica {}", target);
        if (handler instanceof ForwardedWrite.LeaderCallback)
            MessagingService.instance().sendForwardedWriteWithCallback(message, target, (ForwardedWrite.LeaderCallback) handler);
        else
            MessagingService.instance().sendWriteWithCallback(message, target, (AbstractWriteResponseHandler<?>) handler);
        logger.trace("Sending message to {}@{}", message.id(), target);
    }

    private static Replica pickReplica(EndpointsForToken targets)
    {
        EndpointsForToken healthy = targets.filter(r -> DynamicEndpointSnitch.getSeverity(r.endpoint()) == 0);
        EndpointsForToken select = healthy.isEmpty() ? targets : healthy;
        return select.get(ThreadLocalRandom.current().nextInt(0, select.size()));
    }

    static void applyMutationLocally(Mutation mutation, RequestCallback<NoPayload> handler)
    {
        Preconditions.checkArgument(handler instanceof TrackedWriteResponseHandler || handler instanceof ForwardedWrite.LeaderCallback);
        Stage.MUTATION.maybeExecuteImmediately(new LocalMutationRunnable(mutation, handler));
    }

    static void applyCounterMutationLocally(CounterMutation counterMutation,
                                            ReplicaPlan.ForWrite plan,
                                            TrackedWriteResponseHandler handler)
    {
        Stage.COUNTER_MUTATION.maybeExecuteImmediately(new LocalCounterMutationRunnable(counterMutation, plan, handler));
    }

    private static class LocalMutationRunnable implements DebuggableTask.RunnableDebuggableTask
    {
        private final Mutation mutation;
        private final RequestCallback<NoPayload> handler;

        LocalMutationRunnable(Mutation mutation, RequestCallback<NoPayload> handler)
        {
            Preconditions.checkArgument(handler instanceof TrackedWriteResponseHandler || handler instanceof ForwardedWrite.LeaderCallback);
            this.mutation = mutation;
            this.handler = handler;
        }

        @Override
        public final void run()
        {
            long now = MonotonicClock.Global.approxTime.now();
            long deadline = getRequestTime(handler).computeDeadline(MUTATION_REQ.expiresAfterNanos());

            if (now > deadline)
            {
                long timeTakenNanos = now - startTimeNanos();
                MessagingService.instance().metrics.recordSelfDroppedMessage(Verb.MUTATION_REQ, timeTakenNanos, NANOSECONDS);
                return;
            }

            try
            {
                mutation.apply();
                handler.onResponse(null);
            }
            catch (Exception ex)
            {
                if (!(ex instanceof WriteTimeoutException))
                    logger.error("Failed to apply mutation locally : ", ex);
                handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailure.forException(ex));
            }
        }

        @Override
        public long creationTimeNanos()
        {
            return getRequestTime(handler).enqueuedAtNanos();
        }

        @Override
        public long startTimeNanos()
        {
            return getRequestTime(handler).startedAtNanos();
        }

        @Override
        public String description()
        {
            // description is an Object and toString() called so we do not have to evaluate the Mutation.toString()
            // unless expliclitly checked
            return mutation.toString();
        }
    }

    private static class LocalCounterMutationRunnable implements DebuggableTask.RunnableDebuggableTask
    {
        private final CounterMutation counterMutation;
        private final ReplicaPlan.ForWrite plan;
        private final TrackedWriteResponseHandler handler;

        LocalCounterMutationRunnable(CounterMutation counterMutation, ReplicaPlan.ForWrite plan, TrackedWriteResponseHandler handler)
        {
            this.counterMutation = counterMutation;
            this.plan = plan;
            this.handler = handler;
        }

        private Dispatcher.RequestTime getReqestTime()
        {
            return handler.getRequestTime();
        }

        @Override
        public void run()
        {
            long now = MonotonicClock.Global.approxTime.now();
            long deadline = getReqestTime().computeDeadline(COUNTER_MUTATION_REQ.expiresAfterNanos());

            if (now > deadline)
            {
                long timeTakenNanos = now - startTimeNanos();
                MessagingService.instance().metrics.recordSelfDroppedMessage(COUNTER_MUTATION_REQ, timeTakenNanos, NANOSECONDS);
                return;
            }

            try
            {
                Mutation result = counterMutation.applyCounterMutation(counterMutation.id());
                handler.onResponse(null);
                sendToReplicas(result, plan, handler, null);
            }
            catch (Exception ex)
            {
                if(!(ex instanceof WriteTimeoutException))
                    logger.error("Failed to apply counter mutation locally:  ", ex);
                handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailure.forException(ex));
            }
        }

        @Override
        public long creationTimeNanos()
        {
            return getReqestTime().enqueuedAtNanos();
        }

        @Override
        public long startTimeNanos()
        {
            return getReqestTime().startedAtNanos();
        }

        @Override
        public String description()
        {
            return counterMutation.toString();
        }
    }

    private static Dispatcher.RequestTime getRequestTime(RequestCallback<?> callback)
    {
        if (callback instanceof TrackedWriteResponseHandler)
            return ((TrackedWriteResponseHandler) callback).getRequestTime();
        if (callback instanceof ForwardedWrite.LeaderCallback)
            return ((ForwardedWrite.LeaderCallback) callback).getRequestTime();
        throw new IllegalStateException();
    }
}
