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

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
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
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.writeMetrics;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;

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
        Mutation mutation, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        Tracing.trace("Determining replicas for mutation");

        Preconditions.checkArgument(mutation.id().isNone());
        String keyspaceName = mutation.getKeyspaceName();
        Keyspace keyspace = Keyspace.open(keyspaceName);
        Token token = mutation.key().getToken();

        ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(keyspace, consistencyLevel, token, ReplicaPlans.writeNormal);
        AbstractReplicationStrategy rs = plan.replicationStrategy();

        if (plan.lookup(FBUtilities.getBroadcastAddressAndPort()) == null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Remote tracked request {} {}", mutation, plan);
            writeMetrics.remoteRequests.mark();
            return ForwardedWrite.forwardMutation(mutation, plan, rs, requestTime);
        }

        if (logger.isTraceEnabled())
            logger.trace("Local tracked request {} {}", mutation, plan);
        writeMetrics.localRequests.mark();
        MutationId id = MutationTrackingService.instance.nextMutationId(keyspaceName, token);
        mutation = mutation.withMutationId(id);
        TrackedWriteResponseHandler handler = TrackedWriteResponseHandler.wrap(rs.getWriteResponseHandler(plan, null, WriteType.SIMPLE, null, requestTime),
                                         keyspaceName,
                                         mutation.key().getToken(),
                                         id);
        applyLocallyAndSendToReplicas(mutation, plan, handler);
        return handler;
    }

    public static void applyLocallyAndSendToReplicas(Mutation mutation, ReplicaPlan.ForWrite plan, TrackedWriteResponseHandler handler)
    {
        String localDataCenter = DatabaseDescriptor.getLocator().local().datacenter;

        boolean applyLocally = false;

        // this DC replicas
        List<Replica> localDCReplicas = null;

        // extra-DC, grouped by DC
        Map<String, List<Replica>> remoteDCReplicas = null;

        // only need to create a Message for non-local writes
        Message<Mutation> message = null;

        // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
        // computation is not synchronized however, and we will potentially call that method concurrently for each
        // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
        // may result in multiple computations, making the caching optimization moot). So forcing the serialization
        // here to make sure it's already cached/computed when it's concurrently used later.
        // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
        // the current version, but it's just an optimization, and we're ok not optimizing for mixed-version clusters.
        Mutation.serializer.prepareSerializedBuffer(mutation, MessagingService.current_version);

        for (Replica destination : plan.contacts())
        {
            if (!plan.isAlive(destination))
            {
                handler.expired(); // immediately mark the response as expired since the request will not be sent
                continue;
            }

            if (destination.isSelf())
            {
                applyLocally = true;
                continue;
            }

            if (message == null)
            {
                Message.Builder<Mutation> builder = Message.builder(MUTATION_REQ, mutation)
                                 .withRequestTime(handler.getRequestTime())
                                 .withFlag(MessageFlag.CALL_BACK_ON_FAILURE);
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

                List<Replica> messages = remoteDCReplicas.get(dc);
                if (messages == null)
                    messages = remoteDCReplicas.computeIfAbsent(dc, ignore -> new ArrayList<>(3)); // most DCs will have <= 3 replicas
                messages.add(destination);
            }
        }

        Preconditions.checkState(applyLocally); // the coordinator is always a replica
        applyMutationLocally(mutation, handler);

        if (localDCReplicas != null)
            for (Replica destination : localDCReplicas)
                MessagingService.instance().sendWriteWithCallback(message, destination, handler);

        if (remoteDCReplicas != null)
        {
            // for each datacenter, send the message to one node to relay the write to other replicas
            for (List<Replica> dcReplicas : remoteDCReplicas.values())
                sendMessagesToRemoteDC(message, EndpointsForToken.copyOf(mutation.key().getToken(), dcReplicas), handler, null);
        }
    }

    static void applyMutationLocally(Mutation mutation, RequestCallback<NoPayload> handler)
    {
        Preconditions.checkArgument(handler instanceof TrackedWriteResponseHandler || handler instanceof ForwardedWrite.LeaderCallback);
        Stage.MUTATION.maybeExecuteImmediately(new LocalMutationRunnable(mutation, handler));
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

        private Dispatcher.RequestTime getRequestTime()
        {
            if (handler instanceof TrackedWriteResponseHandler)
                return ((TrackedWriteResponseHandler) handler).getRequestTime();
            if (handler instanceof ForwardedWrite.LeaderCallback)
                return ((ForwardedWrite.LeaderCallback) handler).getRequestTime();
            throw new IllegalStateException();
        }

        @Override
        public final void run()
        {
            long now = MonotonicClock.Global.approxTime.now();
            long deadline = getRequestTime().computeDeadline(MUTATION_REQ.expiresAfterNanos());

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
                handler.onFailure(FBUtilities.getBroadcastAddressAndPort(), RequestFailureReason.forException(ex));
            }
        }

        @Override
        public long creationTimeNanos()
        {
            return getRequestTime().enqueuedAtNanos();
        }

        @Override
        public long startTimeNanos()
        {
            return getRequestTime().startedAtNanos();
        }

        @Override
        public String description()
        {
            // description is an Object and toString() called so we do not have to evaluate the Mutation.toString()
            // unless expliclitly checked
            return mutation.toString();
        }
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
                MessagingService.instance().callbacks.addWithExpiration(handler, message, replica.endpoint());
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
}
