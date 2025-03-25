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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.*;
import org.apache.cassandra.locator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.Verb.MUTATION_REQ;

/**
 * For a forwarded write there are 2 nodes involved in coordination, a coordinator and a leader. The coordinator is the
 * node that the client is communicating with, and the leader is the mutation replica that is handling the mutation
 * tracking for that write.
 */
public class ForwardedWrite
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWrite.class);

    public interface Request
    {
        enum Kind
        {
            MUTATION(0);

            private final byte id;

            Kind(int id)
            {
                this.id = (byte) id;
            }

            IVersionedSerializer<Request> serializer()
            {
                switch (this)
                {
                    case MUTATION:
                        return MutationRequest.serializer;
                    default:
                        throw new IllegalStateException("Unhandled kind " + this);
                }
            }

            static final IVersionedSerializer<Kind> serializer = new IVersionedSerializer<Request.Kind>()
            {
                @Override
                public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
                {
                    out.writeByte(kind.id);

                }

                @Override
                public Kind deserialize(DataInputPlus in, int version) throws IOException
                {
                    byte id = in.readByte();
                    switch (id)
                    {
                        case 0:
                            return MUTATION;
                        default:
                            throw new IllegalStateException("Unknown kind: " + id);
                    }
                }

                @Override
                public long serializedSize(Kind kind, int version)
                {
                    return TypeSizes.BYTE_SIZE;
                }
            };
        }

        Kind kind();
        DecoratedKey key();
        void applyLocallyAndForwardToReplicas(CoordinatorAckInfo ackTo);

        IVersionedSerializer<Request> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Request request, DataOutputPlus out, int version) throws IOException
            {
                Kind.serializer.serialize(request.kind(), out, version);
                request.kind().serializer().serialize(request, out, version);
            }

            @Override
            public Request deserialize(DataInputPlus in, int version) throws IOException
            {
                Kind kind = Kind.serializer.deserialize(in, version);
                return kind.serializer().deserialize(in, version);
            }

            @Override
            public long serializedSize(Request request, int version)
            {
                long size = Kind.serializer.serializedSize(request.kind(), version);
                size += request.kind().serializer().serializedSize(request, version);
                return size;
            }
        };
    }

    public static class MutationRequest implements Request
    {
        private final Mutation mutation;
        private final Set<NodeId> recipients;

        private static Set<NodeId> nodeIds(ReplicaPlan.ForWrite plan)
        {
            ClusterMetadata cm = ClusterMetadata.current();
            Set<NodeId> recipients = new HashSet<>(plan.liveAndDown().size());
            for (Replica replica : plan.liveAndDown())
                recipients.add(cm.directory.peerId(replica.endpoint()));
            return recipients;
        }

        MutationRequest(Mutation mutation, ReplicaPlan.ForWrite plan)
        {
            this(mutation, nodeIds(plan));
        }

        public MutationRequest(Mutation mutation, Set<NodeId> recipients)
        {
            Preconditions.checkArgument(mutation.id().isNone());
            this.mutation = mutation;
            this.recipients = recipients;
        }

        @Override
        public Kind kind()
        {
            return Kind.MUTATION;
        }

        @Override
        public DecoratedKey key()
        {
            return mutation.key();
        }

        @Override
        public void applyLocallyAndForwardToReplicas(CoordinatorAckInfo ackTo)
        {
            Preconditions.checkState(ackTo != null);
            Preconditions.checkArgument(mutation.id().isNone());
            String keyspaceName = mutation.getKeyspaceName();
            Token token = mutation.key().getToken();

            MutationId id = MutationTrackingService.instance.nextMutationId(keyspaceName, token);
            // Do not wait for handler completion, since the coordinator is already waiting and we don't want to block the stage
            LeaderCallback handler = new LeaderCallback(keyspaceName, mutation.key().getToken(), id, ackTo);
            applyLocallyAndForwardToReplicas(mutation.withMutationId(id), recipients, handler, ackTo);
        }

        // TODO: refactor common with applyLocallyAndSendToReplicas
        private static void applyLocallyAndForwardToReplicas(Mutation mutation, Set<NodeId> recipients, LeaderCallback handler, CoordinatorAckInfo ackTo)
        {
            Preconditions.checkState(ackTo != null);
            ClusterMetadata cm = ClusterMetadata.current();
            String localDataCenter = cm.locator.local().datacenter;

            boolean applyLocally = false;

            // this DC replicas
            List<Replica> localDCReplicas = null;

            // extra-DC, grouped by DC
            Map<String, List<Replica>> remoteDCReplicas = null;

            // only need to create a Message for non-local writes
            Message<Mutation> message = null;

            // Expensive, but easier to work with Replica than InetAddressAndPort for now
            Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
            EndpointsForToken endpoints = cm.placements.get(keyspace.getMetadata().params.replication).writes.forToken(mutation.key().getToken()).get();
            Map<NodeId, Replica> replicas = new HashMap<>(recipients.size());
            for (Replica replica : endpoints)
                replicas.put(cm.directory.peerId(replica.endpoint()), replica);

            // For performance, Mutation caches serialized buffers that are computed lazily in serializedBuffer(). That
            // computation is not synchronized however, and we will potentially call that method concurrently for each
            // dispatched message (not that concurrent calls to serializedBuffer() are "unsafe" per se, just that they
            // may result in multiple computations, making the caching optimization moot). So forcing the serialization
            // here to make sure it's already cached/computed when it's concurrently used later.
            // Side note: we have one cached buffers for each used EncodingVersion and this only pre-compute the one for
            // the current version, but it's just an optimization, and we're ok not optimizing for mixed-version clusters.
            Mutation.serializer.prepareSerializedBuffer(mutation, MessagingService.current_version);

            for (NodeId recipient : recipients)
            {
                if (cm.myNodeId().equals(recipient))
                {
                    applyLocally = true;
                    continue;
                }

                if (message == null)
                    message = Message.builder(MUTATION_REQ, mutation)
                                     .withRequestTime(handler.getRequestTime())
                                     .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                     .withParam(ParamType.COORDINATOR_ACK_INFO, ackTo)
                                     .withId(ackTo.id)
                                     .build();

                Replica replica = replicas.get(recipient);
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

                    List<Replica> messages = remoteDCReplicas.get(dc);
                    if (messages == null)
                        messages = remoteDCReplicas.computeIfAbsent(dc, ignore -> new ArrayList<>(3)); // most DCs will have <= 3 replicas
                    messages.add(replica);
                }
            }

            Preconditions.checkState(applyLocally); // the leader is always a replica
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

        public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Request r, DataOutputPlus out, int version) throws IOException
            {
                MutationRequest request = (MutationRequest) r;
                Mutation.serializer.serialize(request.mutation, out, version);
                out.writeInt(request.recipients.size());
                for (NodeId recipient : request.recipients)
                    NodeId.messagingSerializer.serialize(recipient, out, version);
            }

            @Override
            public Request deserialize(DataInputPlus in, int version) throws IOException
            {
                Mutation mutation = Mutation.serializer.deserialize(in, version);
                int numRecipients = in.readInt();
                Set<NodeId> recipients = Sets.newHashSetWithExpectedSize(numRecipients);
                for (int i = 0; i < numRecipients; i++)
                    recipients.add(NodeId.messagingSerializer.deserialize(in, version));
                return new MutationRequest(mutation, recipients);
            }

            @Override
            public long serializedSize(Request r, int version)
            {
                MutationRequest request = (MutationRequest) r;
                long size = Mutation.serializer.serializedSize(request.mutation, version);
                size += TypeSizes.INT_SIZE;
                for (NodeId recipient : request.recipients)
                    size += NodeId.messagingSerializer.serializedSize(recipient, version);
                return size;
            }
        };
    }

    public static AbstractWriteResponseHandler<Object> forwardMutation(Mutation mutation, ReplicaPlan.ForWrite plan, AbstractReplicationStrategy strategy, Dispatcher.RequestTime requestTime)
    {
        // find leader
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        ClusterMetadata cm = ClusterMetadata.current();
        Token token = mutation.key().getToken();
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        EndpointsForRange endpoints = cm.placements.get(keyspace.getMetadata().params.replication).writes.forRange(token).get();
        if (logger.isTraceEnabled())
            logger.trace("Finding best leader from replicas {}", endpoints);

        // TODO: Should match ReplicaPlans.findCounterLeaderReplica, including DC-local priority, current health, severity, etc.
        Replica leader = proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), endpoints).get(0);

        // create callback and forward to leader
        if (logger.isTraceEnabled())
            logger.trace("Selected {} as leader for mutation with key {}", leader.endpoint(), mutation.key());

        AbstractWriteResponseHandler<Object> handler = strategy.getWriteResponseHandler(plan, null, WriteType.SIMPLE, null, requestTime);

        // Add callbacks for replicas to respond directly to coordinator
        Message<Request> toLeader = Message.out(Verb.FORWARDING_WRITE, new MutationRequest(mutation, plan));
        for (Replica endpoint : endpoints)
        {
            if (logger.isTraceEnabled())
                logger.trace("Adding forwarding callback for response from {} id {}", endpoint, toLeader.id());
            MessagingService.instance().callbacks.addWithExpiration(handler, toLeader, endpoint);
        }

        MessagingService.instance().send(toLeader, leader.endpoint());

        return handler;
    }

    public static final IVerbHandler<Request> verbHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<Request> incoming)
        {
            if (logger.isTraceEnabled())
                logger.trace("Received incoming ForwardedWriteRequest {} id {}", incoming, incoming.id());
            CoordinatorAckInfo ackTo = CoordinatorAckInfo.toCoordinator(incoming.from(), incoming.id());
            Request request = incoming.payload;

            // Once we support epoch changes, check epoch from coordinator here, after potential queueing on the Stage
            try
            {
                request.applyLocallyAndForwardToReplicas(ackTo);
            }
            catch (Exception e)
            {
                logger.error("Exception while executing forwarded write with key {} on leader", request.key(), e);
                MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, incoming);
            }
        }
    };

    // Leader just needs to acknowledge propagation for its own log, not for client consistency level
    // See org.apache.cassandra.service.TrackedWriteResponseHandler.onResponse, this class should probably merge with that one
    public static class LeaderCallback implements RequestCallback<NoPayload>
    {
        private final String keyspace;
        private final Token token;
        private final MutationId id;
        private final CoordinatorAckInfo ackTo;
        private final Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();

        public LeaderCallback(String keyspace, Token token, MutationId id, CoordinatorAckInfo ackTo)
        {
            this.keyspace = keyspace;
            this.token = token;
            this.id = id;
            this.ackTo = ackTo;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            // Local mutations are witnessed from Keyspace.applyInternalTracked
            if (msg != null)
                MutationTrackingService.instance.witnessedRemoteMutation(keyspace, token, id, msg.from());

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
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.error("Got failure from {} reason {}", from, failureReason);
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
