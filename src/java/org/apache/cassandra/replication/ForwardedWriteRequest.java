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
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ForwardedWriteResponseHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

public class ForwardedWriteRequest
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWriteRequest.class);

    static class FanOutMessage
    {
        final Verb verb;
        final Mutation mutation;
        final Set<NodeId> recipients;

        public FanOutMessage(Verb verb, Mutation mutation, Set<NodeId> recipients)
        {
            this.verb = verb;
            this.mutation = mutation;
            this.recipients = recipients;
        }

        private static final IVersionedSerializer<FanOutMessage> serializer = new IVersionedSerializer<FanOutMessage>()
        {
            @Override
            public void serialize(FanOutMessage t, DataOutputPlus out, int version) throws IOException
            {
                Version v = Version.minCommonSerializationVersion();
                out.writeInt(t.verb.id);
                Mutation.serializer.serialize(t.mutation, out, version);
                out.writeInt(t.recipients.size());
                for (NodeId recipient : t.recipients)
                    NodeId.serializer.serialize(recipient, out, v);
            }

            @Override
            public FanOutMessage deserialize(DataInputPlus in, int version) throws IOException
            {
                Version v = Version.minCommonSerializationVersion();
                Verb verb = Verb.fromId(in.readInt());
                Mutation mutation = Mutation.serializer.deserialize(in, version);
                int numRecipients = in.readInt();
                Set<NodeId> recipients = new HashSet<>(numRecipients);
                for (int i = 0; i < numRecipients; i++)
                    recipients.add(NodeId.serializer.deserialize(in, v));
                return new FanOutMessage(verb, mutation, recipients);
            }

            @Override
            public long serializedSize(FanOutMessage t, int version)
            {
                long size = 0;
                Version v = Version.minCommonSerializationVersion();
                size += TypeSizes.INT_SIZE;
                size += Mutation.serializer.serializedSize(t.mutation, version);
                size += TypeSizes.INT_SIZE;
                for (NodeId recipient : t.recipients)
                    size += NodeId.serializer.serializedSize(recipient, v);
                return size;
            }
        };
    }

    // For now, just supporting a single mutation to multiple recipients. This will develop in the future for different
    // kinds of mutations that each go to different recipients (see PaxosCommit).
    FanOutMessage message;

    private ForwardedWriteRequest(FanOutMessage message)
    {
        this.message = message;
    }

    public ForwardedWriteRequest(Verb verb, Mutation mutation, ReplicaPlan.ForWrite plan)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        Set<NodeId> recipients = new HashSet<>(plan.liveAndDown().size());
        for (Replica replica : plan.liveAndDown())
            recipients.add(cm.directory.peerId(replica.endpoint()));
        this.message = new FanOutMessage(verb, mutation, recipients);
    }

    private Replica getLeader()
    {
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        ClusterMetadata cm = ClusterMetadata.current();
        Token token = message.mutation.key().getToken();
        Keyspace keyspace = Keyspace.open(message.mutation.getKeyspaceName());
        EndpointsForRange endpoints = cm.placements.get(keyspace.getMetadata().params.replication).writes.forRange(token).get();
        if (logger.isTraceEnabled())
            logger.trace("Finding best leader from replicas {}", endpoints);

        // TODO: Should match ReplicaPlans.findCounterLeaderReplica, including DC-local priority, current health, severity, etc.
        return proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), endpoints).get(0);
    }

    public void sendToLeader(ForwardedWriteResponseHandler handler)
    {
        Replica leader = getLeader();
        if (logger.isTraceEnabled())
            logger.trace("Selected {} as leader for mutation with key {}", leader.endpoint(), message.mutation.key());
        Token token = message.mutation.key().getToken();
        Keyspace keyspace = Keyspace.open(message.mutation.getKeyspaceName());
        EndpointsForRange endpoints = ClusterMetadata.current().placements.get(keyspace.getMetadata().params.replication).writes.forRange(token).get();

        // Add callbacks for replicas to respond directly to coordinator
        Message<ForwardedWriteRequest> toLeader = Message.out(Verb.FORWARDING_WRITE, this);
        for (Replica endpoint : endpoints)
        {
            if (logger.isTraceEnabled())
                logger.trace("Adding forwarding callback for response from {} id {}", endpoint, toLeader.id());
            MessagingService.instance().callbacks.addWithExpiration(handler, toLeader, endpoint);
        }

        MessagingService.instance().send(toLeader, leader.endpoint());
    }

    private void executeOnLeader(DirectAcknowledge ackTo)
    {
        new TrackedWriteRequest(ackTo).performForwarding(this);
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<ForwardedWriteRequest>
    {
        @Override
        public void serialize(ForwardedWriteRequest request, DataOutputPlus out, int version) throws IOException
        {
            FanOutMessage.serializer.serialize(request.message, out, version);
        }

        @Override
        public ForwardedWriteRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            FanOutMessage message = FanOutMessage.serializer.deserialize(in, version);
            return new ForwardedWriteRequest(message);
        }

        @Override
        public long serializedSize(ForwardedWriteRequest request, int version)
        {
            return FanOutMessage.serializer.serializedSize(request.message, version);
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    public static class VerbHandler implements IVerbHandler<ForwardedWriteRequest>
    {
        @Override
        public void doVerb(Message<ForwardedWriteRequest> incoming)
        {
            if (logger.isTraceEnabled())
                logger.trace("Received incoming ForwardedWriteRequest {} id {}", incoming, incoming.id());
            Mutation mutation = incoming.payload.message.mutation;
            Preconditions.checkState(mutation.id().isNone());

            Stage.REQUEST_RESPONSE.submit(() -> {
                // Once we support epoch changes, check epoch from coordinator here, after potential queueing on the Stage
                try
                {
                    incoming.payload.executeOnLeader(DirectAcknowledge.toCoordinator(incoming.from(), incoming.id()));
                }
                catch (Exception e)
                {
                    logger.error("Exception while executing forwarded write with key {} on leader", mutation.key(), e);
                    MessagingService.instance().respondWithFailure(RequestFailureReason.UNKNOWN, incoming);
                }
            });
        }
    }

    public static class DirectAcknowledge
    {
        public static IVersionedSerializer<DirectAcknowledge> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(DirectAcknowledge ackTo, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(ackTo.coordinator, out, version);
                out.writeLong(ackTo.id);
            }

            @Override
            public DirectAcknowledge deserialize(DataInputPlus in, int version) throws IOException
            {
                InetAddressAndPort coordinator = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                long id = in.readLong();
                return new DirectAcknowledge(coordinator, id);
            }

            @Override
            public long serializedSize(DirectAcknowledge ackTo, int version)
            {
                long size = 0;
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(ackTo.coordinator, version);
                size += TypeSizes.LONG_SIZE;
                return size;
            }
        };

        public final InetAddressAndPort coordinator;
        public final long id;

        public DirectAcknowledge(InetAddressAndPort coordinator, long id)
        {
            this.coordinator = coordinator;
            this.id = id;
        }

        static DirectAcknowledge toCoordinator(InetAddressAndPort coordinator, long messageId)
        {
            return new DirectAcknowledge(coordinator, messageId);
        }
    }
}
