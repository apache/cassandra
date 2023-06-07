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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.utils.Invariants;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;

public class AccordLocalSyncNotifier implements RequestCallback<AccordLocalSyncNotifier.Acknowledgement>
{
    public static final IVerbHandler<Notification> verbHandler = message -> AccordService.instance().remoteSyncComplete(message);
    private static final Logger logger = LoggerFactory.getLogger(AccordLocalSyncNotifier.class);

    interface Listener
    {
        void onEndpointAck(Node.Id id, long epoch);
        void onComplete(long epoch);
    }

    private final long epoch;
    private final Node.Id from;
    private final Set<Node.Id> pendingNotifications;
    private final AccordEndpointMapper endpointMapper;
    private final IFailureDetector failureDetector;
    private final Listener listener;
    private final MessageDelivery messagingService;

    public AccordLocalSyncNotifier(long epoch,
                                   Node.Id from, Set<Node.Id> pendingNotifications,
                                   AccordEndpointMapper endpointMapper,
                                   MessageDelivery messagingService, IFailureDetector failureDetector,
                                   Listener listener)
    {
        this.epoch = epoch;
        this.from = from;
        this.pendingNotifications = pendingNotifications;
        this.endpointMapper = endpointMapper;
        this.failureDetector = failureDetector;
        this.listener = listener;
        this.messagingService = messagingService;
    }

    private void notify(Node.Id to)
    {
        InetAddressAndPort toEp = endpointMapper.mappedEndpoint(to);
        if (failureDetector.isAlive(toEp))
        {
            Message<Notification> msg = Message.out(Verb.ACCORD_SYNC_NOTIFY_REQ, new Notification(epoch, from, to));
            messagingService.sendWithCallback(msg, toEp, this);
        }
        else
        {
            scheduleNotify(to);
        }
    }

    public void scheduleNotify(Node.Id to)
    {
        ScheduledExecutors.scheduledTasks.schedule(() -> notify(to), 1, TimeUnit.MINUTES);
    }

    public synchronized void start()
    {
        if (pendingNotifications.isEmpty())
        {
            listener.onComplete(epoch);
            return;
        }
        pendingNotifications.forEach(this::notify);
    }

    private synchronized void onResponse(InetAddressAndPort fromEp, Node.Id from)
    {
        try
        {
            Invariants.checkArgument(endpointMapper.mappedId(fromEp).equals(from), "%s != %s", from, endpointMapper.mappedId(fromEp));
            listener.onEndpointAck(from, epoch);
            pendingNotifications.remove(from);
            if (pendingNotifications.isEmpty())
                listener.onComplete(epoch);
        }
        catch (Throwable t)
        {
            logger.error("Unhandled exception handling sync ack on epoch {} from {}", epoch, fromEp, t);
            scheduleNotify(from);
        }
    }

    @Override
    public synchronized void onResponse(Message<Acknowledgement> msg)
    {
        onResponse(msg.from(), msg.payload.from);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        scheduleNotify(endpointMapper.mappedId(from));
    }

    public static class Notification
    {
        public static final IVersionedSerializer<Notification> serializer = new IVersionedSerializer<Notification>()
        {
            @Override
            public void serialize(Notification notification, DataOutputPlus out, int version) throws IOException
            {
                out.writeLong(notification.epoch);
                TopologySerializers.nodeId.serialize(notification.from, out, version);
                TopologySerializers.nodeId.serialize(notification.to, out, version);
            }

            @Override
            public Notification deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Notification(in.readLong(),
                                        TopologySerializers.nodeId.deserialize(in, version),
                                        TopologySerializers.nodeId.deserialize(in, version));
            }

            @Override
            public long serializedSize(Notification notification, int version)
            {
                return TypeSizes.LONG_SIZE
                       + TopologySerializers.nodeId.serializedSize()
                       + TopologySerializers.nodeId.serializedSize();
            }
        };
        final long epoch;
        final Node.Id from;
        final Node.Id to;

        public Notification(long epoch, Node.Id from, Node.Id to)
        {
            this.epoch = epoch;
            this.from = from;
            this.to = to;
        }
    }

    public static class Acknowledgement
    {
        public static final IVersionedSerializer<Acknowledgement> serializer = new IVersionedSerializer<Acknowledgement>()
        {
            @Override
            public void serialize(Acknowledgement acknowledgement, DataOutputPlus out, int version) throws IOException
            {
                TopologySerializers.nodeId.serialize(acknowledgement.from, out, version);
            }

            @Override
            public Acknowledgement deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Acknowledgement(TopologySerializers.nodeId.deserialize(in, version));
            }

            @Override
            public long serializedSize(Acknowledgement acknowledgement, int version)
            {
                return TopologySerializers.nodeId.serializedSize();
            }
        };

        final Node.Id from;

        public Acknowledgement(Node.Id from)
        {
            this.from = from;
        }
    }
}
