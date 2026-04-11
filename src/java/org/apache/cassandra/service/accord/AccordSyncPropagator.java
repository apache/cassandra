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
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.TopologyListener;
import accord.local.Node;
import accord.messages.SimpleReply;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListSet;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Receives information about closed, retired ranges, and about sync completion, and
 * propagates this information to the peers.
 *
 * Notifies remote replicas that the local replica has synchronised coordination
 * information for this epoch.
 */
public class AccordSyncPropagator implements TopologyListener
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSyncPropagator.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public static final IVerbHandler<Notification> verbHandler = message -> {
        if (!AccordService.isSetup())
            return;
        AccordService.instance().receive(message);
    };

    interface TestListener
    {
        void onEndpointAck(Node.Id id, long epoch);
    }

    private interface ReportPending<T>
    {
        Notification report(PendingEpoch epoch, T value);
    }

    static class PendingEpoch
    {
        final long epoch;
        ImmutableSet<Node.Id> syncComplete = ImmutableSet.of(); // TODO (desired): propagate ack's for other nodes
        Ranges closed = Ranges.EMPTY, retired = Ranges.EMPTY;

        PendingEpoch(long epoch)
        {
            this.epoch = epoch;
        }

        Notification syncComplete(Node.Id newSyncComplete)
        {
            if (syncComplete.contains(newSyncComplete))
                return null;

            syncComplete = ImmutableSet.<Node.Id>builder()
                                       .addAll(syncComplete)
                                       .add(newSyncComplete)
                                       .build();

            return new Notification(epoch, Collections.singleton(newSyncComplete), Ranges.EMPTY, Ranges.EMPTY);
        }

        Notification closed(Ranges addClosed)
        {
            if (closed.containsAll(addClosed))
                return null;

            addClosed = addClosed.without(closed);
            closed = closed.with(addClosed);
            return new Notification(epoch, Collections.emptySet(), addClosed, Ranges.EMPTY);
        }

        Notification retired(Ranges addRetired)
        {
            if (retired.containsAll(addRetired))
                return null;

            addRetired = addRetired.without(retired);
            retired = retired.with(addRetired);
            return new Notification(epoch, Collections.emptySet(), Ranges.EMPTY, addRetired);
        }

        boolean isEmpty()
        {
            return syncComplete.isEmpty() && closed.isEmpty() && retired.isEmpty();
        }

        boolean ack(Notification notification)
        {
            if (!notification.readyToCoordinate.isEmpty())
            {
                if (notification.readyToCoordinate.containsAll(syncComplete)) syncComplete = ImmutableSet.of();
                else syncComplete = ImmutableSet.copyOf(Iterables.filter(syncComplete, v -> !notification.readyToCoordinate.contains(v)));
            }
            closed = closed.without(notification.closed);
            retired = retired.without(notification.retired);
            return syncComplete.isEmpty() && closed.isEmpty() && retired.isEmpty();
        }

        @Override
        public String toString()
        {
            return "PendingEpoch{" +
                   "epoch=" + epoch +
                   ", syncComplete=" + syncComplete +
                   ", closed=" + closed +
                   ", retired=" + retired +
                   '}';
        }
    }

    static class PendingEpochs extends Long2ObjectHashMap<PendingEpoch>
    {
        boolean ack(Notification notification)
        {
            PendingEpoch epoch = get(notification.epoch);
            if (epoch != null && epoch.ack(notification))
                remove(notification.epoch);
            return isEmpty();
        }
    }

    static class PendingNodes extends Int2ObjectHashMap<PendingEpochs>
    {
        boolean ack(Node.Id id, Notification notifications)
        {
            PendingEpochs node = get(id.id);
            if (node == null)
                return true;

            if (!node.ack(notifications))
                return false;

            remove(id.id);
            return true;
        }
    }

    private final PendingNodes pending = new PendingNodes();
    private final Node.Id self;
    private final AccordEndpointMapper endpointMapper;
    private final MessageDelivery messagingService;
    private final ScheduledExecutorPlus scheduler;
    private TestListener listener;
    private final ConcurrentHashMap<RetryKey, Notification> retryingNotifications = new ConcurrentHashMap<>();

    public AccordSyncPropagator(Node.Id self, AccordEndpointMapper endpointMapper,
                                MessageDelivery messagingService, ScheduledExecutorPlus scheduler)
    {
        this.self = self;
        this.endpointMapper = endpointMapper;
        this.messagingService = messagingService;
        this.scheduler = scheduler;
    }

    void setTestListener(TestListener listener)
    {
        this.listener = listener;
    }

    boolean hasPending()
    {
        return !pending.isEmpty();
    }

    synchronized boolean hasPending(long epoch)
    {
        if (pending.isEmpty()) return false;
        return pending.values().stream().allMatch(n -> {
            PendingEpoch p = n.get(epoch);
            return p != null && !p.isEmpty();
        });
    }

    @Override
    public String toString()
    {
        return "AccordSyncPropagator{" +
               "localId=" + self +
               ", pending=" + pending +
               '}';
    }

    public void onNodesRemoved(SortedArrayList<Node.Id> removed)
    {
        synchronized (AccordSyncPropagator.this)
        {
            for (Node.Id id : removed)
                pending.remove(id.id);
        }
    }

    @Override
    public void onReadyToCoordinate(Topology topology)
    {
        onReadyToCoordinate(topology.epoch(), topology.nodes());
    }

    @VisibleForTesting
    void onReadyToCoordinate(long epoch, SortedArrayList<Node.Id> nodes)
    {
        SortedListSet<Node.Id> remaining = SortedListSet.allOf(nodes);
        if (remaining.remove(self) && listener != null)
            listener.onEndpointAck(self, epoch);
        report(epoch, remaining, PendingEpoch::syncComplete, self);
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch, Topology topology)
    {
        if (topology != null)
            onEpochClosed(ranges, epoch, topology.nodes());
    }

    @VisibleForTesting
    void onEpochClosed(Ranges ranges, long epoch, Collection<Node.Id> nodes)
    {
        report(epoch, nodes, PendingEpoch::closed, ranges);
    }

    @Override
    public void onEpochRetired(Ranges ranges, long epoch, Topology topology)
    {
        if (topology != null)
            onEpochRetired(ranges, epoch, topology.nodes());
    }

    @VisibleForTesting
    void onEpochRetired(Ranges ranges, long epoch, Collection<Node.Id> nodes)
    {
        report(epoch, nodes, PendingEpoch::retired, ranges);
    }

    private <T> void report(long epoch, Collection<Node.Id> notify, ReportPending<T> report, T param)
    {
        // TODO (efficiency, now): for larger clusters this can be a problem as we trigger 1 msg for each instance, so in a 1k cluster its 1k messages; this can cause a thundering herd problem
        // this is mostly a problem for reportSyncComplete as we include every node in the cluster, for reportClosed/reportRetired these tend to use only the nodes that are replicas of the range,
        // and there is currently an assumption that sub-ranges are done, so only impacting a handful of nodes.
        // TODO (correctness, now): during a host replacement multiple epochs are generated (move the range, remove the node), so its possible that notify will never be able to send the notification as the node is leaving the cluster
        notify.forEach(id -> {
            Notification notification;
            synchronized (this)
            {
                PendingEpoch pendingEpoch = pending.computeIfAbsent(id.id, ignore -> new PendingEpochs())
                                                   .computeIfAbsent(epoch, PendingEpoch::new);
                notification = report.report(pendingEpoch, param);
            }
            if (notification != null)
                notify(id, notification);
        });
    }

    private void scheduleRetry(Node.Id to, Notification notification)
    {
        Notification retry = new Notification(notification.epoch, notification.readyToCoordinate, notification.closed, notification.retired, notification.attempts + 1);
        RetryKey key = new RetryKey(to, notification.epoch);
        retryingNotifications.compute(key, (k, cur) -> {
            if (cur == null)
            {
                scheduler.schedule(() -> retry(k), Math.max(1, Math.min(15, retry.attempts)), TimeUnit.MINUTES);
                return retry;
            }
            return cur.merge(retry);
        });
    }

    private void retry(RetryKey key)
    {
        Notification retry = retryingNotifications.remove(key);
        if (retry != null)
            notify(key.to, retry);
    }

    private boolean notify(Node.Id to, Notification notification)
    {
        InetAddressAndPort toEp = endpointMapper.mappedEndpointOrNull(to, notification);
        if (toEp == null)
            return false;

        // was the endpoint removed from membership?
        AccordEndpointMapper.NodeStatus nodeStatus = endpointMapper.nodeStatus(to);
        switch (nodeStatus)
        {
            default: throw new UnhandledEnum(nodeStatus);
            case UNHEALTHY:
                if (!endpointMapper.isRemoved(to))
                {
                    noSpamLogger.warn("Node{} is not alive, unable to notify of {}", to, notification);
                    scheduleRetry(to, notification);
                    return false;
                }
                // fall through to UNKNOWN, as we have been removed from the cluster in the latest epoch
            case UNKNOWN:
                // endpoint is not a member of the latest epoch
                pending.ack(to, notification);
                return true;

            case HEALTHY:
                Message<Notification> msg = Message.out(Verb.ACCORD_SYNC_NOTIFY_REQ, notification);
                RequestCallback<SimpleReply> cb = new RequestCallback<>()
                {
                    @Override
                    public void onResponse(Message<SimpleReply> msg)
                    {
                        Invariants.require(msg.payload == SimpleReply.Ok, "Unexpected message: %s", msg);
                        synchronized (AccordSyncPropagator.this)
                        {
                            pending.ack(to, notification);
                        }

                        long epoch = notification.epoch;
                        if (listener != null && notification.readyToCoordinate.contains(self))
                            listener.onEndpointAck(to, epoch);
                    }

                    @Override
                    public void onFailure(InetAddressAndPort from, RequestFailure failure)
                    {
                        scheduleRetry(to, notification);
                    }

                    @Override
                    public boolean invokeOnFailure()
                    {
                        return true;
                    }
                };
                messagingService.sendWithCallback(msg, toEp, cb);
                return true;
        }
    }

    public static class Notification
    {
        public static final UnversionedSerializer<Notification> serializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Notification notification, DataOutputPlus out) throws IOException
            {
                out.writeLong(notification.epoch);
                CollectionSerializers.serializeCollection(notification.readyToCoordinate, out, TopologySerializers.nodeId);
                KeySerializers.ranges.serialize(notification.closed, out);
                KeySerializers.ranges.serialize(notification.retired, out);
            }

            @Override
            public Notification deserialize(DataInputPlus in) throws IOException
            {
                return new Notification(in.readLong(),
                                        CollectionSerializers.deserializeList(in, TopologySerializers.nodeId),
                                        KeySerializers.ranges.deserialize(in),
                                        KeySerializers.ranges.deserialize(in));
            }

            @Override
            public long serializedSize(Notification notification)
            {
                return TypeSizes.LONG_SIZE
                        + CollectionSerializers.serializedCollectionSize(notification.readyToCoordinate, TopologySerializers.nodeId)
                        + KeySerializers.ranges.serializedSize(notification.closed)
                        + KeySerializers.ranges.serializedSize(notification.retired);
            }
        };

        final long epoch;
        final Collection<Node.Id> readyToCoordinate;
        final Ranges closed, retired;
        final int attempts;

        public Notification(long epoch, Collection<Node.Id> readyToCoordinate, Ranges closed, Ranges retired)
        {
            this(epoch, readyToCoordinate, closed, retired, 0);
        }

        public Notification(long epoch, Collection<Node.Id> readyToCoordinate, Ranges closed, Ranges retired, int attempts)
        {
            this.epoch = epoch;
            this.readyToCoordinate = readyToCoordinate;
            this.closed = closed;
            this.retired = retired;
            this.attempts = attempts;
        }

        Notification merge(Notification add)
        {
            Invariants.require(add.epoch == this.epoch);
            Collection<Node.Id> syncComplete = ImmutableSet.<Node.Id>builder()
                                                           .addAll(this.readyToCoordinate)
                                                           .addAll(add.readyToCoordinate)
                                                           .build();
            return new Notification(epoch, syncComplete, closed.with(add.closed), retired.with(add.retired), Math.max(add.attempts, this.attempts));
        }

        @Override
        public String toString()
        {
            return "Notification{" +
                   "epoch=" + epoch +
                   ", syncComplete=" + readyToCoordinate +
                   ", closed=" + closed +
                   ", retired=" + retired +
                   '}';
        }
    }

    static class RetryKey
    {
        final Node.Id to;
        final long epoch;

        RetryKey(Node.Id id, long epoch)
        {
            to = id;
            this.epoch = epoch;
        }

        @Override
        public int hashCode()
        {
            return to.id * 31 + (int)epoch;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof RetryKey))
                return false;

            RetryKey that = (RetryKey) obj;
            return that.epoch == this.epoch && that.to.equals(this.to);
        }

        @Override
        public String toString()
        {
            return epoch + "@" + to;
        }
    }
}
