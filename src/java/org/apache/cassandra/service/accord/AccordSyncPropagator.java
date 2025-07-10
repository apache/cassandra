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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.messages.SimpleReply;
import accord.primitives.Ranges;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
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
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Receives information about closed, retired ranges, and about sync completion, and
 * propagates this information to the peers.
 *
 * Notifies remote replicas that the local replica has synchronised coordination
 * information for this epoch.
 */
public class AccordSyncPropagator
{
    private static final Logger logger = LoggerFactory.getLogger(AccordSyncPropagator.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    public static final IVerbHandler<Notification> verbHandler = message -> {
        if (!AccordService.isSetup())
            return;
        AccordService.instance().receive(message);
    };

    interface Listener
    {
        void onEndpointAck(Node.Id id, long epoch);
        void onComplete(long epoch);
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
            if (!notification.syncComplete.isEmpty())
            {
                if (notification.syncComplete.containsAll(syncComplete)) syncComplete = ImmutableSet.of();
                else syncComplete = ImmutableSet.copyOf(Iterables.filter(syncComplete, v -> !notification.syncComplete.contains(v)));
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
    private final Node.Id localId;
    private final AccordEndpointMapper endpointMapper;
    private final MessageDelivery messagingService;
    private final IFailureDetector failureDetector;
    private final ScheduledExecutorPlus scheduler;
    private final Listener listener;
    private final ConcurrentHashMap<RetryKey, Notification> retryingNotifications = new ConcurrentHashMap<>();

    public AccordSyncPropagator(Node.Id localId, AccordEndpointMapper endpointMapper,
                                MessageDelivery messagingService, IFailureDetector failureDetector, ScheduledExecutorPlus scheduler,
                                Listener listener)
    {
        this.localId = localId;
        this.endpointMapper = endpointMapper;
        this.messagingService = messagingService;
        this.failureDetector = failureDetector;
        this.scheduler = scheduler;
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
               "localId=" + localId +
               ", pending=" + pending +
               '}';
    }

    public void onNodesRemoved(Node.Id removed)
    {
        long[] toAck;
        boolean[] syncCompletedFor;

        synchronized (AccordSyncPropagator.this)
        {
            PendingEpochs pendingEpochs = pending.remove(removed.id);
            if (pendingEpochs == null) return;
            toAck = new long[pendingEpochs.size()];
            syncCompletedFor = new boolean[pendingEpochs.size()];
            Long2ObjectHashMap<PendingEpoch>.KeyIterator it = pendingEpochs.keySet().iterator();
            for (int i = 0; it.hasNext(); i++)
            {
                long epoch = it.nextLong();
                toAck[i] = epoch;
                syncCompletedFor[i] = hasSyncCompletedFor(epoch);
            }
            Arrays.sort(toAck);
        }

        for (int i = 0; i < toAck.length; i++)
        {
            long epoch = toAck[i];
            listener.onEndpointAck(removed, epoch);
            if (syncCompletedFor[i])
                listener.onComplete(epoch);
        }
    }

    public void reportSyncComplete(long epoch, Collection<Node.Id> notify, Node.Id syncCompleteId)
    {
        if (notify.isEmpty())
        {
            listener.onComplete(epoch);
            return;
        }
        report(epoch, notify, PendingEpoch::syncComplete, syncCompleteId);
    }

    public void reportClosed(long epoch, Collection<Node.Id> notify, Ranges closed)
    {
        report(epoch, notify, PendingEpoch::closed, closed);
    }

    public void reportRetired(long epoch, Collection<Node.Id> notify, Ranges retired)
    {
        report(epoch, notify, PendingEpoch::retired, retired);
    }

    private synchronized <T> void report(long epoch, Collection<Node.Id> notify, ReportPending<T> report, T param)
    {
        // TODO (efficiency, now): for larger clusters this can be a problem as we trigger 1 msg for each instance, so in a 1k cluster its 1k messages; this can cause a thundering herd problem
        // this is mostly a problem for reportSyncComplete as we include every node in the cluster, for reportClosed/reportRetired these tend to use only the nodes that are replicas of the range,
        // and there is currently an assumption that sub-ranges are done, so only impacting a handful of nodes.
        // TODO (correctness, now): during a host replacement multiple epochs are generated (move the range, remove the node), so its possible that notify will never be able to send the notification as the node is leaving the cluster
        notify.forEach(id -> {
            PendingEpoch pendingEpoch = pending.computeIfAbsent(id.id, ignore -> new PendingEpochs())
                                               .computeIfAbsent(epoch, PendingEpoch::new);
            Notification notification = report.report(pendingEpoch, param);
            if (notification != null)
                notify(id, notification);
        });
    }

    private boolean hasSyncCompletedFor(long epoch)
    {
        return pending.values().stream().noneMatch(node -> {
            PendingEpoch pending = node.get(epoch);
            if (pending == null)
                return false;
            return !pending.syncComplete.isEmpty();
        });
    }

    private void scheduleRetry(Node.Id to, Notification notification)
    {
        Notification retry = new Notification(notification.epoch, notification.syncComplete, notification.closed, notification.retired, notification.attempts + 1);
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
        InetAddressAndPort toEp = endpointMapper.mappedEndpoint(to);
        Message<Notification> msg = Message.out(Verb.ACCORD_SYNC_NOTIFY_REQ, notification);
        RequestCallback<SimpleReply> cb = new RequestCallback<>()
        {
            @Override
            public void onResponse(Message<SimpleReply> msg)
            {
                Invariants.require(msg.payload == SimpleReply.Ok, "Unexpected message: %s", msg);
                Set<Long> completedEpochs = new HashSet<>();
                synchronized (AccordSyncPropagator.this)
                {
                    pending.ack(to, notification);
                    long epoch = notification.epoch;
                    if (notification.syncComplete.contains(localId))
                    {
                        if (hasSyncCompletedFor(epoch))
                            completedEpochs.add(epoch);
                    }
                }

                long epoch = notification.epoch;
                listener.onEndpointAck(to, epoch);
                if (completedEpochs.contains(epoch))
                    listener.onComplete(epoch);
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
        if (!failureDetector.isAlive(toEp))
        {
            // was the endpoint removed from membership?
            ClusterMetadata metadata = ClusterMetadata.current();
            if (Gossiper.instance.getEndpointStateForEndpoint(toEp) == null && !metadata.directory.allJoinedEndpoints().contains(toEp) && !metadata.fullCMSMembers().contains(toEp))
            {
                // endpoint no longer exists...
                cb.onResponse(msg.responseWith(SimpleReply.Ok));
                return true;
            }
            noSpamLogger.warn("Node{} is not alive, unable to notify of {}", to, notification);
            scheduleRetry(to, notification);
            return false;
        }
        messagingService.sendWithCallback(msg, toEp, cb);
        return true;
    }

    public static class Notification
    {
        public static final UnversionedSerializer<Notification> serializer = new UnversionedSerializer<Notification>()
        {
            @Override
            public void serialize(Notification notification, DataOutputPlus out) throws IOException
            {
                out.writeLong(notification.epoch);
                CollectionSerializers.serializeCollection(notification.syncComplete, out, TopologySerializers.nodeId);
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
                        + CollectionSerializers.serializedCollectionSize(notification.syncComplete, TopologySerializers.nodeId)
                        + KeySerializers.ranges.serializedSize(notification.closed)
                        + KeySerializers.ranges.serializedSize(notification.retired);
            }
        };

        final long epoch;
        final Collection<Node.Id> syncComplete;
        final Ranges closed, retired;
        final int attempts;

        public Notification(long epoch, Collection<Node.Id> syncComplete, Ranges closed, Ranges retired)
        {
            this(epoch, syncComplete, closed, retired, 0);
        }

        public Notification(long epoch, Collection<Node.Id> syncComplete, Ranges closed, Ranges retired, int attempts)
        {
            this.epoch = epoch;
            this.syncComplete = syncComplete;
            this.closed = closed;
            this.retired = retired;
            this.attempts = attempts;
        }

        Notification merge(Notification add)
        {
            Invariants.require(add.epoch == this.epoch);
            Collection<Node.Id> syncComplete = ImmutableSet.<Node.Id>builder()
                                                           .addAll(this.syncComplete)
                                                           .addAll(add.syncComplete)
                                                           .build();
            return new Notification(epoch, syncComplete, closed.with(add.closed), retired.with(add.retired), Math.max(add.attempts, this.attempts));
        }

        @Override
        public String toString()
        {
            return "Notification{" +
                   "epoch=" + epoch +
                   ", syncComplete=" + syncComplete +
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
    }
}
