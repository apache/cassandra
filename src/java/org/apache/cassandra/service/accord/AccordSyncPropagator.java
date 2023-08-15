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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import accord.local.Node;
import accord.messages.SimpleReply;
import accord.primitives.Ranges;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
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
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.utils.CollectionSerializers.newListSerializer;

/**
 * Notifies remote replicas that the local replica has synchronised coordination information for this epoch
 */
public class AccordSyncPropagator
{
    public static final IVerbHandler<List<Notification>> verbHandler = message -> AccordService.instance().receive(message);

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
        Ranges closed = Ranges.EMPTY, redundant = Ranges.EMPTY;

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

            addClosed = addClosed.subtract(closed);
            closed = closed.with(addClosed);
            return new Notification(epoch, Collections.emptySet(), addClosed, Ranges.EMPTY);
        }

        Notification redundant(Ranges addRedundant)
        {
            if (redundant.containsAll(addRedundant))
                return null;

            addRedundant = addRedundant.subtract(redundant);
            redundant = redundant.with(addRedundant);
            return new Notification(epoch, Collections.emptySet(), Ranges.EMPTY, addRedundant);
        }

        boolean ack(Notification notification)
        {
            if (!notification.syncComplete.isEmpty())
            {
                if (notification.syncComplete.containsAll(syncComplete)) syncComplete = ImmutableSet.of();
                else syncComplete = ImmutableSet.copyOf(Iterables.filter(syncComplete, v -> !notification.syncComplete.contains(v)));
            }
            closed = closed.subtract(notification.closed);
            redundant = redundant.subtract(notification.redundant);
            return syncComplete.isEmpty() && closed.isEmpty() && redundant.isEmpty();
        }

        @Override
        public String toString()
        {
            return "PendingEpoch{" +
                   "epoch=" + epoch +
                   ", syncComplete=" + syncComplete +
                   ", closed=" + closed +
                   ", redundant=" + redundant +
                   '}';
        }
    }

    static class PendingEpochs extends Long2ObjectHashMap<PendingEpoch>
    {
        boolean ack(List<Notification> notifications)
        {
            for (Notification notification : notifications)
            {
                PendingEpoch epoch = get(notification.epoch);
                if (epoch == null)
                    continue;
                if (epoch.ack(notification))
                    remove(notification.epoch);
            }
            return isEmpty();
        }
    }

    static class PendingNodes extends Int2ObjectHashMap<PendingEpochs>
    {
        boolean ack(Node.Id id, List<Notification> notifications)
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

    @Override
    public String toString()
    {
        return "AccordSyncPropagator{" +
               "localId=" + localId +
               ", pending=" + pending +
               '}';
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

    public void reportRedundant(long epoch, Collection<Node.Id> notify, Ranges redundant)
    {
        report(epoch, notify, PendingEpoch::redundant, redundant);
    }

    private synchronized <T> void report(long epoch, Collection<Node.Id> notify, ReportPending<T> report, T param)
    {
        // TODO (efficiency, now): for larger clusters this can be a problem as we trigger 1 msg for each instance, so in a 1k cluster its 1k messages; this can cause a thundering herd problem
        // this is mostly a problem for reportSyncComplete as we include every node in the cluster, for reportClosed/reportRedundant these tend to use only the nodes that are replicas of the range,
        // and there is currently an assumption that sub-ranges are done, so only impacting a handful of nodes.
        // TODO (correctness, now): during a host replacement multiple epochs are generated (move the range, remove the node), so its possible that notify will never be able to send the notification as the node is leaving the cluster
        notify.forEach(id -> {
            PendingEpoch pendingEpoch = pending.computeIfAbsent(id.id, ignore -> new PendingEpochs())
                                               .computeIfAbsent(epoch, PendingEpoch::new);
            Notification notification = report.report(pendingEpoch, param);
            if (notification != null)
                notify(id, Collections.singletonList(notification));
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

    private boolean notify(Node.Id to, List<Notification> notifications)
    {
        InetAddressAndPort toEp = endpointMapper.mappedEndpoint(to);
        if (!failureDetector.isAlive(toEp))
        {
            scheduler.schedule(() -> notify(to, notifications), 1, TimeUnit.MINUTES);
            return false;
        }
        Message<List<Notification>> msg = Message.out(Verb.ACCORD_SYNC_NOTIFY_REQ, notifications);
        messagingService.sendWithCallback(msg, toEp, new RequestCallback<SimpleReply>(){
            @Override
            public void onResponse(Message<SimpleReply> msg)
            {
                Invariants.checkState(msg.payload == SimpleReply.Ok, "Unexpected message: %s",  msg);
                Set<Long> completedEpochs = new HashSet<>();
                // TODO review is it a good idea to call the listener while not holding the `AccordSyncPropagator` lock?
                synchronized (AccordSyncPropagator.this)
                {
                    pending.ack(to, notifications);
                    for (Notification notification : notifications)
                    {
                        long epoch = notification.epoch;
                        if (notification.syncComplete.contains(localId))
                        {
                            if (hasSyncCompletedFor(epoch))
                                completedEpochs.add(epoch);
                        }
                    }
                }
                for (Notification notification : notifications)
                {
                    long epoch = notification.epoch;
                    listener.onEndpointAck(to, epoch);
                    if (completedEpochs.contains(epoch))
                        listener.onComplete(epoch);
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                scheduler.schedule(() -> AccordSyncPropagator.this.notify(to, notifications), 1, TimeUnit.MINUTES);
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        });
        return true;
    }

    public static class Notification
    {
        public static final IVersionedSerializer<Notification> serializer = new IVersionedSerializer<Notification>()
        {
            @Override
            public void serialize(Notification notification, DataOutputPlus out, int version) throws IOException
            {
                out.writeLong(notification.epoch);
                CollectionSerializers.serializeCollection(notification.syncComplete, out, version, TopologySerializers.nodeId);
                KeySerializers.ranges.serialize(notification.closed, out, version);
                KeySerializers.ranges.serialize(notification.redundant, out, version);
            }

            @Override
            public Notification deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Notification(in.readLong(),
                                        CollectionSerializers.deserializeList(in, version, TopologySerializers.nodeId),
                                        KeySerializers.ranges.deserialize(in, version),
                                        KeySerializers.ranges.deserialize(in, version));
            }

            @Override
            public long serializedSize(Notification notification, int version)
            {
                return TypeSizes.LONG_SIZE
                       + CollectionSerializers.serializedCollectionSize(notification.syncComplete, version, TopologySerializers.nodeId)
                       + KeySerializers.ranges.serializedSize(notification.closed, version)
                       + KeySerializers.ranges.serializedSize(notification.redundant, version);
            }
        };
        public static final IVersionedSerializer<List<Notification>> listSerializer = newListSerializer(serializer);

        final long epoch;
        final Collection<Node.Id> syncComplete;
        final Ranges closed, redundant;

        public Notification(long epoch, Collection<Node.Id> syncComplete, Ranges closed, Ranges redundant)
        {
            this.epoch = epoch;
            this.syncComplete = syncComplete;
            this.closed = closed;
            this.redundant = redundant;
        }
    }
}
