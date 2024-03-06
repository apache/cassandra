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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Discovery is used to idenitify other participants in the cluster. Nodes send TCM_DISCOVER_REQ
 * in several rounds. Node is considered to be discovered by another node when it has responsed
 * to its discovery request, or when another node received its discovery request.
 */
public class Discovery
{
    private static final Logger logger = LoggerFactory.getLogger(Discovery.class);

    public static final Discovery instance = new Discovery();
    public static final Serializer serializer = new Serializer();

    public final IVerbHandler<NoPayload> requestHandler;
    private final Set<InetAddressAndPort> discovered = new ConcurrentSkipListSet<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    // These two have to be suppliers because during simulation we can send out discovery requests
    // rather early, before other node has initialized either MessagingService or DatabaseDescriptor.
    // In order to properly route the timeout/failure, Simulator is checking whether the verb is
    // response verb, which compares the instance of the verb handler with ResponseHandler.
    // This can be improved by refactoring simulator, but this should help meanwhile.
    private final Supplier<MessageDelivery> messaging;
    private final Supplier<Set<InetAddressAndPort>> seeds;

    private final InetAddressAndPort self;

    private Discovery()
    {
        this(MessagingService::instance, DatabaseDescriptor::getSeeds, FBUtilities.getBroadcastAddressAndPort());
    }

    @VisibleForTesting
    public Discovery(Supplier<MessageDelivery> messaging, Supplier<Set<InetAddressAndPort>> seeds, InetAddressAndPort self)
    {
        this.messaging = messaging;
        this.seeds = seeds;
        this.requestHandler = new DiscoveryRequestHandler(messaging);
        this.self = self;
    }

    public DiscoveredNodes discover()
    {
        return discover(5);
    }

    public DiscoveredNodes discover(int rounds)
    {
        boolean res = state.compareAndSet(State.NOT_STARTED, State.IN_PROGRESS);
        assert res : String.format("Can not start discovery as it is in state %s", state.get());

        long deadline = nanoTime() + DatabaseDescriptor.getDiscoveryTimeout(TimeUnit.NANOSECONDS);
        long roundTimeNanos = Math.min(TimeUnit.SECONDS.toNanos(4),
                                       DatabaseDescriptor.getDiscoveryTimeout(TimeUnit.NANOSECONDS) / rounds);
        DiscoveredNodes last = null;
        int lastCount = discovered.size();
        int unchangedFor = -1;

        // we run for at least DatabaseDescriptor.getDiscoveryTimeout, but also need 5 (by default) consecutive rounds where
        // the discovered nodes are unchanged
        while (nanoTime() <= deadline || unchangedFor < rounds)
        {
            long startTimeNanos = nanoTime();
            last = discoverOnce(null, roundTimeNanos, TimeUnit.NANOSECONDS);
            if (last.kind == DiscoveredNodes.Kind.CMS_ONLY)
                break;

            if (lastCount == discovered.size())
            {
                unchangedFor++;
            }
            else
            {
                unchangedFor = 0;
                lastCount = discovered.size();
            }
            long sleeptimeNanos = roundTimeNanos - (nanoTime() - startTimeNanos);
            if (sleeptimeNanos > 0)
                Uninterruptibles.sleepUninterruptibly(sleeptimeNanos, TimeUnit.NANOSECONDS);
        }

        res = state.compareAndSet(State.IN_PROGRESS, State.FINISHED);
        assert res : String.format("Can not finish discovery as it is in state %s", state.get());
        return last;
    }
    public DiscoveredNodes discoverOnce(InetAddressAndPort initiator)
    {
        return discoverOnce(initiator, 1, TimeUnit.SECONDS);
    }
    public DiscoveredNodes discoverOnce(InetAddressAndPort initiator, long timeout, TimeUnit timeUnit)
    {
        Set<InetAddressAndPort> candidates = new HashSet<>();
        if (initiator != null)
            candidates.add(initiator);
        else
            candidates.addAll(discovered);

        if (candidates.isEmpty())
            candidates.addAll(seeds.get());

        candidates.remove(self);

        Collection<Pair<InetAddressAndPort, DiscoveredNodes>> responses = MessageDelivery.fanoutAndWait(messaging.get(), candidates, Verb.TCM_DISCOVER_REQ, NoPayload.noPayload, timeout, timeUnit);

        for (Pair<InetAddressAndPort, DiscoveredNodes> discoveredNodes : responses)
        {
            if (discoveredNodes.right.kind == DiscoveredNodes.Kind.CMS_ONLY)
                return discoveredNodes.right;

            discovered.add(discoveredNodes.left);
            discovered.addAll(discoveredNodes.right.nodes);
        }

        return new DiscoveredNodes(discovered, DiscoveredNodes.Kind.KNOWN_PEERS);
    }

    private final class DiscoveryRequestHandler implements IVerbHandler<NoPayload>
    {
        final Supplier<MessageDelivery> messaging;

        DiscoveryRequestHandler(Supplier<MessageDelivery> messaging)
        {
            this.messaging = messaging;
        }

        @Override
        public void doVerb(Message<NoPayload> message)
        {
            Set<InetAddressAndPort> cms = ClusterMetadata.current().fullCMSMembers();
            logger.debug("Responding to discovery request from {}: {}", message.from(), cms);

            DiscoveredNodes discoveredNodes;
            if (!cms.isEmpty())
                discoveredNodes = new DiscoveredNodes(cms, DiscoveredNodes.Kind.CMS_ONLY);
            else
            {
                discovered.add(message.from());
                discoveredNodes = new DiscoveredNodes(new HashSet<>(discovered), DiscoveredNodes.Kind.KNOWN_PEERS);
            }

            messaging.get().send(message.responseWith(discoveredNodes), message.from());
        }
    }

    public Collection<InetAddressAndPort> discoveredNodes()
    {
        return new ArrayList<>(discovered);
    }

    public static class DiscoveredNodes
    {
        private final Set<InetAddressAndPort> nodes;
        private final Kind kind;

        public DiscoveredNodes(Set<InetAddressAndPort> nodes, Kind kind)
        {
            this.nodes = nodes;
            this.kind = kind;
        }

        public Set<InetAddressAndPort> nodes()
        {
            return this.nodes;
        }

        public Kind kind()
        {
            return kind;
        }

        public String toString()
        {
            return "DiscoveredNodes{" +
                   "nodes=" + nodes +
                   ", kind=" + kind +
                   '}';
        }

        public enum Kind
        {
            CMS_ONLY, KNOWN_PEERS
        }
    }

    public static class Serializer implements IVersionedSerializer<DiscoveredNodes>
    {
        @Override
        public void serialize(DiscoveredNodes t, DataOutputPlus out, int version) throws IOException
        {
            out.write(t.kind.ordinal());
            out.writeUnsignedVInt32(t.nodes.size());
            for (InetAddressAndPort ep : t.nodes)
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(ep, out, version);
        }

        @Override
        public DiscoveredNodes deserialize(DataInputPlus in, int version) throws IOException
        {
            DiscoveredNodes.Kind kind = DiscoveredNodes.Kind.values()[in.readByte()];
            int count = in.readUnsignedVInt32();
            Set<InetAddressAndPort> eps = Sets.newHashSetWithExpectedSize(count);
            for (int i = 0; i < count; i++)
                eps.add(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version));
            return new DiscoveredNodes(eps, kind);
        }

        @Override
        public long serializedSize(DiscoveredNodes t, int version)
        {
            int size = TypeSizes.sizeofUnsignedVInt(t.nodes.size());
            size += TypeSizes.BYTE_SIZE;
            for (InetAddressAndPort ep : t.nodes)
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(ep, version);
            return size;
        }
    }

    private enum State
    {
        NOT_STARTED,
        IN_PROGRESS,
        FINISHED,
        FOUND_CMS
    }
}