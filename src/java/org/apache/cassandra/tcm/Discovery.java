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
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.migration.Election;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class Discovery
{
    private static final Logger logger = LoggerFactory.getLogger(Discovery.class);

    public static final Discovery instance = new Discovery();
    public static final Serializer serializer = new Serializer();

    public final DiscoveryRequestHandler requestHandler = new DiscoveryRequestHandler();
    private final Set<InetAddressAndPort> discovered = new ConcurrentSkipListSet<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);

    public DiscoveredNodes discover()
    {
        return discover(5);
    }

    public DiscoveredNodes discover(int rounds)
    {
        boolean res = state.compareAndSet(State.NOT_STARTED, State.IN_PROGRESS);
        assert res : String.format("Can not start discovery as it is in state %s", state.get());

        discovered.addAll(DatabaseDescriptor.getSeeds());

        long deadline = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        DiscoveredNodes last = null;
        int lastCount = discovered.size();
        int unchangedFor = 0;
        while (Clock.Global.nanoTime() <= deadline && unchangedFor < rounds)
        {
            last = discoverOnce(null);
            if (last.kind == DiscoveredNodes.Kind.CMS_ONLY)
                break;

            if (lastCount == discovered.size())
                unchangedFor++;
            else
                lastCount = discovered.size();
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        res = state.compareAndSet(State.IN_PROGRESS, State.FINISHED);
        assert res : String.format("Can not finish discovery as it is in state %s", state.get());
        return last;
    }

    public DiscoveredNodes discoverOnce(InetAddressAndPort initiator)
    {
        Set<InetAddressAndPort> candidates = new HashSet<>();
        if (initiator != null)
            candidates.add(initiator);
        else
        {
            candidates.addAll(discovered);
            candidates.remove(FBUtilities.getBroadcastAddressAndPort());
        }

        Collection<Pair<InetAddressAndPort, DiscoveredNodes>> responses = Election.fanoutAndWait(MessagingService.instance(), candidates, Verb.TCM_DISCOVER_REQ, NoPayload.noPayload);

        for (Pair<InetAddressAndPort, DiscoveredNodes> discoveredNodes : responses)
        {
            if (discoveredNodes.right.kind == DiscoveredNodes.Kind.CMS_ONLY)
                return discoveredNodes.right;

            discovered.addAll(discoveredNodes.right.nodes);
        }

        return new DiscoveredNodes(discovered, DiscoveredNodes.Kind.KNOWN_PEERS);
    }

    private final class DiscoveryRequestHandler implements IVerbHandler<NoPayload>
    {
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

            MessagingService.instance().send(message.responseWith(discoveredNodes), message.from());
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