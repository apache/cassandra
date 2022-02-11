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

package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

@Ignore
public abstract class AbstractStartupConnectivityCheckerTest
{
    protected static final long TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    protected static final int NUM_PER_DC = 6;
    protected ImmutableSet<InetAddressAndPort> peers;
    protected Set<InetAddressAndPort> dc1Nodes;
    protected Set<InetAddressAndPort> dc1NodesMinusLocal;
    protected Set<InetAddressAndPort> dc2Nodes;
    protected Set<InetAddressAndPort> dc3Nodes;

    protected String mapToDatacenter(InetAddressAndPort endpoint)
    {
        if (dc1Nodes.contains(endpoint))
            return "DC1";
        if (dc2Nodes.contains(endpoint))
            return "DC2";
        else if (dc3Nodes.contains(endpoint))
            return "DC3";
        return null;
    }

    @Before
    public void setUp() throws UnknownHostException
    {
        dc1Nodes = new HashSet<>();
        dc1NodesMinusLocal = new HashSet<>();
        dc1Nodes.add(FBUtilities.getBroadcastAddressAndPort());

        for (int i = 1; i < NUM_PER_DC; i++)
        {
            dc1Nodes.add(InetAddressAndPort.getByName("127.0.1." + i));
            dc1NodesMinusLocal.add(InetAddressAndPort.getByName("127.0.1." + i));
        }

        dc2Nodes = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i++)
            dc2Nodes.add(InetAddressAndPort.getByName("127.0.2." + i));

        dc3Nodes = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i++)
            dc3Nodes.add(InetAddressAndPort.getByName("127.0.3." + i));

        peers = ImmutableSet.copyOf(Iterables.concat(dc1Nodes, dc2Nodes, dc3Nodes));

        initialize();
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().outboundSink.clear();
    }

    protected abstract void initialize() throws UnknownHostException;

    protected static class Sink implements BiPredicate<Message<?>, InetAddressAndPort>
    {
        private final Set<InetAddressAndPort> hostsThatRespondToGossipRequests;
        private final Set<InetAddressAndPort> hostsThatRespondToAckRequests;
        private final Set<InetAddressAndPort> aliveHosts;
        final Map<InetAddressAndPort, ConnectionTypeRecorder> seenConnectionRequests;

        Sink(Set<InetAddressAndPort> hostsThatRespondToGossipRequests, Set<InetAddressAndPort> hostsThatRespondToAckRequests, Set<InetAddressAndPort> aliveHosts)
        {
            this.hostsThatRespondToGossipRequests = hostsThatRespondToGossipRequests;
            this.hostsThatRespondToAckRequests = hostsThatRespondToAckRequests;
            this.aliveHosts = aliveHosts;
            this.seenConnectionRequests = new HashMap<>();
        }

        @Override
        public boolean test(Message<?> message, InetAddressAndPort to)
        {
            ConnectionTypeRecorder recorder = seenConnectionRequests.computeIfAbsent(to, inetAddress -> new ConnectionTypeRecorder());

            if (!aliveHosts.contains(to))
                return false;

            if (hostsThatRespondToAckRequests.contains(to))
            {
                Message msgIn = Message.builder(Verb.REQUEST_RSP, message.payload)
                                       .from(to)
                                       .build();
                MessagingService.instance().callbacks.get(message.id(), to).callback.onResponse(msgIn);
            }

            if (hostsThatRespondToGossipRequests.contains(to))
                Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(to, new EndpointState(new HeartBeatState(1, 1))));

            return false;
        }
    }

    protected static class ConnectionTypeRecorder
    {
        boolean seenSmallMessageRequest;
        boolean seenLargeMessageRequest;
    }
}
