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
import java.util.function.BiPredicate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

public class StartupClusterConnectivityCheckerTest
{
    private StartupClusterConnectivityChecker localQuorumConnectivityChecker;
    private StartupClusterConnectivityChecker globalQuorumConnectivityChecker;
    private StartupClusterConnectivityChecker noopChecker;
    private StartupClusterConnectivityChecker zeroWaitChecker;

    private static final long TIMEOUT_NANOS = 100;
    private static final int NUM_PER_DC = 6;
    private Set<InetAddressAndPort> peers;
    private Set<InetAddressAndPort> peersA;
    private Set<InetAddressAndPort> peersAMinusLocal;
    private Set<InetAddressAndPort> peersB;
    private Set<InetAddressAndPort> peersC;

    private String getDatacenter(InetAddressAndPort endpoint)
    {
        if (peersA.contains(endpoint))
            return "datacenterA";
        if (peersB.contains(endpoint))
            return "datacenterB";
        else if (peersC.contains(endpoint))
            return "datacenterC";
        return null;
    }

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws UnknownHostException
    {
        localQuorumConnectivityChecker = new StartupClusterConnectivityChecker(TIMEOUT_NANOS, false);
        globalQuorumConnectivityChecker = new StartupClusterConnectivityChecker(TIMEOUT_NANOS, true);
        noopChecker = new StartupClusterConnectivityChecker(-1, false);
        zeroWaitChecker = new StartupClusterConnectivityChecker(0, false);

        peersA = new HashSet<>();
        peersAMinusLocal = new HashSet<>();
        peersA.add(FBUtilities.getBroadcastAddressAndPort());

        for (int i = 0; i < NUM_PER_DC - 1; i ++)
        {
            peersA.add(InetAddressAndPort.getByName("127.0.1." + i));
            peersAMinusLocal.add(InetAddressAndPort.getByName("127.0.1." + i));
        }

        peersB = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i ++)
            peersB.add(InetAddressAndPort.getByName("127.0.2." + i));


        peersC = new HashSet<>();
        for (int i = 0; i < NUM_PER_DC; i ++)
            peersC.add(InetAddressAndPort.getByName("127.0.3." + i));

        peers = new HashSet<>();
        peers.addAll(peersA);
        peers.addAll(peersB);
        peers.addAll(peersC);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().outboundSink.clear();
    }

    @Test
    public void execute_HappyPath()
    {
        Sink sink = new Sink(true, true, peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertTrue(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_NotAlive()
    {
        Sink sink = new Sink(false, true, peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_NoConnectionsAcks()
    {
        Sink sink = new Sink(true, false, peers);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_LocalQuorum()
    {
        // local peer plus 3 peers from same dc shouldn't pass (4/6)
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 3);
        checkAvailable(localQuorumConnectivityChecker, available, false);

        // local peer plus 4 peers from same dc should pass (5/6)
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        checkAvailable(localQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_GlobalQuorum()
    {
        // local dc passing shouldn't pass globally with two hosts down in datacenterB
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 2);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, false);

        // All three datacenters should be able to have a single node down
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 1);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, true);

        // Everything being up should work of course
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 1);
        copyCount(peersB, available, NUM_PER_DC);
        copyCount(peersC, available, NUM_PER_DC);
        checkAvailable(globalQuorumConnectivityChecker, available, true);
    }

    @Test
    public void execute_Noop()
    {
        checkAvailable(noopChecker, new HashSet<>(), true);
    }

    @Test
    public void execute_ZeroWaitHasConnections() throws InterruptedException
    {
        Sink sink = new Sink(true, true, new HashSet<>());
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertFalse(zeroWaitChecker.execute(peers, this::getDatacenter));
        MessagingService.instance().outboundSink.clear();
    }

    private void checkAvailable(StartupClusterConnectivityChecker checker, Set<InetAddressAndPort> available,
                                boolean shouldPass)
    {
        Sink sink = new Sink(true, true, available);
        MessagingService.instance().outboundSink.add(sink);
        Assert.assertEquals(shouldPass, checker.execute(peers, this::getDatacenter));
        MessagingService.instance().outboundSink.clear();
    }

    private void copyCount(Set<InetAddressAndPort> source, Set<InetAddressAndPort> dest, int count)
    {
        for (InetAddressAndPort peer : source)
        {
            if (count <= 0)
                break;

            dest.add(peer);
            count -= 1;
        }
    }

    private static class Sink implements BiPredicate<Message<?>, InetAddressAndPort>
    {
        private final boolean markAliveInGossip;
        private final boolean processConnectAck;
        private final Set<InetAddressAndPort> aliveHosts;
        private final Map<InetAddressAndPort, ConnectionTypeRecorder> seenConnectionRequests;

        Sink(boolean markAliveInGossip, boolean processConnectAck, Set<InetAddressAndPort> aliveHosts)
        {
            this.markAliveInGossip = markAliveInGossip;
            this.processConnectAck = processConnectAck;
            this.aliveHosts = aliveHosts;
            seenConnectionRequests = new HashMap<>();
        }

        @Override
        public boolean test(Message message, InetAddressAndPort to)
        {
            ConnectionTypeRecorder recorder = seenConnectionRequests.computeIfAbsent(to, inetAddress ->  new ConnectionTypeRecorder());

            if (!aliveHosts.contains(to))
                return false;

            if (processConnectAck)
            {
                Message msgIn = Message.builder(Verb.REQUEST_RSP, message.payload)
                                       .from(to)
                                       .build();
                MessagingService.instance().callbacks.get(message.id(), to).callback.onResponse(msgIn);
            }

            if (markAliveInGossip)
                Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(to, new EndpointState(new HeartBeatState(1, 1))));
            return false;
        }
    }

    private static class ConnectionTypeRecorder
    {
        boolean seenSmallMessageRequest;
        boolean seenLargeMessageRequest;
    }
}
