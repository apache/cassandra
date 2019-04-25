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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

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
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void execute_HappyPath()
    {
        Sink sink = new Sink(true, true, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertTrue(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
        Assert.assertTrue(checkAllConnectionTypesSeen(sink));
    }

    @Test
    public void execute_NotAlive()
    {
        Sink sink = new Sink(false, true, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
        Assert.assertTrue(checkAllConnectionTypesSeen(sink));
    }

    @Test
    public void execute_NoConnectionsAcks()
    {
        Sink sink = new Sink(true, false, peers);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(localQuorumConnectivityChecker.execute(peers, this::getDatacenter));
    }

    @Test
    public void execute_LocalQuorum()
    {
        // local peer plus 3 peers from same dc shouldn't pass (4/6)
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 3);
        checkAvailable(localQuorumConnectivityChecker, available, false, true);

        // local peer plus 4 peers from same dc should pass (5/6)
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        checkAvailable(localQuorumConnectivityChecker, available, true, true);
    }

    @Test
    public void execute_GlobalQuorum()
    {
        // local dc passing shouldn't pass globally with two hosts down in datacenterB
        Set<InetAddressAndPort> available = new HashSet<>();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 2);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, false, true);

        // All three datacenters should be able to have a single node down
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 2);
        copyCount(peersB, available, NUM_PER_DC - 1);
        copyCount(peersC, available, NUM_PER_DC - 1);
        checkAvailable(globalQuorumConnectivityChecker, available, true, true);

        // Everything being up should work of course
        available.clear();
        copyCount(peersAMinusLocal, available, NUM_PER_DC - 1);
        copyCount(peersB, available, NUM_PER_DC);
        copyCount(peersC, available, NUM_PER_DC);
        checkAvailable(globalQuorumConnectivityChecker, available, true, true);
    }

    @Test
    public void execute_Noop()
    {
        checkAvailable(noopChecker, new HashSet<>(), true, false);
    }

    @Test
    public void execute_ZeroWaitHasConnections() throws InterruptedException
    {
        Sink sink = new Sink(true, true, new HashSet<>());
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(zeroWaitChecker.execute(peers, this::getDatacenter));
        boolean hasConnections = false;
        for (int i = 0; i < TIMEOUT_NANOS; i+= 10)
        {
            hasConnections = checkAllConnectionTypesSeen(sink);
            if (hasConnections)
                break;
            Thread.sleep(0, 10);
        }
        MessagingService.instance().clearMessageSinks();
        Assert.assertTrue(hasConnections);
    }

    private void checkAvailable(StartupClusterConnectivityChecker checker, Set<InetAddressAndPort> available,
                                boolean shouldPass, boolean checkConnections)
    {
        Sink sink = new Sink(true, true, available);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertEquals(shouldPass, checker.execute(peers, this::getDatacenter));
        if (checkConnections)
            Assert.assertTrue(checkAllConnectionTypesSeen(sink));
        MessagingService.instance().clearMessageSinks();
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

    private boolean checkAllConnectionTypesSeen(Sink sink)
    {
        boolean result = true;
        for (InetAddressAndPort peer : peers)
        {
            if (peer.equals(FBUtilities.getBroadcastAddressAndPort()))
                continue;
            ConnectionTypeRecorder recorder = sink.seenConnectionRequests.get(peer);
            result = recorder != null;
            if (!result)
                break;

            result = recorder.seenSmallMessageRequest;
            result &= recorder.seenLargeMessageRequest;
        }
        return result;
    }

    private static class Sink implements IMessageSink
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
        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
        {
            ConnectionTypeRecorder recorder = seenConnectionRequests.computeIfAbsent(to, inetAddress ->  new ConnectionTypeRecorder());
            if (message.connectionType == SMALL_MESSAGE)
            {
                Assert.assertFalse(recorder.seenSmallMessageRequest);
                recorder.seenSmallMessageRequest = true;
            }
            else
            {
                Assert.assertFalse(recorder.seenLargeMessageRequest);
                recorder.seenLargeMessageRequest = true;
            }

            if (!aliveHosts.contains(to))
                return false;

            if (processConnectAck)
            {
                MessageIn msgIn = MessageIn.create(to, message.payload, Collections.emptyMap(), MessagingService.Verb.REQUEST_RESPONSE, 1);
                MessagingService.instance().getRegisteredCallback(id).callback.response(msgIn);
            }

            if (markAliveInGossip)
                Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(to, new EndpointState(new HeartBeatState(1, 1))));
            return false;
        }

        @Override
        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return false;
        }
    }

    private static class ConnectionTypeRecorder
    {
        boolean seenSmallMessageRequest;
        boolean seenLargeMessageRequest;
    }
}
