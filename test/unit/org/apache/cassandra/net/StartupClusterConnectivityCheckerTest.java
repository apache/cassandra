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

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

public class StartupClusterConnectivityCheckerTest
{
    private StartupClusterConnectivityChecker connectivityChecker;
    private Set<InetAddressAndPort> peers;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws UnknownHostException
    {
        connectivityChecker = new StartupClusterConnectivityChecker(70, 10);
        peers = new HashSet<>();
        peers.add(InetAddressAndPort.getByName("127.0.1.0"));
        peers.add(InetAddressAndPort.getByName("127.0.1.1"));
        peers.add(InetAddressAndPort.getByName("127.0.1.2"));
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void execute_HappyPath()
    {
        Sink sink = new Sink(true, true);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertTrue(connectivityChecker.execute(peers));
        checkAllConnectionTypesSeen(sink);
    }

    @Test
    public void execute_NotAlive()
    {
        Sink sink = new Sink(false, true);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(connectivityChecker.execute(peers));
        checkAllConnectionTypesSeen(sink);
    }

    @Test
    public void execute_NoConnectionsAcks()
    {
        Sink sink = new Sink(true, false);
        MessagingService.instance().addMessageSink(sink);
        Assert.assertFalse(connectivityChecker.execute(peers));
    }

    private void checkAllConnectionTypesSeen(Sink sink)
    {
        for (InetAddressAndPort peer : peers)
        {
            ConnectionTypeRecorder recorder = sink.seenConnectionRequests.get(peer);
            Assert.assertNotNull(recorder);
            Assert.assertTrue(recorder.seenSmallMessageRequest);
            Assert.assertTrue(recorder.seenLargeMessageRequest);
        }
    }

    private static class Sink implements IMessageSink
    {
        private final boolean markAliveInGossip;
        private final boolean processConnectAck;
        private final Map<InetAddressAndPort, ConnectionTypeRecorder> seenConnectionRequests;

        Sink(boolean markAliveInGossip, boolean processConnectAck)
        {
            this.markAliveInGossip = markAliveInGossip;
            this.processConnectAck = processConnectAck;
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

            if (processConnectAck)
            {
                MessageIn msgIn = MessageIn.create(to, message.payload, Collections.emptyMap(), MessagingService.Verb.REQUEST_RESPONSE, 1);
                MessagingService.instance().getRegisteredCallback(id).callback.response(msgIn);
            }

            if (markAliveInGossip)
                Gossiper.instance.realMarkAlive(to, new EndpointState(new HeartBeatState(1, 1)));
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
