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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

public class OutboundConnectionsTest
{
    static final InetAddressAndPort LOCAL_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 9476);
    static final InetAddressAndPort REMOTE_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 9476);
    private static final InetAddressAndPort RECONNECT_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.3"), 9476);
    private static final List<ConnectionType> INTERNODE_MESSAGING_CONN_TYPES = ImmutableList.of(ConnectionType.URGENT_MESSAGES, ConnectionType.LARGE_MESSAGES, ConnectionType.SMALL_MESSAGES);

    private OutboundConnections connections;
    // for testing messages larger than the size threshold, we just need a serializer to report a size, as fake as it may be
    public static final IVersionedSerializer<Object> SERIALIZER = new IVersionedSerializer<Object>()
    {
        public void serialize(Object o, DataOutputPlus out, int version)
        {

        }

        public Object deserialize(DataInputPlus in, int version)
        {
            return null;
        }

        public long serializedSize(Object o, int version)
        {
            return LARGE_MESSAGE_THRESHOLD + 1;
        }
    };

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Before
    public void setup()
    {
        connections = OutboundConnections.unsafeCreate(new OutboundConnectionSettings(REMOTE_ADDR));
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException
    {
        if (connections != null)
            connections.close(false).get(10L, TimeUnit.SECONDS);
    }

    @Test
    public void getConnection_Gossip()
    {
        GossipDigestSyn syn = new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0));
        Message<GossipDigestSyn> message = Message.out(Verb.GOSSIP_DIGEST_SYN, syn);
        Assert.assertEquals(ConnectionType.URGENT_MESSAGES, connections.connectionFor(message).type());
    }

    @Test
    public void getConnection_Gossip_Oversized() throws NoSuchFieldException, IllegalAccessException
    {
        IVersionedAsymmetricSerializer<?,?> restore = Verb.GOSSIP_DIGEST_ACK.serializer();
        try
        {
            Verb.GOSSIP_DIGEST_ACK.unsafeSetSerializer(() -> SERIALIZER);
            Message message = Message.out(Verb.GOSSIP_DIGEST_ACK, "payload");
            Assert.assertTrue(message.serializedSize(current_version) > LARGE_MESSAGE_THRESHOLD);
            Assert.assertEquals(ConnectionType.LARGE_MESSAGES, connections.connectionFor(message).type());
        }
        finally
        {
            Verb.GOSSIP_DIGEST_ACK.unsafeSetSerializer(() -> restore);
        }
    }


    @Test
    public void getConnection_SmallMessage()
    {
        Message message = Message.out(Verb.PING_REQ, PingRequest.forSmall);
        Assert.assertEquals(ConnectionType.SMALL_MESSAGES, connections.connectionFor(message).type());
    }

    @Test
    public void getConnection_LargeMessage() throws NoSuchFieldException, IllegalAccessException
    {
        Verb._TEST_2.unsafeSetSerializer(() -> SERIALIZER);
        Message message = Message.out(Verb._TEST_2, "payload");
        Assert.assertEquals(ConnectionType.LARGE_MESSAGES, connections.connectionFor(message).type());
    }

    @Test
    public void close_SoftClose() throws ExecutionException, InterruptedException, TimeoutException
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertFalse(connections.connectionFor(type).isClosed());
        connections.close(true).get(10L, TimeUnit.SECONDS);
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertTrue(connections.connectionFor(type).isClosed());
    }

    @Test
    public void close_NotSoftClose() throws ExecutionException, InterruptedException, TimeoutException
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertFalse(connections.connectionFor(type).isClosed());
        connections.close(false).get(10L, TimeUnit.SECONDS);
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertTrue(connections.connectionFor(type).isClosed());
    }

    @Test
    public void reconnectWithNewIp() throws InterruptedException
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(REMOTE_ADDR, connections.connectionFor(type).settings().connectTo);
        }

        connections.reconnectWithNewIp(RECONNECT_ADDR).await();

        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(RECONNECT_ADDR, connections.connectionFor(type).settings().connectTo);
        }
    }

//    @Test
//    public void timeoutCounter()
//    {
//        long originalValue = connections.getTimeouts();
//        connections.incrementTimeout();
//        Assert.assertEquals(originalValue + 1, connections.getTimeouts());
//    }
}
