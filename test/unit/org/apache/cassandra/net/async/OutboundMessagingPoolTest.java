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

package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;

public class OutboundMessagingPoolTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9476);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 9476);
    private static final InetSocketAddress RECONNECT_ADDR = new InetSocketAddress("127.0.0.3", 9476);
    private static final List<ConnectionType> INTERNODE_MESSAGING_CONN_TYPES = new ArrayList<ConnectionType>()
            {{ add(ConnectionType.GOSSIP); add(ConnectionType.LARGE_MESSAGE); add(ConnectionType.SMALL_MESSAGE); }};

    private OutboundMessagingPool pool;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        BackPressureState backPressureState = DatabaseDescriptor.getBackPressureStrategy().newState(REMOTE_ADDR.getAddress());
        pool = new OutboundMessagingPool(REMOTE_ADDR, LOCAL_ADDR, null, backPressureState, new AllowAllInternodeAuthenticator());
    }

    @After
    public void tearDown()
    {
        if (pool != null)
            pool.close(false);
    }

    @Test
    public void getConnection_Gossip()
    {
        GossipDigestSyn syn = new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0));
        MessageOut<GossipDigestSyn> message = new MessageOut<>(MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                                                              syn, GossipDigestSyn.serializer);
        Assert.assertEquals(ConnectionType.GOSSIP, pool.getConnection(message).getConnectionId().type());
    }

    @Test
    public void getConnection_SmallMessage()
    {
        MessageOut message = WriteResponse.createMessage();
        Assert.assertEquals(ConnectionType.SMALL_MESSAGE, pool.getConnection(message).getConnectionId().type());
    }

    @Test
    public void getConnection_LargeMessage()
    {
        // just need a serializer to report a size, as fake as it may be
        IVersionedSerializer<Object> serializer = new IVersionedSerializer<Object>()
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
                return OutboundMessagingPool.LARGE_MESSAGE_THRESHOLD + 1;
            }
        };
        MessageOut message = new MessageOut<>(MessagingService.Verb.UNUSED_5, "payload", serializer);
        Assert.assertEquals(ConnectionType.LARGE_MESSAGE, pool.getConnection(message).getConnectionId().type());
    }

    @Test
    public void close()
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertNotSame(OutboundMessagingConnection.State.CLOSED, pool.getConnection(type).getState());
        pool.close(false);
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertEquals(OutboundMessagingConnection.State.CLOSED, pool.getConnection(type).getState());
    }

    @Test
    public void reconnectWithNewIp()
    {
        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(REMOTE_ADDR, pool.getPreferredRemoteAddr());
            Assert.assertEquals(REMOTE_ADDR, pool.getConnection(type).getConnectionId().connectionAddress());
        }

        pool.reconnectWithNewIp(RECONNECT_ADDR);

        for (ConnectionType type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(RECONNECT_ADDR, pool.getPreferredRemoteAddr());
            Assert.assertEquals(RECONNECT_ADDR, pool.getConnection(type).getConnectionId().connectionAddress());
        }
    }

    @Test
    public void timeoutCounter()
    {
        long originalValue = pool.getTimeouts();
        pool.incrementTimeout();
        Assert.assertEquals(originalValue + 1, pool.getTimeouts());
    }
}
