/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.sink.IMessageSink;
import org.apache.cassandra.net.sink.SinkManager;
import org.apache.cassandra.streaming.StreamUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class RemoveTest extends CleanupHelper
{
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = ss.getTokenMetadata();
    IPartitioner oldPartitioner;
    ArrayList<Token> endpointTokens;
    ArrayList<Token> keyTokens;
    List<InetAddress> hosts;

    @Before
    public void setup() throws IOException, ConfigurationException
    {
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();

        oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        endpointTokens = new ArrayList<Token>();
        keyTokens = new ArrayList<Token>();
        hosts = new ArrayList<InetAddress>();

        // create a ring of 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 6);

        MessagingService.instance().listen(FBUtilities.getLocalAddress());
        Gossiper.instance.start(FBUtilities.getLocalAddress(), 1);
        for (int i = 0; i < 6; i++)
        {
            Gossiper.instance.initializeNodeUnsafe(hosts.get(i), 1);
        }
    }

    @After
    public void tearDown()
    {
        SinkManager.clear();
        MessagingService.instance().shutdown();
        ss.setPartitionerUnsafe(oldPartitioner);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBadToken()
    {
        final String token = StorageService.getPartitioner().getTokenFactory().toString(keyTokens.get(2));
        ss.removeToken(token);

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalToken()
    {
        //first token should be localhost
        final String token = StorageService.getPartitioner().getTokenFactory().toString(endpointTokens.get(0));
        ss.removeToken(token);
    }

    @Test
    public void testRemoveToken() throws InterruptedException
    {
        IPartitioner partitioner = StorageService.getPartitioner();

        final String token = partitioner.getTokenFactory().toString(endpointTokens.get(5));
        ReplicationSink rSink = new ReplicationSink();
        SinkManager.add(rSink);

        // start removal in background and send replication confirmations
        final AtomicBoolean success = new AtomicBoolean(false);
        Thread remover = new Thread()
        {
            public void run()
            {
                try
                {
                    ss.removeToken(token);
                }
                catch (Exception e)
                {
                    System.err.println(e);
                    e.printStackTrace();
                    return;
                }
                success.set(true);
            }
        };
        remover.start();

        Thread.sleep(1000); // make sure removal is waiting for confirmation

        assertTrue(tmd.isLeaving(hosts.get(5)));
        assertEquals(1, tmd.getLeavingEndpoints().size());

        for (InetAddress host : hosts)
        {
            Message msg = new Message(host, StorageService.Verb.REPLICATION_FINISHED, new byte[0]);
            MessagingService.instance().sendRR(msg, FBUtilities.getLocalAddress());
        }

        remover.join();

        assertTrue(success.get());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
    }

    @Test
    public void testStartRemoving()
    {
        IPartitioner partitioner = StorageService.getPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        NotificationSink nSink = new NotificationSink();
        ReplicationSink rSink = new ReplicationSink();
        SinkManager.add(nSink);
        SinkManager.add(rSink);

        assertEquals(0, tmd.getLeavingEndpoints().size());

        ss.onChange(hosts.get(1),
                    ApplicationState.STATUS,
                    valueFactory.removingNonlocal(endpointTokens.get(1), endpointTokens.get(5)));

        assertEquals(1, nSink.callCount);
        assertTrue(tmd.isLeaving(hosts.get(5)));
        assertEquals(1, tmd.getLeavingEndpoints().size());
    }

    @Test
    public void testFinishRemoving()
    {
        IPartitioner partitioner = StorageService.getPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        assertEquals(0, tmd.getLeavingEndpoints().size());

        ss.onChange(hosts.get(1),
                    ApplicationState.STATUS,
                    valueFactory.removedNonlocal(endpointTokens.get(1), endpointTokens.get(5)));

        assertFalse(Gossiper.instance.getLiveMembers().contains(hosts.get(5)));
        assertFalse(tmd.isMember(hosts.get(5)));
    }

    class ReplicationSink implements IMessageSink
    {

        public Message handleMessage(Message msg, InetAddress to)
        {
            if (!msg.getVerb().equals(StorageService.Verb.STREAM_REQUEST))
                return msg;

            StreamUtil.finishStreamRequest(msg, to);

            return null;
        }
    }

    class NotificationSink implements IMessageSink
    {
        public int callCount = 0;

        public Message handleMessage(Message msg, InetAddress to)
        {
            if (msg.getVerb().equals(StorageService.Verb.REPLICATION_FINISHED))
            {
                callCount++;
                assertEquals(Stage.MISC, msg.getMessageType());
                // simulate a response from remote server
                Message response = msg.getReply(FBUtilities.getLocalAddress(), new byte[]{ });
                MessagingService.instance().sendOneWay(response, FBUtilities.getLocalAddress());
                return null;
            }
            else
            {
                return msg;
            }
        }
    }
}
