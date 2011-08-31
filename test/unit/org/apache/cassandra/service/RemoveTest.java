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
    ArrayList<Token> endpointTokens = new ArrayList<Token>();
    ArrayList<Token> keyTokens = new ArrayList<Token>();
    List<InetAddress> hosts = new ArrayList<InetAddress>();
    InetAddress removalhost;
    Token removaltoken;

    @Before
    public void setup() throws IOException, ConfigurationException
    {
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();

        oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        // create a ring of 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 6);

        MessagingService.instance().listen(FBUtilities.getBroadcastAddress());
        Gossiper.instance.start(1);
        for (int i = 0; i < 6; i++)
        {
            Gossiper.instance.initializeNodeUnsafe(hosts.get(i), 1);
        }
        removalhost = hosts.get(5);
        hosts.remove(removalhost);
        removaltoken = endpointTokens.get(5);
        endpointTokens.remove(removaltoken);
    }

    @After
    public void tearDown()
    {
        SinkManager.clear();
        MessagingService.instance().clearCallbacksUnsafe();
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

        final String token = partitioner.getTokenFactory().toString(removaltoken);
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
                    ss.removeToken(token, 0);
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

        assertTrue(tmd.isLeaving(removalhost));
        assertEquals(1, tmd.getLeavingEndpoints().size());

        for (InetAddress host : hosts)
        {
            Message msg = new Message(host, StorageService.Verb.REPLICATION_FINISHED, new byte[0], MessagingService.version_);
            MessagingService.instance().sendRR(msg, FBUtilities.getBroadcastAddress());
        }

        remover.join();

        assertTrue(success.get());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
    }

    class ReplicationSink implements IMessageSink
    {
        public Message handleMessage(Message msg, String id, InetAddress to)
        {
            if (!msg.getVerb().equals(StorageService.Verb.STREAM_REQUEST))
                return msg;

            StreamUtil.finishStreamRequest(msg, to);

            return null;
        }
    }
}
