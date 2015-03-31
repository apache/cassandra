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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoveTest
{
    static final IPartitioner partitioner = RandomPartitioner.instance;
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = ss.getTokenMetadata();
    static IPartitioner oldPartitioner;
    ArrayList<Token> endpointTokens = new ArrayList<Token>();
    ArrayList<Token> keyTokens = new ArrayList<Token>();
    List<InetAddress> hosts = new ArrayList<InetAddress>();
    List<UUID> hostIds = new ArrayList<UUID>();
    InetAddress removalhost;
    UUID removalId;

    @BeforeClass
    public static void setupClass() throws ConfigurationException
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        SchemaLoader.loadSchema();
    }

    @AfterClass
    public static void tearDownClass()
    {
        StorageService.instance.setPartitionerUnsafe(oldPartitioner);
    }

    @Before
    public void setup() throws IOException, ConfigurationException
    {
        tmd.clearUnsafe();

        // create a ring of 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 6);

        MessagingService.instance().listen(FBUtilities.getBroadcastAddress());
        Gossiper.instance.start(1);
        removalhost = hosts.get(5);
        hosts.remove(removalhost);
        removalId = hostIds.get(5);
        hostIds.remove(removalId);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
        MessagingService.instance().clearCallbacksUnsafe();
        MessagingService.instance().shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBadHostId()
    {
        ss.removeNode("ffffffff-aaaa-aaaa-aaaa-ffffffffffff");

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalHostId()
    {
        //first ID should be localhost
        ss.removeNode(hostIds.get(0).toString());
    }

    @Test
    public void testRemoveHostId() throws InterruptedException
    {
        // start removal in background and send replication confirmations
        final AtomicBoolean success = new AtomicBoolean(false);
        Thread remover = new Thread()
        {
            public void run()
            {
                try
                {
                    ss.removeNode(removalId.toString());
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
            MessageOut msg = new MessageOut(host, MessagingService.Verb.REPLICATION_FINISHED, null, null, Collections.<String, byte[]>emptyMap());
            MessagingService.instance().sendRR(msg, FBUtilities.getBroadcastAddress());
        }

        remover.join();

        assertTrue(success.get());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
    }
}
