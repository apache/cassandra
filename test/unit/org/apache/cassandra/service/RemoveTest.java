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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.*;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.REPLICATION_DONE_REQ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RemoveTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    static final IPartitioner partitioner = RandomPartitioner.instance;
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = ss.getTokenMetadata();
    static IPartitioner oldPartitioner;
    ArrayList<Token> endpointTokens = new ArrayList<Token>();
    ArrayList<Token> keyTokens = new ArrayList<Token>();
    List<InetAddressAndPort> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<UUID>();
    InetAddressAndPort removalhost;
    UUID removalId;

    @BeforeClass
    public static void setupClass() throws ConfigurationException
    {
        oldPartitioner = StorageService.instance.setPartitionerUnsafe(partitioner);
        MessagingService.instance().listen();
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

        removalhost = hosts.get(5);
        hosts.remove(removalhost);
        removalId = hostIds.get(5);
        hostIds.remove(removalId);
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().callbacks.unsafeClear();
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

    @Test(expected = UnsupportedOperationException.class)
    public void testNonmemberId()
    {
        VersionedValueFactory valueFactory = new VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        Collection<Token> tokens = Collections.singleton(DatabaseDescriptor.getPartitioner().getRandomToken());

        InetAddressAndPort joininghost = hosts.get(4);
        UUID joiningId = hostIds.get(4);

        hosts.remove(joininghost);
        hostIds.remove(joiningId);

        // Change a node to a bootstrapping node that is not yet a member of the ring
        Gossiper.instance.injectApplicationState(joininghost, ApplicationState.TOKENS, valueFactory.tokens(tokens));
        ss.onChange(joininghost, ApplicationState.STATUS, valueFactory.bootstrapping(tokens));

        ss.removeNode(joiningId.toString());
    }

    @Test
    public void testRemoveHostId() throws InterruptedException
    {
        // start removal in background and send replication confirmations
        final AtomicBoolean success = new AtomicBoolean(false);
        Thread remover = NamedThreadFactory.createAnonymousThread(() ->
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
        });
        remover.start();

        Thread.sleep(1000); // make sure removal is waiting for confirmation

        assertTrue(tmd.isLeaving(removalhost));
        assertEquals(1, tmd.getSizeOfLeavingEndpoints());

        for (InetAddressAndPort host : hosts)
        {
            Message msg = Message.builder(REPLICATION_DONE_REQ, noPayload)
                                 .from(host)
                                 .build();
            MessagingService.instance().send(msg, FBUtilities.getBroadcastAddressAndPort());
        }

        remover.join();

        assertTrue(success.get());
        assertTrue(tmd.getSizeOfLeavingEndpoints() == 0);
    }
}
