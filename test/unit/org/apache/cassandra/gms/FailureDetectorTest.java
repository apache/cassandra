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

package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailureDetectorTest
{
    @BeforeClass
    public static void setup()
    {
        // slow unit tests can cause problems with FailureDetector's GC pause handling
        System.setProperty("cassandra.max_local_pause_in_ms", "20000");

        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testConvictAfterLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        // we want to convict if there is any heartbeat data present in the FD
        DatabaseDescriptor.setPhiConvictThreshold(0);

        // create a ring of 2 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        InetAddressAndPort leftHost = hosts.get(1);

        IFailureDetector.instance.report(leftHost);

        // trigger handleStateLeft in StorageService
        ss.onChange(leftHost, ApplicationState.STATUS_WITH_PORT,
                    valueFactory.left(Collections.singleton(endpointTokens.get(1)), Gossiper.computeExpireTime()));

        // confirm that handleStateLeft was called and leftEndpoint was removed from TokenMetadata
        assertFalse("Left endpoint not removed from TokenMetadata", tmd.isMember(leftHost));

        // confirm the FD's history for leftHost didn't get wiped by status jump to LEFT
        IFailureDetector.instance.interpret(leftHost);
        assertFalse("Left endpoint not convicted", IFailureDetector.instance.isAlive(leftHost));
    }

    @Test
    public void testConvictAfterReplace() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        // We want to convict if there is any heartbeat data present in the FD
        DatabaseDescriptor.setPhiConvictThreshold(0);

        // Create a ring of 3 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        // Add a new node with old node's tokens
        InetAddressAndPort oldNode = hosts.get(1);
        InetAddressAndPort newNode = InetAddressAndPort.getByName("127.0.0.100");
        Token oldToken = endpointTokens.get(1);
        Gossiper.instance.initializeNodeUnsafe(newNode, UUID.randomUUID(), MessagingService.current_version, 1);
        Gossiper.instance.injectApplicationState(newNode, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(oldToken)));
        ss.onChange(newNode,
                    ApplicationState.STATUS_WITH_PORT,
                    new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(oldToken)));

        // Mark the old node as dead.
        Util.markNodeAsDead(oldNode);

        // Trigger handleStateBootreplacing in StorageService
        ss.onChange(newNode, ApplicationState.STATUS_WITH_PORT,
                    valueFactory.bootReplacingWithPort(oldNode));

        assertEquals("Old node did not replace new node", newNode, tmd.getReplacementNode(oldNode).get());
    }

    @Test
    public void testStateBootReplacingFailsForLiveNode() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        // We want to convict if there is any heartbeat data present in the FD
        DatabaseDescriptor.setPhiConvictThreshold(0);

        // Create a ring of 3 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        // Add a new node with old node's tokens
        InetAddressAndPort oldNode = hosts.get(1);
        InetAddressAndPort newNode = InetAddressAndPort.getByName("127.0.0.100");
        Token token = endpointTokens.get(1);

        Util.joinNodeToRing(newNode, token, partitioner);

        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(oldNode);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(oldNode, endpointState));
        assertTrue(Gossiper.instance.isAlive(oldNode));

        // Trigger handleStateBootreplacing in StorageService
        try
        {
            ss.onChange(newNode, ApplicationState.STATUS_WITH_PORT,
                        valueFactory.bootReplacingWithPort(oldNode));
            fail();
        }
        catch (RuntimeException ex)
        {
            String msg = ex.getMessage();
            final String expected = "trying to replace alive node";
            assertTrue(String.format("Didn't see expected '%s' message", expected), msg.contains(expected));
        }
    }

    @Test
    public void testReplacingLiveNodeFails() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        // We want to convict if there is any heartbeat data present in the FD
        DatabaseDescriptor.setPhiConvictThreshold(0);

        // Create a ring of 3 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 3);

        // Add a new node with old node's tokens
        InetAddressAndPort oldNode = hosts.get(1);
        InetAddressAndPort newNode = InetAddressAndPort.getByName("127.0.0.100");
        Token token = endpointTokens.get(1);

        Gossiper.instance.initializeNodeUnsafe(newNode, UUID.randomUUID(), MessagingService.current_version, 1);
        Gossiper.instance.injectApplicationState(newNode, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));

        // Mark the old node as dead.
        Util.markNodeAsDead(oldNode);

        // Trigger handleStateBootreplacing in StorageService
        ss.onChange(newNode, ApplicationState.STATUS_WITH_PORT,
                    valueFactory.bootReplacingWithPort(oldNode));

        assertEquals("Old node did not replace new node", newNode, tmd.getReplacementNode(oldNode).get());

        // Resurrect old node and mark alive
        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(oldNode);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.realMarkAlive(oldNode, endpointState));

        // Trigger handleStateNormal in StorageService which should fail and cause the old node to still be
        // marked as a live endpoint.
        ss.onChange(newNode,
                    ApplicationState.STATUS_WITH_PORT,
                    new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
        assertTrue("Expected old node to be live but it was removed", Gossiper.instance.liveEndpoints.contains(oldNode));
    }
}
