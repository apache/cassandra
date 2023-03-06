/*
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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LeaveAndBootstrapTest
{
    private static final IPartitioner partitioner = RandomPartitioner.instance;
    private static PartitionerSwitcher partitionerSwitcher;
    private static final String KEYSPACE1 = "LeaveAndBootstrapTestKeyspace1";
    private static final String KEYSPACE2 = "LeaveAndBootstrapTestKeyspace2";
    private static final String KEYSPACE3 = "LeaveAndBootstrapTestKeyspace3";
    private static final String KEYSPACE4 = "LeaveAndBootstrapTestKeyspace4";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        partitionerSwitcher = Util.switchPartitioner(partitioner);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("LeaveAndBootstrapTest");
    }

    @Before
    public void beforeEach()
    {

        ClusterMetadataTestHelper.setInstanceForTest();
    }

    @AfterClass
    public static void tearDown()
    {
        partitionerSwitcher.close();
    }

    /**
     * Test whether write endpoints is correct when the node is leaving. Uses
     * StorageService.onChange and does not manipulate token metadata directly.
     */
    @Test
    public void newTestWriteEndpointsDuringLeave() throws Exception
    {
        final int RING_SIZE = 6;
        final int LEAVING_NODE = 3;

        ClusterMetadata metadata = ClusterMetadata.current();
        Gossiper.instance.clearUnsafe();

        ArrayList<Token> endpointTokens = new ArrayList<>();
        ArrayList<Token> keyTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<>();

        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        Map<Token, List<InetAddressAndPort>> expectedEndpoints = new HashMap<>();
        for (Token token : keyTokens)
        {
            List<InetAddressAndPort> endpoints = new ArrayList<>();
            Iterator<Token> tokenIter = TokenRingUtils.ringIterator(metadata.tokenMap.tokens(), token, false);
            while (tokenIter.hasNext())
            {
                NodeId owner = metadata.tokenMap.owner(tokenIter.next());
                endpoints.add(metadata.directory.endpoint(owner));
            }
            expectedEndpoints.put(token, endpoints);
        }

        // Third node leaves
        ClusterMetadataTestHelper.removeEndpoint(hosts.get(LEAVING_NODE), true);

        AbstractReplicationStrategy strategy;
        for (KeyspaceMetadata ks : ClusterMetadata.current().schema.getKeyspaces())
        {
            strategy = ks.replicationStrategy;
            for (Token token : keyTokens)
            {
                int replicationFactor = strategy.getReplicationFactor().allReplicas;

                Set<InetAddressAndPort> actual = getWriteEndpoints(metadata, ks.params.replication, token).endpoints();
                Set<InetAddressAndPort> expected = new HashSet<>();

                for (int i = 0; i < replicationFactor; i++)
                {
                    expected.add(expectedEndpoints.get(token).get(i));
                }

                // if the leaving node is in the endpoint list,
                // then we should expect it plus one extra for when it's gone
                if (expected.contains(hosts.get(LEAVING_NODE)))
                    expected.add(expectedEndpoints.get(token).get(replicationFactor));

                assertEquals("mismatched endpoint sets", expected, actual);
            }
        }
    }

    /**
     * Test pending ranges and write endpoints when multiple nodes are on the move
     * simultaneously
     */
    @Test
    public void testSimultaneousMove() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 10 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        // nodes 6, 8 and 9 leave
        ClusterMetadataTestHelper.LeaveProcess leave6 = ClusterMetadataTestHelper.lazyLeave(hosts.get(6), true).prepareLeave().startLeave();
        ClusterMetadataTestHelper.LeaveProcess leave8 = ClusterMetadataTestHelper.lazyLeave(hosts.get(8), true).prepareLeave().startLeave();
        ClusterMetadataTestHelper.LeaveProcess leave9 = ClusterMetadataTestHelper.lazyLeave(hosts.get(9), true).prepareLeave().startLeave();

        InetAddressAndPort boot1 = InetAddressAndPort.getByName("127.0.1.1");
        InetAddressAndPort boot2 = InetAddressAndPort.getByName("127.0.1.2");
        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        ClusterMetadataTestHelper.register(boot1, "dc1", "rack1");
        ClusterMetadataTestHelper.JoinProcess join1 = ClusterMetadataTestHelper.lazyJoin(boot1, keyTokens.get(5)).prepareJoin().startJoin();
        ClusterMetadataTestHelper.register(boot2, "dc1", "rack1");
        ClusterMetadataTestHelper.JoinProcess join2 = ClusterMetadataTestHelper.lazyJoin(boot2, keyTokens.get(7)).prepareJoin().startJoin();

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddressAndPort>> expectedEndpoints = new HashMap<String, Multimap<Token, InetAddressAndPort>>();
        expectedEndpoints.put(KEYSPACE1, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE1).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put(KEYSPACE2, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE2).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put(KEYSPACE3, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.2", "127.0.0.1", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.2", "127.0.0.1", "127.0.0.2", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.2", "127.0.0.1", "127.0.0.2", "127.0.1.1", "127.0.0.3"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.1.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.1.2", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get(KEYSPACE3).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.put(KEYSPACE4, HashMultimap.<Token, InetAddressAndPort>create());
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.2", "127.0.0.1", "127.0.1.1"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.1", "127.0.0.2", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.2", "127.0.0.1", "127.0.0.2"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.2", "127.0.0.2", "127.0.0.3"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        expectedEndpoints.get(KEYSPACE4).putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        ClusterMetadata metadata = ClusterMetadata.current();
        for (KeyspaceMetadata ks : metadata.schema.getKeyspaces())
        {
            String keyspaceName = ks.name;
            AbstractReplicationStrategy strategy = ks.replicationStrategy;
            ReplicationParams params = ks.params.replication;
            for (int i = 0; i < keyTokens.size(); i++)
            {
                Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(i)).endpoints();
                assertEquals(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor().allReplicas != 3)
                continue;
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(i)).endpoints();
                assertEquals(3, endpoints.size());
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6, 7 and boot1
            Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(3)).endpoints();
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7, 0, boot1 and boot2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(4)).endpoints();
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 55 should go to nodes 6, 7, 8, 0, 1, boot1 and boot2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(5)).endpoints();
            assertEquals(7, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 7, 8, 9, 0, 1 and boot2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(6)).endpoints();
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0, 1, 2 and boot2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(7)).endpoints();
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 9, 0, 1 and 2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(8)).endpoints();
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 should go to nodes 0, 1 and 2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(9)).endpoints();
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

        }

        // Now finish node 6 and node 9 leaving, as well as boot1 (after this node 8 is still leaving and boot2 in progress
        leave6.finishLeave();
        leave9.finishLeave();
        join1.finishJoin();

        // adjust precalcuated results.  this changes what the epected endpoints are.
        expectedEndpoints.get(KEYSPACE1).get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE1).get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE2).get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE2).get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("15")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("25")).removeAll(makeAddrs("127.0.0.7", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("35")).removeAll(makeAddrs("127.0.0.7", "127.0.0.2"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("45")).removeAll(makeAddrs("127.0.0.7", "127.0.0.10", "127.0.0.3"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.10", "127.0.0.4"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("65")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("75")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE3).get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("35")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("45")).removeAll(makeAddrs("127.0.0.7", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.2", "127.0.0.7"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("65")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("75")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get(KEYSPACE4).get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));

        metadata = ClusterMetadata.current();
        for (KeyspaceMetadata ks : metadata.schema.getKeyspaces())
        {
            String keyspaceName = ks.name;
            AbstractReplicationStrategy strategy = ks.replicationStrategy;
            ReplicationParams params = ks.params.replication;

            for (int i = 0; i < keyTokens.size(); i++)
            {
                Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(i)).endpoints();
                assertEquals(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).containsAll(endpoints));
            }

            if (strategy.getReplicationFactor().allReplicas != 3)
                continue;
            // leave this stuff in to guarantee the old tests work the way they were supposed to.
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(i)).endpoints();
                assertEquals(3, endpoints.size());
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 goes to nodes 4, 5 and boot1
            Collection<InetAddressAndPort> endpoints = getWriteEndpoints(metadata, params, keyTokens.get(3)).endpoints();
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));

            // token 45 goes to nodes 5, boot1 and node7
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(4)).endpoints();
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));

            // token 55 goes to boot1, 7, boot2, 8 and 0
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(5)).endpoints();
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));

            // token 65 goes to nodes 7, boot2, 8, 0 and 1
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(6)).endpoints();
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));

            // token 75 goes to nodes boot2, 8, 0, 1 and 2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(7)).endpoints();
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 85 goes to nodes 0, 1 and 2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(8)).endpoints();
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 goes to nodes 0, 1 and 2
            endpoints = getWriteEndpoints(metadata, params, keyTokens.get(9)).endpoints();
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
        }
    }

    @Ignore // TODO: re-joins are not working properly
    @Test
    public void testStateJumpToBootstrap() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        Gossiper.instance.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, 7);

        // node 2 leaves
        ClusterMetadataTestHelper.LeaveProcess leave2 = ClusterMetadataTestHelper.lazyLeave(hosts.get(2), true).prepareLeave().startLeave();

        // don't bother to test pending ranges here, that is extensively tested by other
        // tests. Just check that the node is in appropriate lists.

        assertTrue(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(2)));
        Set<InetAddressAndPort> leaving = leaving(ClusterMetadata.current());
        assertTrue(leaving.contains(hosts.get(2)));
//        assertTrue(ClusterMetadata.current().directory.allAddresses().getBootstrapTokens().isEmpty());

        leave2.finishLeave();
        // Bootstrap the node immedidiately to keyTokens.get(4) without going through STATE_LEFT
        ClusterMetadataTestHelper.JoinProcess rejoin2 = ClusterMetadataTestHelper.lazyJoin(hosts.get(2), keyTokens.get(4)).prepareJoin().startJoin();

        assertFalse(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(2)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertEquals(hosts.get(2), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(4)));

        // Bootstrap node hosts.get(3) to keyTokens.get(1)
        ClusterMetadataTestHelper.JoinProcess join3 = ClusterMetadataTestHelper.lazyJoin(hosts.get(3), keyTokens.get(1)).prepareJoin().startJoin();

        assertFalse(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(3)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(3)));
        assertEquals(hosts.get(2), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(4)));
        assertEquals(hosts.get(3), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(1)));

        // Bootstrap node hosts.get(2) further to keyTokens.get(3)
        rejoin2.finishJoin();

        assertFalse(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(2)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertEquals(hosts.get(2), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(3)));
        assertNull(bootstrapping(ClusterMetadata.current()).get(keyTokens.get(4)));
        assertEquals(hosts.get(3), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(1)));

        // Go to normal again for both nodes
        join3.finishJoin();

        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(3))));
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(2))));
        ClusterMetadata metadata = ClusterMetadata.current();

        assertTrue(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(2)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertEquals(Collections.singleton(keyTokens.get(3)), getTokens(metadata, hosts.get(2)));
        assertTrue(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(3)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(3)));
        assertEquals(Collections.singleton(keyTokens.get(2)), getTokens(metadata, hosts.get(3)));

        assertTrue(bootstrapping(ClusterMetadata.current()).isEmpty());
    }

    private static Set<InetAddressAndPort> leaving(ClusterMetadata metadata)
    {
        return  metadata.directory.states.entrySet().stream()
                                                .filter(e -> e.getValue() == NodeState.LEAVING)
                                                .map(e -> metadata.directory.endpoint(e.getKey()))
                                                .collect(Collectors.toSet());
    }

    public static Map<Token, InetAddressAndPort> bootstrapping(ClusterMetadata metadata)
    {
        return  metadata.directory.states.entrySet().stream()
                                                .filter(e -> e.getValue() == NodeState.BOOTSTRAPPING)
                                                .collect(Collectors.toMap(e -> metadata.tokenMap.tokens(e.getKey()).iterator().next(),
                                                                          e -> metadata.directory.endpoint(e.getKey())));
    }

    @Ignore // TODO: re-joins are not working properly
    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        Gossiper.instance.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(endpointTokens.get(2))));
        assertTrue(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertEquals(Collections.singleton(endpointTokens.get(2)), getTokens(ClusterMetadata.current(), hosts.get(2)));

        // back to normal
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(2))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(2))));

        assertTrue(ClusterMetadata.current().directory.allAddresses().size() == 0);
        assertEquals(keyTokens.get(2), getTokens(ClusterMetadata.current(), hosts.get(2)));

        // node 3 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(2))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(keyTokens.get(2)), Gossiper.computeExpireTime()));
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(4))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(4))));

        assertTrue(bootstrapping(ClusterMetadata.current()).isEmpty());
        assertTrue(leaving(ClusterMetadata.current()).size() == 0);
        assertEquals(Collections.singleton(keyTokens.get(4)), getTokens(ClusterMetadata.current(), hosts.get(2)));
    }

    @Ignore // TODO: re-joins are not working properly
    @Test
    public void testStateJumpToLeaving() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
//        tmd.clearUnsafe();
        Gossiper.instance.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves with _different_ token
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(0))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(0))));

        assertEquals(Collections.singleton(keyTokens.get(0)), getTokens(ClusterMetadata.current(), hosts.get(2)));
        assertTrue(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertNull(ClusterMetadata.current().tokenMap.owner(endpointTokens.get(2)));

        // go to boostrap
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(1))));
        ss.onChange(hosts.get(2),
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(1))));

        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertEquals(1, bootstrapping(ClusterMetadata.current()).size());
        assertEquals(hosts.get(2), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(1)));

        // jump to leaving again
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(1))));

        assertEquals(hosts.get(2), ClusterMetadata.current().tokenMap.owner((keyTokens.get(1))));
        assertTrue(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
        assertTrue(bootstrapping(ClusterMetadata.current()).isEmpty());

        // go to state left
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(keyTokens.get(1)), Gossiper.computeExpireTime()));

        assertFalse(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(2)));
        assertFalse(leaving(ClusterMetadata.current()).contains(hosts.get(2)));
    }

    @Test
    public void testStateJumpToLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        Gossiper.instance.clearUnsafe();

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring of 6 nodes
        Util.createInitialRing(endpointTokens, keyTokens, hosts, hostIds, 7, false);
        ClusterMetadataTestHelper.addEndpoint(hosts.get(1), keyTokens.get(1));

        ClusterMetadataTestHelper.addEndpoint(hosts.get(2), keyTokens.get(2));
        // node hosts.get(2) goes jumps to left
        ClusterMetadataTestHelper.lazyLeave(hosts.get(2), true).prepareLeave().startLeave();

        // TODO
        // assertFalse(ss.getTokenMetadata().getAllEndpoints().contains(hosts.get(2)));

        // node hosts.get(4) goes to bootstrap
        ClusterMetadataTestHelper.JoinProcess join3 = ClusterMetadataTestHelper.lazyJoin(hosts.get(3), keyTokens.get(3)).prepareJoin().startJoin();

        assertTrue(ClusterMetadata.current().directory.allAddresses().contains(hosts.get(3)));
        assertEquals(1, bootstrapping(ClusterMetadata.current()).size());
        assertEquals(hosts.get(3), bootstrapping(ClusterMetadata.current()).get(keyTokens.get(3)));

        join3.finishJoin();
        assertTrue(bootstrapping(ClusterMetadata.current()).size() == 0);
        //TODO assertFalse(ss.getTokenMetadata().getAllEndpoints().contains(hosts.get(2)));
        //TODO assertFalse(ss.getTokenMetadata().getLeavingEndpoints().contains(hosts.get(2)));
    }

    /**
     * Tests that the system.peers table is not updated after a node has been removed. (See CASSANDRA-6053)
     */
    @Test
    public void testStateChangeOnRemovedNode() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        // create a ring of 2 nodes
        ArrayList<Token> endpointTokens = new ArrayList<>();
        List<InetAddressAndPort> hosts = new ArrayList<>();
        Util.createInitialRing(endpointTokens, new ArrayList<Token>(), hosts, new ArrayList<UUID>(), 2);

        InetAddressAndPort toRemove = hosts.get(1);
        SystemKeyspace.updatePeerInfo(toRemove, "data_center", "dc42");
        SystemKeyspace.updatePeerInfo(toRemove, "rack", "rack42");
        assertEquals("rack42", SystemKeyspace.loadDcRackInfo().get(toRemove).get("rack"));

        // mark the node as removed
        Gossiper.instance.injectApplicationState(toRemove, ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(endpointTokens.get(1)), Gossiper.computeExpireTime()));
        assertTrue(Gossiper.instance.isDeadState(Gossiper.instance.getEndpointStateForEndpoint(hosts.get(1))));

        // state changes made after the endpoint has left should be ignored
        ss.onChange(hosts.get(1), ApplicationState.RACK,
                valueFactory.rack("rack9999"));
        assertEquals("rack42", SystemKeyspace.loadDcRackInfo().get(toRemove).get("rack"));
    }

    @Test
    public void testRemovingStatusForNonMember()  throws UnknownHostException
    {
        // create a ring of 1 node
        StorageService ss = StorageService.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        Util.createInitialRing(new ArrayList<Token>(), new ArrayList<Token>(), new ArrayList<InetAddressAndPort>(), new ArrayList<UUID>(), 1);

        // make a REMOVING state change on a non-member endpoint; without the CASSANDRA-6564 fix, this
        // would result in an ArrayIndexOutOfBoundsException
        ss.onChange(InetAddressAndPort.getByName("192.168.1.42"), ApplicationState.STATUS_WITH_PORT, valueFactory.removingNonlocal(UUID.randomUUID()));
        ss.onChange(InetAddressAndPort.getByName("192.168.1.42"), ApplicationState.STATUS, valueFactory.removingNonlocal(UUID.randomUUID()));
    }

    private static Collection<InetAddressAndPort> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddressAndPort> addrs = new ArrayList<>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddressAndPort.getByName(host));
        return addrs;
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKeyspaceMetadata(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                keyspaceName,
                ksmd.params.replication.klass,
                ksmd.params.replication.options);
    }

    public static EndpointsForToken getWriteEndpoints(ClusterMetadata metadata, ReplicationParams replicationParams, Token token)
    {
        return metadata.placements.get(replicationParams).writes.forToken(token);
    }

    public static Collection<Token> getTokens(ClusterMetadata metadata, InetAddressAndPort endpoint)
    {
        return metadata.tokenMap.tokens(metadata.directory.peerId(endpoint));
    }

}
