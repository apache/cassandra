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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.Util.PartitionerSwitcher;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;

import static org.junit.Assert.*;

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
        partitionerSwitcher = Util.switchPartitioner(partitioner);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition("LeaveAndBootstrapTest");
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
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 6;
        final int LEAVING_NODE = 3;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        Map<Token, List<InetAddress>> expectedEndpoints = new HashMap<Token, List<InetAddress>>();
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (Token token : keyTokens)
            {
                List<InetAddress> endpoints = new ArrayList<InetAddress>();
                Iterator<Token> tokenIter = TokenMetadata.ringIterator(tmd.sortedTokens(), token, false);
                while (tokenIter.hasNext())
                {
                    endpoints.add(tmd.getEndpoint(tokenIter.next()));
                }
                expectedEndpoints.put(token, endpoints);
            }
        }

        // Third node leaves
        ss.onChange(hosts.get(LEAVING_NODE),
                ApplicationState.STATUS,
                valueFactory.leaving(Collections.singleton(endpointTokens.get(LEAVING_NODE))));
        assertTrue(tmd.isLeaving(hosts.get(LEAVING_NODE)));

        Thread.sleep(100); // because there is a tight race between submit and blockUntilFinished
        PendingRangeCalculatorService.instance.blockUntilFinished();

        AbstractReplicationStrategy strategy;
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            strategy = getStrategy(keyspaceName, tmd);
            for (Token token : keyTokens)
            {
                int replicationFactor = strategy.getReplicationFactor();

                HashSet<InetAddress> actual = new HashSet<InetAddress>(tmd.getWriteEndpoints(token, keyspaceName, strategy.calculateNaturalEndpoints(token, tmd.cloneOnlyTokenMap())));
                HashSet<InetAddress> expected = new HashSet<InetAddress>();

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
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 10 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, RING_SIZE);

        // nodes 6, 8 and 9 leave
        final int[] LEAVING = new int[] {6, 8, 9};
        for (int leaving : LEAVING)
            ss.onChange(hosts.get(leaving),
                        ApplicationState.STATUS,
                        valueFactory.leaving(Collections.singleton(endpointTokens.get(leaving))));

        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        Gossiper.instance.initializeNodeUnsafe(boot1, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(boot1, ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(5))));
        ss.onChange(boot1,
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(5))));
        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
        Gossiper.instance.initializeNodeUnsafe(boot2, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(boot2, ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(7))));
        ss.onChange(boot2,
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(7))));

        Collection<InetAddress> endpoints = null;

        /* don't require test update every time a new keyspace is added to test/conf/cassandra.yaml */
        Map<String, AbstractReplicationStrategy> keyspaceStrategyMap = new HashMap<String, AbstractReplicationStrategy>();
        for (int i=1; i<=4; i++)
        {
            keyspaceStrategyMap.put("LeaveAndBootstrapTestKeyspace" + i, getStrategy("LeaveAndBootstrapTestKeyspace" + i, tmd));
        }

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddress>> expectedEndpoints = new HashMap<String, Multimap<Token, InetAddress>>();
        expectedEndpoints.put(KEYSPACE1, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(KEYSPACE2, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(KEYSPACE3, HashMultimap.<Token, InetAddress>create());
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
        expectedEndpoints.put(KEYSPACE4, HashMultimap.<Token, InetAddress>create());
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

        PendingRangeCalculatorService.instance.blockUntilFinished();

        for (Map.Entry<String, AbstractReplicationStrategy> keyspaceStrategy : keyspaceStrategyMap.entrySet())
        {
            String keyspaceName = keyspaceStrategy.getKey();
            AbstractReplicationStrategy strategy = keyspaceStrategy.getValue();

            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertEquals(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor() != 3)
                continue;
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertEquals(3, endpoints.size());
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6, 7 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7, 0, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 55 should go to nodes 6, 7, 8, 0, 1, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertEquals(7, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 7, 8, 9, 0, 1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0, 1, 2 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertEquals(6, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 9, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertEquals(4, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 should go to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

        }

        // Now finish node 6 and node 9 leaving, as well as boot1 (after this node 8 is still
        // leaving and boot2 in progress
        ss.onChange(hosts.get(LEAVING[0]), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(endpointTokens.get(LEAVING[0])), Gossiper.computeExpireTime()));
        ss.onChange(hosts.get(LEAVING[2]), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(endpointTokens.get(LEAVING[2])), Gossiper.computeExpireTime()));
        ss.onChange(boot1, ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(5))));

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

        PendingRangeCalculatorService.instance.blockUntilFinished();

        for (Map.Entry<String, AbstractReplicationStrategy> keyspaceStrategy : keyspaceStrategyMap.entrySet())
        {
            String keyspaceName = keyspaceStrategy.getKey();
            AbstractReplicationStrategy strategy = keyspaceStrategy.getValue();

            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertEquals(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).size(), endpoints.size());
                assertTrue(expectedEndpoints.get(keyspaceName).get(keyTokens.get(i)).containsAll(endpoints));
            }

            if (strategy.getReplicationFactor() != 3)
                continue;
            // leave this stuff in to guarantee the old tests work the way they were supposed to.
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertEquals(3, endpoints.size());
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 goes to nodes 4, 5 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));

            // token 45 goes to nodes 5, boot1 and node7
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));

            // token 55 goes to boot1, 7, boot2, 8 and 0
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));

            // token 65 goes to nodes 7, boot2, 8, 0 and 1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));

            // token 75 goes to nodes boot2, 8, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertEquals(5, endpoints.size());
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 85 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), keyspaceName, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertEquals(3, endpoints.size());
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
        }
    }

    @Test
    public void testStateJumpToBootstrap() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 7);

        // node 2 leaves
        ss.onChange(hosts.get(2),
                    ApplicationState.STATUS,
                    valueFactory.leaving(Collections.singleton(endpointTokens.get(2))));

        // don't bother to test pending ranges here, that is extensively tested by other
        // tests. Just check that the node is in appropriate lists.
        assertTrue(tmd.isMember(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // Bootstrap the node immedidiately to keyTokens.get(4) without going through STATE_LEFT
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(4))));
        ss.onChange(hosts.get(2),
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(4))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertEquals(hosts.get(2), tmd.getBootstrapTokens().get(keyTokens.get(4)));

        // Bootstrap node hosts.get(3) to keyTokens.get(1)
        Gossiper.instance.injectApplicationState(hosts.get(3), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(1))));
        ss.onChange(hosts.get(3),
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertEquals(hosts.get(2), tmd.getBootstrapTokens().get(keyTokens.get(4)));
        assertEquals(hosts.get(3), tmd.getBootstrapTokens().get(keyTokens.get(1)));

        // Bootstrap node hosts.get(2) further to keyTokens.get(3)
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(3))));
        ss.onChange(hosts.get(2),
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(3))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertEquals(hosts.get(2), tmd.getBootstrapTokens().get(keyTokens.get(3)));
        assertNull(tmd.getBootstrapTokens().get(keyTokens.get(4)));
        assertEquals(hosts.get(3), tmd.getBootstrapTokens().get(keyTokens.get(1)));

        // Go to normal again for both nodes
        Gossiper.instance.injectApplicationState(hosts.get(3), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(2))));
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(3))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(3))));
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(2))));

        assertTrue(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertEquals(keyTokens.get(3), tmd.getToken(hosts.get(2)));
        assertTrue(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertEquals(keyTokens.get(2), tmd.getToken(hosts.get(3)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
    }

    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(endpointTokens.get(2))));

        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertEquals(endpointTokens.get(2), tmd.getToken(hosts.get(2)));

        // back to normal
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(2))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(2))));

        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertEquals(keyTokens.get(2), tmd.getToken(hosts.get(2)));

        // node 3 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(2))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(keyTokens.get(2)), Gossiper.computeExpireTime()));
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(4))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(Collections.singleton(keyTokens.get(4))));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertEquals(keyTokens.get(4), tmd.getToken(hosts.get(2)));
    }

    @Test
    public void testStateJumpToLeaving() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 6);

        // node 2 leaves with _different_ token
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(0))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(0))));

        assertEquals(keyTokens.get(0), tmd.getToken(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertNull(tmd.getEndpoint(endpointTokens.get(2)));

        // go to boostrap
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(1))));
        ss.onChange(hosts.get(2),
                    ApplicationState.STATUS,
                    valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(1))));

        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertEquals(1, tmd.getBootstrapTokens().size());
        assertEquals(hosts.get(2), tmd.getBootstrapTokens().get(keyTokens.get(1)));

        // jump to leaving again
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(Collections.singleton(keyTokens.get(1))));

        assertEquals(hosts.get(2), tmd.getEndpoint(keyTokens.get(1)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // go to state left
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(keyTokens.get(1)), Gossiper.computeExpireTime()));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
    }

    @Test
    public void testStateJumpToLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = RandomPartitioner.instance;
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();
        List<UUID> hostIds = new ArrayList<UUID>();

        // create a ring of 6 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 7);

        // node hosts.get(2) goes jumps to left
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(endpointTokens.get(2)), Gossiper.computeExpireTime()));

        assertFalse(tmd.isMember(hosts.get(2)));

        // node hosts.get(4) goes to bootstrap
        Gossiper.instance.injectApplicationState(hosts.get(3), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(1))));
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.bootstrapping(Collections.<Token>singleton(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertEquals(1, tmd.getBootstrapTokens().size());
        assertEquals(hosts.get(3), tmd.getBootstrapTokens().get(keyTokens.get(1)));

        // and then directly to 'left'
        Gossiper.instance.injectApplicationState(hosts.get(2), ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(keyTokens.get(1))));
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(Collections.singleton(keyTokens.get(1)), Gossiper.computeExpireTime()));

        assertTrue(tmd.getBootstrapTokens().size() == 0);
        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
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
        List<InetAddress> hosts = new ArrayList<>();
        Util.createInitialRing(ss, partitioner, endpointTokens, new ArrayList<Token>(), hosts, new ArrayList<UUID>(), 2);

        InetAddress toRemove = hosts.get(1);
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
        Util.createInitialRing(ss, partitioner, new ArrayList<Token>(), new ArrayList<Token>(), new ArrayList<InetAddress>(), new ArrayList<UUID>(), 1);

        // make a REMOVING state change on a non-member endpoint; without the CASSANDRA-6564 fix, this
        // would result in an ArrayIndexOutOfBoundsException
        ss.onChange(InetAddress.getByName("192.168.1.42"), ApplicationState.STATUS, valueFactory.removingNonlocal(UUID.randomUUID()));
    }

    private static Collection<InetAddress> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddress> addrs = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddress.getByName(host));
        return addrs;
    }

    private AbstractReplicationStrategy getStrategy(String keyspaceName, TokenMetadata tmd)
    {
        KeyspaceMetadata ksmd = Schema.instance.getKSMetaData(keyspaceName);
        return AbstractReplicationStrategy.createReplicationStrategy(
                keyspaceName,
                ksmd.params.replication.klass,
                tmd,
                new SimpleSnitch(),
                ksmd.params.replication.options);
    }

}
