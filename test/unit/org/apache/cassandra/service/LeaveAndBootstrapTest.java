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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.Schema;
import org.junit.Test;

import static org.junit.Assert.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

public class LeaveAndBootstrapTest extends CleanupHelper
{
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
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, RING_SIZE);

        Map<Token, List<InetAddress>> expectedEndpoints = new HashMap<Token, List<InetAddress>>();
        for (String table : Schema.instance.getNonSystemTables())
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
                valueFactory.leaving(endpointTokens.get(LEAVING_NODE)));
        assertTrue(tmd.isLeaving(hosts.get(LEAVING_NODE)));

        AbstractReplicationStrategy strategy;
        for (String table : Schema.instance.getNonSystemTables())
        {
            strategy = getStrategy(table, tmd);
            for (Token token : keyTokens)
            {
                int replicationFactor = strategy.getReplicationFactor();

                HashSet<InetAddress> actual = new HashSet<InetAddress>(tmd.getWriteEndpoints(token, table, strategy.calculateNaturalEndpoints(token, tmd)));
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

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    /**
     * Test pending ranges and write endpoints when multiple nodes are on the move
     * simultaneously
     */
    @Test
    public void testSimultaneousMove() throws UnknownHostException, ConfigurationException
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 10 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, RING_SIZE);

        // nodes 6, 8 and 9 leave
        final int[] LEAVING = new int[] {6, 8, 9};
        for (int leaving : LEAVING)
            ss.onChange(hosts.get(leaving), ApplicationState.STATUS, valueFactory.leaving(endpointTokens.get(leaving)));

        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        ss.onChange(boot1, ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(5)));
        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
        ss.onChange(boot2, ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(7)));

        Collection<InetAddress> endpoints = null;

        /* don't require test update every time a new keyspace is added to test/conf/cassandra.yaml */
        Map<String, AbstractReplicationStrategy> tableStrategyMap = new HashMap<String, AbstractReplicationStrategy>();
        for (int i=1; i<=4; i++)
        {
            tableStrategyMap.put("Keyspace" + i, getStrategy("Keyspace" + i, tmd));
        }

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddress>> expectedEndpoints = new HashMap<String, Multimap<Token, InetAddress>>();
        expectedEndpoints.put("Keyspace1", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put("Keyspace2", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put("Keyspace3", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1", "127.0.0.8"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.2", "127.0.0.1", "127.0.1.1"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.2", "127.0.0.1", "127.0.0.2", "127.0.1.1"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.2", "127.0.0.1", "127.0.0.2", "127.0.1.1", "127.0.0.3"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.1.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.1.2", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.put("Keyspace4", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1", "127.0.0.8"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.2", "127.0.0.1", "127.0.1.1"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.1", "127.0.0.2", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.2", "127.0.0.1", "127.0.0.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.2", "127.0.0.2", "127.0.0.3"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        for (Map.Entry<String, AbstractReplicationStrategy> tableStrategy : tableStrategyMap.entrySet())
        {
            String table = tableStrategy.getKey();
            AbstractReplicationStrategy strategy = tableStrategy.getValue();

            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).size() == endpoints.size());
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor() != 3)
                continue;
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertTrue(endpoints.size() == 3);
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6, 7 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), table, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7, 0, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), table, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 55 should go to nodes 6, 7, 8, 0, 1, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), table, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertTrue(endpoints.size() == 7);
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 7, 8, 9, 0, 1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), table, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0, 1, 2 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), table, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 9, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), table, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 should go to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), table, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

        }

        // Now finish node 6 and node 9 leaving, as well as boot1 (after this node 8 is still
        // leaving and boot2 in progress
        ss.onChange(hosts.get(LEAVING[0]), ApplicationState.STATUS,
                valueFactory.left(endpointTokens.get(LEAVING[0]), Gossiper.computeExpireTime()));
        ss.onChange(hosts.get(LEAVING[2]), ApplicationState.STATUS,
                valueFactory.left(endpointTokens.get(LEAVING[2]), Gossiper.computeExpireTime()));
        ss.onChange(boot1, ApplicationState.STATUS, valueFactory.normal(keyTokens.get(5)));

        // adjust precalcuated results.  this changes what the epected endpoints are.
        expectedEndpoints.get("Keyspace1").get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get("Keyspace1").get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace2").get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get("Keyspace2").get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("15")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("25")).removeAll(makeAddrs("127.0.0.7", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("35")).removeAll(makeAddrs("127.0.0.7", "127.0.0.2"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("45")).removeAll(makeAddrs("127.0.0.7", "127.0.0.10", "127.0.0.3"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.7", "127.0.0.10", "127.0.0.4"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("65")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("75")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace3").get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("35")).removeAll(makeAddrs("127.0.0.7", "127.0.0.8"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("45")).removeAll(makeAddrs("127.0.0.7", "127.0.1.2", "127.0.0.1"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("55")).removeAll(makeAddrs("127.0.0.2", "127.0.0.7"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("65")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("75")).removeAll(makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace4").get(new BigIntegerToken("85")).removeAll(makeAddrs("127.0.0.10"));

        for (Map.Entry<String, AbstractReplicationStrategy> tableStrategy : tableStrategyMap.entrySet())
        {
            String table = tableStrategy.getKey();
            AbstractReplicationStrategy strategy = tableStrategy.getValue();

            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).size() == endpoints.size());
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).containsAll(endpoints));
            }

            if (strategy.getReplicationFactor() != 3)
                continue;
            // leave this stuff in to guarantee the old tests work the way they were supposed to.
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertTrue(endpoints.size() == 3);
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 goes to nodes 4, 5 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), table, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));

            // token 45 goes to nodes 5, boot1 and node7
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), table, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));

            // token 55 goes to boot1, 7, boot2, 8 and 0
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), table, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));

            // token 65 goes to nodes 7, boot2, 8, 0 and 1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), table, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));

            // token 75 goes to nodes boot2, 8, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), table, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 85 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), table, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), table, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
        }

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testStateJumpToBootstrap() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 7);

        // node 2 leaves
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(endpointTokens.get(2)));

        // don't bother to test pending ranges here, that is extensively tested by other
        // tests. Just check that the node is in appropriate lists.
        assertTrue(tmd.isMember(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // Bootstrap the node immedidiately to keyTokens.get(4) without going through STATE_LEFT
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(4)));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));

        // Bootstrap node hosts.get(3) to keyTokens.get(1)
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(1)));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Bootstrap node hosts.get(2) further to keyTokens.get(3)
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(3)));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(3)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)) == null);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Go to normal again for both nodes
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(keyTokens.get(3)));
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.normal(keyTokens.get(2)));

        assertTrue(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(3)));
        assertTrue(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getToken(hosts.get(3)).equals(keyTokens.get(2)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 6);

        // node 2 leaves
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(endpointTokens.get(2)));

        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(endpointTokens.get(2)));

        // back to normal
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(keyTokens.get(2)));

        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(2)));

        // node 3 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(keyTokens.get(2)));
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(keyTokens.get(2), Gossiper.computeExpireTime()));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(keyTokens.get(4)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(4)));

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testStateJumpToLeaving() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 6);

        // node 2 leaves with _different_ token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(keyTokens.get(0)));

        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(0)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getEndpoint(endpointTokens.get(2)) == null);

        // go to boostrap
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(1)));

        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(2)));

        // jump to leaving again
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.leaving(keyTokens.get(1)));

        assertTrue(tmd.getEndpoint(keyTokens.get(1)).equals(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // go to state left
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(keyTokens.get(1), Gossiper.computeExpireTime()));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    @Test
    public void testStateJumpToLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(partitioner);

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring of 6 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 7);

        // node hosts.get(2) goes jumps to left
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(endpointTokens.get(2), Gossiper.computeExpireTime()));

        assertFalse(tmd.isMember(hosts.get(2)));

        // node hosts.get(4) goes to bootstrap
        ss.onChange(hosts.get(3), ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(1)));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // and then directly to 'left'
        ss.onChange(hosts.get(2), ApplicationState.STATUS,
                valueFactory.left(keyTokens.get(1), Gossiper.computeExpireTime()));

        assertTrue(tmd.getBootstrapTokens().size() == 0);
        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
    }

    private static Collection<InetAddress> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddress> addrs = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddress.getByName(host));
        return addrs;
    }

    private AbstractReplicationStrategy getStrategy(String table, TokenMetadata tmd) throws ConfigurationException
    {
        KSMetaData ksmd = Schema.instance.getKSMetaData(table);
        return AbstractReplicationStrategy.createReplicationStrategy(
                table,
                ksmd.strategyClass,
                tmd,
                new SimpleSnitch(),
                ksmd.strategyOptions);
    }

}
