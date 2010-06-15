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

import org.junit.Test;

import static org.junit.Assert.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.RackUnawareStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

public class MoveTest extends CleanupHelper
{
    // handy way of creating a mapping of strategies to use in StorageService.
    private static Map<String, AbstractReplicationStrategy> createReplacements(AbstractReplicationStrategy strat)
    {
        Map<String, AbstractReplicationStrategy> replacements = new HashMap<String, AbstractReplicationStrategy>();
        for (String table : DatabaseDescriptor.getTables())
            replacements.put(table, strat);
        return replacements;
    }


    /**
     * Test whether write endpoints is correct when the node is leaving. Uses
     * StorageService.onChange and does not manipulate token metadata directly.
     */
    @Test
    public void testWriteEndpointsDuringLeave() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 5;
        final int LEAVING_NODE = 2;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategies = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, RING_SIZE);

        final Map<String, List<Range>> deadNodesRanges = new HashMap<String, List<Range>>();
        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            List<Range> list = new ArrayList<Range>();
            list.addAll(testStrategy.getAddressRanges(table).get(hosts.get(LEAVING_NODE)));
            Collections.sort(list);
            deadNodesRanges.put(table, list);
        }
        
        // Third node leaves
        ss.onChange(hosts.get(LEAVING_NODE), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(LEAVING_NODE))));

        // check that it is correctly marked as leaving in tmd
        assertTrue(tmd.isLeaving(hosts.get(LEAVING_NODE)));

        // check that pending ranges are correct (primary range should go to 1st node, first
        // replica range to 4th node and 2nd replica range to 5th node)
        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            int replicationFactor = DatabaseDescriptor.getReplicationFactor(table);

            // if the ring minus the leaving node leaves us with less than RF, we're hosed.
            if (hosts.size()-1 < replicationFactor)
                continue;
            
            // verify that the replicationFactor nodes after the leaving node are gatherings it's pending ranges.
            // in the case where rf==5, we're screwed because we basically just lost data.
            for (int i = 0; i < replicationFactor; i++)
            {
                assertTrue(tmd.getPendingRanges(table, hosts.get((LEAVING_NODE + 1 + i) % RING_SIZE)).size() > 0);
                assertEquals(tmd.getPendingRanges(table, hosts.get((LEAVING_NODE + 1 + i) % RING_SIZE)).get(0), deadNodesRanges.get(table).get(i));
            }

            // note that we're iterating over nodes and sample tokens.
            final int replicaStart = (LEAVING_NODE-replicationFactor+RING_SIZE)%RING_SIZE;
            for (int i=0; i<keyTokens.size(); ++i)
            {
                Collection<InetAddress> endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, testStrategy.getNaturalEndpoints(keyTokens.get(i), table));
                // figure out if this node is one of the nodes previous to the failed node (2).
                boolean isReplica = (i - replicaStart + RING_SIZE) % RING_SIZE < replicationFactor;
                // the preceeding leaving_node-replication_factor nodes should have and additional ep (replication_factor+1);
                if (isReplica)
                    assertTrue(endpoints.size() == replicationFactor + 1);
                else
                    assertTrue(endpoints.size() == replicationFactor);

            }
        }

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategies);
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
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategy = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 10 nodes
        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, RING_SIZE);

        // nodes 6, 8 and 9 leave
        final int[] LEAVING = new int[] { 6, 8, 9};
        for (int leaving : LEAVING)
            ss.onChange(hosts.get(leaving), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(leaving))));
        
        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        ss.onChange(boot1, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(5))));
        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
        ss.onChange(boot2, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(7))));

        Collection<InetAddress> endpoints = null;

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

        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, testStrategy.getNaturalEndpoints(keyTokens.get(i), table));
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).size() == endpoints.size());
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (DatabaseDescriptor.getReplicationFactor(table) != 3)
                continue;
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, testStrategy.getNaturalEndpoints(keyTokens.get(i), table));
                assertTrue(endpoints.size() == 3);
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6, 7 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), table, testStrategy.getNaturalEndpoints(keyTokens.get(3), table));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7, 0, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), table, testStrategy.getNaturalEndpoints(keyTokens.get(4), table));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 55 should go to nodes 6, 7, 8, 0, 1, boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), table, testStrategy.getNaturalEndpoints(keyTokens.get(5), table));
            assertTrue(endpoints.size() == 7);
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 7, 8, 9, 0, 1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), table, testStrategy.getNaturalEndpoints(keyTokens.get(6), table));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0, 1, 2 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), table, testStrategy.getNaturalEndpoints(keyTokens.get(7), table));
            assertTrue(endpoints.size() == 6);
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 9, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), table, testStrategy.getNaturalEndpoints(keyTokens.get(8), table));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 should go to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), table, testStrategy.getNaturalEndpoints(keyTokens.get(9), table));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

        }

        // Now finish node 6 and node 9 leaving, as well as boot1 (after this node 8 is still
        // leaving and boot2 in progress
        ss.onChange(hosts.get(LEAVING[0]), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(LEAVING[0]))));
        ss.onChange(hosts.get(LEAVING[2]), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(LEAVING[2]))));
        ss.onChange(boot1, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(5))));

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

        for (String table : DatabaseDescriptor.getNonSystemTables())
        {
            for (int i = 0; i < keyTokens.size(); i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, testStrategy.getNaturalEndpoints(keyTokens.get(i), table));
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).size() == endpoints.size());
                assertTrue(expectedEndpoints.get(table).get(keyTokens.get(i)).containsAll(endpoints));
            }

            if (DatabaseDescriptor.getReplicationFactor(table) != 3)
                continue;
            // leave this stuff in to guarantee the old tests work the way they were supposed to.
            // tokens 5, 15 and 25 should go three nodes
            for (int i=0; i<3; ++i)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, testStrategy.getNaturalEndpoints(keyTokens.get(i), table));
                assertTrue(endpoints.size() == 3);
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 goes to nodes 4, 5 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), table, testStrategy.getNaturalEndpoints(keyTokens.get(3), table));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));

            // token 45 goes to nodes 5, boot1 and node7
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), table, testStrategy.getNaturalEndpoints(keyTokens.get(4), table));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));

            // token 55 goes to boot1, 7, boot2, 8 and 0
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), table, testStrategy.getNaturalEndpoints(keyTokens.get(5), table));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));

            // token 65 goes to nodes 7, boot2, 8, 0 and 1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), table, testStrategy.getNaturalEndpoints(keyTokens.get(6), table));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));

            // token 75 goes to nodes boot2, 8, 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), table, testStrategy.getNaturalEndpoints(keyTokens.get(7), table));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(boot2));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 85 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), table, testStrategy.getNaturalEndpoints(keyTokens.get(8), table));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));

            // token 95 goes to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), table, testStrategy.getNaturalEndpoints(keyTokens.get(9), table));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
        }

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToBootstrap() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategy = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 5);

        // node 2 leaves
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(2))));

        // don't bother to test pending ranges here, that is extensively tested by other
        // tests. Just check that the node is in appropriate lists.
        assertTrue(tmd.isMember(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // Bootstrap the node immedidiately to keyTokens.get(4) without going through STATE_LEFT
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(4))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));

        // Bootstrap node hosts.get(3) to keyTokens.get(1)
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Bootstrap node hosts.get(2) further to keyTokens.get(3)
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(3))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(3)).equals(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(4)) == null);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // Go to normal again for both nodes
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(3))));
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));

        assertTrue(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(3)));
        assertTrue(tmd.isMember(hosts.get(3)));
        assertFalse(tmd.isLeaving(hosts.get(3)));
        assertTrue(tmd.getToken(hosts.get(3)).equals(keyTokens.get(2)));

        assertTrue(tmd.getBootstrapTokens().isEmpty());

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToNormal() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategy = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 5);

        // node 2 leaves
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(2))));

        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(endpointTokens.get(2)));

        // back to normal
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));

        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(2)));

        // node 3 goes through leave and left and then jumps to normal
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(2))));
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(4))));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getLeavingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(4)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToLeaving() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategy = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 5);

        // node 2 leaves with _different_ token
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(0))));

        assertTrue(tmd.getToken(hosts.get(2)).equals(keyTokens.get(0)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getEndpoint(endpointTokens.get(2)) == null);

        // go to boostrap
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(2)));

        // jump to leaving again
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEAVING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertTrue(tmd.getEndpoint(keyTokens.get(1)).equals(hosts.get(2)));
        assertTrue(tmd.isLeaving(hosts.get(2)));
        assertTrue(tmd.getBootstrapTokens().isEmpty());

        // go to state left
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    @Test
    public void testStateJumpToLeft() throws UnknownHostException
    {
        StorageService ss = StorageService.instance;
        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();
        AbstractReplicationStrategy testStrategy = new RackUnawareStrategy(tmd, new SimpleSnitch());

        IPartitioner oldPartitioner = ss.setPartitionerUnsafe(partitioner);
        Map<String, AbstractReplicationStrategy> oldStrategy = ss.setReplicationStrategyUnsafe(createReplacements(testStrategy));

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        // create a ring or 5 nodes
        createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 5);

        // node hosts.get(2) goes jumps to left
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(2))));

        assertFalse(tmd.isMember(hosts.get(2)));

        // node hosts.get(4) goes to bootstrap
        ss.onChange(hosts.get(3), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_BOOTSTRAPPING + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertFalse(tmd.isMember(hosts.get(3)));
        assertTrue(tmd.getBootstrapTokens().size() == 1);
        assertTrue(tmd.getBootstrapTokens().get(keyTokens.get(1)).equals(hosts.get(3)));

        // and then directly to 'left'
        ss.onChange(hosts.get(2), StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_LEFT + StorageService.Delimiter + StorageService.LEFT_NORMALLY + StorageService.Delimiter + partitioner.getTokenFactory().toString(keyTokens.get(1))));

        assertTrue(tmd.getBootstrapTokens().size() == 0);
        assertFalse(tmd.isMember(hosts.get(2)));
        assertFalse(tmd.isLeaving(hosts.get(2)));

        ss.setPartitionerUnsafe(oldPartitioner);
        ss.setReplicationStrategyUnsafe(oldStrategy);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    private void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endpointTokens,
                                   List<Token> keyTokens, List<InetAddress> hosts, int howMany)
        throws UnknownHostException
    {
        for (int i=0; i<howMany; i++)
        {
            endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
        }

        for (int i=0; i<endpointTokens.size(); i++)
        {
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            ss.onChange(ep, StorageService.MOVE_STATE, new ApplicationState(StorageService.STATE_NORMAL + StorageService.Delimiter + partitioner.getTokenFactory().toString(endpointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

    private static Collection<InetAddress> makeAddrs(String... hosts) throws UnknownHostException
    {
        ArrayList<InetAddress> addrs = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
            addrs.add(InetAddress.getByName(host));
        return addrs;
    }
}
