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
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;

public class MoveTest extends CleanupHelper
{
    /*
     * Test whether write endpoints is correct when the node is moving. Uses
     * StorageService.onChange and does not manipulate token metadata directly.
     */
    @Test
    public void newTestWriteEndpointsDuringMove() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 10;
        final int MOVING_NODE = 3; // index of the moving node

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

        // node LEAVING_NODE should move to this token
        Token newToken = positionToken(MOVING_NODE);

        // Third node leaves
        ss.onChange(hosts.get(MOVING_NODE), ApplicationState.STATUS, valueFactory.moving(newToken));

        assertTrue(tmd.isMoving(hosts.get(MOVING_NODE)));

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

                assertEquals("mismatched endpoint sets", expected, actual);
            }
        }

        // moving endpoint back to the normal state
        ss.onChange(hosts.get(MOVING_NODE), ApplicationState.STATUS, valueFactory.normal(newToken));
        ss.setPartitionerUnsafe(oldPartitioner);
    }

    /*
     * Test ranges and write endpoints when multiple nodes are on the move simultaneously
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
        final int[] MOVING = new int[] {6, 8, 9};

        Map<Integer, Token> newTokens = new HashMap<Integer, Token>();

        for (int movingIndex : MOVING)
        {
            Token newToken = positionToken(movingIndex);
            ss.onChange(hosts.get(movingIndex), ApplicationState.STATUS, valueFactory.moving(newToken));

            // storing token associated with a node index
            newTokens.put(movingIndex, newToken);
        }

        Collection<InetAddress> endpoints;

        tmd = tmd.cloneAfterAllSettled();
        ss.setTokenMetadataUnsafe(tmd);

        // boot two new nodes with keyTokens.get(5) and keyTokens.get(7)
        InetAddress boot1 = InetAddress.getByName("127.0.1.1");
        ss.onChange(boot1, ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(5)));
        InetAddress boot2 = InetAddress.getByName("127.0.1.2");
        ss.onChange(boot2, ApplicationState.STATUS, valueFactory.bootstrapping(keyTokens.get(7)));

        // don't require test update every time a new keyspace is added to test/conf/cassandra.yaml
        Map<String, AbstractReplicationStrategy> tableStrategyMap = new HashMap<String, AbstractReplicationStrategy>();
        for (int i = 1; i <= 4; i++)
        {
            tableStrategyMap.put("Keyspace" + i, getStrategy("Keyspace" + i, tmd));
        }

       /**
        *  Keyspace1 & Keyspace2 RF=1
        *  {
        *      /127.0.0.1=[(92,0]],
        *      /127.0.0.2=[(0,10]],
        *      /127.0.0.3=[(10,20]],
        *      /127.0.0.4=[(20,30]],
        *      /127.0.0.5=[(30,40]],
        *      /127.0.0.6=[(40,50]],
        *      /127.0.0.7=[(50,62]],
        *      /127.0.0.8=[(62,70]],
        *      /127.0.0.9=[(70,82]],
        *      /127.0.0.10=[(82,92]]
        *  }
        */

        Multimap<InetAddress, Range> keyspace1ranges = tableStrategyMap.get("Keyspace1").getAddressRanges();
        Collection<Range> ranges1 = keyspace1ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(collectionSize(ranges1), 1);
        assertTrue(ranges1.iterator().next().equals(generateRange(92, 0)));
        Collection<Range> ranges2 = keyspace1ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(collectionSize(ranges2), 1);
        assertTrue(ranges2.iterator().next().equals(generateRange(0, 10)));
        Collection<Range> ranges3 = keyspace1ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(collectionSize(ranges3), 1);
        assertTrue(ranges3.iterator().next().equals(generateRange(10, 20)));
        Collection<Range> ranges4 = keyspace1ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(collectionSize(ranges4), 1);
        assertTrue(ranges4.iterator().next().equals(generateRange(20, 30)));
        Collection<Range> ranges5 = keyspace1ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(collectionSize(ranges5), 1);
        assertTrue(ranges5.iterator().next().equals(generateRange(30, 40)));
        Collection<Range> ranges6 = keyspace1ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(collectionSize(ranges6), 1);
        assertTrue(ranges6.iterator().next().equals(generateRange(40, 50)));
        Collection<Range> ranges7 = keyspace1ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(collectionSize(ranges7), 1);
        assertTrue(ranges7.iterator().next().equals(generateRange(50, 62)));
        Collection<Range> ranges8 = keyspace1ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(collectionSize(ranges8), 1);
        assertTrue(ranges8.iterator().next().equals(generateRange(62, 70)));
        Collection<Range> ranges9 = keyspace1ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(collectionSize(ranges9), 1);
        assertTrue(ranges9.iterator().next().equals(generateRange(70, 82)));
        Collection<Range> ranges10 = keyspace1ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(collectionSize(ranges10), 1);
        assertTrue(ranges10.iterator().next().equals(generateRange(82, 92)));


        /**
        * Keyspace3 RF=5
        * {
        *      /127.0.0.1=[(92,0], (70,82], (50,62], (82,92], (62,70]],
        *      /127.0.0.2=[(92,0], (70,82], (82,92], (0,10], (62,70]],
        *      /127.0.0.3=[(92,0], (70,82], (82,92], (0,10], (10,20]],
        *      /127.0.0.4=[(92,0], (20,30], (82,92], (0,10], (10,20]],
        *      /127.0.0.5=[(92,0], (30,40], (20,30], (0,10], (10,20]],
        *      /127.0.0.6=[(40,50], (30,40], (20,30], (0,10], (10,20]],
        *      /127.0.0.7=[(40,50], (30,40], (50,62], (20,30], (10,20]],
        *      /127.0.0.8=[(40,50], (30,40], (50,62], (20,30], (62,70]],
        *      /127.0.0.9=[(40,50], (70,82], (30,40], (50,62], (62,70]],
        *      /127.0.0.10=[(40,50], (70,82], (50,62], (82,92], (62,70]]
        * }
        */

        Multimap<InetAddress, Range> keyspace3ranges = tableStrategyMap.get("Keyspace3").getAddressRanges();
        ranges1 = keyspace3ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(collectionSize(ranges1), 5);
        assertTrue(ranges1.equals(generateRanges(92, 0, 70, 82, 50, 62, 82, 92, 62, 70)));
        ranges2 = keyspace3ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(collectionSize(ranges2), 5);
        assertTrue(ranges2.equals(generateRanges(92, 0, 70, 82, 82, 92, 0, 10, 62, 70)));
        ranges3 = keyspace3ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(collectionSize(ranges3), 5);
        assertTrue(ranges3.equals(generateRanges(92, 0, 70, 82, 82, 92, 0, 10, 10, 20)));
        ranges4 = keyspace3ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(collectionSize(ranges4), 5);
        assertTrue(ranges4.equals(generateRanges(92, 0, 20, 30, 82, 92, 0, 10, 10, 20)));
        ranges5 = keyspace3ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(collectionSize(ranges5), 5);
        assertTrue(ranges5.equals(generateRanges(92, 0, 30, 40, 20, 30, 0, 10, 10, 20)));
        ranges6 = keyspace3ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(collectionSize(ranges6), 5);
        assertTrue(ranges6.equals(generateRanges(40, 50, 30, 40, 20, 30, 0, 10, 10, 20)));
        ranges7 = keyspace3ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(collectionSize(ranges7), 5);
        assertTrue(ranges7.equals(generateRanges(40, 50, 30, 40, 50, 62, 20, 30, 10, 20)));
        ranges8 = keyspace3ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(collectionSize(ranges8), 5);
        assertTrue(ranges8.equals(generateRanges(40, 50, 30, 40, 50, 62, 20, 30, 62, 70)));
        ranges9 = keyspace3ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(collectionSize(ranges9), 5);
        assertTrue(ranges9.equals(generateRanges(40, 50, 70, 82, 30, 40, 50, 62, 62, 70)));
        ranges10 = keyspace3ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(collectionSize(ranges10), 5);
        assertTrue(ranges10.equals(generateRanges(40, 50, 70, 82, 50, 62, 82, 92, 62, 70)));


        /**
         * Keyspace4 RF=3
         * {
         *      /127.0.0.1=[(92,0], (70,82], (82,92]],
         *      /127.0.0.2=[(92,0], (82,92], (0,10]],
         *      /127.0.0.3=[(92,0], (0,10], (10,20]],
         *      /127.0.0.4=[(20,30], (0,10], (10,20]],
         *      /127.0.0.5=[(30,40], (20,30], (10,20]],
         *      /127.0.0.6=[(40,50], (30,40], (20,30]],
         *      /127.0.0.7=[(40,50], (30,40], (50,62]],
         *      /127.0.0.8=[(40,50], (50,62], (62,70]],
         *      /127.0.0.9=[(70,82], (50,62], (62,70]],
         *      /127.0.0.10=[(70,82], (82,92], (62,70]]
         *  }
         */
        Multimap<InetAddress, Range> keyspace4ranges = tableStrategyMap.get("Keyspace4").getAddressRanges();
        ranges1 = keyspace4ranges.get(InetAddress.getByName("127.0.0.1"));
        assertEquals(collectionSize(ranges1), 3);
        assertTrue(ranges1.equals(generateRanges(92, 0, 70, 82, 82, 92)));
        ranges2 = keyspace4ranges.get(InetAddress.getByName("127.0.0.2"));
        assertEquals(collectionSize(ranges2), 3);
        assertTrue(ranges2.equals(generateRanges(92, 0, 82, 92, 0, 10)));
        ranges3 = keyspace4ranges.get(InetAddress.getByName("127.0.0.3"));
        assertEquals(collectionSize(ranges3), 3);
        assertTrue(ranges3.equals(generateRanges(92, 0, 0, 10, 10, 20)));
        ranges4 = keyspace4ranges.get(InetAddress.getByName("127.0.0.4"));
        assertEquals(collectionSize(ranges4), 3);
        assertTrue(ranges4.equals(generateRanges(20, 30, 0, 10, 10, 20)));
        ranges5 = keyspace4ranges.get(InetAddress.getByName("127.0.0.5"));
        assertEquals(collectionSize(ranges5), 3);
        assertTrue(ranges5.equals(generateRanges(30, 40, 20, 30, 10, 20)));
        ranges6 = keyspace4ranges.get(InetAddress.getByName("127.0.0.6"));
        assertEquals(collectionSize(ranges6), 3);
        assertTrue(ranges6.equals(generateRanges(40, 50, 30, 40, 20, 30)));
        ranges7 = keyspace4ranges.get(InetAddress.getByName("127.0.0.7"));
        assertEquals(collectionSize(ranges7), 3);
        assertTrue(ranges7.equals(generateRanges(40, 50, 30, 40, 50, 62)));
        ranges8 = keyspace4ranges.get(InetAddress.getByName("127.0.0.8"));
        assertEquals(collectionSize(ranges8), 3);
        assertTrue(ranges8.equals(generateRanges(40, 50, 50, 62, 62, 70)));
        ranges9 = keyspace4ranges.get(InetAddress.getByName("127.0.0.9"));
        assertEquals(collectionSize(ranges9), 3);
        assertTrue(ranges9.equals(generateRanges(70, 82, 50, 62, 62, 70)));
        ranges10 = keyspace4ranges.get(InetAddress.getByName("127.0.0.10"));
        assertEquals(collectionSize(ranges10), 3);
        assertTrue(ranges10.equals(generateRanges(70, 82, 82, 92, 62, 70)));

        // pre-calculate the results.
        Map<String, Multimap<Token, InetAddress>> expectedEndpoints = new HashMap<String, Multimap<Token, InetAddress>>();
        expectedEndpoints.put("Keyspace1", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace1").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put("Keyspace2", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.1.2"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10"));
        expectedEndpoints.get("Keyspace2").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1"));
        expectedEndpoints.put("Keyspace3", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.1.2"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get("Keyspace3").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.put("Keyspace4", HashMultimap.<Token, InetAddress>create());
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("5"), makeAddrs("127.0.0.2", "127.0.0.3", "127.0.0.4"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("15"), makeAddrs("127.0.0.3", "127.0.0.4", "127.0.0.5"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("25"), makeAddrs("127.0.0.4", "127.0.0.5", "127.0.0.6"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("35"), makeAddrs("127.0.0.5", "127.0.0.6", "127.0.0.7", "127.0.1.1"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("45"), makeAddrs("127.0.0.6", "127.0.0.7", "127.0.0.8", "127.0.1.1"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("55"), makeAddrs("127.0.0.7", "127.0.0.8", "127.0.0.9", "127.0.1.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("65"), makeAddrs("127.0.0.8", "127.0.0.9", "127.0.0.10", "127.0.1.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("75"), makeAddrs("127.0.0.9", "127.0.0.10", "127.0.0.1", "127.0.1.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("85"), makeAddrs("127.0.0.10", "127.0.0.1", "127.0.0.2"));
        expectedEndpoints.get("Keyspace4").putAll(new BigIntegerToken("95"), makeAddrs("127.0.0.1", "127.0.0.2", "127.0.0.3"));

        for (Map.Entry<String, AbstractReplicationStrategy> tableStrategy : tableStrategyMap.entrySet())
        {
            String table = tableStrategy.getKey();
            AbstractReplicationStrategy strategy = tableStrategy.getValue();

            for (Token token : keyTokens)
            {
                endpoints = tmd.getWriteEndpoints(token, table, strategy.getNaturalEndpoints(token));
                assertTrue(expectedEndpoints.get(table).get(token).size() == endpoints.size());
                assertTrue(expectedEndpoints.get(table).get(token).containsAll(endpoints));
            }

            // just to be sure that things still work according to the old tests, run them:
            if (strategy.getReplicationFactor() != 3)
                continue;

            // tokens 5, 15 and 25 should go three nodes
            for (int i = 0; i < 3; i++)
            {
                endpoints = tmd.getWriteEndpoints(keyTokens.get(i), table, strategy.getNaturalEndpoints(keyTokens.get(i)));
                assertTrue(endpoints.size() == 3);
                assertTrue(endpoints.contains(hosts.get(i+1)));
                assertTrue(endpoints.contains(hosts.get(i+2)));
                assertTrue(endpoints.contains(hosts.get(i+3)));
            }

            // token 35 should go to nodes 4, 5, 6 and boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(3), table, strategy.getNaturalEndpoints(keyTokens.get(3)));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(4)));
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(boot1));

            // token 45 should go to nodes 5, 6, 7 boot1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(4), table, strategy.getNaturalEndpoints(keyTokens.get(4)));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(5)));
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(boot1));

            // token 55 should go to nodes 6, 7, 8 boot1 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(5), table, strategy.getNaturalEndpoints(keyTokens.get(5)));
            assertTrue(endpoints.size() == 5);
            assertTrue(endpoints.contains(hosts.get(6)));
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(boot1));
            assertTrue(endpoints.contains(boot2));

            // token 65 should go to nodes 7, 8, 9 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(6), table, strategy.getNaturalEndpoints(keyTokens.get(6)));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(7)));
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(boot2));

            // token 75 should to go nodes 8, 9, 0 and boot2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(7), table, strategy.getNaturalEndpoints(keyTokens.get(7)));
            assertTrue(endpoints.size() == 4);
            assertTrue(endpoints.contains(hosts.get(8)));
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(boot2));

            // token 85 should go to nodes 9, 0, 1
            endpoints = tmd.getWriteEndpoints(keyTokens.get(8), table, strategy.getNaturalEndpoints(keyTokens.get(8)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(9)));
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));

            // token 95 should go to nodes 0, 1 and 2
            endpoints = tmd.getWriteEndpoints(keyTokens.get(9), table, strategy.getNaturalEndpoints(keyTokens.get(9)));
            assertTrue(endpoints.size() == 3);
            assertTrue(endpoints.contains(hosts.get(0)));
            assertTrue(endpoints.contains(hosts.get(1)));
            assertTrue(endpoints.contains(hosts.get(2)));
        }

        // all moving nodes are back to the normal state
        for (Integer movingIndex : MOVING)
        {
            ss.onChange(hosts.get(movingIndex), ApplicationState.STATUS, valueFactory.normal(newTokens.get(movingIndex)));
        }

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

        // create a ring or 6 nodes
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, 6);

        // node 2 leaves
        Token newToken = positionToken(7);
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));

        assertTrue(tmd.isMoving(hosts.get(2)));
        assertTrue(tmd.getToken(hosts.get(2)).equals(endpointTokens.get(2)));

        // back to normal
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(newToken));

        assertTrue(tmd.getMovingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(newToken));

        newToken = positionToken(8);
        // node 2 goes through leave and left and then jumps to normal at its new token
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.moving(newToken));
        ss.onChange(hosts.get(2), ApplicationState.STATUS, valueFactory.normal(newToken));

        assertTrue(tmd.getBootstrapTokens().isEmpty());
        assertTrue(tmd.getMovingEndpoints().isEmpty());
        assertTrue(tmd.getToken(hosts.get(2)).equals(newToken));

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

    private Token positionToken(int position)
    {
        return new BigIntegerToken(String.valueOf(10 * position + 2));
    }

    private int collectionSize(Collection<?> collection)
    {
        if (collection.isEmpty())
            return 0;

        Iterator<?> iterator = collection.iterator();

        int count = 0;
        while (iterator.hasNext())
        {
            iterator.next();
            count++;
        }

        return count;
    }

    private Collection<Range> generateRanges(int... rangePairs)
    {
        if (rangePairs.length % 2 == 1)
            throw new RuntimeException("generateRanges argument count should be even");

        Set<Range> ranges = new HashSet<Range>();

        for (int i = 0; i < rangePairs.length; i+=2)
        {
            ranges.add(generateRange(rangePairs[i], rangePairs[i+1]));
        }

        return ranges;
    }

    private Range generateRange(int left, int right)
    {
        return new Range(new BigIntegerToken(String.valueOf(left)), new BigIntegerToken(String.valueOf(right)));
    }
}
