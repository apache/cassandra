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
package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.Util.token;


@RunWith(OrderedJUnit4ClassRunner.class)
public class TokenMetadataTest
{
    public final static String ONE = "1";
    public final static String SIX = "6";

    static TokenMetadata tmd;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(token(ONE), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(token(SIX), InetAddress.getByName("127.0.0.6"));
    }

    private static void testRingIterator(ArrayList<Token> ring, String start, boolean includeMin, String... expected)
    {
        ArrayList<Token> actual = new ArrayList<>();
        Iterators.addAll(actual, TokenMetadata.ringIterator(ring, token(start), includeMin));
        assertEquals(actual.toString(), expected.length, actual.size());
        for (int i = 0; i < expected.length; i++)
            assertEquals("Mismatch at index " + i + ": " + actual, token(expected[i]), actual.get(i));
    }

    @Test
    public void testRingIterator()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", false, "6", "1");
        testRingIterator(ring, "7", false, "1", "6");
        testRingIterator(ring, "0", false, "1", "6");
        testRingIterator(ring, "", false, "1", "6");
    }

    @Test
    public void testRingIteratorIncludeMin()
    {
        ArrayList<Token> ring = tmd.sortedTokens();
        testRingIterator(ring, "2", true, "6", "", "1");
        testRingIterator(ring, "7", true, "", "1", "6");
        testRingIterator(ring, "0", true, "1", "6", "");
        testRingIterator(ring, "", true, "1", "6", "");
    }

    @Test
    public void testRingIteratorEmptyRing()
    {
        testRingIterator(new ArrayList<Token>(), "2", false);
    }

    @Test
    public void testTopologyUpdate_RackConsolidation() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, ImmutableMultimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tokenMetadata.updateTopology(first);
        topology = tokenMetadata.updateTopology(second);

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));
    }

    @Test
    public void testTopologyUpdate_RackExpansion() throws UnknownHostException
    {
        final InetAddress first = InetAddress.getByName("127.0.0.1");
        final InetAddress second = InetAddress.getByName("127.0.0.6");
        final String DATA_CENTER = "datacenter1";
        final String RACK1 = "rack1";
        final String RACK2 = "rack2";

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        tmd.updateNormalToken(token(ONE), first);
        tmd.updateNormalToken(token(SIX), second);

        TokenMetadata tokenMetadata = tmd.cloneOnlyTokenMap();
        assertNotNull(tokenMetadata);

        TokenMetadata.Topology topology = tokenMetadata.getTopology();
        assertNotNull(topology);

        Multimap<String, InetAddress> allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        Map<String, ImmutableMultimap<String, InetAddress>> racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertFalse(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(second));

        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return endpoint.equals(first) ? RACK1 : RACK2;
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });

        topology = tokenMetadata.updateTopology();

        allEndpoints = topology.getDatacenterEndpoints();
        assertNotNull(allEndpoints);
        assertTrue(allEndpoints.size() == 2);
        assertTrue(allEndpoints.containsKey(DATA_CENTER));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(first));
        assertTrue(allEndpoints.get(DATA_CENTER).contains(second));

        racks = topology.getDatacenterRacks();
        assertNotNull(racks);
        assertTrue(racks.size() == 1);
        assertTrue(racks.containsKey(DATA_CENTER));
        assertTrue(racks.get(DATA_CENTER).size() == 2);
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK1));
        assertTrue(racks.get(DATA_CENTER).containsKey(RACK2));
        assertTrue(racks.get(DATA_CENTER).get(RACK1).contains(first));
        assertTrue(racks.get(DATA_CENTER).get(RACK2).contains(second));
    }
}
