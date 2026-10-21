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

package org.apache.cassandra.tcm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.tcm.discovery.Discovery;
import org.apache.cassandra.tcm.membership.Location;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RemoteProcessorTest
{
    @Test
    public void simpleTestCMSIterator()
    {
        int endpointCount = 10;
        List<InetAddressAndPort> allEndpoints = eps(endpointCount);
        Set<InetAddressAndPort> cms = new HashSet<>(allEndpoints.subList(0, 2));
        Set<InetAddressAndPort> discovery = new HashSet<>(allEndpoints.subList(2, 4));
        RemoteProcessor.CandidateIterator iter = new RemoteProcessor.CandidateIterator(discovery, false);

        InetAddressAndPort returned = iter.next();
        assertTrue(discovery.contains(returned));
        iter.addCandidates(new Discovery.DiscoveredNodes(cms, Discovery.DiscoveredNodes.Kind.CMS_ONLY));
        returned = iter.next();
        assertFalse(discovery.contains(returned));
        assertTrue(cms.contains(returned));
    }

    @Test
    public void timeoutTest()
    {
        // make sure that a node marked as timed out will not be returned until we've cycled through all other candidates
        // when using the iterator in a RemoteProcessor::sendWithCallback call, the Backoff will trigger the breaking
        // out of the cycle.
        int endpointCount = 10;
        List<InetAddressAndPort> allEndpoints = eps(endpointCount);
        Set<InetAddressAndPort> discovery = new HashSet<>(allEndpoints.subList(0, 4));
        RemoteProcessor.CandidateIterator iter = new RemoteProcessor.CandidateIterator(discovery, false);
        InetAddressAndPort timeout = iter.peek();
        for (int i = 1; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            InetAddressAndPort returned = iter.next();
            if (returned.equals(timeout))
            {
                iter.timeout(returned);
                assertEquals(timeout, iter.peekLast());
            }
        }
    }

    private List<InetAddressAndPort> eps(int endpointCount)
    {
        List<InetAddressAndPort> allEndpoints = new ArrayList<>(endpointCount);
        for (int i = 0; i < endpointCount; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByNameUnchecked("127.0.0."+i);
            allEndpoints.add(ep);
        }
        return allEndpoints;
    }

    // ========== CMS Member Selection Tests ==========

    @Test
    public void testCandidatesDeterministic()
    {
        // Create endpoints in non-sorted order
        List<InetAddressAndPort> candidates = new ArrayList<>(Arrays.asList(
            InetAddressAndPort.getByNameUnchecked("127.0.0.5"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.1"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.9"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.3")
        ));

        // Deterministic policy should sort by address
        RemoteProcessor.sortCandidates(candidates, Config.CMSCommitMemberPreferencePolicy.deterministic, null);

        assertEquals(InetAddressAndPort.getByNameUnchecked("127.0.0.1"), candidates.get(0));
        assertEquals(InetAddressAndPort.getByNameUnchecked("127.0.0.3"), candidates.get(1));
        assertEquals(InetAddressAndPort.getByNameUnchecked("127.0.0.5"), candidates.get(2));
        assertEquals(InetAddressAndPort.getByNameUnchecked("127.0.0.9"), candidates.get(3));
    }

    @Test
    public void testCandidatesRandom()
    {
        // Create endpoints
        List<InetAddressAndPort> original = Arrays.asList(
            InetAddressAndPort.getByNameUnchecked("127.0.0.1"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.2"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.3"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.4")
        );
        List<InetAddressAndPort> candidates = new ArrayList<>(original);

        // Random policy should shuffle (but contain same elements)
        RemoteProcessor.sortCandidates(candidates, Config.CMSCommitMemberPreferencePolicy.random, null);

        // Same elements, possibly different order
        assertEquals(new HashSet<>(original), new HashSet<>(candidates));
        assertEquals(original.size(), candidates.size());
    }

    @Test
    public void testCandidatesLocalDeterministic()
    {
        // DC1 endpoints (local)
        InetAddressAndPort dc1_1 = InetAddressAndPort.getByNameUnchecked("127.0.0.5");
        InetAddressAndPort dc1_2 = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
        // DC2 endpoints (remote)
        InetAddressAndPort dc2_1 = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
        InetAddressAndPort dc2_2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2");

        List<InetAddressAndPort> candidates = new ArrayList<>(Arrays.asList(dc2_1, dc1_1, dc2_2, dc1_2));

        // Create a test locator
        Location dc1 = new Location("DC1", "rack1");
        Location dc2 = new Location("DC2", "rack1");
        Map<InetAddressAndPort, Location> locationMap = new HashMap<>();
        locationMap.put(dc1_1, dc1);
        locationMap.put(dc1_2, dc1);
        locationMap.put(dc2_1, dc2);
        locationMap.put(dc2_2, dc2);

        TestLocator locator = new TestLocator(dc1, locationMap);

        // local_deterministic: local DC sorted first, then remote DC sorted
        RemoteProcessor.sortCandidates(candidates, Config.CMSCommitMemberPreferencePolicy.local_deterministic, locator);

        // Local DC first (sorted), then remote DC (sorted)
        assertEquals(dc1_2, candidates.get(0)); // 127.0.0.1 (DC1)
        assertEquals(dc1_1, candidates.get(1)); // 127.0.0.5 (DC1)
        assertEquals(dc2_2, candidates.get(2)); // 127.0.0.2 (DC2)
        assertEquals(dc2_1, candidates.get(3)); // 127.0.0.3 (DC2)
    }

    @Test
    public void testCandidatesLocalRandom()
    {
        // DC1 endpoints (local)
        InetAddressAndPort dc1_1 = InetAddressAndPort.getByNameUnchecked("127.0.0.1");
        InetAddressAndPort dc1_2 = InetAddressAndPort.getByNameUnchecked("127.0.0.2");
        // DC2 endpoints (remote)
        InetAddressAndPort dc2_1 = InetAddressAndPort.getByNameUnchecked("127.0.0.3");
        InetAddressAndPort dc2_2 = InetAddressAndPort.getByNameUnchecked("127.0.0.4");

        List<InetAddressAndPort> candidates = new ArrayList<>(Arrays.asList(dc2_1, dc1_1, dc2_2, dc1_2));

        // Create a test locator
        Location dc1 = new Location("DC1", "rack1");
        Location dc2 = new Location("DC2", "rack1");
        Map<InetAddressAndPort, Location> locationMap = new HashMap<>();
        locationMap.put(dc1_1, dc1);
        locationMap.put(dc1_2, dc1);
        locationMap.put(dc2_1, dc2);
        locationMap.put(dc2_2, dc2);

        TestLocator locator = new TestLocator(dc1, locationMap);

        // local_random: local DC shuffled first, then remote DC shuffled
        RemoteProcessor.sortCandidates(candidates, Config.CMSCommitMemberPreferencePolicy.local_random, locator);

        // Local DC should be in first 2 positions, remote DC in last 2
        Set<InetAddressAndPort> localDcEndpoints = new HashSet<>(Arrays.asList(dc1_1, dc1_2));
        Set<InetAddressAndPort> remoteDcEndpoints = new HashSet<>(Arrays.asList(dc2_1, dc2_2));

        assertTrue(localDcEndpoints.contains(candidates.get(0)));
        assertTrue(localDcEndpoints.contains(candidates.get(1)));
        assertTrue(remoteDcEndpoints.contains(candidates.get(2)));
        assertTrue(remoteDcEndpoints.contains(candidates.get(3)));
    }

    @Test
    public void testDeterministicPolicyProducesSameOrderAcrossCalls()
    {
        List<InetAddressAndPort> candidates1 = new ArrayList<>(Arrays.asList(
            InetAddressAndPort.getByNameUnchecked("127.0.0.5"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.1"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.3")
        ));
        List<InetAddressAndPort> candidates2 = new ArrayList<>(Arrays.asList(
            InetAddressAndPort.getByNameUnchecked("127.0.0.3"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.5"),
            InetAddressAndPort.getByNameUnchecked("127.0.0.1")
        ));

        RemoteProcessor.sortCandidates(candidates1, Config.CMSCommitMemberPreferencePolicy.deterministic, null);
        RemoteProcessor.sortCandidates(candidates2, Config.CMSCommitMemberPreferencePolicy.deterministic, null);

        // Both should produce identical ordering regardless of initial order
        assertEquals(candidates1, candidates2);
    }

    /**
     * Test Locator implementation for unit testing
     */
    private static class TestLocator extends Locator
    {
        private final Location localLocation;
        private final Map<InetAddressAndPort, Location> locationMap;

        public TestLocator(Location localLocation, Map<InetAddressAndPort, Location> locationMap)
        {
            super(null, null, () -> localLocation, null);
            this.localLocation = localLocation;
            this.locationMap = locationMap;
        }

        @Override
        public Location local()
        {
            return localLocation;
        }

        @Override
        public Location location(InetAddressAndPort endpoint)
        {
            return locationMap.getOrDefault(endpoint, Location.UNKNOWN);
        }
    }
}
