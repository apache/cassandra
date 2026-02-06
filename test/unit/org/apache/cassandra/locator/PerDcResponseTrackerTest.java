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

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.tcm.membership.Location;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PerDcResponseTrackerTest
{
    private static final RequestFailureReason TIMEOUT = RequestFailureReason.TIMEOUT;

    private InetAddressAndPort endpoint(String ip) throws UnknownHostException
    {
        return InetAddressAndPort.getByName(ip);
    }

    private Locator mockLocator(Map<InetAddressAndPort, String> endpointToDc) throws Exception
    {
        Locator locator = mock(Locator.class);
        for (Map.Entry<InetAddressAndPort, String> entry : endpointToDc.entrySet())
        {
            when(locator.location(entry.getKey())).thenReturn(new Location(entry.getValue(), "rack1"));
        }
        return locator;
    }

    /**
     * Helper to create a map of SimpleResponseTrackers from blockFor/totalReplicas config.
     */
    private Map<String, ResponseTracker> simpleTrackers(Object... dcBlockForTotal)
    {
        Map<String, ResponseTracker> trackers = new HashMap<>();
        for (int i = 0; i < dcBlockForTotal.length; i += 3)
        {
            String dc = (String) dcBlockForTotal[i];
            int blockFor = (Integer) dcBlockForTotal[i + 1];
            int totalReplicas = (Integer) dcBlockForTotal[i + 2];
            trackers.put(dc, new SimpleResponseTracker(blockFor, totalReplicas));
        }
        return trackers;
    }

    @Test
    public void testAllDcsReachQuorum() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("127.0.0.3"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");
        endpointToDc.put(endpoint("192.168.1.2"), "DC2");
        endpointToDc.put(endpoint("192.168.1.3"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC1 reaches quorum
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse(tracker.isComplete());  // DC2 not done yet

        // DC2 reaches quorum
        tracker.onResponse(endpoint("192.168.1.1"));
        tracker.onResponse(endpoint("192.168.1.2"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(4, tracker.received());
        assertEquals(4, tracker.required());  // 2 + 2
    }

    @Test
    public void testOneDcFails() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("127.0.0.3"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");
        endpointToDc.put(endpoint("192.168.1.2"), "DC2");
        endpointToDc.put(endpoint("192.168.1.3"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC1 reaches quorum
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        // DC2 fails (all fail)
        tracker.onFailure(endpoint("192.168.1.1"), TIMEOUT);
        tracker.onFailure(endpoint("192.168.1.2"), TIMEOUT);
        tracker.onFailure(endpoint("192.168.1.3"), TIMEOUT);

        assertTrue(tracker.isComplete());  // DC2 reached definite failure
        assertFalse(tracker.isSuccessful());  // DC2 failed
        assertEquals(2, tracker.received());
        assertEquals(3, tracker.failures());
    }

    @Test
    public void testPartialDcProgress() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC1 gets one response
        tracker.onResponse(endpoint("127.0.0.1"));

        assertFalse(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(1, tracker.received());
        assertEquals(4, tracker.required());  // 2 + 2
    }

    @Test
    public void testIgnoresUnknownDc() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");  // DC2 not in config

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC2 response is ignored
        tracker.onResponse(endpoint("192.168.1.1"));
        assertFalse(tracker.countsTowardQuorum(endpoint("192.168.1.1")));
        assertEquals(0, tracker.received());

        // DC1 responses count
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.received());
    }

    @Test
    public void testAsymmetricRequirements() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 3, 5,  // Need 3 of 5
            "DC2", 2, 3   // Need 2 of 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("127.0.0.3"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");
        endpointToDc.put(endpoint("192.168.1.2"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC2 reaches quorum (2 of 3)
        tracker.onResponse(endpoint("192.168.1.1"));
        tracker.onResponse(endpoint("192.168.1.2"));
        assertFalse(tracker.isComplete());  // DC1 not done

        // DC1 reaches quorum (3 of 5)
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(5, tracker.received());
        assertEquals(5, tracker.required());  // 3 + 2
    }

    // Aggregation tests

    @Test
    public void testRequiredSum() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 3, 5,
            "DC2", 2, 3,
            "DC3", 1, 2
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        Locator locator = mockLocator(endpointToDc);

        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        assertEquals(6, tracker.required());  // 3 + 2 + 1
    }

    @Test
    public void testReceivedSum() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("192.168.1.1"));

        assertEquals(3, tracker.received());  // 2 from DC1 + 1 from DC2
    }

    @Test
    public void testFailuresSum() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");
        endpointToDc.put(endpoint("192.168.1.2"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        tracker.onFailure(endpoint("127.0.0.1"), TIMEOUT);
        tracker.onFailure(endpoint("192.168.1.1"), TIMEOUT);
        tracker.onFailure(endpoint("192.168.1.2"), TIMEOUT);

        assertEquals(3, tracker.failures());  // 1 from DC1 + 2 from DC2
    }

    // Composition tests

    @Test
    public void testCountsTowardQuorum() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3
        );

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        assertTrue(tracker.countsTowardQuorum(endpoint("127.0.0.1")));  // DC1 tracked
        assertFalse(tracker.countsTowardQuorum(endpoint("192.168.1.1")));  // DC2 not tracked
    }

    @Test
    public void testGetTrackerForDc() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers(
            "DC1", 2, 3,
            "DC2", 1, 2
        );

        Locator locator = mock(Locator.class);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        assertNotNull(tracker.getTrackerForDc("DC1"));
        assertNotNull(tracker.getTrackerForDc("DC2"));
        assertNull(tracker.getTrackerForDc("DC3"));
        assertEquals(2, tracker.getTrackerForDc("DC1").required());
        assertEquals(1, tracker.getTrackerForDc("DC2").required());
    }

    @Test
    public void testWithWriteResponseTrackers() throws Exception
    {
        // Test that PerDcResponseTracker works with WriteResponseTrackers (double-count model)
        Map<String, ResponseTracker> trackers = new HashMap<>();
        // DC1: baseBlockFor=2, totalBlockFor=3, committed=3, pending=1
        trackers.put("DC1", new WriteResponseTracker(2, 3, 3, 1,
            addr -> addr.getHostAddress(false).equals("127.0.0.4")));  // .4 is pending
        // DC2: no pending, degenerates to simple case
        trackers.put("DC2", new SimpleResponseTracker(2, 3));

        Map<InetAddressAndPort, String> endpointToDc = new HashMap<>();
        endpointToDc.put(endpoint("127.0.0.1"), "DC1");
        endpointToDc.put(endpoint("127.0.0.2"), "DC1");
        endpointToDc.put(endpoint("127.0.0.3"), "DC1");
        endpointToDc.put(endpoint("127.0.0.4"), "DC1");  // pending
        endpointToDc.put(endpoint("192.168.1.1"), "DC2");
        endpointToDc.put(endpoint("192.168.1.2"), "DC2");
        endpointToDc.put(endpoint("192.168.1.3"), "DC2");

        Locator locator = mockLocator(endpointToDc);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        // DC1: 2 committed successes (meets base requirement but not total)
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse(tracker.isComplete());

        // DC2: 2 successes (meets requirement)
        tracker.onResponse(endpoint("192.168.1.1"));
        tracker.onResponse(endpoint("192.168.1.2"));
        assertFalse(tracker.isComplete());  // DC1 still needs pending

        // DC1: 1 pending success (now meets total requirement)
        tracker.onResponse(endpoint("127.0.0.4"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
    }

    // Validation tests

    @Test(expected = IllegalArgumentException.class)
    public void testNullTrackers()
    {
        Locator locator = mock(Locator.class);
        new PerDcResponseTracker(null, locator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTrackers()
    {
        Locator locator = mock(Locator.class);
        new PerDcResponseTracker(new HashMap<>(), locator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullLocator()
    {
        Map<String, ResponseTracker> trackers = simpleTrackers("DC1", 2, 3);
        new PerDcResponseTracker(trackers, null);
    }

    @Test
    public void testToString() throws Exception
    {
        Map<String, ResponseTracker> trackers = simpleTrackers("DC1", 2, 3);

        Locator locator = mock(Locator.class);
        PerDcResponseTracker tracker = new PerDcResponseTracker(trackers, locator);

        String str = tracker.toString();
        assertTrue(str.contains("PerDcResponseTracker"));
        assertTrue(str.contains("DC1"));
    }
}
