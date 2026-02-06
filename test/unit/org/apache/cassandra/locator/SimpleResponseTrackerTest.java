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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Test;

import static org.junit.Assert.*;

public class SimpleResponseTrackerTest
{
    private InetAddressAndPort endpoint(String ip) throws UnknownHostException
    {
        return InetAddressAndPort.getByName(ip);
    }

    @Test
    public void testQuorumReached() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 3);

        assertFalse(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(0, tracker.received());

        tracker.onResponse(endpoint("127.0.0.1"));
        assertFalse(tracker.isComplete());
        assertEquals(1, tracker.received());

        tracker.onResponse(endpoint("127.0.0.2"));
        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.received());
    }

    @Test
    public void testPartialProgress() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(3, 5);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        assertFalse(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(2, tracker.received());
        assertEquals(3, tracker.required());
    }

    @Test
    public void testEarlyFailure() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(3, 5);

        // Need 3, have 5 total
        tracker.onResponse(endpoint("127.0.0.1"));  // 1 success
        tracker.onFailure(endpoint("127.0.0.2"));  // 1 failure
        tracker.onFailure(endpoint("127.0.0.3"));  // 2 failures
        tracker.onFailure(endpoint("127.0.0.4"));  // 3 failures

        // Have 1 success, 3 failures, 1 remaining
        // Need 2 more but only 1 remaining -> impossible
        assertTrue(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(1, tracker.received());
        assertEquals(3, tracker.failures());
    }

    @Test
    public void testAllSucceed() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(3, 3);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(3, tracker.received());
        assertEquals(0, tracker.failures());
    }

    @Test
    public void testAllFail() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 3);

        tracker.onFailure(endpoint("127.0.0.1"));
        tracker.onFailure(endpoint("127.0.0.2"));
        tracker.onFailure(endpoint("127.0.0.3"));

        assertTrue(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(0, tracker.received());
        assertEquals(3, tracker.failures());
    }

    @Test
    public void testBlockForOne() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(1, 3);

        tracker.onResponse(endpoint("127.0.0.1"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(1, tracker.received());
    }

    @Test
    public void testBlockForAll() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(3, 3);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse(tracker.isComplete());

        tracker.onResponse(endpoint("127.0.0.3"));
        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
    }

    @Test
    public void testZeroResponses() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 3);

        assertFalse(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(0, tracker.received());
        assertEquals(0, tracker.failures());
        assertEquals(2, tracker.required());
    }

    @Test
    public void testConcurrentResponses() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(50, 100);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(50);

        try
        {
            // Launch 50 threads to call onResponse concurrently
            for (int i = 0; i < 50; i++)
            {
                final int index = i;
                executor.submit(() -> {
                    try
                    {
                        startLatch.await();
                        tracker.onResponse(endpoint("127.0.0." + index));
                        doneLatch.countDown();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Start all threads at once
            startLatch.countDown();

            // Wait for completion
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

            // Verify no lost updates
            assertTrue(tracker.isComplete());
            assertTrue(tracker.isSuccessful());
            assertEquals(50, tracker.received());
            assertEquals(0, tracker.failures());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    // Filtering tests

    @Test
    public void testUnfilteredTracker() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 4);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("192.168.1.1"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.received());
    }

    @Test
    public void testFilteredTracker() throws Exception
    {
        // Filter that only accepts local endpoints (127.0.0.*)
        Predicate<InetAddressAndPort> localFilter = endpoint ->
            endpoint.getHostAddress(false).startsWith("127.0.0.");

        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 3, localFilter);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.received());
        assertTrue(tracker.countsTowardQuorum(endpoint("127.0.0.1")));
    }

    @Test
    public void testFilteredIgnoresNonMatching() throws Exception
    {
        // Filter that only accepts local endpoints
        Predicate<InetAddressAndPort> localFilter = endpoint ->
            endpoint.getHostAddress(false).startsWith("127.0.0.");

        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 2, localFilter);

        // Remote endpoint response is ignored
        tracker.onResponse(endpoint("192.168.1.1"));
        assertFalse(tracker.isComplete());
        assertEquals(0, tracker.received());
        assertFalse(tracker.countsTowardQuorum(endpoint("192.168.1.1")));

        // Local endpoints count
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.received());
    }

    @Test
    public void testCountsTowardQuorum() throws Exception
    {
        Predicate<InetAddressAndPort> filter = endpoint ->
            endpoint.getHostAddress(false).startsWith("127.0.0.");

        SimpleResponseTracker unfilteredTracker = new SimpleResponseTracker(2, 3);
        assertTrue(unfilteredTracker.countsTowardQuorum(endpoint("127.0.0.1")));
        assertTrue(unfilteredTracker.countsTowardQuorum(endpoint("192.168.1.1")));

        SimpleResponseTracker filteredTracker = new SimpleResponseTracker(2, 3, filter);
        assertTrue(filteredTracker.countsTowardQuorum(endpoint("127.0.0.1")));
        assertFalse(filteredTracker.countsTowardQuorum(endpoint("192.168.1.1")));
    }

    // Usage pattern tests

    @Test
    public void testQuorumUsage() throws Exception
    {
        // Simulates QUORUM with RF=5
        int rf = 5;
        int blockFor = rf / 2 + 1;  // 3
        SimpleResponseTracker tracker = new SimpleResponseTracker(blockFor, rf);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(3, tracker.required());
    }

    @Test
    public void testLocalQuorumUsage() throws Exception
    {
        // Simulates LOCAL_QUORUM with localRF=3
        int localRf = 3;
        int blockFor = localRf / 2 + 1;  // 2
        Predicate<InetAddressAndPort> localFilter = endpoint ->
            endpoint.getHostAddress(false).startsWith("127.0.0.");

        SimpleResponseTracker tracker = new SimpleResponseTracker(blockFor, localRf, localFilter);

        // Remote response ignored
        tracker.onResponse(endpoint("192.168.1.1"));
        assertFalse(tracker.isComplete());

        // Local responses count
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.required());
    }

    @Test
    public void testSerialUsage() throws Exception
    {
        // Simulates SERIAL paxos with participants=5 (RF=4 + 1 pending)
        int participants = 5;
        int blockFor = participants / 2 + 1;  // 3
        SimpleResponseTracker tracker = new SimpleResponseTracker(blockFor, participants);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(3, tracker.required());
    }

    // Validation tests

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeBlockFor()
    {
        new SimpleResponseTracker(-1, 3);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTotalReplicas()
    {
        new SimpleResponseTracker(2, -1);
    }

    @Test
    public void testToString() throws Exception
    {
        SimpleResponseTracker tracker = new SimpleResponseTracker(2, 3);
        String str = tracker.toString();

        assertTrue(str.contains("SimpleResponseTracker"));
        assertTrue(str.contains("blockFor=2"));
        assertTrue(str.contains("totalReplicas=3"));
    }
}
