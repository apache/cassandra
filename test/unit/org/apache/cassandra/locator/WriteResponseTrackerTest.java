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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.junit.Test;

import org.apache.cassandra.exceptions.RequestFailureReason;

import static org.junit.Assert.*;

/**
 * Tests for WriteResponseTracker implementing the double count model.
 */
public class WriteResponseTrackerTest
{
    private static final RequestFailureReason TIMEOUT = RequestFailureReason.TIMEOUT;

    private InetAddressAndPort endpoint(String ip) throws UnknownHostException
    {
        return InetAddressAndPort.getByName(ip);
    }

    @Test
    public void testBothRequirementsMet() throws Exception
    {
        // RF=3, pending=1: baseBlockFor=2, totalBlockFor=3
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        // 2 committed successes
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse("Should not be complete - need 3 total", tracker.isComplete());

        // 1 pending success -> 3 total
        tracker.onResponse(endpoint("127.0.0.4"));
        assertTrue("Should be complete", tracker.isComplete());
        assertTrue("Should be successful", tracker.isSuccessful());
        assertEquals(2, tracker.committedReceived());
        assertEquals(1, tracker.pendingReceived());
        assertEquals(3, tracker.received());
    }

    @Test
    public void testCommittedRequirementNotMet() throws Exception
    {
        // RF=3, pending=1: baseBlockFor=2, totalBlockFor=3
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        // 1 committed success, 1 pending success
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.4"));
        assertFalse("Should not be complete - need 2 committed", tracker.isComplete());
        assertEquals(1, tracker.committedReceived());
        assertEquals(1, tracker.pendingReceived());

        // 2 committed failures -> can't reach 2 committed
        tracker.onFailure(endpoint("127.0.0.2"), TIMEOUT);
        tracker.onFailure(endpoint("127.0.0.3"), TIMEOUT);
        assertTrue("Should be complete - impossible to reach committed requirement", tracker.isComplete());
        assertFalse("Should not be successful", tracker.isSuccessful());
    }

    @Test
    public void testTotalRequirementNotMet() throws Exception
    {
        // RF=3, pending=2: baseBlockFor=2, totalBlockFor=4
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        pending.add(endpoint("127.0.0.5"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 4, 3, 2, isPending);

        // 2 committed successes (meets base requirement)
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse("Should not be complete - need 4 total", tracker.isComplete());
        assertEquals(2, tracker.committedReceived());

        // 1 committed failure, 2 pending failures -> only 3 total possible, need 4
        tracker.onFailure(endpoint("127.0.0.3"), TIMEOUT);
        tracker.onFailure(endpoint("127.0.0.4"), TIMEOUT);
        tracker.onFailure(endpoint("127.0.0.5"), TIMEOUT);
        assertTrue("Should be complete - impossible to reach total requirement", tracker.isComplete());
        assertFalse("Should not be successful", tracker.isSuccessful());
    }

    @Test
    public void testNoPendingReplicas() throws Exception
    {
        // RF=3, pending=0: baseBlockFor=2, totalBlockFor=2 (degenerates to simple case)
        Predicate<InetAddressAndPort> isPending = addr -> false;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 2, 3, 0, isPending);

        tracker.onResponse(endpoint("127.0.0.1"));
        assertFalse(tracker.isComplete());

        tracker.onResponse(endpoint("127.0.0.2"));
        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(2, tracker.committedReceived());
        assertEquals(0, tracker.pendingReceived());
    }

    @Test
    public void testAllFail() throws Exception
    {
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        tracker.onFailure(endpoint("127.0.0.1"), TIMEOUT);
        tracker.onFailure(endpoint("127.0.0.2"), TIMEOUT);
        // After 2 committed failures, can't reach baseBlockFor=2 with only 1 remaining
        assertTrue(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
        assertEquals(0, tracker.received());
        assertEquals(2, tracker.committedFailures());
    }

    @Test
    public void testAllSucceed() throws Exception
    {
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));
        tracker.onResponse(endpoint("127.0.0.4"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(3, tracker.committedReceived());
        assertEquals(1, tracker.pendingReceived());
        assertEquals(4, tracker.received());
        assertEquals(0, tracker.failures());
    }

    @Test
    public void testPendingSuccessBeforeCommitted() throws Exception
    {
        // Pending responses arrive first
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        // Pending arrives first
        tracker.onResponse(endpoint("127.0.0.4"));
        assertFalse(tracker.isComplete());
        assertEquals(0, tracker.committedReceived());
        assertEquals(1, tracker.pendingReceived());

        // Then committed
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
    }

    @Test
    public void testExactlyMeetsRequirements() throws Exception
    {
        // RF=2, pending=1: baseBlockFor=2, totalBlockFor=3
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.3"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 2, 1, isPending);

        // Exactly 2 committed (all of them)
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        assertFalse("Need 3 total", tracker.isComplete());

        // Exactly 1 pending (all of them)
        tracker.onResponse(endpoint("127.0.0.3"));
        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
    }

    @Test
    public void testMixedSuccessesAndFailures() throws Exception
    {
        // RF=5, pending=2: baseBlockFor=3, totalBlockFor=5
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.6"));
        pending.add(endpoint("127.0.0.7"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(3, 5, 5, 2, isPending);

        // 3 committed successes, 2 committed failures
        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onResponse(endpoint("127.0.0.2"));
        tracker.onResponse(endpoint("127.0.0.3"));
        tracker.onFailure(endpoint("127.0.0.4"), TIMEOUT);
        tracker.onFailure(endpoint("127.0.0.5"), TIMEOUT);

        assertFalse("Need 5 total, only have 3", tracker.isComplete());
        assertEquals(3, tracker.committedReceived());
        assertEquals(2, tracker.committedFailures());

        // 2 pending successes
        tracker.onResponse(endpoint("127.0.0.6"));
        tracker.onResponse(endpoint("127.0.0.7"));

        assertTrue(tracker.isComplete());
        assertTrue(tracker.isSuccessful());
        assertEquals(3, tracker.committedReceived());
        assertEquals(2, tracker.pendingReceived());
        assertEquals(5, tracker.received());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeBaseBlockFor()
    {
        new WriteResponseTracker(-1, 2, 3, 1, addr -> false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTotalRequiredLessThanBase()
    {
        new WriteResponseTracker(3, 2, 3, 1, addr -> false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBaseBlockForExceedsCommitted()
    {
        new WriteResponseTracker(4, 5, 3, 2, addr -> false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTotalBlockForExceedsTotalReplicas()
    {
        new WriteResponseTracker(2, 6, 3, 2, addr -> false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPredicate()
    {
        new WriteResponseTracker(2, 3, 3, 1, null);
    }

    @Test
    public void testAccessors() throws Exception
    {
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        assertEquals(2, tracker.baseBlockFor());
        assertEquals(3, tracker.required()); // Returns totalBlockFor for error messages

        tracker.onResponse(endpoint("127.0.0.1"));
        tracker.onFailure(endpoint("127.0.0.2"), TIMEOUT);
        tracker.onResponse(endpoint("127.0.0.4"));

        assertEquals(1, tracker.committedReceived());
        assertEquals(1, tracker.pendingReceived());
        assertEquals(1, tracker.committedFailures());
        assertEquals(0, tracker.pendingFailures());
        assertEquals(2, tracker.received());
        assertEquals(1, tracker.failures());
    }

    @Test
    public void testCountsTowardQuorum() throws Exception
    {
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);

        // All endpoints count toward quorum in writes
        assertTrue(tracker.countsTowardQuorum(endpoint("127.0.0.1")));
        assertTrue(tracker.countsTowardQuorum(endpoint("127.0.0.4")));
        assertTrue(tracker.countsTowardQuorum(endpoint("192.168.1.1")));
    }

    @Test
    public void testToString() throws Exception
    {
        Set<InetAddressAndPort> pending = new HashSet<>();
        pending.add(endpoint("127.0.0.4"));
        Predicate<InetAddressAndPort> isPending = pending::contains;

        WriteResponseTracker tracker = new WriteResponseTracker(2, 3, 3, 1, isPending);
        String str = tracker.toString();

        assertTrue(str.contains("WriteResponseTracker"));
        assertTrue(str.contains("baseBlockFor=2"));
        assertTrue(str.contains("totalBlockFor=3"));
    }
}
