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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.exceptions.RequestFailureReason;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CompositeTrackerTest
{
    private static final RequestFailureReason TIMEOUT = RequestFailureReason.TIMEOUT;

    private InetAddressAndPort endpoint(String ip) throws UnknownHostException
    {
        return InetAddressAndPort.getByName(ip);
    }

    private ResponseTracker createMockTracker(boolean successful, boolean complete)
    {
        ResponseTracker tracker = mock(ResponseTracker.class);
        when(tracker.isSuccessful()).thenReturn(successful);
        when(tracker.isComplete()).thenReturn(complete);
        when(tracker.required()).thenReturn(2);
        when(tracker.received()).thenReturn(successful ? 2 : 0);
        when(tracker.failures()).thenReturn(successful ? 0 : 2);
        return tracker;
    }

    @Test
    public void testQuorumCalculation()
    {
        assertEquals(1, CompositeTracker.quorum(1));
        assertEquals(2, CompositeTracker.quorum(2));
        assertEquals(2, CompositeTracker.quorum(3));
        assertEquals(3, CompositeTracker.quorum(4));
        assertEquals(3, CompositeTracker.quorum(5));
    }

    @Test
    public void testAllCalculation()
    {
        assertEquals(1, CompositeTracker.all(1));
        assertEquals(3, CompositeTracker.all(3));
        assertEquals(5, CompositeTracker.all(5));
    }

    @Test
    public void testQuorumSuccessAndFailure()
    {
        // N=1: quorum=1
        assertTrue(new CompositeTracker(CompositeTracker.quorum(1), Arrays.asList(
            createMockTracker(true, true)
        )).isSuccessful());

        // N=2: quorum=2 (both required)
        assertFalse(new CompositeTracker(CompositeTracker.quorum(2), Arrays.asList(
            createMockTracker(true, true), createMockTracker(false, false)
        )).isSuccessful());

        assertTrue(new CompositeTracker(CompositeTracker.quorum(2), Arrays.asList(
            createMockTracker(true, true), createMockTracker(true, true)
        )).isSuccessful());

        // N=3: quorum=2, last child fails
        assertTrue(new CompositeTracker(CompositeTracker.quorum(3), Arrays.asList(
            createMockTracker(true, true), createMockTracker(true, true), createMockTracker(false, false)
        )).isSuccessful());

        // N=3: quorum=2, first child fails (any child can be the failure)
        assertTrue(new CompositeTracker(CompositeTracker.quorum(3), Arrays.asList(
            createMockTracker(false, true), createMockTracker(true, true), createMockTracker(true, true)
        )).isSuccessful());

        // N=3: only 1 succeeds → not successful
        assertFalse(new CompositeTracker(CompositeTracker.quorum(3), Arrays.asList(
            createMockTracker(true, true), createMockTracker(false, true), createMockTracker(false, false)
        )).isSuccessful());

        // N=4: quorum=3
        assertFalse(new CompositeTracker(CompositeTracker.quorum(4), Arrays.asList(
            createMockTracker(true, true), createMockTracker(true, true), createMockTracker(false, false), createMockTracker(false, false)
        )).isSuccessful());

        assertTrue(new CompositeTracker(CompositeTracker.quorum(4), Arrays.asList(
            createMockTracker(true, true), createMockTracker(true, true), createMockTracker(true, true), createMockTracker(false, false)
        )).isSuccessful());

        // N=5: quorum=3
        assertTrue(new CompositeTracker(CompositeTracker.quorum(5), Arrays.asList(
            createMockTracker(true, true), createMockTracker(true, true), createMockTracker(true, true), createMockTracker(false, false), createMockTracker(false, false)
        )).isSuccessful());
    }

    @Test
    public void testAllSuccessAndFailure()
    {
        // All succeed
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(2),
            createMockTracker(true, true),
            createMockTracker(true, true)
        );
        assertTrue(tracker.isSuccessful());
        assertTrue(tracker.isComplete());

        // First fails
        assertFalse(new CompositeTracker(CompositeTracker.all(2),
            createMockTracker(false, true),
            createMockTracker(true, true)
        ).isSuccessful());

        // Second fails
        assertFalse(new CompositeTracker(CompositeTracker.all(2),
            createMockTracker(true, true),
            createMockTracker(false, true)
        ).isSuccessful());

        // Both fail
        tracker = new CompositeTracker(CompositeTracker.all(2),
            createMockTracker(false, true),
            createMockTracker(false, true)
        );
        assertFalse(tracker.isSuccessful());
        assertTrue(tracker.isComplete());

        // Any single failure in N children → overall failure
        assertFalse(new CompositeTracker(CompositeTracker.all(3),
            createMockTracker(true, true),
            createMockTracker(true, true),
            createMockTracker(false, true)
        ).isSuccessful());
    }

    @Test
    public void testQuorumEarlyCompletionWhenSuccessful()
    {
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(3), Arrays.asList(
            createMockTracker(true, true),
            createMockTracker(true, true),
            createMockTracker(false, false)
        ));

        assertTrue(tracker.isComplete());
    }

    @Test
    public void testQuorumEarlyCompletionWhenImpossible()
    {
        // 4 children, 2 failed → max possible = 2 < quorum(3) → complete
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(4), Arrays.asList(
            createMockTracker(true, false),
            createMockTracker(false, true),
            createMockTracker(false, true),
            createMockTracker(false, false)
        ));

        assertFalse(tracker.isSuccessful());
        assertTrue(tracker.isComplete());
    }

    @Test
    public void testQuorumNotCompleteWhenStillPossible()
    {
        // 4 children: 1 succeeded, 1 failed, 2 pending → max possible = 3 >= quorum(3)
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(4), Arrays.asList(
            createMockTracker(true, true),
            createMockTracker(false, true),
            createMockTracker(false, false),
            createMockTracker(false, false)
        ));

        assertFalse(tracker.isComplete());
    }

    @Test
    public void testAllEarlyCompletionOnAnyFailure()
    {
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(3),
            createMockTracker(true, true),
            createMockTracker(false, true),   // Failed
            createMockTracker(false, false)    // Still pending
        );

        // One child has definitively failed → can't all succeed
        assertTrue(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
    }

    @Test
    public void testAllNotCompleteWhenPending()
    {
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(2),
            createMockTracker(true, true),
            createMockTracker(false, false)  // Pending, not failed
        );

        assertFalse(tracker.isComplete());
        assertFalse(tracker.isSuccessful());
    }

    @Test
    public void testOnResponseDelegatesToAll() throws Exception
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        ResponseTracker c1 = mock(ResponseTracker.class);
        ResponseTracker c2 = mock(ResponseTracker.class);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(3), c0, c1, c2);

        InetAddressAndPort ep = endpoint("127.0.0.1");
        tracker.onResponse(ep);

        verify(c0).onResponse(ep);
        verify(c1).onResponse(ep);
        verify(c2).onResponse(ep);
    }

    @Test
    public void testOnFailureDelegatesToAll() throws Exception
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        ResponseTracker c1 = mock(ResponseTracker.class);
        ResponseTracker c2 = mock(ResponseTracker.class);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(3), c0, c1, c2);

        InetAddressAndPort ep = endpoint("127.0.0.1");
        tracker.onFailure(ep, TIMEOUT);

        verify(c0).onFailure(ep, TIMEOUT);
        verify(c1).onFailure(ep, TIMEOUT);
        verify(c2).onFailure(ep, TIMEOUT);
    }

    @Test
    public void testAggregatesSums()
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        when(c0.received()).thenReturn(2);
        ResponseTracker c1 = mock(ResponseTracker.class);
        when(c1.received()).thenReturn(1);
        ResponseTracker c2 = mock(ResponseTracker.class);
        when(c2.received()).thenReturn(3);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(3), c0, c1, c2);

        assertEquals(6, tracker.received());
    }

    @Test
    public void testFailuresSum()
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        when(c0.failures()).thenReturn(1);
        ResponseTracker c1 = mock(ResponseTracker.class);
        when(c1.failures()).thenReturn(2);
        ResponseTracker c2 = mock(ResponseTracker.class);
        when(c2.failures()).thenReturn(0);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(3), c0, c1, c2);

        assertEquals(3, tracker.failures());
    }

    @Test
    public void testCountsTowardQuorumFromAny() throws Exception
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        when(c0.countsTowardQuorum(any())).thenReturn(true);
        ResponseTracker c1 = mock(ResponseTracker.class);
        when(c1.countsTowardQuorum(any())).thenReturn(false);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.all(2), c0, c1);

        assertTrue(tracker.countsTowardQuorum(endpoint("127.0.0.1")));
    }

    @Test
    public void testCountsTowardQuorumFromNone() throws Exception
    {
        ResponseTracker c0 = mock(ResponseTracker.class);
        when(c0.countsTowardQuorum(any())).thenReturn(false);
        ResponseTracker c1 = mock(ResponseTracker.class);
        when(c1.countsTowardQuorum(any())).thenReturn(false);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(2), c0, c1);

        assertFalse(tracker.countsTowardQuorum(endpoint("127.0.0.1")));
    }

    @Test
    public void testNestedComposition()
    {
        // CompositeTracker(all) containing CompositeTracker(quorum)s
        CompositeTracker inner1 = new CompositeTracker(CompositeTracker.quorum(3),
            createMockTracker(true, true),
            createMockTracker(true, true),
            createMockTracker(false, false)
        );

        CompositeTracker inner2 = new CompositeTracker(CompositeTracker.quorum(2),
            createMockTracker(true, true),
            createMockTracker(true, true)
        );

        CompositeTracker outer = new CompositeTracker(CompositeTracker.all(2), inner1, inner2);

        assertTrue(outer.isSuccessful());
        assertTrue(outer.isComplete());
    }

    @Test
    public void testNestedCompositionWithFailure()
    {
        CompositeTracker inner1 = new CompositeTracker(CompositeTracker.quorum(2),
            createMockTracker(true, true),
            createMockTracker(true, true)
        );

        CompositeTracker inner2 = new CompositeTracker(CompositeTracker.quorum(2),
            createMockTracker(false, true),
            createMockTracker(false, true)
        );

        CompositeTracker outer = new CompositeTracker(CompositeTracker.all(2), inner1, inner2);

        assertFalse(outer.isSuccessful());
        assertTrue(outer.isComplete());
    }

    @Test
    public void testConcurrentResponses() throws Exception
    {
        SimpleResponseTracker c0 = new SimpleResponseTracker(2, 3);
        SimpleResponseTracker c1 = new SimpleResponseTracker(2, 3);
        SimpleResponseTracker c2 = new SimpleResponseTracker(2, 3);

        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(3), c0, c1, c2);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(9);

        try
        {
            for (int i = 0; i < 9; i++)
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

            startLatch.countDown();
            assertTrue(doneLatch.await(10, TimeUnit.SECONDS));

            assertTrue(tracker.isSuccessful());
            assertTrue(tracker.isComplete());
            assertEquals(27, tracker.received());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullChildren()
    {
        new CompositeTracker(1, (ResponseTracker[]) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyChildren()
    {
        new CompositeTracker(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlockForZero()
    {
        new CompositeTracker(0, mock(ResponseTracker.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlockForExceedsChildren()
    {
        new CompositeTracker(3, mock(ResponseTracker.class), mock(ResponseTracker.class));
    }

    @Test
    public void testToString()
    {
        CompositeTracker tracker = new CompositeTracker(CompositeTracker.quorum(3),
            mock(ResponseTracker.class),
            mock(ResponseTracker.class),
            mock(ResponseTracker.class)
        );

        String str = tracker.toString();
        assertTrue(str.contains("CompositeTracker"));
        assertTrue(str.contains("children=3"));
        assertTrue(str.contains("blockFor=2"));
    }
}
