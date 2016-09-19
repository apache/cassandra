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
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static org.apache.cassandra.net.RateBasedBackPressure.FACTOR;
import static org.apache.cassandra.net.RateBasedBackPressure.FLOW;
import static org.apache.cassandra.net.RateBasedBackPressure.HIGH_RATIO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RateBasedBackPressureTest
{
    @Test(expected = IllegalArgumentException.class)
    public void testAcceptsNoLessThanThreeArguments() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "1"), new TestTimeSource(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeBiggerThanZero() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0", FACTOR, "2", FLOW, "FAST"), new TestTimeSource(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeSmallerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "2", FACTOR, "2", FLOW, "FAST"), new TestTimeSource(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFactorMustBeBiggerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "0", FLOW, "FAST"), new TestTimeSource(), 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWindowSizeMustBeBiggerEqualThanTen() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "5", FLOW, "FAST"), new TestTimeSource(), 1);
    }

    @Test
    public void testFlowMustBeEitherFASTorSLOW() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "FAST"), new TestTimeSource(), 10);
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "SLOW"), new TestTimeSource(), 10);
        try
        {
            new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "WRONG"), new TestTimeSource(), 10);
            fail("Expected to fail with wrong flow type.");
        }
        catch (Exception ex)
        {
        }
    }

    @Test
    public void testBackPressureStateUpdates()
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);

        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onMessageSent(null);
        assertEquals(0, state.incomingRate.size());
        assertEquals(0, state.outgoingRate.size());

        state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onResponseReceived();
        assertEquals(1, state.incomingRate.size());
        assertEquals(1, state.outgoingRate.size());

        state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onResponseTimeout();
        assertEquals(0, state.incomingRate.size());
        assertEquals(1, state.outgoingRate.size());
    }

    @Test
    public void testBackPressureIsNotUpdatedBeyondInfinity() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());

        // Get initial rate:
        double initialRate = state.rateLimiter.getRate();
        assertEquals(Double.POSITIVE_INFINITY, initialRate, 0.0);

        // Update incoming and outgoing rate equally:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the rate doesn't change because already at infinity:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        assertEquals(initialRate, state.rateLimiter.getRate(), 0.0);
    }

    @Test
    public void testBackPressureIsUpdatedOncePerWindowSize() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());

        // Get initial time:
        long current = state.getLastIntervalAcquire();
        assertEquals(0, current);

        // Update incoming and outgoing rate:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);

        // Move time ahead by window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the timestamp changed:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        current = state.getLastIntervalAcquire();
        assertEquals(timeSource.currentTimeMillis(), current);

        // Move time ahead by less than interval:
        long previous = current;
        timeSource.sleep(windowSize / 2, TimeUnit.MILLISECONDS);

        // Verify the last timestamp didn't change because below the window size:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        current = state.getLastIntervalAcquire();
        assertEquals(previous, current);
    }

    @Test
    public void testBackPressureWhenBelowHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());

        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the rate is decreased by factor:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        assertEquals(7.4, state.rateLimiter.getRate(), 0.1);
    }

    @Test
    public void testBackPressureRateLimiterIsIncreasedAfterGoingAgainAboveHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());

        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the rate decreased:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        assertEquals(7.4, state.rateLimiter.getRate(), 0.1);

        // Update incoming and outgoing rate back above high rate:
        state.incomingRate.update(50);
        state.outgoingRate.update(50);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify rate limiter is increased by factor:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        assertEquals(8.25, state.rateLimiter.getRate(), 0.1);

        // Update incoming and outgoing rate to keep it below the limiter rate:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify rate limiter is not increased as already higher than the actual rate:
        strategy.apply(Sets.newHashSet(state), 1, TimeUnit.SECONDS);
        assertEquals(8.25, state.rateLimiter.getRate(), 0.1);
    }

    @Test
    public void testBackPressureFastFlow() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        RateBasedBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        RateBasedBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(50);
        state1.outgoingRate.update(100);
        state2.incomingRate.update(80); // fast
        state2.outgoingRate.update(100);
        state3.incomingRate.update(20);
        state3.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the fast replica rate limiting has been applied:
        Set<RateBasedBackPressureState> replicaGroup = Sets.newHashSet(state1, state2, state3);
        strategy.apply(replicaGroup, 1, TimeUnit.SECONDS);
        assertTrue(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(12.0, strategy.getRateLimiterForReplicaGroup(replicaGroup).getRate(), 0.1);
    }

    @Test
    public void testBackPressureSlowFlow() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "SLOW"), timeSource, windowSize);
        RateBasedBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        RateBasedBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        RateBasedBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(50);
        state1.outgoingRate.update(100);
        state2.incomingRate.update(100);
        state2.outgoingRate.update(100);
        state3.incomingRate.update(20); // slow
        state3.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the slow replica rate limiting has been applied:
        Set<RateBasedBackPressureState> replicaGroup = Sets.newHashSet(state1, state2, state3);
        strategy.apply(replicaGroup, 1, TimeUnit.SECONDS);
        assertTrue(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(3.0, strategy.getRateLimiterForReplicaGroup(replicaGroup).getRate(), 0.1);
    }

    @Test
    public void testBackPressureWithDifferentGroups() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "SLOW"), timeSource, windowSize);
        RateBasedBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        RateBasedBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        RateBasedBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));
        RateBasedBackPressureState state4 = strategy.newState(InetAddress.getByName("127.0.0.4"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(50); // this
        state1.outgoingRate.update(100);
        state2.incomingRate.update(100);
        state2.outgoingRate.update(100);
        state3.incomingRate.update(20); // this
        state3.outgoingRate.update(100);
        state4.incomingRate.update(80);
        state4.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the first group:
        Set<RateBasedBackPressureState> replicaGroup = Sets.newHashSet(state1, state2);
        strategy.apply(replicaGroup, 1, TimeUnit.SECONDS);
        assertTrue(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(7.4, strategy.getRateLimiterForReplicaGroup(replicaGroup).getRate(), 0.1);

        // Verify the second group:
        replicaGroup = Sets.newHashSet(state3, state4);
        strategy.apply(replicaGroup, 1, TimeUnit.SECONDS);
        assertTrue(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(3.0, strategy.getRateLimiterForReplicaGroup(replicaGroup).getRate(), 0.1);
    }

    @Test
    public void testBackPressurePastTimeout() throws Exception
    {
        long windowSize = 10000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "SLOW"), timeSource, windowSize);
        RateBasedBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        RateBasedBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        RateBasedBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(5); // slow
        state1.outgoingRate.update(100);
        state2.incomingRate.update(100);
        state2.outgoingRate.update(100);
        state3.incomingRate.update(100);
        state3.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the slow replica rate limiting has been applied:
        Set<RateBasedBackPressureState> replicaGroup = Sets.newHashSet(state1, state2, state3);
        strategy.apply(replicaGroup, 4, TimeUnit.SECONDS);
        assertTrue(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(0.5, strategy.getRateLimiterForReplicaGroup(replicaGroup).getRate(), 0.1);

        // Make one more apply call to saturate the rate limit timeout (0.5 requests per second means 2 requests span
        // 4 seconds, but we can only make one as we have to subtract the incoming response time):
        strategy.apply(replicaGroup, 4, TimeUnit.SECONDS);

        // Now verify another call to apply doesn't acquire the rate limit because of the max timeout of 4 seconds minus
        // 2 seconds of response time, so the time source itself sleeps two second:
        long start = timeSource.currentTimeMillis();
        strategy.apply(replicaGroup, 4, TimeUnit.SECONDS);
        assertFalse(strategy.checkAcquired());
        assertTrue(strategy.checkApplied());
        assertEquals(TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS),
                     strategy.timeout);
        assertEquals(strategy.timeout,
                     TimeUnit.NANOSECONDS.convert(timeSource.currentTimeMillis() - start, TimeUnit.MILLISECONDS));
    }

    public static class TestableBackPressure extends RateBasedBackPressure
    {
        public volatile boolean acquired = false;
        public volatile boolean applied = false;
        public volatile long timeout;

        public TestableBackPressure(Map<String, Object> args, TimeSource timeSource, long windowSize)
        {
            super(args, timeSource, windowSize);
        }

        @Override
        public boolean doRateLimit(RateLimiter rateLimiter, long timeoutInNanos)
        {
            acquired = super.doRateLimit(rateLimiter, timeoutInNanos);
            applied = true;
            timeout = timeoutInNanos;
            return acquired;
        }

        public boolean checkAcquired()
        {
            boolean checked = acquired;
            acquired = false;
            return checked;
        }

        public boolean checkApplied()
        {
            boolean checked = applied;
            applied = false;
            return checked;
        }
    }
}
