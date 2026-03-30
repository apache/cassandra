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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Meter;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.WaitStrategy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryTest
{
    private static final Logger logger = LoggerFactory.getLogger(RetryTest.class);
    private Random random;
    private static final Meter testMeter = new Meter();

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        long seed = System.nanoTime();
        logger.info("Running test with seed {}", seed);
        random = new Random(seed);
    }

    @Test
    public void testRetryWithNoTimeLimitObservesTimeUnit()
    {
        Meter meter = new Meter();
        final long waitTimeNanos = Math.abs(random.nextLong());
        WaitStrategy fixed = new WaitStrategy()
        {
            @Override
            public long computeWaitUntil(int attempts)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long computeWait(int attempts, TimeUnit units)
            {
                assertEquals(NANOSECONDS, units);
                return waitTimeNanos;
            }
        };

        Retry retry = Retry.withNoTimeLimit(meter, fixed);
        long waitTimeMillis = retry.computeWait(1, MILLISECONDS);
        assertEquals(MILLISECONDS.convert(waitTimeNanos, NANOSECONDS), waitTimeMillis);
    }


    @Test
    public void testProcessorIndefiniteRetryBehaviour()
    {
        Retry retryPolicy = Retry.unsafeRetryIndefinitely();
        // Assert the properties of the Retry provided by the private static Processor::unsafeRetryIndefinitely
        for (int i = 1; i < 1000; i++)
        {
            // backoff increases in 100ms steps, up to a max of 10000ms
            long waitTime = retryPolicy.computeWait(i, MILLISECONDS);
            assertEquals(Math.min((i + 1) * 100, 10000), waitTime);
        }
        // Retry indefinitely means no explicit deadline is set
        assertEquals(Long.MAX_VALUE, retryPolicy.deadlineNanos);
    }

    @Test
    public void testExponentialJitterValueDistribution()
    {
        String spec = String.format("0ms ... %dms * 2^attempts <= %dms", 100, 10000);
        WaitStrategy jitter = RetryStrategy.parse(spec,
                                                  TimeoutStrategy.LatencySourceFactory.none(),
                                                  RetryStrategy.randomizers.uniform());
        for (int i = 0; i < 1000; i++)
        {
            long sleep = jitter.computeWait(i, TimeUnit.MILLISECONDS);
            Assertions.assertThat(sleep).isNotNegative().isLessThanOrEqualTo(10000);
        }
    }

    @Test
    public void testExponentialJitterEarlyAttemptsSmall()
    {
        // With base=100ms and cap=60000ms, first attempt max = min(60000, 100 * 2^1) = 200ms
        String spec = String.format("0ms ... %dms * 2^attempts <= %dms", 100, 60000);
        WaitStrategy jitter = RetryStrategy.parse(spec,
                                                  TimeoutStrategy.LatencySourceFactory.none(),
                                                  RetryStrategy.randomizers.uniform());
        long firstSleep = jitter.computeWait(1, TimeUnit.MILLISECONDS);
        // tries is 0 initially, so expBackoff = min(60000, 100 * 2^0) = 100
        Assertions.assertThat(firstSleep).isNotNegative().isLessThan(100);
    }

    @Test
    public void testExponentialJitterWithDeadline()
    {
        long deadlineNanos = System.nanoTime() + 100_000_000L; // 100ms from now
        String spec = String.format("0ms ... %dms * 2^attempts <= %dms", 100, 1000);
        WaitStrategy jitter = RetryStrategy.parse(spec,
                                                  TimeoutStrategy.LatencySourceFactory.none(),
                                                  RetryStrategy.randomizers.uniform());
        Retry deadline = Retry.until(deadlineNanos, testMeter, jitter);

        assertFalse("Should not have reached deadline yet", deadline.hasExpired());
        assertTrue("Remaining should be positive", deadline.remainingNanos() > 0);
    }

    @Test
    public void testExponentialJitterWithExpiredDeadline() throws InterruptedException
    {
        long deadlineNanos = System.nanoTime() + 1_000_000L; // 1ms from now
        String spec = String.format("0ms ... %dms * 2^attempts <= %dms", 100, 1000);
        WaitStrategy jitter = RetryStrategy.parse(spec,
                                                  TimeoutStrategy.LatencySourceFactory.none(),
                                                  RetryStrategy.randomizers.uniform());
        Retry deadline = Retry.until(deadlineNanos, testMeter, jitter);

        Thread.sleep(5); // ensure deadline passes
        assertTrue("Should have reached deadline", deadline.hasExpired());
        assertEquals("Remaining should be 0", 0, deadline.remainingNanos());
    }

    @Test
    public void testExponentialJitterOverflowProtection()
    {
        // Regardless of the number of attemps, the jittered wait should be capped at 5s
        String spec = String.format("0ms ... %dms * 2^attempts <= %dms", 100, 5000);
        WaitStrategy jitter = RetryStrategy.parse(spec,
                                                  TimeoutStrategy.LatencySourceFactory.none(),
                                                  RetryStrategy.randomizers.uniform());
        // Should not throw or return negative
        long sleep = jitter.computeWait(500, TimeUnit.MILLISECONDS);
        assertTrue("Sleep should be within specified min/max even after 500 tries: " + sleep, sleep >= 0 && sleep <= 5000);
    }
}
