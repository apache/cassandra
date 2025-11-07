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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.WaitStrategy;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.Startup;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.tcm.membership.MembershipUtils.node;
import static org.apache.cassandra.tcm.membership.MembershipUtils.nodeAddresses;
import static org.junit.Assert.assertEquals;

public class RetryTest
{
    private static final Logger logger = LoggerFactory.getLogger(RetryTest.class);
    private Random random;

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
            public long computeWaitUntil(int attempts) {throw new UnsupportedOperationException();}

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
        new Processor()
        {
            @Override
            public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy) {return null;}

            @Override
            public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry retryPolicy)
            {
                // Assert the properties of the Retry provided by the private static Processor::unsafeRetryIndefinitely
                for (int i = 1; i<1000;i++)
                {
                    // backoff increases in 100ms steps, up to a max of 10000ms
                    long waitTime = retryPolicy.computeWait(i, MILLISECONDS);
                    assertEquals(Math.min((i+1) * 100, 10000), waitTime);
                }
                // Retry indefinitely means no explicit deadline is set
                assertEquals(Long.MAX_VALUE, retryPolicy.deadlineNanos);
                return null;
            }
        }.commit(new Entry.Id(0L), new Startup(node(random), nodeAddresses(random), NodeVersion.CURRENT), Epoch.EMPTY);
    }
}
