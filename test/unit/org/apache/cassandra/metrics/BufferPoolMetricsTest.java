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

package org.apache.cassandra.metrics;

import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPoolTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BufferPoolMetricsTest
{
    private static final BufferPoolMetrics metrics = new BufferPoolMetrics();

    @BeforeClass()
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = 16 * 1024L * 1024L;
    }

    @After
    public void cleanUp()
    {
        BufferPoolTest.resetBufferPool();
        metrics.misses.mark(metrics.misses.getCount() * -1);
    }

    @Test
    public void testMetricsSize()
    {
        // basically want to test changes in the metric being reported as the buffer pool grows - starts at zero
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isEqualTo(0);

        // the idea is to test changes in the sizeOfBufferPool metric which starts at zero. it will bump up
        // after the first request for a ByteBuffer and the idea from there will be to keep requesting them
        // until it bumps a second time at which point there should be some confidence that thie metric is
        // behaving as expected. these assertions should occur well within the value of the MEMORY_USAGE_THRESHOLD
        // given the maxBufferSize (just covering the case of the weirdest random seed in the multiverse i guess - a
        // while loop might have sufficed as well but a definitive termination seemed nicer)
        final long seed = System.currentTimeMillis();
        final Random rand = new Random(seed);
        final String assertionMessage = String.format("Failed with seed of %s", seed);
        final long maxIterations = BufferPool.MEMORY_USAGE_THRESHOLD;
        final int maxBufferSize = BufferPool.NORMAL_CHUNK_SIZE - 1;
        int nextSizeToRequest;
        long totalBytesRequestedFromPool = 0;
        long initialSizeInBytesAfterZero = 0;
        boolean exitedBeforeMax = false;
        for (int ix = 0; ix < maxIterations; ix++)
        {
            nextSizeToRequest = rand.nextInt(maxBufferSize) + 1;
            totalBytesRequestedFromPool = totalBytesRequestedFromPool + nextSizeToRequest;
            BufferPool.get(nextSizeToRequest, BufferType.OFF_HEAP);

            assertThat(metrics.size.getValue()).as(assertionMessage)
                                               .isEqualTo(BufferPool.sizeInBytes())
                                               .isGreaterThanOrEqualTo(totalBytesRequestedFromPool);

            if (initialSizeInBytesAfterZero == 0)
            {
                initialSizeInBytesAfterZero = BufferPool.sizeInBytes();
            }
            else
            {
                // when the total bytes requested from the pool exceeds the initial size we should have
                // asserted a bump in the sizeInBytes which means that we've asserted the metric increasing
                // as a result of that bump - can stop trying to grow the pool further
                if (totalBytesRequestedFromPool > initialSizeInBytesAfterZero)
                {
                    exitedBeforeMax = true;
                    break;
                }
            }
        }

        assertThat(exitedBeforeMax).as(assertionMessage).isTrue();
        assertEquals(0, metrics.misses.getCount());
    }

    @Test
    public void testMetricsMisses()
    {
        assertEquals(0, metrics.misses.getCount());

        final int tinyBufferSizeThatHits = BufferPool.NORMAL_CHUNK_SIZE - 1;
        final int bigBufferSizeThatMisses = BufferPool.NORMAL_CHUNK_SIZE + 1;

        int iterations = 16;
        for (int ix = 0; ix < iterations; ix++)
        {
            BufferPool.get(tinyBufferSizeThatHits, BufferType.OFF_HEAP);
            assertEquals(0, metrics.misses.getCount());
        }

        for (int ix = 0; ix < iterations; ix++)
        {
            BufferPool.get(bigBufferSizeThatMisses + ix, BufferType.OFF_HEAP);
            assertEquals(ix + 1, metrics.misses.getCount());
        }
    }

    @Test
    public void testZeroSizeRequestsDontChangeMetrics()
    {
        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isEqualTo(0);

        BufferPool.get(0, BufferType.OFF_HEAP);

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isEqualTo(0);

        BufferPool.get(65536, BufferType.OFF_HEAP);
        BufferPool.get(0, BufferType.OFF_HEAP);
        BufferPool.get(0, BufferType.OFF_HEAP);
        BufferPool.get(0, BufferType.OFF_HEAP);
        BufferPool.get(0, BufferType.OFF_HEAP);

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isGreaterThanOrEqualTo(65536);
    }

    @Test
    public void testFailedRequestsDontChangeMetrics()
    {
        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isEqualTo(0);

        tryRequestNegativeBufferSize();

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isEqualTo(0);

        BufferPool.get(65536, BufferType.OFF_HEAP);
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(BufferPool.sizeInBytes())
                                           .isGreaterThanOrEqualTo(65536);
    }

    private void tryRequestNegativeBufferSize()
    {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(
        () -> BufferPool.get(-1, BufferType.OFF_HEAP));
    }
}
