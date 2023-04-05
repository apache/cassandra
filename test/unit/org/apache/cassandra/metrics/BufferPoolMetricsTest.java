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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

public class BufferPoolMetricsTest
{
    private BufferPool bufferPool;
    private BufferPoolMetrics metrics;

    @BeforeClass()
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        this.bufferPool = new BufferPool("test_" + System.currentTimeMillis(), 16 * 1024L * 1024L, true);
        this.metrics = bufferPool.metrics();
    }

    @Test
    public void testMetricsSize()
    {
        // basically want to test changes in the metric being reported as the buffer pool grows - starts at zero
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
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
        final long maxIterations = bufferPool.memoryUsageThreshold();
        final int maxBufferSize = BufferPool.NORMAL_CHUNK_SIZE - 1;
        int nextSizeToRequest;
        long totalBytesRequestedFromPool = 0;
        long initialSizeInBytesAfterZero = 0;
        boolean exitedBeforeMax = false;
        for (int ix = 0; ix < maxIterations; ix++)
        {
            nextSizeToRequest = rand.nextInt(maxBufferSize) + 1;
            totalBytesRequestedFromPool = totalBytesRequestedFromPool + nextSizeToRequest;
            bufferPool.get(nextSizeToRequest, BufferType.OFF_HEAP);

            assertThat(metrics.size.getValue()).as(assertionMessage)
                                               .isEqualTo(bufferPool.sizeInBytes())
                                               .isGreaterThanOrEqualTo(totalBytesRequestedFromPool);

            if (initialSizeInBytesAfterZero == 0)
            {
                initialSizeInBytesAfterZero = bufferPool.sizeInBytes();
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
    public void testMetricsOverflowSize()
    {
        assertEquals(0, metrics.overflowSize.getValue().longValue());

        final int tinyBufferSizeThatHits = BufferPool.NORMAL_CHUNK_SIZE - 1;
        final int bigBufferSizeThatMisses = BufferPool.NORMAL_CHUNK_SIZE + 1;

        int iterations = 16;
        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(tinyBufferSizeThatHits, BufferType.OFF_HEAP);
            assertEquals(0, metrics.overflowSize.getValue().longValue());
        }

        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(bigBufferSizeThatMisses, BufferType.OFF_HEAP);
            assertEquals(bigBufferSizeThatMisses * (ix + 1), metrics.overflowSize.getValue().longValue());
        }
    }

    @Test
    public void testMetricsUsedSize()
    {
        assertEquals(0, metrics.usedSize.getValue().longValue());

        final int tinyBufferSizeThatHits = BufferPool.NORMAL_CHUNK_SIZE - 1;
        final int bigBufferSizeThatMisses = BufferPool.NORMAL_CHUNK_SIZE + 1;

        long usedSize = 0;
        int iterations = 16;
        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(tinyBufferSizeThatHits, BufferType.OFF_HEAP);
            assertEquals(usedSize += tinyBufferSizeThatHits, metrics.usedSize.getValue().longValue());
        }

        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(bigBufferSizeThatMisses, BufferType.OFF_HEAP);
            assertEquals(usedSize += bigBufferSizeThatMisses, metrics.usedSize.getValue().longValue());
        }
    }

    @Test
    public void testMetricsHits()
    {
        assertEquals(0, metrics.hits.getCount());

        final int tinyBufferSizeThatHits = BufferPool.NORMAL_CHUNK_SIZE - 1;
        final int bigBufferSizeThatMisses = BufferPool.NORMAL_CHUNK_SIZE + 1;

        int iterations = 16;
        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(tinyBufferSizeThatHits, BufferType.OFF_HEAP);
            assertEquals(ix + 1, metrics.hits.getCount());
        }

        long currentHits = metrics.hits.getCount();
        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(bigBufferSizeThatMisses + ix, BufferType.OFF_HEAP);
            assertEquals(currentHits, metrics.hits.getCount());
        }
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
            bufferPool.get(tinyBufferSizeThatHits, BufferType.OFF_HEAP);
            assertEquals(0, metrics.misses.getCount());
        }

        for (int ix = 0; ix < iterations; ix++)
        {
            bufferPool.get(bigBufferSizeThatMisses + ix, BufferType.OFF_HEAP);
            assertEquals(ix + 1, metrics.misses.getCount());
        }
    }

    @Test
    public void testZeroSizeRequestsDontChangeMetrics()
    {
        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isEqualTo(0);

        bufferPool.get(0, BufferType.OFF_HEAP);

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isEqualTo(0);

        bufferPool.get(65536, BufferType.OFF_HEAP);
        bufferPool.get(0, BufferType.OFF_HEAP);
        bufferPool.get(0, BufferType.OFF_HEAP);
        bufferPool.get(0, BufferType.OFF_HEAP);
        bufferPool.get(0, BufferType.OFF_HEAP);

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isGreaterThanOrEqualTo(65536);
    }

    @Test
    public void testFailedRequestsDontChangeMetrics()
    {
        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isEqualTo(0);

        tryRequestNegativeBufferSize();

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isEqualTo(0);

        bufferPool.get(65536, BufferType.OFF_HEAP);
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();
        tryRequestNegativeBufferSize();

        assertEquals(0, metrics.misses.getCount());
        assertThat(metrics.size.getValue()).isEqualTo(bufferPool.sizeInBytes())
                                           .isGreaterThanOrEqualTo(65536);
    }

    private void tryRequestNegativeBufferSize()
    {
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(
        () -> bufferPool.get(-1, BufferType.OFF_HEAP));
    }
}
