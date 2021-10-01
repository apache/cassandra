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

package org.apache.cassandra.streaming;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter.BYTES_PER_MEGABIT;
import static org.junit.Assert.assertEquals;

public class StreamManagerTest
{
    private static int defaultStreamThroughputMbPerSec;
    private static int defaultInterDCStreamThroughputMbPerSec;

    @BeforeClass
    public static void setupClass()
    {
        Config c = DatabaseDescriptor.loadConfig();
        defaultStreamThroughputMbPerSec = c.stream_throughput_outbound_megabits_per_sec;
        defaultInterDCStreamThroughputMbPerSec = c.inter_dc_stream_throughput_outbound_megabits_per_sec;
        DatabaseDescriptor.daemonInitialization(() -> c);
    }

    @Test
    public void testUpdateStreamThroughput()
    {
        // Initialized value check
        assertEquals(defaultStreamThroughputMbPerSec * BYTES_PER_MEGABIT, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setStreamThroughputMbPerSec(500);
        assertEquals(500.0d * BYTES_PER_MEGABIT, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Max positive value check
        StorageService.instance.setStreamThroughputMbPerSec(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE * BYTES_PER_MEGABIT, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setStreamThroughputMbPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Negative value check
        StorageService.instance.setStreamThroughputMbPerSec(-200);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getRateLimiterRateInBytes(), 0);
    }

    @Test
    public void testUpdateInterDCStreamThroughput()
    {
        // Initialized value check
        assertEquals(defaultInterDCStreamThroughputMbPerSec * BYTES_PER_MEGABIT, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(200);
        assertEquals(200.0d * BYTES_PER_MEGABIT, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Max positive value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE * BYTES_PER_MEGABIT, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Negative value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(-200);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);
    }
}
