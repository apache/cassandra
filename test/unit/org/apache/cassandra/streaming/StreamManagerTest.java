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
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import static org.apache.cassandra.streaming.StreamManager.StreamRateLimiter.BYTES_PER_MEBIBYTE;
import static org.junit.Assert.assertEquals;

public class StreamManagerTest
{
    private static int defaultStreamThroughputMebibytesPerSec;
    private static int defaultInterDCStreamThroughputMebibytesPerSec;
    private static final int INTEGER_MAX_VALUE_MEGABITS_IN_MEBIBYTES = DataRateSpec
                                                                       .megabitsPerSecondInMebibytesPerSecond(Integer.MAX_VALUE)
                                                                       .toMebibytesPerSecondAsInt();

    private static int defaultEntireSSTableStreamThroughputMebibytesPerSec;
    private static int defaultEntireSSTableInterDCStreamThroughputMebibytesPerSec;


    @BeforeClass
    public static void setupClass()
    {
        Config c = DatabaseDescriptor.loadConfig();

        defaultStreamThroughputMebibytesPerSec = c.stream_throughput_outbound.toMebibytesPerSecondAsInt();
        defaultInterDCStreamThroughputMebibytesPerSec = c.inter_dc_stream_throughput_outbound.toMebibytesPerSecondAsInt();
        defaultEntireSSTableStreamThroughputMebibytesPerSec = c.entire_sstable_stream_throughput_outbound.toMebibytesPerSecondAsInt();
        defaultEntireSSTableInterDCStreamThroughputMebibytesPerSec = c.entire_sstable_inter_dc_stream_throughput_outbound.toMebibytesPerSecondAsInt();

        DatabaseDescriptor.daemonInitialization(() -> c);
    }

    @Test
    public void testUpdateStreamThroughput()
    {
        // Initialized value check
        assertEquals(defaultStreamThroughputMebibytesPerSec * BYTES_PER_MEBIBYTE, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setStreamThroughputMbPerSec(500); //60MiB/s
        assertEquals(60d * BYTES_PER_MEBIBYTE, Math.round(StreamRateLimiter.getRateLimiterRateInBytes()), 0);

        // Max positive value check
        StorageService.instance.setStreamThroughputMbPerSec(Integer.MAX_VALUE);
        assertEquals(INTEGER_MAX_VALUE_MEGABITS_IN_MEBIBYTES * BYTES_PER_MEBIBYTE, StreamRateLimiter.getRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setStreamThroughputMbPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getRateLimiterRateInBytes(), 0);
    }

    @Test
    public void testUpdateEntireSSTableStreamThroughput()
    {
        // Initialized value check (defaults to StreamRateLimiter.getRateLimiterRateInBytes())
        assertEquals(defaultEntireSSTableStreamThroughputMebibytesPerSec * BYTES_PER_MEBIBYTE, StreamRateLimiter.getEntireSSTableRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(1500);
        assertEquals(1500d * BYTES_PER_MEBIBYTE, Math.round(StreamRateLimiter.getEntireSSTableRateLimiterRateInBytes()), 0);

        // Max positive value check
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE * BYTES_PER_MEBIBYTE, StreamRateLimiter.getEntireSSTableRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setEntireSSTableStreamThroughputMebibytesPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getEntireSSTableRateLimiterRateInBytes(), 0);
    }

    @Test
    public void testUpdateInterDCStreamThroughput()
    {
        // Initialized value check
        assertEquals(defaultInterDCStreamThroughputMebibytesPerSec * BYTES_PER_MEBIBYTE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(200); //approximately 24MiB/s
        assertEquals(24.0d * BYTES_PER_MEBIBYTE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Max positive value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(Integer.MAX_VALUE);
        assertEquals(INTEGER_MAX_VALUE_MEGABITS_IN_MEBIBYTES * BYTES_PER_MEBIBYTE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setInterDCStreamThroughputMbPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getInterDCRateLimiterRateInBytes(), 0);
    }

    @Test
    public void testUpdateEntireSSTableInterDCStreamThroughput()
    {
        // Initialized value check (Defaults to StreamRateLimiter.getInterDCRateLimiterRateInBytes())
        assertEquals(defaultEntireSSTableInterDCStreamThroughputMebibytesPerSec * BYTES_PER_MEBIBYTE, StreamRateLimiter.getEntireSSTableInterDCRateLimiterRateInBytes(), 0);

        // Positive value check
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(1200);
        assertEquals(1200.0d * BYTES_PER_MEBIBYTE, StreamRateLimiter.getEntireSSTableInterDCRateLimiterRateInBytes(), 0);

        // Max positive value check
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE * BYTES_PER_MEBIBYTE, StreamRateLimiter.getEntireSSTableInterDCRateLimiterRateInBytes(), 0);

        // Zero value check
        StorageService.instance.setEntireSSTableInterDCStreamThroughputMebibytesPerSec(0);
        assertEquals(Double.MAX_VALUE, StreamRateLimiter.getEntireSSTableInterDCRateLimiterRateInBytes(), 0);
    }
}
