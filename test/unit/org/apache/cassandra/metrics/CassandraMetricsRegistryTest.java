/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.metrics;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

import org.junit.Test;

import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.apache.cassandra.utils.EstimatedHistogram;


public class CassandraMetricsRegistryTest
{
    // A class with a name ending in '$'
    private static class StrangeName$
    {
    }

    @Test
    public void testChooseType()
    {
        assertEquals("StrangeName", MetricName.chooseType(null, StrangeName$.class));
        assertEquals("StrangeName", MetricName.chooseType("", StrangeName$.class));
        assertEquals("String", MetricName.chooseType(null, String.class));
        assertEquals("String", MetricName.chooseType("", String.class));

        assertEquals("a", MetricName.chooseType("a", StrangeName$.class));
        assertEquals("b", MetricName.chooseType("b", String.class));
    }

    @Test
    public void testMetricName()
    {
        MetricName name = new MetricName(StrangeName$.class, "NaMe", "ScOpE");
        assertEquals("StrangeName", name.getType());
    }

    @Test
    public void testJvmMetricsRegistration()
    {
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;

        // Same registration as CassandraDaemon
        registry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        registry.register("jvm.gc", new GarbageCollectorMetricSet());
        registry.register("jvm.memory", new MemoryUsageGaugeSet());

        Collection<String> names = registry.getNames();

        // No metric with ".." in name
        assertTrue(names.stream()
                        .filter(name -> name.contains(".."))
                        .count()
                   == 0);

        // There should be several metrics within each category
        for (String category : new String[]{"jvm.buffers","jvm.gc","jvm.memory"})
        {
            assertTrue(names.stream()
                            .filter(name -> name.startsWith(category+'.'))
                            .count() > 1);
        }
    }

    @Test
    public void testDeltaBaseCase()
    {
        long[] last = new long[10];
        long[] now = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        // difference between all zeros and a value should be the value
        assertArrayEquals(now, CassandraMetricsRegistry.delta(now, last));
        // the difference between itself should be all 0s
        assertArrayEquals(last, CassandraMetricsRegistry.delta(now, now));
        // verifying each value is calculated
        assertArrayEquals(new long[] {1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
                CassandraMetricsRegistry.delta(new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, now));
    }

    @Test
    public void testDeltaHistogramSizeChange()
    {
        long[] count = new long[]{0, 1, 2, 3, 4, 5};
        assertArrayEquals(count, CassandraMetricsRegistry.delta(count, new long[3]));
        assertArrayEquals(new long[6], CassandraMetricsRegistry.delta(count, new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    }

    /**
     * Test the updated timer values are estimated correctly (i.e., in the valid range, 1.2) in the micros based histogram.
     */
    @Test
    public void testTimer()
    {
        long[] offsets = EstimatedHistogram.newOffsets(DecayingEstimatedHistogramReservoir.LOW_BUCKET_COUNT, false);
        Timer timer = new Timer(CassandraMetricsRegistry.createReservoir(TimeUnit.MICROSECONDS));
        timer.update(42, TimeUnit.NANOSECONDS);
        timer.update(100, TimeUnit.NANOSECONDS);
        timer.update(42, TimeUnit.MICROSECONDS);
        timer.update(100, TimeUnit.MICROSECONDS);
        timer.update(42, TimeUnit.MILLISECONDS);
        timer.update(100, TimeUnit.MILLISECONDS);
        timer.update(100, TimeUnit.SECONDS);
        timer.update(100, TimeUnit.MINUTES);
        int maxSeconds = 21356;
        timer.update(maxSeconds, TimeUnit.SECONDS);
        long[] counts = timer.getSnapshot().getValues();
        int expectedBucketsWithValues = 8;
        int bucketsWithValues = 0;
        for (int i = 0; i < counts.length; i++)
        {
            if (counts[i] != 0)
            {
                bucketsWithValues ++;
                assertTrue(
                inRange(offsets[i], TimeUnit.NANOSECONDS.toMicros(42), 1.2)
                || inRange(offsets[i], TimeUnit.NANOSECONDS.toMicros(100), 1.2)
                || inRange(offsets[i], TimeUnit.MICROSECONDS.toMicros(42), 1.2)
                || inRange(offsets[i], TimeUnit.MICROSECONDS.toMicros(100), 1.2)
                || inRange(offsets[i], TimeUnit.MILLISECONDS.toMicros(42), 1.2)
                || inRange(offsets[i], TimeUnit.MILLISECONDS.toMicros(100), 1.2)
                || inRange(offsets[i], TimeUnit.SECONDS.toMicros(100), 1.2)
                || inRange(offsets[i], TimeUnit.MINUTES.toMicros(100), 1.2)
                || inRange(offsets[i], TimeUnit.SECONDS.toMicros(maxSeconds), 1.2)
                );
            }
        }
        assertEquals("42 and 100 nanos should both be put in the first bucket",
                            2, counts[0]);
        assertEquals(expectedBucketsWithValues, bucketsWithValues);
    }

    private boolean inRange(long anchor, long input, double range)
    {
        return input / ((double) anchor) < range;
    }
}
