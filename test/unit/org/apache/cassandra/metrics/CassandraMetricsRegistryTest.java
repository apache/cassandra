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

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testTickMeters() throws InterruptedException
    {
        CassandraMetricsRegistry cmr = new CassandraMetricsRegistry(TimeUnit.SECONDS.toMicros(1));
        int numMeters = 1000;
        CountDownLatch ticked = new CountDownLatch(numMeters);
        AtomicInteger counted = new AtomicInteger();
        Meter m = new Meter()
        {
            @Override
            public double getOneMinuteRate() {
                if (counted.incrementAndGet() % 2 == 0)
                    throw new RuntimeException("test failure handling");
                ticked.countDown();
                return super.getOneMinuteRate();
            }
        };
        for (int ii = 0; ii < numMeters; ii++)
        {
            cmr.register("ignored" + ii, m);
        }
        assertNotNull(cmr.periodicMeterTicker);
        assertTrue(cmr.periodicMeterTicker.getDelay(TimeUnit.SECONDS) <= 1);
        assertTrue(ticked.await(1, TimeUnit.MINUTES));
    }
}
