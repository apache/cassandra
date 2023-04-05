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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LatencyMetricsTest
{
    private final MetricNameFactory factory = new TestMetricsNameFactory();

    private class TestMetricsNameFactory implements MetricNameFactory
    {

        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            return new CassandraMetricsRegistry.MetricName(TestMetricsNameFactory.class, metricName);
        }
    }

    /**
     * Test bitsets in a "real-world" environment, i.e., bloom filters
     */
    @Test
    public void testGetRecentLatency()
    {
        final LatencyMetrics l = new LatencyMetrics("test", "test");
        Runnable r = () -> {
            for (int i = 0; i < 10000; i++)
            {
                l.addNano(1000);
            }
        };
        new Thread(r).start();

        for (int i = 0; i < 10000; i++)
        {
            Double recent = l.latency.getOneMinuteRate();
            assertFalse(recent.equals(Double.POSITIVE_INFINITY));
        }
    }

    /**
     * Test that parent LatencyMetrics are receiving updates from child metrics when reading
     */
    @Test
    public void testReadMerging()
    {
        final LatencyMetrics parent = new LatencyMetrics("testMerge", "testMerge");
        final LatencyMetrics child = new LatencyMetrics(factory, "testChild", parent);

        for (int i = 0; i < 100; i++)
        {
            child.addNano(TimeUnit.NANOSECONDS.convert(i, TimeUnit.MILLISECONDS));
        }

        assertEquals(4950000, child.totalLatency.getCount());
        assertEquals(child.totalLatency.getCount(), parent.totalLatency.getCount());
        assertEquals(child.latency.getSnapshot().getMean(), parent.latency.getSnapshot().getMean(), 50D);

        child.release();
        parent.release();
    }

    @Test
    public void testRelease()
    {
        final LatencyMetrics parent = new LatencyMetrics("testRelease", "testRelease");
        final LatencyMetrics child = new LatencyMetrics(factory, "testChildRelease", parent);

        for (int i = 0; i < 100; i++)
        {
            child.addNano(TimeUnit.NANOSECONDS.convert(i, TimeUnit.MILLISECONDS));
        }

        double mean = parent.latency.getSnapshot().getMean();
        long count = parent.totalLatency.getCount();

        child.release();

        // Check that no value was lost with the release
        assertEquals(count, parent.totalLatency.getCount());
        assertEquals(mean, parent.latency.getSnapshot().getMean(), 50D);

        parent.release();
    }
}
