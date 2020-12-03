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

package org.apache.cassandra.distributed.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

/**
 * Pulls metrics out of in-JVM dtest cluster instance.
 */
class InstanceMetrics implements Metrics
{
    private final CassandraMetricsRegistry metricsRegistry;

    InstanceMetrics(CassandraMetricsRegistry metricsRegistry)
    {
        this.metricsRegistry = metricsRegistry;
    }

    public List<String> getNames()
    {
        return new ArrayList<>(metricsRegistry.getNames());
    }

    public long getCounter(String name)
    {
        return metricsRegistry.getCounters().get(name).getCount();
    }

    public Map<String, Long> getCounters(Predicate<String> filter)
    {
        Map<String, Long> values = new HashMap<>();
        for (Map.Entry<String, Counter> e : metricsRegistry.getCounters().entrySet())
        {
            if (filter.test(e.getKey()))
                values.put(e.getKey(), e.getValue().getCount());
        }
        return values;
    }

    public double getHistogram(String name, MetricValue value)
    {
        Histogram histogram = metricsRegistry.getHistograms().get(name);
        if (value == MetricValue.COUNT)
            return histogram.getCount();

        return getValue(histogram.getSnapshot(), value);
    }

    public Map<String, Double> getHistograms(Predicate<String> filter, MetricValue value)
    {
        Map<String, Double> values = new HashMap<>();
        for (Map.Entry<String, Histogram> e : metricsRegistry.getHistograms().entrySet())
        {
            if (filter.test(e.getKey()))
                values.put(e.getKey(), getValue(e.getValue().getSnapshot(), value));
        }
        return values;
    }

    public Object getGauge(String name)
    {
        return metricsRegistry.getGauges().get(name).getValue();
    }

    public Map<String, Object> getGauges(Predicate<String> filter)
    {
        Map<String, Object> values = new HashMap<>();
        for (Map.Entry<String, Gauge> e : metricsRegistry.getGauges().entrySet())
        {
            if (filter.test(e.getKey()))
                values.put(e.getKey(), e.getValue().getValue());
        }
        return values;
    }

    public double getMeter(String name, Rate value)
    {
        return getRate(metricsRegistry.getMeters().get(name), value);
    }

    public Map<String, Double> getMeters(Predicate<String> filter, Rate rate)
    {
        Map<String, Double> values = new HashMap<>();
        for (Map.Entry<String, Meter> e : metricsRegistry.getMeters().entrySet())
        {
            if (filter.test(e.getKey()))
                values.put(e.getKey(), getRate(e.getValue(), rate));
        }
        return values;
    }

    public double getTimer(String name, MetricValue value)
    {
        return getValue(metricsRegistry.getTimers().get(name).getSnapshot(), value);
    }

    public Map<String, Double> getTimers(Predicate<String> filter, MetricValue value)
    {
        Map<String, Double> values = new HashMap<>();
        for (Map.Entry<String, Timer> e : metricsRegistry.getTimers().entrySet())
        {
            if (filter.test(e.getKey()))
                values.put(e.getKey(), getValue(e.getValue().getSnapshot(), value));
        }

        return values;
    }

    static double getValue(Snapshot snapshot, MetricValue value)
    {
        switch (value)
        {
            case MEDIAN:
                return snapshot.getMedian();
            case P75:
                return snapshot.get75thPercentile();
            case P95:
                return snapshot.get95thPercentile();
            case P98:
                return snapshot.get98thPercentile();
            case P99:
                return snapshot.get99thPercentile();
            case P999:
                return snapshot.get999thPercentile();
            case MAX:
                return snapshot.getMax();
            case MEAN:
                return snapshot.getMean();
            case MIN:
                return snapshot.getMin();
            case STDDEV:
                return snapshot.getStdDev();
            default:
                throw new RuntimeException("Shouldn't happen");
        }
    }

    static double getRate(Meter meter, Rate rate)
    {
        switch (rate)
        {
            case RATE15_MIN:
                return meter.getFifteenMinuteRate();
            case RATE5_MIN:
                return meter.getFiveMinuteRate();
            case RATE1_MIN:
                return meter.getOneMinuteRate();
            case RATE_MEAN:
                return meter.getMeanRate();
            default:
                throw new RuntimeException("Shouldn't happen");
        }
    }
}
