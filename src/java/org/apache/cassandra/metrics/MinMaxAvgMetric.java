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

import com.codahale.metrics.Gauge;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class MinMaxAvgMetric
{
    private final MetricNameFactory factory;
    private final String namePrefix;

    final Gauge<Long> minGauge;
    final Gauge<Long> maxGauge;
    final Gauge<Double> avgGauge;
    final Gauge<Double> stddevGauge;
    final Gauge<Integer> numSamplesGauge;

    private long min;
    private long max;
    private long sum;
    private long sumSquares;
    private int numSamples;

    public MinMaxAvgMetric(MetricNameFactory factory, String namePrefix)
    {
        this.factory = factory;
        this.namePrefix = namePrefix;

        minGauge = Metrics.register(factory.createMetricName(namePrefix + "Min"), () -> min);
        maxGauge = Metrics.register(factory.createMetricName(namePrefix + "Max"), () -> max);
        avgGauge = Metrics.register(factory.createMetricName(namePrefix + "Avg"), () -> numSamples > 0 ? ((double) sum) / numSamples : 0);
        stddevGauge = Metrics.register(factory.createMetricName(namePrefix + "StdDev"), () -> stddev());
        numSamplesGauge = Metrics.register(factory.createMetricName(namePrefix + "NumSamples"), () -> numSamples);
    }

    public void release()
    {
        Metrics.remove(factory.createMetricName(namePrefix + "Min"));
        Metrics.remove(factory.createMetricName(namePrefix + "Max"));
        Metrics.remove(factory.createMetricName(namePrefix + "Avg"));
        Metrics.remove(factory.createMetricName(namePrefix + "StdDev"));
        Metrics.remove(factory.createMetricName(namePrefix + "NumSamples"));
    }

    public void reset()
    {
        sum = 0;
        sumSquares = 0;
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
        numSamples = 0;
    }

    public void update(long value)
    {
        max = max > value ? max : value;
        min = min < value ? min : value;
        sum += value;
        sumSquares += value * value;
        numSamples++;
    }

    private Double stddev()
    {
        if (numSamples > 0)
        {
            double avgSquare = ((double) sumSquares) / numSamples;
            double avg = ((double) sum) / numSamples;
            return Math.sqrt(avgSquare - avg * avg);
        }
        else
        {
            return 0.0;
        }
    }

    @Override
    public String toString()
    {
        return "{" +
               "min=" + min +
               ", max=" + max +
               ", avg=" + (sum * 1.0 / numSamples) +
               ", stdDev=" + stddev() +
               ", numSamples=" + numSamples +
               '}';
    }
}
