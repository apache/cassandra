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

package org.apache.cassandra.locator.dynamicsnitch;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ExponentialMovingAverage;


/**
 * A dynamic snitching implementation that uses Exponentially Moving Averages as a low pass filter to prefer
 * or de-prefer hosts
 *
 * This implementation generates a few orders of magnitude less garbage than histograms and is close to 10x faster,
 * but as it is not a Median LPF (it is an Average LPF), it is more vulnerable to noise. This may be acceptable but
 * given the significant change in behavior this is not the default in 4.0
 */
public class DynamicEndpointSnitchEMA extends DynamicEndpointSnitch
{
    // A ~10 sample EMA heavily weighted to the past values to minimize noise
    private static final double EMA_ALPHA = 0.10;

    protected static class EMASnitchMeasurement implements ISnitchMeasurement
    {
        public final ExponentialMovingAverage avg;

        EMASnitchMeasurement(double initial)
        {
            avg = new ExponentialMovingAverage(EMA_ALPHA, initial);
        }

        @Override
        public void sample(long value)
        {
            avg.update(value);
        }

        @Override
        public double measure()
        {
            return avg.getAvg();
        }

        @Override
        public Iterable<Double> measurements()
        {
            return Collections.singletonList(avg.getAvg());
        }
    }

    // Called via reflection
    public DynamicEndpointSnitchEMA(IEndpointSnitch snitch)
    {
        this(snitch, "ema");
    }

    public DynamicEndpointSnitchEMA(IEndpointSnitch snitch, String instance)
    {
        super(snitch, instance);
    }

    @Override
    protected ISnitchMeasurement measurementImpl(long initialValue)
    {
        return new EMASnitchMeasurement(initialValue);
    }

    /**
     * Unlike the Histogram implementation, calling this measure method is reasonably cheap (doesn't require a
     * Snapshot or anything) so we can skip a round of iterations and just normalize the scores slightly
     * differently
     */
    @Override
    public Map<InetAddressAndPort, Double> calculateScores()
    {
        // We're going to weight the latency for each host against the worst one we see, to
        // arrive at sort of a 'badness percentage' for them. First, find the worst for each:
        HashMap<InetAddressAndPort, Double> newScores = new HashMap<>(samples.size());
        Optional<Double> maxObservedAvgLatency = samples.values().stream()
                                                        .map(am -> am.measurement)
                                                        .map(ISnitchMeasurement::measure)
                                                        .max(Double::compare);

        final double maxAvgLatency = maxObservedAvgLatency.isPresent() ? maxObservedAvgLatency.get() : 1;

        // now make another pass to do the weighting based on the maximums we found before
        for (Map.Entry<InetAddressAndPort, AnnotatedMeasurement> entry : samples.entrySet())
        {
            // Samples may have changed but rather than creating garbage by copying samples we just ensure
            // that all scores are less than 1.0
            double addrAvg = entry.getValue().measurement.measure();
            double score = addrAvg / Math.max(addrAvg, maxAvgLatency);
            // finally, add the severity without any weighting, since hosts scale this relative to their own load
            // and the size of the task causing the severity.
            // "Severity" is basically a measure of compaction activity (CASSANDRA-3722).
            if (USE_SEVERITY)
                score += getSeverity(entry.getKey());
            // lowest score (least amount of badness) wins.
            newScores.put(entry.getKey(), score);
        }
        return newScores;
    }

    @VisibleForTesting
    protected void reset()
    {
        super.reset();
    }
}
