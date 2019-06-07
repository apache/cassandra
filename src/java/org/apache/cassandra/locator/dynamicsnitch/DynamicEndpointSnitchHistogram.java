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

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;

/**
 * A dynamic snitching implementation that uses Exponentially Decaying Histograms to prefer or
 * de-prefer hosts
 *
 * This measurement technique was the default implementation prior to Cassandra 4.0 and is being left as the default
 * in 4.0, although this class uses the superclass provided latency probes.
 */
public class DynamicEndpointSnitchHistogram extends DynamicEndpointSnitch
{
    private static final double ALPHA = 0.75; // set to 0.75 to make DES more biased to towards the newer values
    private static final int WINDOW_SIZE = 100;

    // Essentially a Median filter
    protected static class ReservoirSnitchMeasurement implements ISnitchMeasurement
    {
        public final Reservoir reservoir;

        ReservoirSnitchMeasurement()
        {
            reservoir = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);
        }

        public void sample(long value)
        {
            reservoir.update(value);
        }

        @Override
        public double measure()
        {
            return reservoir.getSnapshot().getMedian();
        }

        @Override
        public Iterable<Double> measurements()
        {
            long[] timings = reservoir.getSnapshot().getValues();
            List<Double> measurements = new ArrayList<>(timings.length);
            for (double timing : timings)
                measurements.add(timing);
            return measurements;
        }
    }

    // Called via reflection
    public DynamicEndpointSnitchHistogram(IEndpointSnitch snitch)
    {
        this(snitch, null);
    }

    public DynamicEndpointSnitchHistogram(IEndpointSnitch snitch, String instance)
    {
        super(snitch, instance);
    }

    @Override
    protected ISnitchMeasurement measurementImpl(long initialValue)
    {
        return new ReservoirSnitchMeasurement();
    }

    @VisibleForTesting
    protected void reset()
    {
        super.reset();
    }
}
