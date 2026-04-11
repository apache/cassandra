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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;

/**
 * An alternative to Dropwizard Timer which implements the same kind of API.
 * it has more efficent latency histogram implementation and consumes less memory.
 *
 * NOTE: Dropwizard Timer is a concrete class and there is no an interface for Dropwizard Timer logic,
 *   so we have to create an alternative hierarchy.
 */
public class ThreadLocalTimer extends OverrideTimer
{
    /**
     * Creates a new {@link Timer} using an {@link ExponentiallyDecayingReservoir} and the default
     * {@link MetricClock}.
     */
    public ThreadLocalTimer()
    {
        this(new DecayingEstimatedHistogramReservoir());
    }

    /**
     * Creates a new {@link Timer} that uses the given {@link Reservoir}.
     *
     * @param reservoir the {@link Reservoir} implementation the timer should use
     */
    public ThreadLocalTimer(CassandraReservoir reservoir)
    {
        this(reservoir, MetricClock.defaultClock());
    }

    /**
     * Creates a new {@link Timer} that uses the given {@link Reservoir} and {@link MetricClock}.
     *
     * @param reservoir the {@link Reservoir} implementation the timer should use
     * @param clock     the {@link MetricClock} implementation the timer should use
     */
    public ThreadLocalTimer(CassandraReservoir reservoir, MetricClock clock)
    {
        // the precise clock is intentionally not propagated to ThreadLocalMeter
        // we do not need a precise and more expensive time within the meter
        this(new ThreadLocalMeter(), new ThreadLocalHistogram(reservoir), clock);
    }

    public ThreadLocalTimer(Meter meter, ThreadLocalHistogram histogram, MetricClock clock)
    {
        // original Codahale meter and histogram are set to null to reduce memory footprint
        super(meter, histogram, clock);
    }
}
