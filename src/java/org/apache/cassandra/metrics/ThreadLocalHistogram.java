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

import com.codahale.metrics.Snapshot;

/**
 * An alternative to Dropwizard Histogram which implements the same kind of API.
 * it has more efficent counting operations and consumes less memory.
 * The counter logic is implemented using {@link ThreadLocalMetrics} functionality.
 *
 * NOTE: Dropwizard Histogram is a concrete class and there is no an interface for Dropwizard Histogram logic,
 *   so we have to create an alternative API hierarchy.
 */
public class ThreadLocalHistogram extends OverrideHistogram
{
    private final int countMetricId;

    /**
     * Creates a new {@link ThreadLocalHistogram} with the given reservoir.
     *
     * @param reservoir the reservoir to create a histogram from
     */
    public ThreadLocalHistogram(CassandraReservoir reservoir)
    {
        super(reservoir);
        this.countMetricId = ThreadLocalMetrics.allocateMetricId();
        ThreadLocalMetrics.destroyWhenUnreachable(this, countMetricId);
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(int value)
    {
        update((long) value);
    }

    /**
     * Adds a recorded value.
     *
     * @param value the length of the value
     */
    public void update(long value)
    {
        ThreadLocalMetrics.add(countMetricId, 1);
        reservoir.update(value);
    }

    /**
     * Returns the number of values recorded.
     *
     * @return the number of values recorded
     */
    @Override
    public long getCount()
    {
        return ThreadLocalMetrics.getCount(countMetricId);
    }

    public void reset()
    {
        ThreadLocalMetrics.getCountAndReset(countMetricId);
    }

    @Override
    public Snapshot getSnapshot()
    {
        return reservoir.getSnapshot();
    }
}
