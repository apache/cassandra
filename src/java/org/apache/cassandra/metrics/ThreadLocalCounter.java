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

import org.apache.cassandra.utils.ReflectionUtils;

/**
 * An alternative to Dropwizard Counter which implements the same kind of API.
 * it has more efficient inc/dec operations and consumes less memory.
 * The counter logic is implemented using {@link ThreadLocalMetrics} functionality.
 *
 * NOTE: Dropwizard Counter is a concrete class and there is no an interface for Dropwizard Counter logic,
 *   so we have to create an alternative hierarchy.
*/
public class ThreadLocalCounter extends com.codahale.metrics.Counter implements Counter
{
    private final int metricId;

    ThreadLocalCounter(int metricId)
    {
        this.metricId = metricId;
        ThreadLocalMetrics.destroyWhenUnreachable(this, metricId);
        ReflectionUtils.setFieldToNull(this, com.codahale.metrics.Counter.class, "count"); // reduce metrics memory footprint
    }

    public ThreadLocalCounter()
    {
        this(ThreadLocalMetrics.allocateMetricId());
    }

    @Override
    public void inc()
    {
        ThreadLocalMetrics.add(metricId, 1);
    }

    @Override
    public void inc(long n)
    {
        ThreadLocalMetrics.add(metricId, n);
    }

    @Override
    public void dec()
    {
        ThreadLocalMetrics.add(metricId, -1);
    }

    @Override
    public void dec(long n)
    {
        ThreadLocalMetrics.add(metricId, -n);
    }

    @Override
    public long getCount()
    {
        return ThreadLocalMetrics.getCount(metricId);
    }
}
