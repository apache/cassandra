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

public class ThreadLocalCounter implements Counter
{
    private final int metricId;

    ThreadLocalCounter(int metricId)
    {
        this.metricId = metricId;
    }

    public ThreadLocalCounter()
    {
        this(PiggybackArrayThreadLocalMetrics.getMetricId());
    }

    @Override
    public void inc()
    {
        PiggybackArrayThreadLocalMetrics.add(metricId, 1);
    }

    @Override
    public void inc(long n)
    {
        PiggybackArrayThreadLocalMetrics.add(metricId, n);
    }

    @Override
    public void dec()
    {
        PiggybackArrayThreadLocalMetrics.add(metricId, -1);
    }

    @Override
    public void dec(long n)
    {
        PiggybackArrayThreadLocalMetrics.add(metricId, -n);
    }

    @Override
    public long getCount()
    {
        return PiggybackArrayThreadLocalMetrics.getCount(metricId);
    }

    @Override
    public void destroy()
    {
        PiggybackArrayThreadLocalMetrics.destroyMetric(metricId);
    }
}
