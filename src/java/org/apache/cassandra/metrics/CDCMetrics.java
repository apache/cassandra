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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Gauge;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CDCMetrics
{
    public static final MetricNameFactory factory = new DefaultNameFactory("CDCManager");

    public final Meter successes;
    public final Meter failures;

    // errors and timeouts are not related to the count of mutations, but the generic occurrences of such cases
    public final Meter errors;
    public final Meter timeouts;
    public final Timer latency;

    public CDCMetrics()
    {
        successes = Metrics.meter(factory.createMetricName("Successes"));
        failures = Metrics.meter(factory.createMetricName("Failures"));
        errors = Metrics.meter(factory.createMetricName("Errors"));
        timeouts = Metrics.meter(factory.createMetricName("Timeouts"));
        latency = Metrics.timer(factory.createMetricName("Latency"));
    }
}
