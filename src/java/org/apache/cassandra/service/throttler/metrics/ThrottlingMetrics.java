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

package org.apache.cassandra.service.throttler.metrics;

import com.codahale.metrics.Counter;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

public class ThrottlingMetrics
{
    public static final MetricNameFactory factory = new DefaultNameFactory("CassandraResourceUtilization");

    public Counter needsThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("NeedsThrottling"));
    public Counter doesNotNeedThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("DoesNotNeedsThrottling"));
    public Counter resetThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ResetThrottling"));
    public Counter doubleThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("DoubleThrottling"));
}
