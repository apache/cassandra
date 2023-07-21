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

package org.apache.cassandra.service.throttler.dynamic.metrics;

import com.codahale.metrics.Counter;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

public class KeyspaceThrottlingMetrics
{
    private KeyspaceThrottlingMetricsFactory factory;

    public Counter addKSForThrottling ;
    public Counter skipSystemKSThrottling;
    public Counter trendingUpward;
    public Counter minThrottling;
    public Counter maxThrottling;
    public Counter noThrottling;


    public KeyspaceThrottlingMetrics(String ksName) {
        factory = new KeyspaceThrottlingMetricsFactory(ksName);

        addKSForThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("AddKSForThrottling"));
        skipSystemKSThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("SkipSystemKSThrottling"));
        trendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TrendingUpward"));
        minThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MinThrottling"));
        maxThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MaxThrottling"));
        noThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("NoThrottling"));
    }
}
