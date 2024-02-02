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
    public KeyspaceThrottlingMetricsFactory factory;

    public Counter addKSForReadThrottling;
    public Counter addKSForRangeThrottling;
    public Counter addKSForWriteThrottling;
    public Counter skipKSThrottling;
    public Counter readRequestsTrendingUpward;
    public Counter rangeRequestsTrendingUpward;
    public Counter writeRequestsTrendingUpward;
    public Counter readLatencyTrendingUpward;
    public Counter rangeLatencyTrendingUpward;
    public Counter writeLatencyTrendingUpward;
    public Counter minReadThrottling;
    public Counter minWriteThrottling;
    public Counter maxReadThrottling;
    public Counter maxWriteThrottling;
    public Counter noReadThrottling;
    public Counter noWriteThrottling;
    public Counter aggressiveThrottling;
    public Counter hardBlockCoordReads;
    public Counter hardBlockCoordWrites;
    public Counter hardBlockReplicaReads;
    public Counter hardBlockReplicaWrites;


    public KeyspaceThrottlingMetrics(String ksName) {
        factory = new KeyspaceThrottlingMetricsFactory(ksName);

        addKSForReadThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("AddKSForReadThrottling"));
        addKSForRangeThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("AddKSForRangeThrottling"));
        addKSForWriteThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("AddKSForWriteThrottling"));
        skipKSThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("SkipKSThrottling"));
        readRequestsTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ReadRequestsTrendingUpward"));
        rangeRequestsTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("RangeRequestsTrendingUpward"));
        writeRequestsTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("WriteRequestsTrendingUpward"));
        readLatencyTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ReadLatencyTrendingUpward"));
        rangeLatencyTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("RangeLatencyTrendingUpward"));
        writeLatencyTrendingUpward = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("WriteLatencyTrendingUpward"));
        minReadThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MinReadThrottling"));
        minWriteThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MinWriteThrottling"));
        maxReadThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MaxReadThrottling"));
        maxWriteThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("MaxWriteThrottling"));
        noReadThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("NoReadThrottling"));
        noWriteThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("NoWriteThrottling"));
        aggressiveThrottling = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("AggressiveThrottling"));
        hardBlockCoordReads = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HardBlockCoordReads"));
        hardBlockCoordWrites = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HardBlockCoordWrites"));
        hardBlockReplicaReads = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HardBlockReplicaReads"));
        hardBlockReplicaWrites = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HardBlockReplicaWrites"));
    }
}
