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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class DenylistMetrics
{
    private final Meter writesRejected;
    private final Meter readsRejected;
    private final Meter rangeReadsRejected;
    private final Meter totalRequestsRejected;

    public DenylistMetrics()
    {
        final MetricNameFactory factory = new DefaultNameFactory("StorageProxy", "PartitionDenylist");
        writesRejected = Metrics.meter(factory.createMetricName("WriteRejected"));
        readsRejected = Metrics.meter(factory.createMetricName("ReadRejected"));
        rangeReadsRejected = Metrics.meter(factory.createMetricName("RangeReadRejected"));
        totalRequestsRejected = Metrics.meter(factory.createMetricName("TotalRejected"));
    }

    public void incrementWritesRejected()
    {
        writesRejected.mark();
        totalRequestsRejected.mark();
    }

    public void incrementReadsRejected()
    {
        readsRejected.mark();
        totalRequestsRejected.mark();
    }

    public void incrementRangeReadsRejected()
    {
        rangeReadsRejected.mark();
        totalRequestsRejected.mark();
    }
}