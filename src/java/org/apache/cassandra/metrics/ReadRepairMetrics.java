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

/**
 * Metrics related to Read Repair.
 */
public class ReadRepairMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("ReadRepair");

    public static final Meter repairedBlocking = Metrics.meter(factory.createMetricName("RepairedBlocking"));
    public static final Meter reconcileRead = Metrics.meter(factory.createMetricName("ReconcileRead"));

    /** @deprecated See CASSANDRA-13910 */
    @Deprecated(since = "4.0")
    public static final Meter repairedBackground = Metrics.meter(factory.createMetricName("RepairedBackground"));
    /** @deprecated See CASSANDRA-13910 */
    @Deprecated(since = "4.0")
    public static final Meter attempted = Metrics.meter(factory.createMetricName("Attempted"));
    public static final Meter timedOut = Metrics.meter(factory.createMetricName("RepairTimedOut"));

    // Incremented when additional requests were sent during blocking read repair due to unavailable or slow nodes
    public static final Meter speculatedRead = Metrics.meter(factory.createMetricName("SpeculatedRead"));
    public static final Meter speculatedWrite = Metrics.meter(factory.createMetricName("SpeculatedWrite"));

    public static void init() {}
}
