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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

public class AvailableDiskMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("AvailableDisk");

    /* disk needed for a compaction activity */
    public static Histogram compactionNeeded = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("CompactionNeeded"), false);

    /* disk available for a compaction activity */
    public static Histogram compactionAvailable = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("CompactionAvailable"), false);

    /* track the insufficient disk issue for a compaction activity */
    public static Meter compactionInsufficient = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("CompactionInsufficient"));

    /* disk needed for a other activities */
    public static Histogram otherNeeded = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("OtherNeeded"), false);

    /* disk available for a other activities */
    public static Histogram otherAvailable = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("OtherAvailable"), false);

    /* track the insufficient disk issue for other activities */
    public static Meter otherInsufficient = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("OtherInsufficient"));
}
