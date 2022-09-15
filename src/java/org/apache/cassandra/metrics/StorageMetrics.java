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

import java.util.function.ToLongFunction;
import java.util.stream.StreamSupport;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.Keyspace;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Storage");

    public static final Counter load = Metrics.counter(factory.createMetricName("Load"));
    public static final Counter uncompressedLoad = Metrics.counter(factory.createMetricName("UncompressedLoad"));

    public static final Gauge<Long> unreplicatedLoad =
        createSummingGauge("UnreplicatedLoad", metric -> metric.unreplicatedLiveDiskSpaceUsed.getValue());
    public static final Gauge<Long> unreplicatedUncompressedLoad =
        createSummingGauge("UnreplicatedUncompressedLoad", metric -> metric.unreplicatedUncompressedLiveDiskSpaceUsed.getValue());

    public static final Counter uncaughtExceptions = Metrics.counter(factory.createMetricName("Exceptions"));
    public static final Counter totalHintsInProgress  = Metrics.counter(factory.createMetricName("TotalHintsInProgress"));
    public static final Counter totalHints = Metrics.counter(factory.createMetricName("TotalHints"));
    public static final Counter repairExceptions = Metrics.counter(factory.createMetricName("RepairExceptions"));

    private static Gauge<Long> createSummingGauge(String name, ToLongFunction<KeyspaceMetrics> extractor)
    {
        return Metrics.register(factory.createMetricName(name),
                                () -> StreamSupport.stream(Keyspace.all().spliterator(), false)
                                                   .mapToLong(keyspace -> extractor.applyAsLong(keyspace.metric))
                                                   .sum());
    }
}
