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
    public static final String TYPE_NAME = "ReadRepair";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    public static final Meter repairedBlocking = Metrics.meter(factory.createMetricName("RepairedBlocking"));

    /**
     * Non-transactional read did a blocking read repair via an Accord transaction. This is expected/normal if non-transactional
     * reads are interoperating with Accord.
     */
    public static final Meter repairedBlockingViaAccord = Metrics.meter(factory.createMetricName("RepairedBlockingViaAccord"));

    /**
     * This should be zero if you are trying to run Accord in a 100% correct way and interoperating with non-transactional writes.
     *
     * An Accord transaction read at QUORUM and ended up having to do BRR to make something it read monotonic. While it
     * will be monotonic this is not 100% deterministic for transaction recovery because different Accord coordinators could
     * read different things when computing a transaction's writes.
     *
     * If Accord is operating in TransactionalMode.full and the range is migrated then this metric will be zero just
     * because Accord is reading at ONE not QUORUM and there are should be no non-transactional writes anywyas.
     */
    public static final Meter repairedBlockingFromAccord = Metrics.meter(factory.createMetricName("RepairedBlockingFromAccord"));
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
