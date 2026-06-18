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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.replication.MutationTrackingService.DISABLED_MESSAGE;

public class MutationTrackingMetrics
{
    public static final String TYPE_NAME = "MutationTracking";
    private static final MetricNameFactory factory = new DefaultNameFactory(TYPE_NAME);

    private static final MutationTrackingMetrics instance = DatabaseDescriptor.getMutationTrackingEnabled() ? new MutationTrackingMetrics() : null;

    public static MutationTrackingMetrics instance()
    {
        if (instance == null)
            throw new IllegalStateException(DISABLED_MESSAGE);
        return instance;
    }

    public final Counter broadcastOffsetsDiscovered; // Newly-witnessed offsets discovered via broadcast
    public final Counter writeTimeOffsetsDiscovered; // Newly-witnessed offsets discovered at write time
    public final Histogram readSummarySize; // Read summary sizes
    public final Gauge<Long> unreconciledMutationCount; // Number of unreconciled mutations
    public final Gauge<Long> journalDiskSpaceUsed; // Size of MutationJournal on disk
    public final Gauge<Integer> pendingClearReplaySize; // Static segments awaiting clearNeedsReplay

    @SuppressWarnings("Convert2MethodRef")
    private MutationTrackingMetrics()
    {
        broadcastOffsetsDiscovered = Metrics.counter(factory.createMetricName("BroadcastOffsetsDiscovered"));
        writeTimeOffsetsDiscovered = Metrics.counter(factory.createMetricName("WriteTimeOffsetsDiscovered"));
        readSummarySize = Metrics.histogram(factory.createMetricName("ReadSummarySize"), true);
        unreconciledMutationCount = Metrics.register(
                factory.createMetricName("UnreconciledMutationCount"),
                () -> MutationTrackingService.instance().getUnreconciledMutationCount()
        );
        journalDiskSpaceUsed = Metrics.register(
                factory.createMetricName("JournalDiskSpaceUsed"),
                () -> MutationJournal.instance().getDiskSpaceUsed()
        );
        pendingClearReplaySize = Metrics.register(
                factory.createMetricName("PendingClearReplaySize"),
                () -> MutationJournal.pendingClearReplaySize()
        );
    }
}