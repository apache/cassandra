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

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import accord.impl.progresslog.DefaultProgressLog;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.Invariants;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.metrics.LogLinearHistogram.LogLinearSnapshot;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.utils.Clock;

import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static org.apache.cassandra.metrics.AccordMetricUtils.fromDurabilityService;
import static org.apache.cassandra.metrics.AccordMetricUtils.fromTopologyManager;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordSystemMetrics
{
    public final static AccordSystemMetrics metrics = new AccordSystemMetrics();
    private static final long REFRESH_RATE = TimeUnit.SECONDS.toNanos(30);

    public static final String ACCORD_SYSTEM = "AccordSystem";
    public static final String MIN_EPOCH = "MinEpoch";
    public static final String MAX_EPOCH = "MaxEpoch";
    public static final String PROGRESS_LOG_ACTIVE = "ProgressLogActive";
    public static final String PROGRESS_LOG_SIZE = "ProgressLogSize";
    public static final String PROGRESS_LOG_AGE = "ProgressLogAge";
    public static final String DURABILITY_QUEUE_ACTIVE = "DurabilityQueueActive";
    public static final String DURABILITY_QUEUE_PENDING = "DurabilityQueuePending";
    public static final String SYNCPOINT_AGREED_LAG = "SyncPointAgreedLag";
    public static final String LOCALLY_APPLIED_LAG = "LocallyAppliedLag";
    public static final String LOCALLY_DURABLE_LAG = "LocallyDurableLag";
    public static final String QUORUM_APPLIED_LAG = "QuorumAppliedLag";
    public static final String SHARD_APPLIED_LAG = "ShardAppliedLag";
    public static final String GC_LAG = "GCLag";

    public final Gauge<Long> minEpoch;
    public final Gauge<Long> maxEpoch;
    public final Gauge<Long> progressLogActive;
    public final Gauge<Long> durabilityQueueActive;
    public final Gauge<Long> durabilityQueuePending;
    public final OnDemandHistogram progressLogSize;
    public final OnDemandHistogram progressLogAge;
    public final OnDemandHistogram syncPointAgreedLag;
    public final OnDemandHistogram locallyAppliedLag;
    public final OnDemandHistogram locallyDurableLag;
    public final OnDemandHistogram quorumAppliedLag;
    public final OnDemandHistogram shardAppliedLag;
    public final OnDemandHistogram gcLag;

    private Snapshot snapshot;

    static class Snapshot
    {
        final long at;
        final long progressLogActive;
        final LogLinearSnapshot progressLogSize;
        final LogLinearSnapshot progressLogAge;
        final LogLinearSnapshot syncPointAgreedLag;
        final LogLinearSnapshot locallyAppliedLag;
        final LogLinearSnapshot locallyDurableLag;
        final LogLinearSnapshot quorumAppliedLag;
        final LogLinearSnapshot shardAppliedLag;
        final LogLinearSnapshot gcLag;

        Snapshot()
        {
            this.at = Clock.Global.nanoTime();
            progressLogActive = 0;
            progressLogSize = progressLogAge = syncPointAgreedLag = locallyAppliedLag = locallyDurableLag = gcLag = quorumAppliedLag = shardAppliedLag = new LogLinearSnapshot(0);
        }

        Snapshot(SnapshotBuilder builder)
        {
            this.at = Clock.Global.nanoTime();
            this.progressLogActive = builder.progressLogActive;
            this.progressLogSize = builder.progressLogSize.destroyToSnapshot();
            this.progressLogAge = builder.progressLogAge.destroyToSnapshot();
            this.syncPointAgreedLag = builder.syncPointAgreedLag.destroyToSnapshot();
            this.locallyAppliedLag = builder.locallyAppliedLag.destroyToSnapshot();
            this.locallyDurableLag = builder.locallyDurableLag.destroyToSnapshot();
            this.quorumAppliedLag = builder.quorumAppliedLag.destroyToSnapshot();
            this.shardAppliedLag = builder.shardAppliedLag.destroyToSnapshot();
            this.gcLag = builder.gcLag.destroyToSnapshot();
        }
    }

    static class SnapshotBuilder
    {
        long progressLogActive;
        final LogLinearHistogram progressLogSize = new LogLinearHistogram(100);
        final LogLinearHistogram progressLogAge = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram syncPointAgreedLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram locallyAppliedLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram locallyDurableLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram quorumAppliedLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram shardAppliedLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));
        final LogLinearHistogram gcLag = new LogLinearHistogram(TimeUnit.HOURS.toSeconds(1L));

        SnapshotBuilder()
        {
        }
    }

    private AccordSystemMetrics()
    {
        Invariants.expect(AccordService.isSetup());
        DefaultNameFactory factory = new DefaultNameFactory(ACCORD_SYSTEM);
        minEpoch = Metrics.gauge(factory.createMetricName(MIN_EPOCH), fromTopologyManager(TopologyManager::minEpoch));
        maxEpoch = Metrics.gauge(factory.createMetricName(MAX_EPOCH), fromTopologyManager(TopologyManager::epoch));
        durabilityQueueActive = Metrics.gauge(factory.createMetricName(DURABILITY_QUEUE_ACTIVE), fromDurabilityService(durability -> (long)durability.queue().activeCount()));
        durabilityQueuePending = Metrics.gauge(factory.createMetricName(DURABILITY_QUEUE_PENDING), fromDurabilityService(durability -> (long)durability.queue().pendingCount()));
        progressLogActive = Metrics.gauge(factory.createMetricName(PROGRESS_LOG_ACTIVE), fromDurabilityService(durability -> (long)durability.queue().activeCount()));
        progressLogSize = Metrics.onDemandHistogram(factory.createMetricName(PROGRESS_LOG_SIZE), () -> maybeRefreshHistograms().progressLogSize);
        progressLogAge = Metrics.onDemandHistogram(factory.createMetricName(PROGRESS_LOG_AGE), () -> maybeRefreshHistograms().progressLogAge);
        syncPointAgreedLag = Metrics.onDemandHistogram(factory.createMetricName(SYNCPOINT_AGREED_LAG), () -> maybeRefreshHistograms().syncPointAgreedLag);
        locallyAppliedLag = Metrics.onDemandHistogram(factory.createMetricName(LOCALLY_APPLIED_LAG), () -> maybeRefreshHistograms().locallyAppliedLag);
        locallyDurableLag = Metrics.onDemandHistogram(factory.createMetricName(LOCALLY_DURABLE_LAG), () -> maybeRefreshHistograms().locallyDurableLag);
        quorumAppliedLag = Metrics.onDemandHistogram(factory.createMetricName(QUORUM_APPLIED_LAG), () -> maybeRefreshHistograms().quorumAppliedLag);
        shardAppliedLag = Metrics.onDemandHistogram(factory.createMetricName(SHARD_APPLIED_LAG), () -> maybeRefreshHistograms().shardAppliedLag);
        gcLag = Metrics.onDemandHistogram(factory.createMetricName(GC_LAG), () -> maybeRefreshHistograms().gcLag);
    }

    private synchronized Snapshot maybeRefreshHistograms()
    {
        if (snapshot == null || Clock.Global.nanoTime() >= snapshot.at + REFRESH_RATE)
            refreshHistograms();
        return snapshot;
    }

    private synchronized void refreshHistograms()
    {
        if (!AccordService.isSetup())
        {
            snapshot = new Snapshot();
            return;
        }

        IAccordService service = AccordService.instance();
        if (!service.isEnabled())
        {
            snapshot = new Snapshot();
            return;
        }

        int nowSeconds = (int) (Clock.Global.currentTimeMillis() / 1000);
        SnapshotBuilder builder = new SnapshotBuilder();
        service.node().commandStores().forEachCommandStore(commandStore -> {
            DefaultProgressLog.ImmutableView view = ((DefaultProgressLog)commandStore.unsafeProgressLog()).immutableView();
            builder.progressLogActive += view.activeCount();
            builder.progressLogSize.increment(view.size());
            while (view.advance())
                builder.progressLogAge.increment(ageSeconds(nowSeconds, view.txnId()));

            RedundantBefore redundantBefore = commandStore.unsafeGetRedundantBefore();
            for (int i = 0 ; i < redundantBefore.size() ; ++i)
            {
                RedundantBefore.Bounds bounds = redundantBefore.valueAt(i);
                builder.locallyAppliedLag.increment(ageSeconds(nowSeconds, bounds.maxBound(LOCALLY_APPLIED)));
                builder.locallyDurableLag.increment(ageSeconds(nowSeconds, bounds.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE)));
                builder.quorumAppliedLag.increment(ageSeconds(nowSeconds, bounds.maxBound(QUORUM_APPLIED)));
                builder.shardAppliedLag.increment(ageSeconds(nowSeconds, bounds.maxBound(SHARD_APPLIED)));
                builder.gcLag.increment(ageSeconds(nowSeconds, bounds.maxBound(GC_BEFORE)));
            }

            MaxDecidedRX maxDecidedRX = commandStore.unsafeGetMaxDecidedRX();
            for (int i = 0 ; i < maxDecidedRX.size() ; ++i)
                builder.syncPointAgreedLag.increment(ageSeconds(nowSeconds, maxDecidedRX.valueAt(i).any));
        });
        this.snapshot = new Snapshot(builder);
    }

    static int ageSeconds(int nowSeconds, TxnId txnId)
    {
        return Math.max(0, nowSeconds - (int)TimeUnit.MICROSECONDS.toSeconds(txnId.hlc()));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AccordSystemMetrics [");

        try
        {
            for (Field f : getClass().getDeclaredFields())
            {
                f.setAccessible(true);
                if (Counting.class.isAssignableFrom(f.getType()))
                {
                    Counting metric = (Counting) f.get(this);
                    builder.append(String.format("%s: count=%d, ", f.getName(), metric.getCount()));
                }
            }
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        builder.append(']');
        return builder.toString();
    }
}
