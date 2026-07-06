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

import com.codahale.metrics.Counting;

import accord.api.ReplicaEventListener;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.TxnId;

import org.apache.cassandra.metrics.LogLinearDecayingHistograms.LogLinearDecayingHistogram;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.DecayingHistogramsShard;
import org.apache.cassandra.metrics.ShardedDecayingHistograms.ShardedDecayingHistogram;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.HISTOGRAMS;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class AccordReplicaMetrics
{
    public final static AccordReplicaMetrics readMetrics = new AccordReplicaMetrics("ro");
    public final static AccordReplicaMetrics writeMetrics = new AccordReplicaMetrics("rw");
    public final static AccordReplicaMetrics syncPointMetrics = new AccordReplicaMetrics("rx");

    public static final String ACCORD_REPLICA = "AccordReplica";
    public static final String REPLICA_STABLE_LATENCY = "StableLatency";
    public static final String REPLICA_PREAPPLY_LATENCY = "PreApplyLatency";
    public static final String REPLICA_APPLY_LATENCY = "ApplyLatency";
    public static final String REPLICA_DEPENDENCIES = "Dependencies";

    static final class SubShard
    {
        final LogLinearDecayingHistogram stableLatency;
        final LogLinearDecayingHistogram preapplyLatency;
        final LogLinearDecayingHistogram applyLatency;
        final LogLinearDecayingHistogram dependencies;

        private SubShard(AccordReplicaMetrics metrics, DecayingHistogramsShard shard)
        {
            this.stableLatency = metrics.stableLatency.forShard(shard);
            this.preapplyLatency = metrics.preapplyLatency.forShard(shard);
            this.applyLatency = metrics.applyLatency.forShard(shard);
            this.dependencies = metrics.dependencies.forShard(shard);
        }
    }

    public static final class Shard
    {
        final SubShard reads, writes, syncPoints;
        public Shard(DecayingHistogramsShard shard)
        {
            reads = new SubShard(readMetrics, shard);
            writes = new SubShard(writeMetrics, shard);
            syncPoints = new SubShard(syncPointMetrics, shard);
        }
    }

    /**
     * The time between start on the coordinator and commit on this replica.
     */
    public final ShardedDecayingHistogram stableLatency = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));

    /**
     * The time between start on the coordinator and arrival of the result on this replica.
     */
    public final ShardedDecayingHistogram preapplyLatency = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));

    /**
     * The time between start on the coordinator and application on this replica.
     */
    public final ShardedDecayingHistogram applyLatency = HISTOGRAMS.newHistogram(TimeUnit.SECONDS.toNanos(1L));

    /**
     * A histogram of the number of dependencies per transaction at this replica.
     */
    public final ShardedDecayingHistogram dependencies = HISTOGRAMS.newHistogram(1 << 12);

    private AccordReplicaMetrics(String scope)
    {
        DefaultNameFactory replica = new DefaultNameFactory(ACCORD_REPLICA, scope);
        Metrics.register(replica.createMetricName(REPLICA_STABLE_LATENCY), stableLatency);
        Metrics.register(replica.createMetricName(REPLICA_PREAPPLY_LATENCY), preapplyLatency);
        Metrics.register(replica.createMetricName(REPLICA_APPLY_LATENCY), applyLatency);
        Metrics.register(replica.createMetricName(REPLICA_DEPENDENCIES), dependencies);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AccordReplicaMetrics [");

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

    public static class Listener implements ReplicaEventListener
    {
        private SubShard forTransaction(SafeCommandStore safeStore, TxnId txnId)
        {
            if (txnId != null)
            {
                Shard shard = ((AccordCommandStore) safeStore.commandStore()).executor().replicaMetrics;
                if (txnId.isWrite())
                    return shard.writes;
                else if (txnId.isSomeRead())
                    return shard.reads;
                else if (txnId.isSyncPoint())
                    return shard.syncPoints;
            }
            return null;
        }

        private static long unixNanos()
        {
            return currentTimeMillis() * 1_000_000;
        }

        private static long elapsed(TxnId txnId)
        {
            return elapsed(unixNanos(), txnId);
        }

        private static long elapsed(long unixNanos, TxnId txnId)
        {
            return Math.max(0, unixNanos - (txnId.hlc() * 1000));
        }

        private static LogLinearDecayingHistograms.Buffer buffer(SafeCommandStore safeStore)
        {
            return ((SaferCommandStore) safeStore).histogramBuffer();
        }

        @Override
        public void onStable(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Stable {} on {}", cmd.txnId(), safeStore.commandStore());
            SubShard metrics = forTransaction(safeStore, cmd.txnId());
            if (metrics != null)
                metrics.stableLatency.add(buffer(safeStore), elapsed(cmd.txnId()));
        }

        @Override
        public void onPreApplied(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Preapplied {} on {}", cmd.txnId(), safeStore.commandStore());
            SubShard metrics = forTransaction(safeStore, cmd.txnId());
            if (metrics != null)
            {
                long elapsed = elapsed(cmd.txnId());
                metrics.preapplyLatency.add(buffer(safeStore), elapsed);
                PartialDeps deps = cmd.partialDeps();
                metrics.dependencies.add(buffer(safeStore), deps != null ? deps.txnIdCount() : 0);
            }
        }

        @Override
        public void onApplied(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Applied {} on {}", cmd.txnId(), safeStore.commandStore());
            SubShard metrics = forTransaction(safeStore, cmd.txnId());
            if (metrics != null)
            {
                long now = unixNanos();
                metrics.applyLatency.add(buffer(safeStore), elapsed(now, cmd.txnId()));
            }
        }
    }

    // to ensure initialised
    public static void touch() {}
}
