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

import accord.api.ReplicaEventListener;
import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordReplicaMetrics
{
    public final static AccordReplicaMetrics readMetrics = new AccordReplicaMetrics("ro");
    public final static AccordReplicaMetrics writeMetrics = new AccordReplicaMetrics("rw");

    public static final String ACCORD_REPLICA = "AccordReplica";
    public static final String REPLICA_STABLE_LATENCY = "StableLatency";
    public static final String REPLICA_PREAPPLY_LATENCY = "PreApplyLatency";
    public static final String REPLICA_APPLY_LATENCY = "ApplyLatency";
    public static final String REPLICA_APPLY_DURATION = "ApplyDuration";
    public static final String REPLICA_DEPENDENCIES = "Dependencies";

    /**
     * The time between start on the coordinator and commit on this replica.
     */
    public final Timer stableLatency;

    /**
     * The time between start on the coordinator and arrival of the result on this replica.
     */
    public final Timer preapplyLatency;

    /**
     * The time between start on the coordinator and application on this replica.
     */
    public final Timer applyLatency;

    /**
     * TODO (expected): probably more interesting is latency from preapplied to apply;
     *  we already track local write latencies, whch this effectively duplicates (but including queueing latencies)
     * Duration of applying changes.
     */
    public final Timer applyDuration;

    /**
     * A histogram of the number of dependencies per transaction at this replica.
     */
    public final Histogram dependencies;

    private AccordReplicaMetrics(String scope)
    {
        DefaultNameFactory replica = new DefaultNameFactory(ACCORD_REPLICA, scope);
        stableLatency = Metrics.timer(replica.createMetricName(REPLICA_STABLE_LATENCY));
        preapplyLatency = Metrics.timer(replica.createMetricName(REPLICA_PREAPPLY_LATENCY));
        applyLatency = Metrics.timer(replica.createMetricName(REPLICA_APPLY_LATENCY));
        applyDuration = Metrics.timer(replica.createMetricName(REPLICA_APPLY_DURATION));
        dependencies = Metrics.histogram(replica.createMetricName(REPLICA_DEPENDENCIES), true);
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
        builder.append("]");
        return builder.toString();
    }

    public static class Listener implements ReplicaEventListener
    {
        public static final Listener instance = new Listener();

        private AccordReplicaMetrics forTransaction(TxnId txnId)
        {
            if (txnId != null)
            {
                if (txnId.isWrite())
                    return writeMetrics;
                else if (txnId.isSomeRead())
                    return readMetrics;
            }
            return null;
        }

        @Override
        public void onStable(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Stable {} on {}", cmd.txnId(), safeStore.commandStore());
            long now = AccordTimeService.nowMicros();
            AccordReplicaMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                long trxTimestamp = cmd.txnId().hlc();
                metrics.stableLatency.update(now - trxTimestamp, TimeUnit.MICROSECONDS);
            }
        }

        @Override
        public void onPreApplied(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Preapplied {} on {}", cmd.txnId(), safeStore.commandStore());
            long now = AccordTimeService.nowMicros();
            AccordReplicaMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.preapplyLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                PartialDeps deps = cmd.partialDeps();
                metrics.dependencies.update(deps != null ? deps.txnIdCount() : 0);
            }
        }

        @Override
        public void onApplied(SafeCommandStore safeStore, Command cmd, long applyStartedAt)
        {
            Tracing.trace("Applied {} on {}", cmd.txnId(), safeStore.commandStore());
            long now = AccordTimeService.nowMicros();
            AccordReplicaMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.applyLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                if (applyStartedAt > 0)
                    metrics.applyDuration.update(now - applyStartedAt, TimeUnit.MICROSECONDS);
            }
        }
    }

    // to ensure initialised
    public static void touch() {}
}
