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
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import accord.api.CoordinatorEventListener;
import accord.api.LocalEventListener;
import accord.api.Result;
import accord.coordinate.ExecutePath;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordMetrics
{
    public final static AccordMetrics readMetrics = new AccordMetrics("ro");
    public final static AccordMetrics writeMetrics = new AccordMetrics("rw");

    public static final String ACCORD_REPLICA = "AccordReplica";
    public static final String REPLICA_STABLE_LATENCY = "StableLatency";
    public static final String REPLICA_PREAPPLY_LATENCY = "PreApplyLatency";
    public static final String REPLICA_APPLY_LATENCY = "ApplyLatency";
    public static final String REPLICA_APPLY_DURATION = "ApplyDuration";
    public static final String REPLICA_DEPENDENCIES = "Dependencies";
    public static final String PROGRESS_LOG_SIZE = "ProgressLogSize";

    public static final String ACCORD_COORDINATOR = "AccordCoordinator";
    public static final String COORDINATOR_DEPENDENCIES = "Dependencies";
    public static final String COORDINATOR_PREACCEPT_LATENCY = "PreAcceptLatency";
    public static final String COORDINATOR_EXECUTE_LATENCY = "ExecuteLatency";
    public static final String COORDINATOR_APPLY_LATENCY = "ApplyLatency";
    public static final String FAST_PATHS = "FastPaths";
    public static final String MEDIUM_PATHS = "MediumPaths";
    public static final String SLOW_PATHS = "SlowPaths";
    public static final String PREEMPTED = "Preempted";
    public static final String TIMEOUTS = "Timeouts";
    public static final String INVALIDATIONS = "Invalidations";
    public static final String RECOVERY_DELAY = "RecoveryDelay";
    public static final String RECOVERY_TIME = "RecoveryTime";
    public static final String FAST_PATH_TO_TOTAL = "FastPathToTotal";

    /**
     * The time between start on the coordinator and commit on this replica.
     */
    public final Timer replicaStableLatency;

    /**
     * The time between start on the coordinator and arrival of the result on this replica.
     */
    public final Timer replicaPreapplyLatency;

    /**
     * The time between start on the coordinator and application on this replica.
     */
    public final Timer replicaApplyLatency;

    /**
     * TODO (expected): probably more interesting is latency from preapplied to apply;
     *  we already track local write latencies, whch this effectively duplicates (but including queueing latencies)
     * Duration of applying changes.
     */
    public final Timer replicaApplyDuration;

    /**
     * A histogram of the number of dependencies per transaction at this replica.
     */
    public final Histogram replicaDependencies;

    public final Gauge<Long> progressLogSize;

    /**
     * A histogram of the number of dependencies per transaction at this coordinator.
     */
    public final Histogram coordinatorDependencies;

    /**
     * A histogram of the time to preaccept on this coordinator
     */
    public final Histogram coordinatorPreacceptLatency;

    /**
     * A histogram of the time to begin execution on this coordinator
     */
    public final Histogram coordinatorExecuteLatency;

    /**
     * A histogram of the time to complete execution on this coordinator
     */
    public final Histogram coordinatorApplyLatency;

    /**
     * The number of fast path transactions executed on this coordinator.
     */
    public final Meter fastPaths;

    /**
     * The number of medium path transactions executed on this coordinator.
     */
    public final Meter mediumPaths;

    /**
     * The number of slow path transactions executed on this coordinator.
     */
    public final Meter slowPaths;

    /**
     * The number of preempted transactions on this coordinator.
     */
    public final Meter preempted;

    /**
     * The number of timed out transactions on this coordinator.
     */
    public final Meter timeouts;

    /**
     * The number of invalidated transactions on this coordinator.
     */
    public final Meter invalidations;

    /**
     * The time between the start of the transaction and the start of the recovery, if the transaction is recovered.
     */
    public final Timer recoveryDelay;

    /**
     * The time between the start of the recovery and the execution of the transaction, if the transaction is recovered.
     */
    public final Timer recoveryDuration;

    /**
     * The ratio of the number of fast path transactions to the total number of transactions.
     */
    public final RatioGaugeSet fastPathToTotal;

    private AccordMetrics(String scope)
    {
        DefaultNameFactory replica = new DefaultNameFactory(ACCORD_REPLICA, scope);
        replicaStableLatency = Metrics.timer(replica.createMetricName(REPLICA_STABLE_LATENCY));
        replicaPreapplyLatency = Metrics.timer(replica.createMetricName(REPLICA_PREAPPLY_LATENCY));
        replicaApplyLatency = Metrics.timer(replica.createMetricName(REPLICA_APPLY_LATENCY));
        replicaApplyDuration = Metrics.timer(replica.createMetricName(REPLICA_APPLY_DURATION));
        replicaDependencies = Metrics.histogram(replica.createMetricName(REPLICA_DEPENDENCIES), true);
        progressLogSize = Metrics.gauge(replica.createMetricName(PROGRESS_LOG_SIZE), () -> {
            AtomicLong i = new AtomicLong();
            AccordService.instance().node().commandStores().forEachCommandStore(store -> {
                i.addAndGet(((DefaultProgressLog)store.unsafeProgressLog()).size());
            });
            return i.get();
        });

        DefaultNameFactory coordinator = new DefaultNameFactory(ACCORD_COORDINATOR, scope);
        coordinatorDependencies = Metrics.histogram(coordinator.createMetricName(COORDINATOR_DEPENDENCIES), true);
        coordinatorPreacceptLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_PREACCEPT_LATENCY), true);
        coordinatorExecuteLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_EXECUTE_LATENCY), true);
        coordinatorApplyLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_APPLY_LATENCY), true);
        fastPaths = Metrics.meter(coordinator.createMetricName(FAST_PATHS));
        mediumPaths = Metrics.meter(coordinator.createMetricName(MEDIUM_PATHS));
        slowPaths = Metrics.meter(coordinator.createMetricName(SLOW_PATHS));
        preempted = Metrics.meter(coordinator.createMetricName(PREEMPTED));
        timeouts = Metrics.meter(coordinator.createMetricName(TIMEOUTS));
        invalidations = Metrics.meter(coordinator.createMetricName(INVALIDATIONS));
        recoveryDelay = Metrics.timer(coordinator.createMetricName(RECOVERY_DELAY));
        recoveryDuration = Metrics.timer(coordinator.createMetricName(RECOVERY_TIME));
        fastPathToTotal = new RatioGaugeSet(fastPaths, RatioGaugeSet.sum(fastPaths, mediumPaths, slowPaths), coordinator, FAST_PATH_TO_TOTAL + ".%s");
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("AccordMetrics [");

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

    public static class Listener implements CoordinatorEventListener, LocalEventListener
    {
        public final static Listener instance = new Listener(AccordMetrics.readMetrics, AccordMetrics.writeMetrics);

        private final AccordMetrics readMetrics;
        private final AccordMetrics writeMetrics;

        public Listener(AccordMetrics readMetrics, AccordMetrics writeMetrics)
        {
            this.readMetrics = readMetrics;
            this.writeMetrics = writeMetrics;
        }

        private AccordMetrics forTransaction(TxnId txnId)
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
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                long trxTimestamp = cmd.txnId().hlc();
                metrics.replicaStableLatency.update(now - trxTimestamp, TimeUnit.MICROSECONDS);
            }
        }

        @Override
        public void onPreApplied(SafeCommandStore safeStore, Command cmd)
        {
            Tracing.trace("Preapplied {} on {}", cmd.txnId(), safeStore.commandStore());
            long now = AccordTimeService.nowMicros();
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.replicaPreapplyLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                PartialDeps deps = cmd.partialDeps();
                metrics.replicaDependencies.update(deps != null ? deps.txnIdCount() : 0);
            }
        }

        @Override
        public void onApplied(SafeCommandStore safeStore, Command cmd, long applyStartedAt)
        {
            Tracing.trace("Applied {} on {}", cmd.txnId(), safeStore.commandStore());
            long now = AccordTimeService.nowMicros();
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.replicaApplyLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                if (applyStartedAt > 0)
                    metrics.replicaApplyDuration.update(now - applyStartedAt, TimeUnit.MICROSECONDS);
            }
        }

        @Override
        public void onPreAccepted(TxnId txnId)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.coordinatorPreacceptLatency.update(Math.max(0, now - txnId.hlc()));
            }
        }

        @Override
        public void onExecuting(TxnId txnId, @Nullable Ballot ballot, Deps deps, @Nullable ExecutePath path)
        {
            Tracing.trace("{} agreed {}", path, txnId);
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                metrics.coordinatorDependencies.update(deps.txnIdCount());
                long now = AccordTimeService.nowMicros();
                metrics.coordinatorExecuteLatency.update(Math.max(0, now - txnId.hlc()));
                if (path != null)
                {
                    switch (path)
                    {
                        case FAST: metrics.fastPaths.mark(); break;
                        case MEDIUM: metrics.mediumPaths.mark(); break;
                        case SLOW: metrics.slowPaths.mark(); break;
                    }
                }
            }
        }

        @Override
        public void onExecuted(TxnId txnId, Ballot ballot)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.coordinatorApplyLatency.update(Math.max(0, now - txnId.hlc()));
            }
        }

        @Override
        public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result result, Throwable failure)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();

                metrics.recoveryDuration.update(Math.max(0, now - ballot.hlc()), MICROSECONDS);
                metrics.recoveryDelay.update(Math.max(0, ballot.hlc() - txnId.hlc()), MICROSECONDS);
            }
        }

        @Override
        public void onInvalidated(TxnId txnId)
        {
            Tracing.trace("Invalidated {}", txnId);
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.invalidations.mark();
        }
    }
}
