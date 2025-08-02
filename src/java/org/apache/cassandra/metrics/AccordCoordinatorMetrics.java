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
import javax.annotation.Nullable;

import accord.api.CoordinatorEventListener;
import accord.api.Result;
import accord.coordinate.ExecutePath;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.TxnId;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordCoordinatorMetrics
{
    public final static AccordCoordinatorMetrics readMetrics = new AccordCoordinatorMetrics("ro");
    public final static AccordCoordinatorMetrics writeMetrics = new AccordCoordinatorMetrics("rw");

    public static final String ACCORD_COORDINATOR = "AccordCoordinator";
    public static final String COORDINATOR_EPOCHS = "Epochs";
    public static final String COORDINATOR_KEYS = "Keys";
    public static final String COORDINATOR_TABLES = "Tables";
    public static final String COORDINATOR_DEPENDENCIES = "Dependencies";
    public static final String COORDINATOR_PREACCEPT_LATENCY = "PreAcceptLatency";
    public static final String COORDINATOR_EXECUTE_LATENCY = "ExecuteLatency";
    public static final String COORDINATOR_APPLY_LATENCY = "ApplyLatency";
    public static final String FAST_PATHS = "FastPaths";
    public static final String MEDIUM_PATHS = "MediumPaths";
    public static final String SLOW_PATHS = "SlowPaths";
    public static final String PREEMPTED = "Preempted";
    public static final String REJECTED = "Rejected";
    public static final String TIMEOUTS = "Timeouts";
    public static final String INVALIDATIONS = "Invalidations";
    public static final String RECOVERY_DELAY = "RecoveryDelay";
    public static final String RECOVERY_TIME = "RecoveryTime";
    public static final String FAST_PATH_TO_TOTAL = "FastPathToTotal";

    /**
     * A histogram of the number of dependencies per transaction at this coordinator.
     */
    public final Histogram dependencies;

    /**
     * A histogram of the time to preaccept on this coordinator
     */
    public final Histogram preacceptLatency;

    /**
     * A histogram of the time to begin execution on this coordinator
     */
    public final Histogram executeLatency;

    /**
     * A histogram of the time to complete execution on this coordinator
     */
    public final Histogram applyLatency;

    /**
     * The number of epochs used to coordinate the transaction
     */
    public final Histogram epochs;

    /**
     * The number of keys involved in a transaction
     */
    public final Histogram keys;

    /**
     * The number of tables involved in a transaction
     */
    public final Histogram tables;

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
     * The number of preempted transactions on this coordinator.
     */
    public final Meter rejected;

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

    private AccordCoordinatorMetrics(String scope)
    {
        DefaultNameFactory coordinator = new DefaultNameFactory(ACCORD_COORDINATOR, scope);
        dependencies = Metrics.histogram(coordinator.createMetricName(COORDINATOR_DEPENDENCIES), true);
        preacceptLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_PREACCEPT_LATENCY), true);
        executeLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_EXECUTE_LATENCY), true);
        applyLatency = Metrics.histogram(coordinator.createMetricName(COORDINATOR_APPLY_LATENCY), true);
        epochs = Metrics.histogram(coordinator.createMetricName(COORDINATOR_EPOCHS), true);
        keys = Metrics.histogram(coordinator.createMetricName(COORDINATOR_KEYS), true);
        tables = Metrics.histogram(coordinator.createMetricName(COORDINATOR_TABLES), true);

        fastPaths = Metrics.meter(coordinator.createMetricName(FAST_PATHS));
        mediumPaths = Metrics.meter(coordinator.createMetricName(MEDIUM_PATHS));
        slowPaths = Metrics.meter(coordinator.createMetricName(SLOW_PATHS));
        preempted = Metrics.meter(coordinator.createMetricName(PREEMPTED));
        rejected = Metrics.meter(coordinator.createMetricName(REJECTED));
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
        builder.append("AccordCoordinatorMetrics [");

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

    public static class Listener implements CoordinatorEventListener
    {
        public static final Listener instance = new Listener();

        private AccordCoordinatorMetrics forTransaction(TxnId txnId)
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
        public void onPreAccepted(TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.preacceptLatency.update(Math.max(0, now - txnId.hlc()));
            }
        }

        @Override
        public void onExecuting(TxnId txnId, @Nullable Ballot ballot, Deps deps, @Nullable ExecutePath path)
        {
            Tracing.trace("{} agreed {}", path, txnId);
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                metrics.dependencies.update(deps.txnIdCount());
                long now = AccordTimeService.nowMicros();
                metrics.executeLatency.update(Math.max(0, now - txnId.hlc()));
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
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordTimeService.nowMicros();
                metrics.applyLatency.update(Math.max(0, now - txnId.hlc()));
            }
        }

        @Override
        public void onRecoveryStopped(Node node, TxnId txnId, Ballot ballot, Result result, Throwable failure)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
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
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.invalidations.mark();
        }

        @Override
        public void onTimeout(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.timeouts.mark();
        }

        @Override
        public void onRejected(TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.rejected.mark();
        }

        @Override
        public void onExhausted(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.timeouts.mark();
            CoordinatorEventListener.super.onExhausted(txnId);
        }

        @Override
        public void onPreempted(@Nullable TxnId txnId)
        {
            AccordCoordinatorMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.preempted.mark();
        }
    }
}
