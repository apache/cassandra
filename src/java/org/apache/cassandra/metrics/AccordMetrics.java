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

import accord.api.EventsListener;
import accord.local.Command;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.service.accord.AccordService;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class AccordMetrics
{
    public final static AccordMetrics readMetrics = new AccordMetrics("ro");
    public final static AccordMetrics writeMetrics = new AccordMetrics("rw");

    public static final String STABLE_LATENCY = "StableLatency";
    public static final String EXECUTE_LATENCY = "ExecuteLatency";
    public static final String APPLY_LATENCY = "ApplyLatency";
    public static final String APPLY_DURATION = "ApplyDuration";
    public static final String PARTIAL_DEPENDENCIES = "PartialDependencies";
    public static final String PROGRESS_LOG_SIZE = "ProgressLogSize";

    public static final String DEPENDENCIES = "Dependencies";
    public static final String FAST_PATHS = "FastPaths";
    public static final String SLOW_PATHS = "SlowPaths";
    public static final String PREEMPTS = "Preempts";
    public static final String TIMEOUTS = "Timeouts";
    public static final String INVALIDATIONS = "Invalidations";
    public static final String RECOVERY_DELAY = "RecoveryDelay";
    public static final String RECOVERY_TIME = "RecoveryTime";
    public static final String FAST_PATH_TO_TOTAL = "FastPathToTotal";
    public static final String ACCORD_REPLICA = "accord-replica";
    public static final String ACCORD_COORDINATOR = "accord-coordinator";

    /**
     * The time between start on the coordinator and commit on this replica.
     */
    public final Timer stableLatency;

    /**
     * The time between start on the coordinator and execution on this replica.
     */
    public final Timer executeLatency;

    /**
     * The time between start on the coordinator and application on this replica.
     */
    public final Timer applyLatency;

    /**
     * Duration of applying changes.
     */
    public final Timer applyDuration;

    /**
     * A histogram of the number of dependencies per partial transaction at this replica.
     */
    public final Histogram partialDependencies;

    public final Meter progressLogSize;

    /**
     * A histogram of the number of dependencies per transaction at this coordinator.
     */
    public final Histogram dependencies;

    /**
     * The number of fast path transactions executed on this coordinator.
     */
    public final Meter fastPaths;

    /**
     * The number of slow path transactions executed on this coordinator.
     */
    public final Meter slowPaths;

    /**
     * The number of preempted transactions on this coordinator.
     */
    public final Meter preempts;

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
        stableLatency = Metrics.timer(replica.createMetricName(STABLE_LATENCY));
        executeLatency = Metrics.timer(replica.createMetricName(EXECUTE_LATENCY));
        applyLatency = Metrics.timer(replica.createMetricName(APPLY_LATENCY));
        applyDuration = Metrics.timer(replica.createMetricName(APPLY_DURATION));
        partialDependencies = Metrics.histogram(replica.createMetricName(PARTIAL_DEPENDENCIES), true);
        progressLogSize = Metrics.meter(replica.createMetricName(PROGRESS_LOG_SIZE));

        DefaultNameFactory coordinator = new DefaultNameFactory(ACCORD_COORDINATOR, scope);
        dependencies = Metrics.histogram(coordinator.createMetricName(DEPENDENCIES), true);
        fastPaths = Metrics.meter(coordinator.createMetricName(FAST_PATHS));
        slowPaths = Metrics.meter(coordinator.createMetricName(SLOW_PATHS));
        preempts = Metrics.meter(coordinator.createMetricName(PREEMPTS));
        timeouts = Metrics.meter(coordinator.createMetricName(TIMEOUTS));
        invalidations = Metrics.meter(coordinator.createMetricName(INVALIDATIONS));
        recoveryDelay = Metrics.timer(coordinator.createMetricName(RECOVERY_DELAY));
        recoveryDuration = Metrics.timer(coordinator.createMetricName(RECOVERY_TIME));
        fastPathToTotal = new RatioGaugeSet(fastPaths, RatioGaugeSet.sum(fastPaths, slowPaths), coordinator, FAST_PATH_TO_TOTAL + ".%s");
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

    public static class Listener implements EventsListener
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
            if (txnId.isWrite())
                return writeMetrics;
            else if (txnId.isRead())
                return readMetrics;
            else
                return null;
        }

        @Override
        public void onStable(Command cmd)
        {
            long now = AccordService.uniqueNow();
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                long trxTimestamp = cmd.txnId().hlc();
                metrics.stableLatency.update(now - trxTimestamp, TimeUnit.MICROSECONDS);
            }
        }

        @Override
        public void onExecuted(Command cmd)
        {
            long now = AccordService.uniqueNow();
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.executeLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                PartialDeps deps = cmd.partialDeps();
                metrics.partialDependencies.update(deps != null ? deps.txnIdCount() : 0);
            }
        }

        @Override
        public void onApplied(Command cmd, long applyStartTimestamp)
        {
            long now = AccordService.uniqueNow();
            AccordMetrics metrics = forTransaction(cmd.txnId());
            if (metrics != null)
            {
                Timestamp trxTimestamp = cmd.txnId();
                metrics.applyLatency.update(now - trxTimestamp.hlc(), TimeUnit.MICROSECONDS);
                metrics.applyDuration.update(now - applyStartTimestamp, TimeUnit.MICROSECONDS);
            }
        }

        @Override
        public void onFastPathTaken(TxnId txnId, Deps deps)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                metrics.fastPaths.mark();
                metrics.dependencies.update(deps.txnIdCount());
            }
        }

        @Override
        public void onSlowPathTaken(TxnId txnId, Deps deps)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                metrics.slowPaths.mark();
                metrics.dependencies.update(deps.txnIdCount());
            }
        }

        @Override
        public void onRecover(TxnId txnId, Timestamp recoveryTimestamp)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
            {
                long now = AccordService.uniqueNow();

                metrics.recoveryDuration.update(now - recoveryTimestamp.hlc(), MICROSECONDS);
                metrics.recoveryDelay.update(recoveryTimestamp.hlc() - txnId.hlc(), MICROSECONDS);
            }
        }

        @Override
        public void onPreempted(TxnId txnId)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.preempts.mark();
        }

        @Override
        public void onTimeout(TxnId txnId)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.timeouts.mark();
        }

        @Override
        public void onInvalidated(TxnId txnId)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.invalidations.mark();
        }

        @Override
        public void onProgressLogSizeChange(TxnId txnId, int delta)
        {
            AccordMetrics metrics = forTransaction(txnId);
            if (metrics != null)
                metrics.progressLogSize.mark(delta);
        }
    }
}
