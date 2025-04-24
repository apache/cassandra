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
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.service.AutoRepairService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * Metrics related to AutoRepair.
 */
public class AutoRepairMetrics
{
    public static final String TYPE_NAME = "autorepair";
    public final Gauge<Integer> repairsInProgress;
    public final Gauge<Integer> nodeRepairTimeInSec;
    public final Gauge<Integer> clusterRepairTimeInSec;
    public final Gauge<Integer> longestUnrepairedSec;
    public final Gauge<Integer> repairStartLagSec;
    public final Gauge<Integer> succeededTokenRangesCount;
    public final Gauge<Integer> failedTokenRangesCount;
    public final Gauge<Integer> skippedTokenRangesCount;
    public final Gauge<Integer> skippedTablesCount;
    public final Gauge<Integer> totalMVTablesConsideredForRepair;
    public final Gauge<Integer> totalDisabledRepairTables;
    public Counter repairTurnMyTurn;
    public Counter repairTurnMyTurnDueToPriority;
    public Counter repairTurnMyTurnForceRepair;
    public Counter repairDelayedByReplica;
    public Counter repairDelayedBySchedule;

    private final RepairType repairType;

    private volatile int repairStartLagSecVal;

    public AutoRepairMetrics(RepairType repairType)
    {
        this.repairType = repairType;
        AutoRepairMetricsFactory factory = new AutoRepairMetricsFactory(repairType);

        repairsInProgress = Metrics.register(factory.createMetricName("RepairsInProgress"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).isRepairInProgress() ? 1 : 0;
            }
        });

        nodeRepairTimeInSec = Metrics.register(factory.createMetricName("NodeRepairTimeInSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getNodeRepairTimeInSec();
            }
        });

        clusterRepairTimeInSec = Metrics.register(factory.createMetricName("ClusterRepairTimeInSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getClusterRepairTimeInSec();
            }
        });

        skippedTokenRangesCount = Metrics.register(factory.createMetricName("SkippedTokenRangesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getSkippedTokenRangesCount();
            }
        });

        skippedTablesCount = Metrics.register(factory.createMetricName("SkippedTablesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getSkippedTablesCount();
            }
        });

        longestUnrepairedSec = Metrics.register(factory.createMetricName("LongestUnrepairedSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getLongestUnrepairedSec();
            }
        });

        repairStartLagSec = Metrics.register(factory.createMetricName("RepairStartLagSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return repairStartLagSecVal;
            }
        });

        succeededTokenRangesCount = Metrics.register(factory.createMetricName("SucceededTokenRangesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getSucceededTokenRangesCount();
            }
        });

        failedTokenRangesCount = Metrics.register(factory.createMetricName("FailedTokenRangesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getFailedTokenRangesCount();
            }
        });

        repairTurnMyTurn = Metrics.counter(factory.createMetricName("RepairTurnMyTurn"));
        repairTurnMyTurnDueToPriority = Metrics.counter(factory.createMetricName("RepairTurnMyTurnDueToPriority"));
        repairTurnMyTurnForceRepair = Metrics.counter(factory.createMetricName("RepairTurnMyTurnForceRepair"));

        repairDelayedByReplica = Metrics.counter(factory.createMetricName("RepairDelayedByReplica"));
        repairDelayedBySchedule = Metrics.counter(factory.createMetricName("RepairDelayedBySchedule"));

        totalMVTablesConsideredForRepair = Metrics.register(factory.createMetricName("TotalMVTablesConsideredForRepair"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getTotalMVTablesConsideredForRepair();
            }
        });

        totalDisabledRepairTables = Metrics.register(factory.createMetricName("TotalDisabledRepairTables"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.instance.getRepairState(repairType).getTotalDisabledTablesRepairCount();
            }
        });
    }

    public void recordTurn(AutoRepairUtils.RepairTurn turn)
    {
        switch (turn)
        {
            case MY_TURN:
                repairTurnMyTurn.inc();
                break;
            case MY_TURN_FORCE_REPAIR:
                repairTurnMyTurnForceRepair.inc();
                break;
            case MY_TURN_DUE_TO_PRIORITY:
                repairTurnMyTurnDueToPriority.inc();
                break;
            default:
                throw new RuntimeException(String.format("Unrecoginized turn: %s", turn.name()));
        }
        this.repairStartLagSecVal = 0;
    }

    /**
     * Record perceived lag in scheduling repair.
     * <p/>
     * Takes the current time and subtracts it from the given last repair finish time.  It then compares the difference
     * with the min repair interval for this repair type, and if that value is greater than 0, records it.
     */
    public void recordRepairStartLag(long lastFinishTimeInMs)
    {
        long now = AutoRepair.instance.currentTimeMs();
        long deltaFinish = now - lastFinishTimeInMs;
        long deltaMinRepairInterval = deltaFinish - AutoRepairService.instance
                                                    .getAutoRepairConfig().getRepairMinInterval(repairType)
                                                    .toMilliseconds();
        this.repairStartLagSecVal = deltaMinRepairInterval > 0 ? (int) MILLISECONDS.toSeconds(deltaMinRepairInterval) : 0;
    }

    @VisibleForTesting
    protected static class AutoRepairMetricsFactory implements MetricNameFactory
    {
        private static final String TYPE = "AutoRepair";
        @VisibleForTesting
        protected final String repairType;

        protected AutoRepairMetricsFactory(RepairType repairType)
        {
            this.repairType = toLowerCaseLocalized(repairType.toString());
        }

        @Override
        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(DefaultNameFactory.GROUP_NAME).append(':');
            mbeanName.append("type=").append(TYPE);
            mbeanName.append(",name=").append(metricName);
            mbeanName.append(",repairType=").append(repairType);

            StringBuilder scope = new StringBuilder();
            scope.append("repairType=").append(repairType);

            return new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME, toLowerCaseLocalized(TYPE),
                                                           metricName, scope.toString(), mbeanName.toString());
        }
    }
}
