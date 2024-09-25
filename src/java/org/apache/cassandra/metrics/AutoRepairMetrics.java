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

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to AutoRepair.
 */
public class AutoRepairMetrics
{
    public Gauge<Integer> repairsInProgress;
    public Gauge<Integer> nodeRepairTimeInSec;
    public Gauge<Integer> clusterRepairTimeInSec;
    public Gauge<Integer> longestUnrepairedSec;
    public Gauge<Integer> succeededTokenRangesCount;
    public Gauge<Integer> failedTokenRangesCount;
    public Gauge<Integer> skippedTokenRangesCount;
    public Gauge<Integer> skippedTablesCount;
    public Counter repairTurnMyTurn;
    public Counter repairTurnMyTurnDueToPriority;
    public Counter repairTurnMyTurnForceRepair;
    public Gauge<Integer> totalMVTablesConsideredForRepair;
    public Gauge<Integer> totalDisabledRepairTables;

    public AutoRepairMetrics(RepairType repairType)
    {
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
    }

    @VisibleForTesting
    protected static class AutoRepairMetricsFactory implements MetricNameFactory
    {
        private static final String TYPE = "AutoRepair";
        @VisibleForTesting
        protected final String repairType;

        protected AutoRepairMetricsFactory(RepairType repairType)
        {
            this.repairType = repairType.toString().toLowerCase();
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

            return new CassandraMetricsRegistry.MetricName(DefaultNameFactory.GROUP_NAME, TYPE.toLowerCase(),
                                                           metricName, scope.toString(), mbeanName.toString());
        }
    }
}
