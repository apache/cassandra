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
import org.apache.cassandra.repair.AutoRepair;
import org.apache.cassandra.repair.AutoRepairUtils;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics related to AutoRepair.
 */
public class AutoRepairMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("AutoRepair");

    public static Gauge<Integer> repairsInProgress;
    public static Gauge<Integer> nodeRepairTimeInSec;
    public static Gauge<Integer> clusterRepairTimeInSec;
    public static Gauge<Integer> skippedTablesCount;
    public static Gauge<Integer> longestUnrepairedSec;
    public static Gauge<Integer> failedTablesCount;
    public static Counter repairTurnMyTurn;
    public static Counter repairTurnMyTurnDueToPriority;
    public static Counter repairTurnMyTurnForceRepair;

    public static void setup()
    {
        repairsInProgress = Metrics.register(factory.createMetricName("RepairsInProgress"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.isRepairInProgress();
            }
        });
        nodeRepairTimeInSec = Metrics.register(factory.createMetricName("NodeRepairTimeInSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.getNodeRepairTimeInSec();
            }
        });
        clusterRepairTimeInSec = Metrics.register(factory.createMetricName("ClusterRepairTimeInSec"), new
                Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.getClusterRepairTimeInSec();
            }
        });

        skippedTablesCount = Metrics.register(factory.createMetricName("SkippedTablesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.getRepairSkippedTablesCount();
            }
        });

        longestUnrepairedSec = Metrics.register(factory.createMetricName("LongestUnrepairedSec"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.getLongestUnrepairedSec();
            }
        });

        failedTablesCount = Metrics.register(factory.createMetricName("FailedTablesCount"), new Gauge<Integer>()
        {
            public Integer getValue()
            {
                return AutoRepair.getRepairFailedTablesCount();
            }
        });

        repairTurnMyTurn = Metrics.counter(factory.createMetricName("RepairTurnMyTurn"));
        repairTurnMyTurnDueToPriority = Metrics.counter(factory.createMetricName("RepairTurnMyTurnDueToPriority"));
        repairTurnMyTurnForceRepair = Metrics.counter(factory.createMetricName("RepairTurnMyTurnForceRepair"));
    }

    public static void recordTurn(AutoRepairUtils.RepairTurn turn){
        switch (turn) {
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
                throw new RuntimeException(String.format("Unrecoginized turn: {}", turn.name()));
        }
    }
}
