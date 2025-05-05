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

package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.Util;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

/**
 * Distributed tests for {@link org.apache.cassandra.repair.autorepair.AutoRepair} scheduler's
 * allow_parallel_replica_repair_across_schedules feature.
 */
public class AutoRepairSchedulerDisallowParallelReplicaRepairAcrossSchedulesTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void init() throws IOException
    {
        // Configure a cluster with preview and incremental repair enabled in a way that preview repair can be
        // run on all three nodes concurrently, but incremental repair can only be run when there are no parallel
        // repairs.  We should detect contention in the incremental repair scheduler but not preview repaired
        // scheduler as a result.
        cluster = Cluster.build(3)
                         .withConfig(config -> config
                                               .set("auto_repair",
                                                    ImmutableMap.of(
                                                    "repair_type_overrides",
                                                    ImmutableMap.of(AutoRepairConfig.RepairType.PREVIEW_REPAIRED.getConfigName(),
                                                                    ImmutableMap.of(
                                                                    // Configure preview repair to run frequently to
                                                                    // provoke contention with incremental scheduler.
                                                                    "initial_scheduler_delay", "5s",
                                                                    "enabled", "true",
                                                                    "parallel_repair_count", "3",
                                                                    "allow_parallel_replica_repair", "true",
                                                                    "min_repair_interval", "5s"),
                                                                    AutoRepairConfig.RepairType.INCREMENTAL.getConfigName(),
                                                                    ImmutableMap.of(
                                                                    "initial_scheduler_delay", "5s",
                                                                    "enabled", "true",
                                                                    "parallel_repair_count", "3",
                                                                    // Don't allow parallel replica repair across
                                                                    // schedules
                                                                    "allow_parallel_replica_repair", "false",
                                                                    "allow_parallel_replica_repair_across_schedules", "false",
                                                                    "min_repair_interval", "5s"))))
                                               .set("auto_repair.enabled", "true")
                                               .set("auto_repair.global_settings.repair_retry_backoff", "5s")
                                               .set("auto_repair.repair_task_min_duration", "0s")
                                               .set("auto_repair.repair_check_interval", "5s"))
                         .start();

        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck text, v1 int, v2 int, PRIMARY KEY (pk, ck)) WITH read_repair='NONE'"));
    }

    @AfterClass
    public static void tearDown()
    {
        cluster.close();
    }

    @Test
    public void testScheduler()
    {
        cluster.forEach(i -> i.runOnInstance(() -> {
            try
            {
                AutoRepairService.setup();
                AutoRepair.instance.setup();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }));

        // validate that the repair ran on all nodes
        cluster.forEach(i -> i.runOnInstance(() -> {
            // Expect contention on incremental repair across schedules
            AutoRepairMetrics incrementalMetrics = AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.INCREMENTAL);
            Util.spinAssert(String.format("%s: AutoRepair has not observed any replica contention in INCREMENTAL repair",
                                          FBUtilities.getJustBroadcastAddress().toString()),
                            greaterThan(0L),
                            incrementalMetrics.repairDelayedBySchedule::getCount,
                            5,
                            TimeUnit.MINUTES);

            // No repair contention should be observed for preview repaired since allow_parallel_replica_repair was true
            AutoRepairMetrics previewMetrics = AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.PREVIEW_REPAIRED);
            assertEquals(0L, previewMetrics.repairDelayedByReplica.getCount());
            assertEquals(0L, previewMetrics.repairDelayedBySchedule.getCount());
        }));
    }
}
