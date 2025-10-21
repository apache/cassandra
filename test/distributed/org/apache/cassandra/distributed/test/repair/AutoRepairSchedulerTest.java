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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.autorepair.AutoRepair;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;

/**
 * Distributed tests for {@link org.apache.cassandra.repair.autorepair.AutoRepair} scheduler
 */
public class AutoRepairSchedulerTest extends TestBaseImpl
{
    private static Cluster cluster;
    static SimpleDateFormat sdf;

    @BeforeClass
    public static void init() throws IOException
    {
        // Define the expected date format pattern
        String pattern = "EEE MMM dd HH:mm:ss z yyyy";
        // Create SimpleDateFormat object with the given pattern
        sdf = new SimpleDateFormat(pattern);
        sdf.setLenient(false);
        // Configure a 3-node cluster with num_tokens: 4 and auto_repair enabled
        cluster = Cluster.build(3)
                         .withTokenCount(4)
                         .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3, 4))
                         .withConfig(config -> config
                                               .set("num_tokens", 4)
                                               .set("auto_repair",
                                                    ImmutableMap.of(
                                                    "repair_type_overrides",
                                                    ImmutableMap.of(AutoRepairConfig.RepairType.FULL.getConfigName(),
                                                                    ImmutableMap.of(
                                                                    "initial_scheduler_delay", "5s",
                                                                    "enabled", "true",
                                                                    "parallel_repair_count", "3",
                                                                    // Allow parallel replica repair to allow replicas
                                                                    // to execute full repair at same time.
                                                                    "allow_parallel_replica_repair", "true",
                                                                    "min_repair_interval", "5s"),
                                                                    AutoRepairConfig.RepairType.INCREMENTAL.getConfigName(),
                                                                    ImmutableMap.of(
                                                                    "initial_scheduler_delay", "5s",
                                                                    "enabled", "true",
                                                                    // Set parallel repair count to 3 to provoke
                                                                    // contention between replicas when scheduling.
                                                                    "parallel_repair_count", "3",
                                                                    // Disallow parallel replica repair to prevent
                                                                    // replicas from issuing incremental repair at
                                                                    // same time.
                                                                    "allow_parallel_replica_repair", "false",
                                                                    // Run more aggressively since full repair is
                                                                    // less restrictive about when it can run repair,
                                                                    // so need to check more frequently to allow
                                                                    // incremental to get an attempt in.
                                                                    "min_repair_interval", "5s"))))
                                               .set("auto_repair.enabled", "true")
                                               .set("auto_repair.global_settings.repair_by_keyspace", "true")
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
    public void testScheduler() throws ParseException
    {
        // ensure there was no history of previous repair runs through the scheduler
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s", DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY), ConsistencyLevel.QUORUM);
        assertEquals(0, rows.length);

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
            String broadcastAddress  = FBUtilities.getJustBroadcastAddress().toString();

            // Reduce sleeping if repair finishes quickly to speed up test but make it non-zero to provoke some
            // contention.
            AutoRepair.SLEEP_IF_REPAIR_FINISHES_QUICKLY = new DurationSpec.IntSecondsBound("2s");

            AutoRepairMetrics incrementalMetrics = AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.INCREMENTAL);
            // Since the AutoRepair sleeps up to SLEEP_IF_REPAIR_FINISHES_QUICKLY if the repair finishes quickly,
            // so the "nodeRepairTimeInSec" metric should at least be greater than or equal to
            // SLEEP_IF_REPAIR_FINISHES_QUICKLY
            Util.spinAssert(String.format("%s: AutoRepair has not yet completed one INCREMENTAL repair cycle", broadcastAddress),
                            greaterThanOrEqualTo(2L),
                            () -> incrementalMetrics.nodeRepairTimeInSec.getValue().longValue(),
                            5,
                            TimeUnit.MINUTES);

            // Expect some contention on incremental repair.
            Util.spinAssert(String.format("%s: AutoRepair has not observed any replica contention in INCREMENTAL repair", broadcastAddress),
                            greaterThan(0L),
                            incrementalMetrics.repairDelayedByReplica::getCount,
                            5,
                            TimeUnit.MINUTES);
            // Do not expect any contention across schedules since allow_parallel_replica_repairs across schedules
            // was not configured.
            assertEquals(0L, incrementalMetrics.repairDelayedBySchedule.getCount());

            AutoRepairMetrics fullMetrics = AutoRepairMetricsManager.getMetrics(AutoRepairConfig.RepairType.FULL);
            Util.spinAssert(String.format("%s: AutoRepair has not yet completed one FULL repair cycle", broadcastAddress),
                            greaterThanOrEqualTo(2L),
                            () -> fullMetrics.nodeRepairTimeInSec.getValue().longValue(),
                            5,
                            TimeUnit.MINUTES);

            // No repair contention should be observed for full repair since allow_parallel_replica_repair was true
            assertEquals(0L, fullMetrics.repairDelayedByReplica.getCount());
            assertEquals(0L, fullMetrics.repairDelayedBySchedule.getCount());
        }));

        validate(AutoRepairConfig.RepairType.FULL.toString());
        validate(AutoRepairConfig.RepairType.INCREMENTAL.toString());
    }

    private void validate(String repairType) throws ParseException
    {
        Object[][] rows = cluster.coordinator(1).execute(String.format("SELECT repair_type, host_id, repair_start_ts, repair_finish_ts, repair_turn FROM %s.%s where repair_type='%s'", DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.AUTO_REPAIR_HISTORY, repairType), ConsistencyLevel.QUORUM);
        assertEquals(3, rows.length);
        for (int node = 0; node < rows.length; node++)
        {
            Object[] row = rows[node];
            // repair_type
            Assert.assertEquals(repairType, row[0].toString());
            // host_id
            Assert.assertNotNull(UUID.fromString(row[1].toString()));
            // ensure there is a legit repair_start_ts and repair_finish_ts
            sdf.parse(row[2].toString());
            sdf.parse(row[3].toString());
            // the reason why the repair was scheduled
            Assert.assertNotNull(row[4]);
            Assert.assertEquals("MY_TURN", row[4].toString());
        }
    }
}
